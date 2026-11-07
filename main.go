package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// The increment and expiry execute atomically on Redis, so a process failure
// cannot leave a counter without its expiration.
var counterScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 or redis.call('PTTL', KEYS[1]) < 0 then
 redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return n
`)

func resetRateCounters(rdb *redis.Client) {
	iter := rdb.Scan(ctx, 0, "rate_limit:*", 100).Iterator()
	for iter.Next(ctx) {
		if err := rdb.Del(ctx, iter.Val()).Err(); err != nil {
			log.Printf("reset: %v", err)
		}
	}
	if err := iter.Err(); err != nil {
		log.Printf("reset scan: %v", err)
	}
}

var limiterEnabled int32 = 1
var localRequestCount int32 = 0

type Event struct {
	Type    string      `json:"type"` // "traffic", "control", or "attack", or "burst"
	Traffic TrafficData `json:"traffic,omitempty"`
	Control ControlData `json:"control,omitempty"`
	Attack  AttackData  `json:"attack,omitempty"`
	Burst   BurstData   `json:"burst,omitempty"`
}

type BurstData struct {
	Count  int    `json:"count"`
	Node   string `json:"node"`
	Status int    `json:"status"`
}

type TrafficData struct {
	IP       string `json:"ip"`
	Instance string `json:"instance"`
	Status   int    `json:"status"`
	Count    int64  `json:"count"`
}

type ControlData struct {
	LimiterEnabled bool `json:"limiter_enabled"`
}

type AttackData struct {
	Enabled bool `json:"enabled"`
	RPS     int  `json:"rps"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		u, err := url.Parse(origin)
		return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host == r.Host
	},
}

type Hub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
	rdb        *redis.Client
}

func newHub(rdb *redis.Client) *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		clients:    make(map[*websocket.Conn]bool),
		rdb:        rdb,
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				client.Close()
			}
			h.mu.Unlock()
		case message := <-h.broadcast:
			h.mu.Lock()
			for client := range h.clients {
				_ = client.SetWriteDeadline(time.Now().Add(5 * time.Second))
				err := client.WriteMessage(websocket.TextMessage, message)
				if err != nil {
					client.Close()
					delete(h.clients, client)
				}
			}
			h.mu.Unlock()
		}
	}
}

func getIP(r *http.Request) string {
	// Forwarded headers are honored only in an explicitly configured proxy deployment.
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	if os.Getenv("TRUST_PROXY") == "true" {
		forwarded := r.Header.Get("X-Real-IP")
		if forwarded == "" {
			forwarded = strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-For"), ",")[0])
		}
		if net.ParseIP(forwarded) != nil {
			ip = forwarded
		}
	}
	return ip
}

func rateLimitMiddleware(rdb *redis.Client, instanceName string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := getIP(r)

		status := 200
		var globalCount int64 = 0

		isLimiterEnabled := atomic.LoadInt32(&limiterEnabled) == 1

		if isLimiterEnabled {
			// Redis Distributed Logic
			key := fmt.Sprintf("rate_limit:%s", ip)
			requestCtx, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			count, err := counterScript.Run(requestCtx, rdb, []string{key}, 1000).Int64()
			if err != nil {
				log.Printf("Redis error: %v", err)
				http.Error(w, "Rate limiter unavailable", http.StatusServiceUnavailable)
				return
			}

			globalCount = count

			if count > 10 {
				status = 429
			}
		} else {
			// Hardware Limit Simulation (10 RPS local limit)
			localCount := atomic.AddInt32(&localRequestCount, 1)
			if localCount > 10 {
				status = 503 // Hardware Crash!
			}
		}

		// Publish to Redis Pub/Sub
		event := Event{
			Type: "traffic",
			Traffic: TrafficData{
				IP:       ip,
				Instance: instanceName,
				Status:   status,
				Count:    globalCount,
			},
		}

		jsonData, _ := json.Marshal(event)
		rdb.Publish(ctx, "cluster_events", jsonData)

		if status == 429 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		if status == 503 {
			http.Error(w, "Service Unavailable - Server Overloaded", http.StatusServiceUnavailable)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	hub.register <- conn

	// Read from client to catch toggle commands
	go func() {
		defer func() {
			hub.unregister <- conn
		}()
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				break
			}

			var cmd struct {
				Type    string `json:"type"`
				Enabled bool   `json:"enabled"`
				RPS     int    `json:"rps"`
			}
			if err := json.Unmarshal(message, &cmd); err == nil {
				if cmd.Type == "toggle" {
					// Broadcast the control event to the cluster via Redis Pub/Sub
					event := Event{
						Type: "control",
						Control: ControlData{
							LimiterEnabled: cmd.Enabled,
						},
					}
					jsonData, _ := json.Marshal(event)
					hub.rdb.Publish(ctx, "cluster_events", jsonData)
				} else if cmd.Type == "attack" {
					// Broadcast the attack event to the cluster
					event := Event{
						Type: "attack",
						Attack: AttackData{
							Enabled: cmd.Enabled,
							RPS:     cmd.RPS,
						},
					}
					jsonData, _ := json.Marshal(event)
					hub.rdb.Publish(ctx, "cluster_events", jsonData)
				} else if cmd.Type == "reset" {
					event := Event{Type: "reset"}
					jsonData, _ := json.Marshal(event)
					hub.rdb.Publish(ctx, "cluster_events", jsonData)
				}
			}
		}
	}()
}

var attackCancel context.CancelFunc
var attackMutex sync.Mutex

func runAttack(ctx context.Context, totalRequests int, port string) {
	if totalRequests <= 0 {
		return
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        5000,
			MaxIdleConnsPerHost: 5000,
		},
		Timeout: 5 * time.Second,
	}

	url := "http://localhost:" + port + "/api/test"

	// Distribute the requests evenly over 1 second
	interval := time.Second / time.Duration(totalRequests)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}

	for i := 0; i < totalRequests; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			go func() {
				req, err := http.NewRequest("GET", url, nil)
				if err == nil {
					// Stable client identity makes repeated-IP limiting demonstrable.
					ip := "192.0.2.10"
					req.Header.Set("X-Real-IP", ip)
					resp, err := client.Do(req)
					if err == nil {
						resp.Body.Close()
					}
				}
			}()
			time.Sleep(interval)
		}
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	instanceName := os.Getenv("INSTANCE_NAME")
	if instanceName == "" {
		instanceName = "unknown-instance"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisURL,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis at %s: %v", redisURL, err)
	}

	hub := newHub(rdb)
	go hub.run()

	// Hardware Limit Reset Timer (resets localRequestCount every 1 second)
	go func() {
		for {
			time.Sleep(1 * time.Second)
			atomic.StoreInt32(&localRequestCount, 0)
		}
	}()

	// Subscribe to Redis Pub/Sub to listen for global events
	pubsub := rdb.Subscribe(ctx, "cluster_events")
	go func() {
		ch := pubsub.Channel()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		type nodeStats struct {
			count  int
			status int
		}

		trafficBatch := make(map[string]*nodeStats)
		var trafficEvents []Event

		for {
			select {
			case msg := <-ch:
				var event Event
				if err := json.Unmarshal([]byte(msg.Payload), &event); err == nil {
					if event.Type == "control" {
						hub.broadcast <- []byte(msg.Payload)
						if event.Control.LimiterEnabled {
							atomic.StoreInt32(&limiterEnabled, 1)
							log.Println("Cluster commanded to ENABLE limiter")
						} else {
							atomic.StoreInt32(&limiterEnabled, 0)
							log.Println("Cluster commanded to DISABLE limiter")
						}
					} else if event.Type == "attack" {
						hub.broadcast <- []byte(msg.Payload)
						attackMutex.Lock()
						if attackCancel != nil {
							attackCancel()
							attackCancel = nil
						}
						if event.Attack.Enabled {
							var attackCtx context.Context
							attackCtx, attackCancel = context.WithCancel(context.Background())
							// Divide RPS by 2 since there are two instances to match expected total exactly.
							targetRPS := event.Attack.RPS / 2
							if targetRPS == 0 && event.Attack.RPS > 0 {
								targetRPS = 1
							}
							go runAttack(attackCtx, targetRPS, port)
						}
						attackMutex.Unlock()
					} else if event.Type == "reset" {
						hub.broadcast <- []byte(msg.Payload)
						// Clear Redis database and local request count
						resetRateCounters(rdb)
						atomic.StoreInt32(&localRequestCount, 0)
						log.Println("Cluster commanded to RESET counters")
					} else if event.Type == "traffic" {
						trafficEvents = append(trafficEvents, event)
						node := event.Traffic.Instance
						if trafficBatch[node] == nil {
							trafficBatch[node] = &nodeStats{}
						}
						trafficBatch[node].count++
						if event.Traffic.Status == 429 || event.Traffic.Status == 503 {
							trafficBatch[node].status = event.Traffic.Status
						} else if trafficBatch[node].status == 0 {
							trafficBatch[node].status = event.Traffic.Status
						}
					}
				}
			case <-ticker.C:
				burstNodes := make(map[string]bool)

				for node, stats := range trafficBatch {
					if stats.count > 50 {
						burstNodes[node] = true
						burstEvent := Event{
							Type: "burst",
							Burst: BurstData{
								Count:  stats.count,
								Node:   node,
								Status: stats.status,
							},
						}
						burstData, _ := json.Marshal(burstEvent)
						hub.broadcast <- burstData
					}
				}

				for _, event := range trafficEvents {
					if !burstNodes[event.Traffic.Instance] {
						eventData, _ := json.Marshal(event)
						hub.broadcast <- eventData
					}
				}

				trafficBatch = make(map[string]*nodeStats)
				trafficEvents = []Event{}
			}
		}
	}()

	helloHandler := func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello World! Responding from instance: %s\n", instanceName)
	}

	http.HandleFunc("/api/test", rateLimitMiddleware(rdb, instanceName, helloHandler))
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	log.Printf("Starting server %s on port %s...", instanceName, port)

	server := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
