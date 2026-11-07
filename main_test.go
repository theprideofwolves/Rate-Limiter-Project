package main

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedLimitAndExpiry(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	atomic.StoreInt32(&limiterEnabled, 1)
	handlers := []http.HandlerFunc{rateLimitMiddleware(r, "a", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }), rateLimitMiddleware(r, "b", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })}
	var ok, blocked int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest("GET", "http://localhost/api/test", nil)
			req.RemoteAddr = "192.0.2.10:1234"
			w := httptest.NewRecorder()
			handlers[i%2](w, req)
			switch w.Code {
			case 200:
				atomic.AddInt32(&ok, 1)
			case 429:
				atomic.AddInt32(&blocked, 1)
			default:
				t.Errorf("unexpected status %d", w.Code)
			}
		}(i)
	}
	wg.Wait()
	if ok != 10 || blocked != 30 {
		t.Fatalf("allowed=%d blocked=%d", ok, blocked)
	}
	m.FastForward(time.Second + time.Millisecond)
	req := httptest.NewRequest("GET", "http://localhost/api/test", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	w := httptest.NewRecorder()
	handlers[0](w, req)
	if w.Code != 200 {
		t.Fatal("window did not expire")
	}
	if err := r.Set(ctx, "unrelated", "preserved", 0).Err(); err != nil {
		t.Fatal(err)
	}
	resetRateCounters(r)
	if !m.Exists("unrelated") {
		t.Fatal("reset deleted unrelated data")
	}
}
func TestUntrustedHeadersIgnored(t *testing.T) {
	t.Setenv("TRUST_PROXY", "false")
	r := httptest.NewRequest("GET", "http://localhost", nil)
	r.RemoteAddr = "192.0.2.10:1"
	r.Header.Set("X-Real-IP", "198.51.100.9")
	if getIP(r) != "192.0.2.10" {
		t.Fatal("trusted spoofed header")
	}
}
func TestRedisFailure(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	if err := r.Set(ctx, "rate_limit:192.0.2.10", "invalid-counter", 0).Err(); err != nil {
		t.Fatal(err)
	}
	h := rateLimitMiddleware(r, "a", func(w http.ResponseWriter, r *http.Request) { t.Error("allowed request after Redis failure") })
	req := httptest.NewRequest("GET", "http://localhost/api/test", nil)
	req.RemoteAddr = "192.0.2.10:1"
	w := httptest.NewRecorder()
	h(w, req)
	if w.Code != 503 {
		t.Fatalf("status=%d", w.Code)
	}
}
