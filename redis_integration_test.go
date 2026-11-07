package main

import (
	"context"
	"github.com/redis/go-redis/v9"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// CI supplies a dedicated real Redis service. Ordinary local tests use miniredis.
func TestRealRedisAtomicCounter(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR for real Redis integration")
	}
	cx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a := redis.NewClient(&redis.Options{Addr: addr})
	b := redis.NewClient(&redis.Options{Addr: addr})
	defer a.Close()
	defer b.Close()
	key := "rate_limit:test:" + time.Now().Format("150405.000000000")
	defer a.Del(context.Background(), key)
	var accepted int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client := a
			if i%2 == 1 {
				client = b
			}
			n, err := counterScript.Run(cx, client, []string{key}, 10000).Int64()
			if err != nil {
				t.Error(err)
				return
			}
			if n <= 10 {
				atomic.AddInt32(&accepted, 1)
			}
		}(i)
	}
	wg.Wait()
	if accepted != 10 {
		t.Fatalf("accepted=%d", accepted)
	}
	ttl, err := a.PTTL(cx, key).Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("counter has no expiration: %v %v", ttl, err)
	}
}
