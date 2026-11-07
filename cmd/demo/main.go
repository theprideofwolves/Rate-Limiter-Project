// demo sends one client's requests to both running instances.
package main

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	urls := os.Args[1:]
	if len(urls) == 0 {
		urls = []string{"http://localhost:8081/api/test", "http://localhost:8082/api/test"}
	}
	var ok, blocked, other int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}
			r, err := client.Get(urls[i%len(urls)])
			if err != nil {
				atomic.AddInt32(&other, 1)
				return
			}
			defer r.Body.Close()
			switch r.StatusCode {
			case 200:
				atomic.AddInt32(&ok, 1)
			case 429:
				atomic.AddInt32(&blocked, 1)
			default:
				atomic.AddInt32(&other, 1)
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("Allowed=%d Blocked=%d Other=%d\n", ok, blocked, other)
	if ok != 10 || blocked != 10 || other != 0 {
		fmt.Println("Expected 10/10 on a fresh one-second window; wait, then retry with limiter enabled.")
		os.Exit(1)
	}
}
