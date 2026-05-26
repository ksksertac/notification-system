package scenarios

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type SingleConfig struct {
	BaseURL     string
	Total       int
	Concurrency int
}

func RunSingle(cfg SingleConfig) {
	fmt.Printf("\n=== Single Notification Load Test ===\n")
	fmt.Printf("Target:      %s\n", cfg.BaseURL)
	fmt.Printf("Total:       %d notifications\n", cfg.Total)
	fmt.Printf("Concurrency: %d workers\n\n", cfg.Concurrency)

	var (
		success   atomic.Int64
		failed    atomic.Int64
		rateLimit atomic.Int64
		latencies = &latencyCollector{}
	)

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Concurrency * 2,
			MaxIdleConnsPerHost: cfg.Concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	work := make(chan int, cfg.Concurrency*2)
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			channels := []string{"email", "sms", "push"}
			priorities := []string{"high", "normal", "low"}

			for idx := range work {
				ch := channels[idx%3]
				pri := priorities[idx%3]

				var recipient string
				switch ch {
				case "email":
					recipient = fmt.Sprintf("user%d@loadtest.com", idx)
				case "sms":
					recipient = fmt.Sprintf("+9055500%05d", idx%100000)
				case "push":
					recipient = fmt.Sprintf("fcm-token-loadtest-%d", idx)
				}

				payload, _ := json.Marshal(map[string]string{
					"recipient": recipient,
					"channel":   ch,
					"content":   fmt.Sprintf("Load test notification #%d", idx),
					"priority":  pri,
				})

				t := time.Now()
				resp, err := client.Post(
					cfg.BaseURL+"/api/v1/notifications",
					"application/json",
					bytes.NewReader(payload),
				)
				elapsed := time.Since(t)
				latencies.Add(elapsed)

				if err != nil {
					failed.Add(1)
					continue
				}
				resp.Body.Close()

				switch {
				case resp.StatusCode == 201:
					success.Add(1)
				case resp.StatusCode == 429:
					rateLimit.Add(1)
				default:
					failed.Add(1)
				}
			}
		}()
	}

	ticker := time.NewTicker(2 * time.Second)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				total := success.Load() + failed.Load() + rateLimit.Load()
				elapsed := time.Since(start).Seconds()
				rps := float64(total) / elapsed
				pct := float64(total) / float64(cfg.Total) * 100
				fmt.Printf("\r  Progress: %d/%d (%.1f%%) | %.0f req/s | OK: %d | 429: %d | Err: %d",
					total, cfg.Total, pct, rps, success.Load(), rateLimit.Load(), failed.Load())
			case <-done:
				return
			}
		}
	}()

	for i := 0; i < cfg.Total; i++ {
		work <- i
	}
	close(work)
	wg.Wait()

	ticker.Stop()
	close(done)
	elapsed := time.Since(start)

	fmt.Printf("\r%s\n", spaces(120))
	printResults("Single Notification", cfg.Total, elapsed, success.Load(), failed.Load(), rateLimit.Load(), latencies)
}
