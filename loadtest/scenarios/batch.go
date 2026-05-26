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

type BatchConfig struct {
	BaseURL       string
	Total         int
	BatchSize     int
	Concurrency   int
}

type batchRequest struct {
	Notifications []map[string]string `json:"notifications"`
}

func RunBatch(cfg BatchConfig) {
	if cfg.BatchSize > 1000 {
		cfg.BatchSize = 1000
	}

	totalBatches := cfg.Total / cfg.BatchSize
	if cfg.Total%cfg.BatchSize != 0 {
		totalBatches++
	}

	fmt.Printf("\n=== Batch Notification Load Test ===\n")
	fmt.Printf("Target:         %s\n", cfg.BaseURL)
	fmt.Printf("Total:          %d notifications\n", cfg.Total)
	fmt.Printf("Batch size:     %d\n", cfg.BatchSize)
	fmt.Printf("Total batches:  %d\n", totalBatches)
	fmt.Printf("Concurrency:    %d workers\n\n", cfg.Concurrency)

	var (
		successBatches atomic.Int64
		successNotifs  atomic.Int64
		failedBatches  atomic.Int64
		rateLimited    atomic.Int64
		latencies      = &latencyCollector{}
	)

	client := &http.Client{
		Timeout: 30 * time.Second,
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

			for batchIdx := range work {
				offset := batchIdx * cfg.BatchSize
				remaining := cfg.Total - offset
				size := cfg.BatchSize
				if remaining < size {
					size = remaining
				}

				notifs := make([]map[string]string, size)
				for j := 0; j < size; j++ {
					idx := offset + j
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

					notifs[j] = map[string]string{
						"recipient": recipient,
						"channel":   ch,
						"content":   fmt.Sprintf("Batch load test #%d", idx),
						"priority":  pri,
					}
				}

				payload, _ := json.Marshal(batchRequest{Notifications: notifs})

				t := time.Now()
				resp, err := client.Post(
					cfg.BaseURL+"/api/v1/notifications/batch",
					"application/json",
					bytes.NewReader(payload),
				)
				elapsed := time.Since(t)
				latencies.Add(elapsed)

				if err != nil {
					failedBatches.Add(1)
					continue
				}
				resp.Body.Close()

				switch {
				case resp.StatusCode == 201:
					successBatches.Add(1)
					successNotifs.Add(int64(size))
				case resp.StatusCode == 429:
					rateLimited.Add(1)
				default:
					failedBatches.Add(1)
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
				totalDone := successBatches.Load() + failedBatches.Load() + rateLimited.Load()
				elapsed := time.Since(start).Seconds()
				nps := float64(successNotifs.Load()) / elapsed
				pct := float64(totalDone) / float64(totalBatches) * 100
				fmt.Printf("\r  Progress: %d/%d batches (%.1f%%) | %.0f notifs/s | OK: %d | 429: %d | Err: %d",
					totalDone, totalBatches, pct, nps, successNotifs.Load(), rateLimited.Load(), failedBatches.Load())
			case <-done:
				return
			}
		}
	}()

	for i := 0; i < totalBatches; i++ {
		work <- i
	}
	close(work)
	wg.Wait()

	ticker.Stop()
	close(done)
	elapsed := time.Since(start)

	fmt.Printf("\r%s\n", spaces(120))
	fmt.Printf("\n╔══════════════════════════════════════════════════╗\n")
	fmt.Printf("║          BATCH LOAD TEST RESULTS                ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Duration:          %-28s║\n", elapsed.Round(time.Millisecond))
	fmt.Printf("║  Total Notifs:      %-28d║\n", cfg.Total)
	fmt.Printf("║  Successful Notifs: %-28d║\n", successNotifs.Load())
	fmt.Printf("║  Successful Batches:%-28d║\n", successBatches.Load())
	fmt.Printf("║  Failed Batches:    %-28d║\n", failedBatches.Load())
	fmt.Printf("║  Rate Limited:      %-28d║\n", rateLimited.Load())
	fmt.Printf("║  Notifs/sec:        %-28.0f║\n", float64(successNotifs.Load())/elapsed.Seconds())
	fmt.Printf("║  Batches/sec:       %-28.1f║\n", float64(successBatches.Load())/elapsed.Seconds())
	fmt.Printf("╠══════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Latency (per batch request):                   ║\n")
	p50, p95, p99, avg := latencies.Percentiles()
	fmt.Printf("║    avg:  %-40s║\n", avg.Round(time.Millisecond))
	fmt.Printf("║    p50:  %-40s║\n", p50.Round(time.Millisecond))
	fmt.Printf("║    p95:  %-40s║\n", p95.Round(time.Millisecond))
	fmt.Printf("║    p99:  %-40s║\n", p99.Round(time.Millisecond))
	fmt.Printf("╚══════════════════════════════════════════════════╝\n")
}
