package scenarios

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type MixedConfig struct {
	BaseURL     string
	Duration    time.Duration
	Concurrency int
}

type mixedStats struct {
	createOK      atomic.Int64
	createFail    atomic.Int64
	listOK        atomic.Int64
	listFail      atomic.Int64
	getOK         atomic.Int64
	getFail       atomic.Int64
	cancelOK      atomic.Int64
	cancelFail    atomic.Int64
	rateLimited   atomic.Int64
	latencies     *latencyCollector
}

func RunMixed(cfg MixedConfig) {
	fmt.Printf("\n=== Mixed Scenario Load Test ===\n")
	fmt.Printf("Target:      %s\n", cfg.BaseURL)
	fmt.Printf("Duration:    %s\n", cfg.Duration)
	fmt.Printf("Concurrency: %d workers\n", cfg.Concurrency)
	fmt.Printf("Mix:         70%% create, 15%% list, 10%% get, 5%% cancel\n\n")

	stats := &mixedStats{latencies: &latencyCollector{}}

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Concurrency * 2,
			MaxIdleConnsPerHost: cfg.Concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	var createdIDs sync.Map
	var idCounter atomic.Int64

	ctx := make(chan struct{})
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				select {
				case <-ctx:
					return
				default:
				}

				roll := rng.Intn(100)
				var t time.Time

				switch {
				case roll < 70:
					idx := idCounter.Add(1)
					payload, _ := json.Marshal(map[string]string{
						"recipient": fmt.Sprintf("mixed%d@loadtest.com", idx),
						"channel":   "email",
						"content":   fmt.Sprintf("Mixed test #%d", idx),
						"priority":  "normal",
					})
					t = time.Now()
					resp, err := client.Post(cfg.BaseURL+"/api/v1/notifications", "application/json", bytes.NewReader(payload))
					stats.latencies.Add(time.Since(t))
					if err != nil {
						stats.createFail.Add(1)
						continue
					}
					if resp.StatusCode == 429 {
						stats.rateLimited.Add(1)
						resp.Body.Close()
						continue
					}
					if resp.StatusCode == 201 {
						var result struct {
							Data struct {
								ID string `json:"id"`
							} `json:"data"`
						}
						json.NewDecoder(resp.Body).Decode(&result)
						if result.Data.ID != "" {
							createdIDs.Store(result.Data.ID, true)
						}
						stats.createOK.Add(1)
					} else {
						stats.createFail.Add(1)
					}
					resp.Body.Close()

				case roll < 85:
					t = time.Now()
					resp, err := client.Get(cfg.BaseURL + "/api/v1/notifications?limit=10")
					stats.latencies.Add(time.Since(t))
					if err != nil {
						stats.listFail.Add(1)
						continue
					}
					resp.Body.Close()
					if resp.StatusCode == 429 {
						stats.rateLimited.Add(1)
					} else if resp.StatusCode == 200 {
						stats.listOK.Add(1)
					} else {
						stats.listFail.Add(1)
					}

				case roll < 95:
					var id string
					createdIDs.Range(func(key, _ any) bool {
						id = key.(string)
						return false
					})
					if id == "" {
						continue
					}
					t = time.Now()
					resp, err := client.Get(cfg.BaseURL + "/api/v1/notifications/" + id)
					stats.latencies.Add(time.Since(t))
					if err != nil {
						stats.getFail.Add(1)
						continue
					}
					resp.Body.Close()
					if resp.StatusCode == 429 {
						stats.rateLimited.Add(1)
					} else if resp.StatusCode == 200 {
						stats.getOK.Add(1)
					} else {
						stats.getFail.Add(1)
					}

				default:
					var id string
					createdIDs.Range(func(key, _ any) bool {
						id = key.(string)
						createdIDs.Delete(key)
						return false
					})
					if id == "" {
						continue
					}
					req, _ := http.NewRequest(http.MethodPatch, cfg.BaseURL+"/api/v1/notifications/"+id+"/cancel", nil)
					t = time.Now()
					resp, err := client.Do(req)
					stats.latencies.Add(time.Since(t))
					if err != nil {
						stats.cancelFail.Add(1)
						continue
					}
					resp.Body.Close()
					if resp.StatusCode == 429 {
						stats.rateLimited.Add(1)
					} else if resp.StatusCode == 200 {
						stats.cancelOK.Add(1)
					} else {
						stats.cancelFail.Add(1)
					}
				}
			}
		}(i)
	}

	ticker := time.NewTicker(2 * time.Second)
	go func() {
		for range ticker.C {
			total := stats.createOK.Load() + stats.createFail.Load() +
				stats.listOK.Load() + stats.listFail.Load() +
				stats.getOK.Load() + stats.getFail.Load() +
				stats.cancelOK.Load() + stats.cancelFail.Load() +
				stats.rateLimited.Load()
			elapsed := time.Since(start).Seconds()
			remaining := cfg.Duration.Seconds() - elapsed
			fmt.Printf("\r  [%.0fs remaining] %.0f req/s | create: %d | list: %d | get: %d | cancel: %d | 429: %d",
				remaining, float64(total)/elapsed,
				stats.createOK.Load(), stats.listOK.Load(), stats.getOK.Load(), stats.cancelOK.Load(),
				stats.rateLimited.Load())
		}
	}()

	time.Sleep(cfg.Duration)
	close(ctx)
	wg.Wait()
	ticker.Stop()
	elapsed := time.Since(start)

	totalOK := stats.createOK.Load() + stats.listOK.Load() + stats.getOK.Load() + stats.cancelOK.Load()
	totalFail := stats.createFail.Load() + stats.listFail.Load() + stats.getFail.Load() + stats.cancelFail.Load()
	totalAll := totalOK + totalFail + stats.rateLimited.Load()

	fmt.Printf("\r%s\n", spaces(120))
	fmt.Printf("\n╔══════════════════════════════════════════════════╗\n")
	fmt.Printf("║          MIXED SCENARIO RESULTS                 ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Duration:       %-31s║\n", elapsed.Round(time.Millisecond))
	fmt.Printf("║  Total Requests: %-31d║\n", totalAll)
	fmt.Printf("║  Req/sec:        %-31.0f║\n", float64(totalAll)/elapsed.Seconds())
	fmt.Printf("╠══════════════════════════════════════════════════╣\n")
	fmt.Printf("║  CREATE:  %-6d OK  %-6d Fail                ║\n", stats.createOK.Load(), stats.createFail.Load())
	fmt.Printf("║  LIST:    %-6d OK  %-6d Fail                ║\n", stats.listOK.Load(), stats.listFail.Load())
	fmt.Printf("║  GET:     %-6d OK  %-6d Fail                ║\n", stats.getOK.Load(), stats.getFail.Load())
	fmt.Printf("║  CANCEL:  %-6d OK  %-6d Fail                ║\n", stats.cancelOK.Load(), stats.cancelFail.Load())
	fmt.Printf("║  RATE LIMITED (429): %-28d║\n", stats.rateLimited.Load())
	fmt.Printf("╠══════════════════════════════════════════════════╣\n")
	p50, p95, p99, avg := stats.latencies.Percentiles()
	fmt.Printf("║  Latency:                                       ║\n")
	fmt.Printf("║    avg:  %-40s║\n", avg.Round(time.Millisecond))
	fmt.Printf("║    p50:  %-40s║\n", p50.Round(time.Millisecond))
	fmt.Printf("║    p95:  %-40s║\n", p95.Round(time.Millisecond))
	fmt.Printf("║    p99:  %-40s║\n", p99.Round(time.Millisecond))
	fmt.Printf("╚══════════════════════════════════════════════════╝\n")
}
