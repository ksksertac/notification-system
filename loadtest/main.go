package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sertacyildirim/notification-system/loadtest/scenarios"
)

func main() {
	scenario := flag.String("scenario", "", "Test scenario: single, batch, mixed")
	baseURL := flag.String("url", "http://localhost:8080", "Base URL of the notification API")
	total := flag.Int("total", 100000, "Total notifications to send")
	concurrency := flag.Int("concurrency", 50, "Number of concurrent workers")
	batchSize := flag.Int("batch-size", 1000, "Batch size (for batch scenario, max 1000)")
	duration := flag.Duration("duration", 60*time.Second, "Test duration (for mixed scenario)")
	flag.Parse()

	if *scenario == "" {
		fmt.Println("Notification System Load Test")
		fmt.Println("=============================")
		fmt.Println()
		fmt.Println("Usage: go run . -scenario <name> [options]")
		fmt.Println()
		fmt.Println("Scenarios:")
		fmt.Println("  single   Send N individual notifications (default 100K)")
		fmt.Println("  batch    Send N notifications via batch endpoint (default 1M, 1000/batch)")
		fmt.Println("  mixed    Mixed traffic: 70% create, 15% list, 10% get, 5% cancel")
		fmt.Println()
		fmt.Println("Options:")
		fmt.Println("  -url          Base URL (default: http://localhost:8080)")
		fmt.Println("  -total        Total notifications (default: 100000)")
		fmt.Println("  -concurrency  Concurrent workers (default: 50)")
		fmt.Println("  -batch-size   Notifications per batch (default: 1000, max: 1000)")
		fmt.Println("  -duration     Test duration for mixed scenario (default: 60s)")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  go run . -scenario single -total 100000 -concurrency 50")
		fmt.Println("  go run . -scenario batch -total 1000000 -concurrency 20")
		fmt.Println("  go run . -scenario mixed -duration 2m -concurrency 100")
		os.Exit(0)
	}

	switch *scenario {
	case "single":
		scenarios.RunSingle(scenarios.SingleConfig{
			BaseURL:     *baseURL,
			Total:       *total,
			Concurrency: *concurrency,
		})
	case "batch":
		if *total == 100000 {
			*total = 1000000
		}
		scenarios.RunBatch(scenarios.BatchConfig{
			BaseURL:     *baseURL,
			Total:       *total,
			BatchSize:   *batchSize,
			Concurrency: *concurrency,
		})
	case "mixed":
		scenarios.RunMixed(scenarios.MixedConfig{
			BaseURL:     *baseURL,
			Duration:    *duration,
			Concurrency: *concurrency,
		})
	default:
		fmt.Fprintf(os.Stderr, "Unknown scenario: %s (use: single, batch, mixed)\n", *scenario)
		os.Exit(1)
	}
}
