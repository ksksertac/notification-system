package scenarios

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type latencyCollector struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (l *latencyCollector) Add(d time.Duration) {
	l.mu.Lock()
	l.samples = append(l.samples, d)
	l.mu.Unlock()
}

func (l *latencyCollector) Percentiles() (p50, p95, p99, avg time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.samples) == 0 {
		return
	}

	sort.Slice(l.samples, func(i, j int) bool { return l.samples[i] < l.samples[j] })

	var total time.Duration
	for _, s := range l.samples {
		total += s
	}
	avg = total / time.Duration(len(l.samples))

	p50 = l.samples[len(l.samples)*50/100]
	p95 = l.samples[len(l.samples)*95/100]
	p99 = l.samples[len(l.samples)*99/100]
	return
}

func spaces(n int) string {
	return strings.Repeat(" ", n)
}

func printResults(name string, total int, elapsed time.Duration, success, failed, rateLimited int64, latencies *latencyCollector) {
	fmt.Printf("\n╔══════════════════════════════════════════════════╗\n")
	fmt.Printf("║          %-39s ║\n", name+" RESULTS")
	fmt.Printf("╠══════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Duration:     %-33s║\n", elapsed.Round(time.Millisecond))
	fmt.Printf("║  Total:        %-33d║\n", total)
	fmt.Printf("║  Successful:   %-33d║\n", success)
	fmt.Printf("║  Failed:       %-33d║\n", failed)
	fmt.Printf("║  Rate Limited: %-33d║\n", rateLimited)
	fmt.Printf("║  Req/sec:      %-33.0f║\n", float64(success+failed+rateLimited)/elapsed.Seconds())
	fmt.Printf("╠══════════════════════════════════════════════════╣\n")
	p50, p95, p99, avg := latencies.Percentiles()
	fmt.Printf("║  Latency:                                       ║\n")
	fmt.Printf("║    avg:  %-40s║\n", avg.Round(time.Millisecond))
	fmt.Printf("║    p50:  %-40s║\n", p50.Round(time.Millisecond))
	fmt.Printf("║    p95:  %-40s║\n", p95.Round(time.Millisecond))
	fmt.Printf("║    p99:  %-40s║\n", p99.Round(time.Millisecond))
	fmt.Printf("╚══════════════════════════════════════════════════╝\n")
}
