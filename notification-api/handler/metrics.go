package handler

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sertacyildirim/notification-system/shared/queue"
)

type MetricsCollector struct {
	consumer queue.Consumer
	registry *prometheus.Registry

	queueDepth  *prometheus.GaugeVec
	httpTotal   *prometheus.CounterVec
	httpLatency *prometheus.HistogramVec
}

func NewMetricsCollector(consumer queue.Consumer) *MetricsCollector {
	reg := prometheus.NewRegistry()
	// Include default Go runtime and process metrics
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	m := &MetricsCollector{
		consumer: consumer,
		registry: reg,
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Current number of messages in each priority stream",
		}, []string{"stream"}),
		httpTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests",
		}, []string{"method", "path", "status"}),
		httpLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}, []string{"method", "path"}),
	}

	reg.MustRegister(m.queueDepth, m.httpTotal, m.httpLatency)

	return m
}

func (m *MetricsCollector) Metrics(w http.ResponseWriter, r *http.Request) {
	m.updateQueueDepths(r.Context())
	promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}

func (m *MetricsCollector) RecordHTTP(method, path, status string, durationSeconds float64) {
	m.httpTotal.WithLabelValues(method, path, status).Inc()
	m.httpLatency.WithLabelValues(method, path).Observe(durationSeconds)
}

func (m *MetricsCollector) updateQueueDepths(ctx context.Context) {
	for _, stream := range []string{queue.StreamHigh, queue.StreamNormal, queue.StreamLow} {
		length, err := m.consumer.Len(ctx, stream)
		if err != nil {
			continue
		}
		m.queueDepth.WithLabelValues(stream).Set(float64(length))
	}
}
