package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type PrometheusRecorder struct {
	deliveryTotal   *prometheus.CounterVec
	failureTotal    *prometheus.CounterVec
	deliveryLatency *prometheus.HistogramVec
	rateLimitHits   prometheus.Counter
	cbOpenTotal     prometheus.Counter
}

func NewPrometheusRecorder() *PrometheusRecorder {
	m := &PrometheusRecorder{
		deliveryTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_delivered_total",
			Help: "Total notifications delivered successfully",
		}, []string{"channel"}),
		failureTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_failed_total",
			Help: "Total notification delivery failures",
		}, []string{"channel"}),
		deliveryLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_delivery_duration_seconds",
			Help:    "Notification delivery latency",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"channel"}),
		rateLimitHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "rate_limit_hits_total",
			Help: "Total rate limit hits",
		}),
		cbOpenTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "circuit_breaker_open_total",
			Help: "Total times circuit breaker opened",
		}),
	}

	prometheus.MustRegister(m.deliveryTotal, m.failureTotal, m.deliveryLatency, m.rateLimitHits, m.cbOpenTotal)
	return m
}

func (m *PrometheusRecorder) RecordDelivery(channel string, latency time.Duration) {
	m.deliveryTotal.WithLabelValues(channel).Inc()
	m.deliveryLatency.WithLabelValues(channel).Observe(latency.Seconds())
}

func (m *PrometheusRecorder) RecordFailure(channel string) {
	m.failureTotal.WithLabelValues(channel).Inc()
}

func (m *PrometheusRecorder) RecordRateLimitHit() {
	m.rateLimitHits.Inc()
}

func (m *PrometheusRecorder) RecordCircuitBreakerOpen() {
	m.cbOpenTotal.Inc()
}

func (m *PrometheusRecorder) Handler() http.Handler {
	return promhttp.Handler()
}
