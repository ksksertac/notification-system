package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sertacyildirim/notification-system/shared/queue"
)

type mockConsumer struct {
	lenFn func(ctx context.Context, stream string) (int64, error)
}

func (m *mockConsumer) Read(ctx context.Context, stream string, group string, consumer string, count int64) ([]queue.Message, error) {
	return nil, nil
}

func (m *mockConsumer) Ack(ctx context.Context, stream string, group string, ids ...string) error {
	return nil
}

func (m *mockConsumer) ClaimStale(ctx context.Context, stream string, group string, consumer string, minIdle time.Duration, count int64) ([]queue.Message, error) {
	return nil, nil
}

func (m *mockConsumer) Len(ctx context.Context, stream string) (int64, error) {
	if m.lenFn != nil {
		return m.lenFn(ctx, stream)
	}
	return 0, nil
}

// Shared metrics collector to avoid duplicate prometheus registration panics.
var (
	sharedConsumer *mockConsumer
	sharedMetrics  *MetricsCollector
	metricsOnce    sync.Once
)

func getSharedMetrics() (*MetricsCollector, *mockConsumer) {
	metricsOnce.Do(func() {
		sharedConsumer = &mockConsumer{}
		sharedMetrics = NewMetricsCollector(sharedConsumer)
	})
	return sharedMetrics, sharedConsumer
}

func TestMetricsCollector_RecordHTTP(t *testing.T) {
	mc, _ := getSharedMetrics()
	// RecordHTTP should not panic
	mc.RecordHTTP("GET", "/api/v1/notifications", "200", 0.05)
	mc.RecordHTTP("POST", "/api/v1/notifications", "201", 0.1)
	mc.RecordHTTP("GET", "/api/v1/notifications", "500", 0.2)
}

func TestMetricsCollector_Metrics(t *testing.T) {
	mc, consumer := getSharedMetrics()
	consumer.lenFn = func(ctx context.Context, stream string) (int64, error) {
		return 42, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	mc.Metrics(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	body := rr.Body.String()
	if len(body) == 0 {
		t.Error("expected non-empty metrics response")
	}
}

func TestMetricsCollector_UpdateQueueDepths_Error(t *testing.T) {
	mc, consumer := getSharedMetrics()
	consumer.lenFn = func(ctx context.Context, stream string) (int64, error) {
		return 0, context.DeadlineExceeded
	}

	// Should not panic even when Len returns errors
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	mc.Metrics(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}
