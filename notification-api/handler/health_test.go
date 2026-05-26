package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestHealth_Healthy(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	h := NewHealthHandler(client, nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	h.Health(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	var status healthStatus
	json.NewDecoder(rr.Body).Decode(&status)
	if status.Status != "healthy" {
		t.Errorf("expected status 'healthy', got '%s'", status.Status)
	}
	if status.Components["redis"] != "healthy" {
		t.Errorf("expected redis component 'healthy', got '%s'", status.Components["redis"])
	}
}

func TestHealth_Unhealthy(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	// Close miniredis to simulate unhealthy Redis
	mr.Close()

	h := NewHealthHandler(client, nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	h.Health(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rr.Code)
	}

	var status healthStatus
	json.NewDecoder(rr.Body).Decode(&status)
	if status.Status != "unhealthy" {
		t.Errorf("expected status 'unhealthy', got '%s'", status.Status)
	}
}
