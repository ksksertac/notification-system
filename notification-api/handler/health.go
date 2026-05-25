package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

type HealthHandler struct {
	redis *redis.Client
}

func NewHealthHandler(redis *redis.Client) *HealthHandler {
	return &HealthHandler{redis: redis}
}

type healthStatus struct {
	Status     string            `json:"status"`
	Components map[string]string `json:"components"`
}

// Health godoc
// @Summary Health check
// @Description Check health status of Redis (primary data store)
// @Tags system
// @Produce json
// @Success 200 {object} healthStatus
// @Failure 503 {object} healthStatus
// @Router /health [get]
func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	status := healthStatus{
		Status:     "healthy",
		Components: make(map[string]string),
	}

	if err := h.redis.Ping(ctx).Err(); err != nil {
		status.Components["redis"] = "unhealthy: " + err.Error()
		status.Status = "unhealthy"
	} else {
		status.Components["redis"] = "healthy"
	}

	w.Header().Set("Content-Type", "application/json")
	if status.Status == "unhealthy" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(status)
}
