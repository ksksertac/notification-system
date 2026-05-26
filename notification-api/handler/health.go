package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

type HealthHandler struct {
	redis *redis.Client
	db    *sql.DB // optional — nil when Postgres is not used by this service
}

func NewHealthHandler(redis *redis.Client, db *sql.DB) *HealthHandler {
	return &HealthHandler{redis: redis, db: db}
}

type healthStatus struct {
	Status     string            `json:"status"`
	Components map[string]string `json:"components"`
}

// Health godoc
// @Summary Health check
// @Description Check health status of Redis (primary data store) and optionally Postgres
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

	if h.db != nil {
		if err := h.db.PingContext(ctx); err != nil {
			status.Components["postgres"] = "unhealthy: " + err.Error()
			status.Status = "unhealthy"
		} else {
			status.Components["postgres"] = "healthy"
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if status.Status == "unhealthy" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(status)
}
