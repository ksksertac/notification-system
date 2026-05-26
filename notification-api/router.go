package main

import (
	"database/sql"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
	httpSwagger "github.com/swaggo/http-swagger"

	_ "github.com/sertacyildirim/notification-system/notification-api/docs"
	"github.com/sertacyildirim/notification-system/notification-api/handler"
	"github.com/sertacyildirim/notification-system/notification-api/middleware"
	"github.com/sertacyildirim/notification-system/notification-api/service"
	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/tracing"
	ws "github.com/sertacyildirim/notification-system/notification-api/websocket"
)

func NewRouter(
	svc service.NotificationService,
	redisClient *redis.Client,
	consumer queue.Consumer,
	metrics *handler.MetricsCollector,
	wsHub *ws.Hub,
	logger *slog.Logger,
	apiKey string,
	db *sql.DB,
) http.Handler {
	r := chi.NewRouter()

	r.Use(tracing.HTTPMiddleware)
	r.Use(middleware.CorrelationID)
	r.Use(middleware.Recovery(logger))
	r.Use(middleware.RateLimit(redisClient, 1000))
	r.Use(middleware.Logging(logger, metrics))
	r.Use(middleware.MaxBodySize(2 << 20))

	nh := handler.NewNotificationHandler(svc)
	hh := handler.NewHealthHandler(redisClient, db)

	r.Get("/health", hh.Health)
	r.Get("/metrics", metrics.Metrics)
	r.Get("/ws", wsHub.HandleWS)
	r.Get("/swagger", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/index.html", http.StatusMovedPermanently)
	})
	r.Get("/swagger/*", httpSwagger.WrapHandler)

	r.Route("/api/v1", func(r chi.Router) {
		if apiKey != "" {
			r.Use(middleware.APIKeyAuth(apiKey))
		}
		r.Post("/notifications", nh.Create)
		r.Post("/notifications/batch", nh.CreateBatch)
		r.Get("/notifications", nh.List)
		r.Get("/notifications/{id}", nh.GetByID)
		r.Get("/notifications/batch/{batchId}", nh.GetByBatchID)
		r.Patch("/notifications/{id}/cancel", nh.Cancel)
	})

	return r
}
