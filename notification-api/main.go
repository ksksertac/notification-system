package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/notification-api/handler"
	"github.com/sertacyildirim/notification-system/notification-api/service"
	ws "github.com/sertacyildirim/notification-system/notification-api/websocket"
	"github.com/sertacyildirim/notification-system/shared/config"
	"github.com/sertacyildirim/notification-system/shared/database"
	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
	"github.com/sertacyildirim/notification-system/shared/tracing"
)

// @title Notification System API
// @version 1.0
// @description Event-driven notification system with Redis-first hot/cold tiered storage. Supports SMS, Email, and Push channels with priority queuing, retry logic, and real-time status tracking.
// @host localhost:8080
// @BasePath /api/v1
// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name X-API-Key
func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	logger := setupLogger(cfg.Log.Level)
	logger.Info("starting notification api")

	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	shutdownTracer, err := tracing.InitTracer(context.Background(), "notification-api", otlpEndpoint)
	if err != nil {
		logger.Warn("failed to init tracer, continuing without tracing", "error", err)
	} else {
		defer shutdownTracer(context.Background())
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := redisClient.Ping(ctx).Err(); err != nil {
		cancel()
		return fmt.Errorf("connecting to redis: %w", err)
	}
	cancel()
	logger.Info("connected to redis")

	db, err := database.NewPostgres(cfg.DB)
	if err != nil {
		return fmt.Errorf("connecting to postgres (read fallback): %w", err)
	}
	defer db.Close()
	logger.Info("connected to postgres (cold read fallback)")

	streamMgr := queue.NewRedisStreamManager(redisClient)
	if err := streamMgr.EnsureStreams(context.Background(), cfg.Queue.ConsumerGroup); err != nil {
		return fmt.Errorf("ensuring streams: %w", err)
	}

	hotRepo := repository.NewRedisNotificationRepo(redisClient)
	coldRepo := repository.NewPostgresNotificationRepo(db)
	repo := repository.NewTieredNotificationRepo(hotRepo, coldRepo)
	publisher := queue.NewRedisPublisher(redisClient)
	consumer := queue.NewRedisConsumer(redisClient)

	writeBuffer := service.NewWriteBuffer(repo, publisher, 500, 50*time.Millisecond, logger)
	defer writeBuffer.Stop()

	svc := service.NewNotificationService(repo, publisher, writeBuffer, cfg.Retry.MaxAttempts, logger)

	// WebSocket allowed origins from environment (comma-separated), e.g. "https://example.com,https://app.example.com"
	var wsAllowedOrigins []string
	if origins := os.Getenv("WS_ALLOWED_ORIGINS"); origins != "" {
		wsAllowedOrigins = strings.Split(origins, ",")
	}
	wsHub := ws.NewHub(logger, wsAllowedOrigins)
	metrics := handler.NewMetricsCollector(consumer)

	apiKey := os.Getenv("API_KEY")
	router := NewRouter(svc, redisClient, consumer, metrics, wsHub, logger, apiKey, db.DB)

	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("HTTP server starting", "port", cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		logger.Info("shutdown signal received", "signal", sig)
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	logger.Info("stopping HTTP server")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("api shutdown complete")
	return nil
}

func setupLogger(level string) *slog.Logger {
	var logLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
}
