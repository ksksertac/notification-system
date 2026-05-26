package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/notification-dbwriter/migrator"
	"github.com/sertacyildirim/notification-system/notification-dbwriter/writer"
	"github.com/sertacyildirim/notification-system/shared/config"
	"github.com/sertacyildirim/notification-system/shared/database"
	"github.com/sertacyildirim/notification-system/shared/repository"
)

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
	logger.Info("starting notification dbwriter")

	db, err := database.NewPostgres(cfg.DB)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer db.Close()
	logger.Info("connected to postgres")

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

	if err := migrator.Run(context.Background(), redisClient, cfg.DB.DSN(), "migrations", logger); err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}

	repo := repository.NewPostgresNotificationRepo(db)

	batchSize := 500
	flushInterval := 100 * time.Millisecond

	w := writer.New(redisClient, repo, batchSize, flushInterval, logger)

	writerCtx, writerCancel := context.WithCancel(context.Background())
	defer writerCancel()

	w.Start(writerCtx)
	w.StartCleanup(writerCtx)
	logger.Info("dbwriter running", "batch_size", batchSize, "flush_interval", flushInterval, "hot_window", "1h")

	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(resp http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		status := map[string]string{"status": "healthy"}
		code := http.StatusOK
		if err := redisClient.Ping(ctx).Err(); err != nil {
			status["status"] = "unhealthy"
			status["redis"] = err.Error()
			code = http.StatusServiceUnavailable
		}
		if err := db.PingContext(ctx); err != nil {
			status["status"] = "unhealthy"
			status["postgres"] = err.Error()
			code = http.StatusServiceUnavailable
		}
		resp.Header().Set("Content-Type", "application/json")
		resp.WriteHeader(code)
		json.NewEncoder(resp).Encode(status)
	})
	healthSrv := &http.Server{Addr: ":9092", Handler: healthMux}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("health server error", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("shutdown signal received", "signal", sig)
	healthSrv.Shutdown(context.Background())
	writerCancel()
	w.Stop()
	logger.Info("dbwriter shutdown complete")
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
