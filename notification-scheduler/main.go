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
	"github.com/sertacyildirim/notification-system/notification-scheduler/scheduler"
	"github.com/sertacyildirim/notification-system/shared/config"
	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
	"github.com/sertacyildirim/notification-system/shared/tracing"
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
	logger.Info("starting notification scheduler")

	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	shutdownTracer, err := tracing.InitTracer(context.Background(), "notification-scheduler", otlpEndpoint)
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

	streamMgr := queue.NewRedisStreamManager(redisClient)
	if err := streamMgr.EnsureStreams(context.Background(), cfg.Queue.ConsumerGroup); err != nil {
		return fmt.Errorf("ensuring streams: %w", err)
	}

	repo := repository.NewRedisNotificationRepo(redisClient)
	publisher := queue.NewRedisPublisher(redisClient)

	schedCtx, schedCancel := context.WithCancel(context.Background())
	defer schedCancel()

	sched := scheduler.NewWithConfig(repo, publisher, scheduler.Config{
		PollInterval:     cfg.Scheduler.PollInterval,
		BatchSize:        cfg.Scheduler.BatchSize,
		StuckThreshold:   cfg.Scheduler.StuckThreshold,
		RecoveryInterval: cfg.Scheduler.RecoveryInterval,
		RetryInterval:    cfg.Scheduler.RetryInterval,
		OrphanThreshold:  cfg.Scheduler.OrphanThreshold,
	}, logger, nil)
	sched.Start(schedCtx)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		status := map[string]string{"status": "healthy"}
		code := http.StatusOK
		if err := redisClient.Ping(ctx).Err(); err != nil {
			status["status"] = "unhealthy"
			status["redis"] = err.Error()
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(status)
	})
	healthSrv := &http.Server{Addr: ":9091", Handler: mux}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("health server error", "error", err)
		}
	}()

	logger.Info("scheduler running",
		"poll_interval", cfg.Scheduler.PollInterval,
		"batch_size", cfg.Scheduler.BatchSize,
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("shutdown signal received", "signal", sig)
	healthSrv.Shutdown(context.Background())
	schedCancel()
	sched.Stop()
	logger.Info("scheduler shutdown complete")
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
