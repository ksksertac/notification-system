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
	"github.com/sertacyildirim/notification-system/notification-consumer/delivery"
	"github.com/sertacyildirim/notification-system/notification-consumer/metrics"
	tmpl "github.com/sertacyildirim/notification-system/notification-consumer/template"
	"github.com/sertacyildirim/notification-system/notification-consumer/worker"
	"github.com/sertacyildirim/notification-system/shared/config"
	"github.com/sertacyildirim/notification-system/shared/queue"
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
	logger.Info("starting notification consumer")

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
	consumer := queue.NewRedisConsumer(redisClient)

	provider := delivery.NewWebhookProvider(cfg.Provider.WebhookURL, cfg.Provider.Timeout)
	rateLimiter := delivery.NewRedisRateLimiter(redisClient, cfg.Rate.LimitPerSecond)
	retryStrategy := delivery.NewExponentialBackoff(cfg.Retry.BaseDelay, cfg.Retry.MaxDelay)

	cbRegistry := delivery.NewCircuitBreakerRegistry(delivery.CircuitBreakerConfig{
		FailureThreshold: cfg.CB.FailureThreshold,
		OpenDuration:     cfg.CB.OpenDuration,
		HalfOpenMax:      cfg.CB.HalfOpenMaxRequests,
		OnStateChange: func(from, to delivery.CircuitState) {
			logger.Warn("circuit breaker state change", "from", from.String(), "to", to.String())
		},
	})

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	tmplEngine := tmpl.NewEngine()
	metricsRecorder := metrics.NewPrometheusRecorder()

	workerPool := worker.NewWorkerPool(
		worker.WorkerPoolConfig{
			ConsumerGroup: cfg.Queue.ConsumerGroup,
			WorkerCount:   cfg.Queue.WorkerCount,
			WeightHigh:    cfg.Queue.WeightHigh,
			WeightNormal:  cfg.Queue.WeightNormal,
			WeightLow:     cfg.Queue.WeightLow,
			ClaimMinIdle:  cfg.Queue.ClaimMinIdle,
			ClaimInterval: cfg.Queue.ClaimInterval,
			MaxRetries:    cfg.Retry.MaxAttempts,
		},
		consumer,
		publisher,
		provider,
		rateLimiter,
		cbRegistry,
		retryStrategy,
		repo,
		nil,
		metricsRecorder,
		tmplEngine,
		logger,
	)
	workerPool.Start(workerCtx)

	metricsSrv := &http.Server{Addr: ":9090", Handler: metricsRecorder.Handler()}
	go func() {
		logger.Info("metrics server starting", "port", 9090)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "error", err)
		}
	}()

	logger.Info("consumer running", "workers", cfg.Queue.WorkerCount)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("shutdown signal received", "signal", sig)
	workerCancel()
	workerPool.Stop()
	metricsSrv.Shutdown(context.Background())
	logger.Info("consumer shutdown complete")
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
