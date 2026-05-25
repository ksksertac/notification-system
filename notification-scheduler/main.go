package main

import (
	"context"
	"fmt"
	"log/slog"
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

	sched := scheduler.New(repo, publisher, cfg.Scheduler.PollInterval, cfg.Scheduler.BatchSize, logger)
	sched.Start(schedCtx)

	logger.Info("scheduler running",
		"poll_interval", cfg.Scheduler.PollInterval,
		"batch_size", cfg.Scheduler.BatchSize,
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("shutdown signal received", "signal", sig)
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
