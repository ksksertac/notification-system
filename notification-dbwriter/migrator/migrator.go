package migrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/redis/go-redis/v9"
)

const (
	lockKey    = "migration-leader-lock"
	lockTTL    = 60 * time.Second
	retryDelay = 2 * time.Second
	maxRetries = 15
)

func Run(ctx context.Context, redisClient *redis.Client, databaseURL string, migrationsPath string, logger *slog.Logger) error {
	acquired, err := tryAcquireLock(ctx, redisClient)
	if err != nil {
		return fmt.Errorf("acquiring migration lock: %w", err)
	}

	if acquired {
		defer releaseLock(ctx, redisClient)
		logger.Info("migration leader elected, running migrations")

		if err := runMigrations(databaseURL, migrationsPath); err != nil {
			return fmt.Errorf("running migrations: %w", err)
		}

		logger.Info("migrations completed successfully")
		return nil
	}

	logger.Info("another pod is running migrations, waiting for completion")
	return waitForMigrations(ctx, redisClient, logger)
}

func tryAcquireLock(ctx context.Context, client *redis.Client) (bool, error) {
	return client.SetNX(ctx, lockKey, "leader", lockTTL).Result()
}

func releaseLock(ctx context.Context, client *redis.Client) {
	client.Del(ctx, lockKey)
}

func runMigrations(databaseURL string, migrationsPath string) error {
	m, err := migrate.New("file://"+migrationsPath, databaseURL)
	if err != nil {
		return fmt.Errorf("creating migrator: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("applying migrations: %w", err)
	}

	return nil
}

func waitForMigrations(ctx context.Context, client *redis.Client, logger *slog.Logger) error {
	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
		}

		exists, err := client.Exists(ctx, lockKey).Result()
		if err != nil {
			logger.Warn("error checking migration lock", "error", err)
			continue
		}
		if exists == 0 {
			logger.Info("migration lock released, proceeding")
			return nil
		}
		logger.Debug("waiting for migration leader to finish", "attempt", i+1)
	}

	return fmt.Errorf("timed out waiting for migration leader")
}
