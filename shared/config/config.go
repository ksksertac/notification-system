package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Server   ServerConfig
	DB       DBConfig
	Redis    RedisConfig
	Queue    QueueConfig
	Rate     RateConfig
	CB       CircuitBreakerConfig
	Retry    RetryConfig
	Provider ProviderConfig
	Scheduler SchedulerConfig
	Log      LogConfig
}

type ServerConfig struct {
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

type DBConfig struct {
	Host            string
	Port            string
	User            string
	Password        string
	Name            string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

func (c DBConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Name, c.SSLMode,
	)
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type QueueConfig struct {
	ConsumerGroup string
	WeightHigh    int
	WeightNormal  int
	WeightLow     int
	WorkerCount   int
	ClaimMinIdle  time.Duration
	ClaimInterval time.Duration
}

type RateConfig struct {
	LimitPerSecond int
}

type CircuitBreakerConfig struct {
	FailureThreshold   int
	OpenDuration       time.Duration
	HalfOpenMaxRequests int
}

type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type ProviderConfig struct {
	WebhookURL string
	Timeout    time.Duration
}

type SchedulerConfig struct {
	PollInterval      time.Duration
	BatchSize         int
	StuckThreshold    time.Duration
	RecoveryInterval  time.Duration
	RetryInterval     time.Duration
	OrphanThreshold   time.Duration
}

type LogConfig struct {
	Level string
}

func Load() (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{
			Port:            envOrDefault("SERVER_PORT", "8080"),
			ReadTimeout:     envDurationOrDefault("SERVER_READ_TIMEOUT", 15*time.Second),
			WriteTimeout:    envDurationOrDefault("SERVER_WRITE_TIMEOUT", 15*time.Second),
			ShutdownTimeout: envDurationOrDefault("SERVER_SHUTDOWN_TIMEOUT", 30*time.Second),
		},
		DB: DBConfig{
			Host:            envOrDefault("DB_HOST", "localhost"),
			Port:            envOrDefault("DB_PORT", "5432"),
			User:            envOrDefault("DB_USER", "notification"),
			Password:        envOrDefault("DB_PASSWORD", "notification_secret"),
			Name:            envOrDefault("DB_NAME", "notification_db"),
			SSLMode:         envOrDefault("DB_SSL_MODE", "disable"),
			MaxOpenConns:    envIntOrDefault("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    envIntOrDefault("DB_MAX_IDLE_CONNS", 10),
			ConnMaxLifetime: envDurationOrDefault("DB_CONN_MAX_LIFETIME", 5*time.Minute),
		},
		Redis: RedisConfig{
			Addr:     envOrDefault("REDIS_ADDR", "localhost:6379"),
			Password: envOrDefault("REDIS_PASSWORD", ""),
			DB:       envIntOrDefault("REDIS_DB", 0),
		},
		Queue: QueueConfig{
			ConsumerGroup: envOrDefault("QUEUE_CONSUMER_GROUP", "notification-workers"),
			WeightHigh:    envIntOrDefault("QUEUE_WEIGHT_HIGH", 10),
			WeightNormal:  envIntOrDefault("QUEUE_WEIGHT_NORMAL", 5),
			WeightLow:     envIntOrDefault("QUEUE_WEIGHT_LOW", 2),
			WorkerCount:   envIntOrDefault("QUEUE_WORKER_COUNT", 5),
			ClaimMinIdle:  envDurationOrDefault("QUEUE_CLAIM_MIN_IDLE", 30*time.Second),
			ClaimInterval: envDurationOrDefault("QUEUE_CLAIM_INTERVAL", 15*time.Second),
		},
		Rate: RateConfig{
			LimitPerSecond: envIntOrDefault("RATE_LIMIT_PER_SECOND", 100),
		},
		CB: CircuitBreakerConfig{
			FailureThreshold:    envIntOrDefault("CB_FAILURE_THRESHOLD", 5),
			OpenDuration:        envDurationOrDefault("CB_OPEN_DURATION", 30*time.Second),
			HalfOpenMaxRequests: envIntOrDefault("CB_HALF_OPEN_MAX_REQUESTS", 1),
		},
		Retry: RetryConfig{
			MaxAttempts: envIntOrDefault("RETRY_MAX_ATTEMPTS", 5),
			BaseDelay:   envDurationOrDefault("RETRY_BASE_DELAY", 2*time.Second),
			MaxDelay:    envDurationOrDefault("RETRY_MAX_DELAY", 60*time.Second),
		},
		Provider: ProviderConfig{
			WebhookURL: envOrDefault("WEBHOOK_URL", "https://webhook.site/test"),
			Timeout:    envDurationOrDefault("PROVIDER_TIMEOUT", 10*time.Second),
		},
		Scheduler: SchedulerConfig{
			PollInterval:     envDurationOrDefault("SCHEDULER_POLL_INTERVAL", 5*time.Second),
			BatchSize:        envIntOrDefault("SCHEDULER_BATCH_SIZE", 500),
			StuckThreshold:   envDurationOrDefault("SCHEDULER_STUCK_THRESHOLD", 2*time.Minute),
			RecoveryInterval: envDurationOrDefault("SCHEDULER_RECOVERY_INTERVAL", 30*time.Second),
			RetryInterval:    envDurationOrDefault("SCHEDULER_RETRY_INTERVAL", 10*time.Second),
			OrphanThreshold:  envDurationOrDefault("SCHEDULER_ORPHAN_THRESHOLD", 30*time.Second),
		},
		Log: LogConfig{
			Level: envOrDefault("LOG_LEVEL", "info"),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.Redis.Addr == "" {
		return fmt.Errorf("REDIS_ADDR is required")
	}
	if c.Queue.WorkerCount < 1 {
		return fmt.Errorf("QUEUE_WORKER_COUNT must be at least 1")
	}
	if c.Rate.LimitPerSecond < 1 {
		return fmt.Errorf("RATE_LIMIT_PER_SECOND must be at least 1")
	}
	if c.DB.Password == "notification_secret" {
		slog.Warn("DB_PASSWORD is using the default hardcoded value; set DB_PASSWORD env var for production")
	}
	return nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("invalid integer for env var, using default", "key", key, "value", v, "default", fallback, "error", err)
		return fallback
	}
	return i
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration for env var, using default", "key", key, "value", v, "default", fallback, "error", err)
		return fallback
	}
	return d
}
