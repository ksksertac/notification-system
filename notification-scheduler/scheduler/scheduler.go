package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
	"github.com/sertacyildirim/notification-system/shared/tracing"
)

// MetricsRecorder records scheduler operational metrics.
type MetricsRecorder interface {
	RecordClaimed(count int)
	RecordRecovered(kind string, count int)
	RecordRetryReady(count int)
}

// noopMetrics is a no-op implementation of MetricsRecorder.
type noopMetrics struct{}

func (noopMetrics) RecordClaimed(int)          {}
func (noopMetrics) RecordRecovered(string, int) {}
func (noopMetrics) RecordRetryReady(int)        {}

// Config holds all configurable scheduler parameters.
type Config struct {
	PollInterval     time.Duration
	BatchSize        int
	StuckThreshold   time.Duration
	RecoveryInterval time.Duration
	RetryInterval    time.Duration
	OrphanThreshold  time.Duration
}

type Scheduler struct {
	repo             repository.NotificationRepository
	publisher        queue.Publisher
	pollInterval     time.Duration
	batchSize        int
	stuckThreshold   time.Duration
	recoveryInterval time.Duration
	retryInterval    time.Duration
	orphanThreshold  time.Duration
	logger           *slog.Logger
	metrics          MetricsRecorder
	wg               sync.WaitGroup
}

func New(
	repo repository.NotificationRepository,
	publisher queue.Publisher,
	pollInterval time.Duration,
	batchSize int,
	logger *slog.Logger,
) *Scheduler {
	return NewWithConfig(repo, publisher, Config{
		PollInterval:     pollInterval,
		BatchSize:        batchSize,
		StuckThreshold:   2 * time.Minute,
		RecoveryInterval: 30 * time.Second,
		RetryInterval:    10 * time.Second,
		OrphanThreshold:  30 * time.Second,
	}, logger, nil)
}

func NewWithConfig(
	repo repository.NotificationRepository,
	publisher queue.Publisher,
	cfg Config,
	logger *slog.Logger,
	metrics MetricsRecorder,
) *Scheduler {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.StuckThreshold <= 0 {
		cfg.StuckThreshold = 2 * time.Minute
	}
	if cfg.RecoveryInterval <= 0 {
		cfg.RecoveryInterval = 30 * time.Second
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = 10 * time.Second
	}
	if cfg.OrphanThreshold <= 0 {
		cfg.OrphanThreshold = 30 * time.Second
	}
	if metrics == nil {
		metrics = noopMetrics{}
	}
	return &Scheduler{
		repo:             repo,
		publisher:        publisher,
		pollInterval:     cfg.PollInterval,
		batchSize:        cfg.BatchSize,
		stuckThreshold:   cfg.StuckThreshold,
		recoveryInterval: cfg.RecoveryInterval,
		retryInterval:    cfg.RetryInterval,
		orphanThreshold:  cfg.OrphanThreshold,
		logger:           logger,
		metrics:          metrics,
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(3)
	go s.runScheduler(ctx)
	go s.runRecovery(ctx)
	go s.runRetryRecovery(ctx)
	s.logger.Info("scheduler started", "poll_interval", s.pollInterval, "batch_size", s.batchSize)
}

func (s *Scheduler) Stop() {
	s.wg.Wait()
	s.logger.Info("scheduler stopped")
}

func (s *Scheduler) runScheduler(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.drainScheduled(ctx)
		}
	}
}

func (s *Scheduler) runRecovery(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.recoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recoverStuck(ctx)
		}
	}
}

func (s *Scheduler) drainScheduled(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		count := s.processBatch(ctx)
		if count < s.batchSize {
			return
		}
	}
}

func (s *Scheduler) processBatch(ctx context.Context) int {
	ctx, span := tracing.StartSpan(ctx, "scheduler.ProcessBatch")
	defer span.End()

	notifications, err := s.repo.ClaimScheduledBatch(ctx, s.batchSize)
	if err != nil {
		tracing.RecordError(span, err)
		s.logger.Error("failed to claim scheduled notifications", "error", err)
		return 0
	}

	if len(notifications) == 0 {
		return 0
	}

	tracing.SetIntAttr(span, "scheduler.claimed", len(notifications))
	s.logger.Info("claimed scheduled notifications", "count", len(notifications))
	s.metrics.RecordClaimed(len(notifications))

	if err := s.publisher.PublishBatch(ctx, notifications); err != nil {
		tracing.RecordError(span, err)
		s.logger.Error("failed to publish batch to stream", "count", len(notifications), "error", err)
		return 0
	}

	return len(notifications)
}

func (s *Scheduler) runRetryRecovery(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.processRetryReady(ctx)
		}
	}
}

func (s *Scheduler) processRetryReady(ctx context.Context) {
	ctx, span := tracing.StartSpan(ctx, "scheduler.ProcessRetryReady")
	defer span.End()

	notifications, err := s.repo.GetRetryReady(ctx, s.batchSize)
	if err != nil {
		tracing.RecordError(span, err)
		s.logger.Error("failed to get retry-ready notifications", "error", err)
		return
	}

	if len(notifications) == 0 {
		return
	}

	tracing.SetIntAttr(span, "scheduler.retry_count", len(notifications))
	s.logger.Info("re-enqueuing retry-ready notifications", "count", len(notifications))
	s.metrics.RecordRetryReady(len(notifications))

	if err := s.publisher.PublishBatch(ctx, notifications); err != nil {
		tracing.RecordError(span, err)
		s.logger.Error("failed to publish retry-ready batch to stream", "count", len(notifications), "error", err)
	}
}

func (s *Scheduler) recoverStuck(ctx context.Context) {
	ctx, span := tracing.StartSpan(ctx, "scheduler.RecoverStuck")
	defer span.End()

	// Recover stuck queued notifications (claimed but never published to stream)
	recovered, err := s.repo.RecoverStuckQueued(ctx, s.stuckThreshold, s.batchSize)
	if err != nil {
		s.logger.Error("failed to recover stuck queued notifications", "error", err)
	} else if len(recovered) > 0 {
		s.logger.Warn("recovered stuck queued notifications", "count", len(recovered))
		s.metrics.RecordRecovered("queued", len(recovered))
		if err := s.publisher.PublishBatch(ctx, recovered); err != nil {
			s.logger.Error("failed to publish recovered queued notifications", "error", err)
		}
	}

	// Recover stuck processing notifications
	recoveredProcessing, err := s.repo.RecoverStuckProcessing(ctx, s.stuckThreshold, s.batchSize)
	if err != nil {
		s.logger.Error("failed to recover stuck processing notifications", "error", err)
	} else if len(recoveredProcessing) > 0 {
		s.logger.Warn("recovered stuck processing notifications", "count", len(recoveredProcessing))
		s.metrics.RecordRecovered("processing", len(recoveredProcessing))
		if err := s.publisher.PublishBatch(ctx, recoveredProcessing); err != nil {
			s.logger.Error("failed to publish recovered processing notifications", "error", err)
		}
	}

	// Recover orphaned pending notifications (instant notifications stuck beyond threshold)
	orphaned, err := s.repo.RecoverOrphanedPending(ctx, s.orphanThreshold, s.batchSize)
	if err != nil {
		s.logger.Error("failed to recover orphaned pending notifications", "error", err)
	} else if len(orphaned) > 0 {
		s.logger.Warn("recovered orphaned pending notifications", "count", len(orphaned))
		s.metrics.RecordRecovered("orphaned", len(orphaned))
		if err := s.publisher.PublishBatch(ctx, orphaned); err != nil {
			s.logger.Error("failed to publish orphaned pending notifications", "error", err)
		}
	}
}
