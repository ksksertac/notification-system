package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
)

type Scheduler struct {
	repo           repository.NotificationRepository
	publisher      queue.Publisher
	pollInterval   time.Duration
	batchSize      int
	stuckThreshold time.Duration
	logger         *slog.Logger
	wg             sync.WaitGroup
}

func New(
	repo repository.NotificationRepository,
	publisher queue.Publisher,
	pollInterval time.Duration,
	batchSize int,
	logger *slog.Logger,
) *Scheduler {
	if batchSize <= 0 {
		batchSize = 500
	}
	return &Scheduler{
		repo:           repo,
		publisher:      publisher,
		pollInterval:   pollInterval,
		batchSize:      batchSize,
		stuckThreshold: 2 * time.Minute,
		logger:         logger,
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(2)
	go s.runScheduler(ctx)
	go s.runRecovery(ctx)
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

	ticker := time.NewTicker(30 * time.Second)
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
	notifications, err := s.repo.ClaimScheduledBatch(ctx, s.batchSize)
	if err != nil {
		s.logger.Error("failed to claim scheduled notifications", "error", err)
		return 0
	}

	if len(notifications) == 0 {
		return 0
	}

	s.logger.Info("claimed scheduled notifications", "count", len(notifications))

	if err := s.publisher.PublishBatch(ctx, notifications); err != nil {
		s.logger.Error("failed to publish batch to stream", "count", len(notifications), "error", err)
		return 0
	}

	return len(notifications)
}

func (s *Scheduler) recoverStuck(ctx context.Context) {
	recovered, err := s.repo.RecoverStuckQueued(ctx, s.stuckThreshold, s.batchSize)
	if err != nil {
		s.logger.Error("failed to recover stuck notifications", "error", err)
		return
	}

	if len(recovered) > 0 {
		s.logger.Warn("recovered stuck queued notifications", "count", len(recovered))
	}
}
