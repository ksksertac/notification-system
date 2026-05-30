package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
)

type WriteBuffer struct {
	repo          repository.NotificationRepository
	publisher     queue.Publisher
	logger        *slog.Logger
	incoming      chan *domain.Notification
	flushSize     int
	flushInterval time.Duration
	wg            sync.WaitGroup
}

func NewWriteBuffer(
	repo repository.NotificationRepository,
	publisher queue.Publisher,
	flushSize int,
	flushInterval time.Duration,
	logger *slog.Logger,
) *WriteBuffer {
	if flushSize <= 0 {
		flushSize = 500
	}
	if flushInterval <= 0 {
		flushInterval = 50 * time.Millisecond
	}
	if logger == nil {
		logger = slog.Default()
	}
	wb := &WriteBuffer{
		repo:          repo,
		publisher:     publisher,
		logger:        logger,
		incoming:      make(chan *domain.Notification, flushSize*2),
		flushSize:     flushSize,
		flushInterval: flushInterval,
	}
	wb.wg.Add(1)
	go wb.run()
	return wb
}

// Submit persists the notification to Redis immediately (crash-safe), then
// queues it for batched stream publish. The client gets a response as soon as
// the Redis write succeeds — if the pod crashes before stream publish, the
// scheduler's orphaned-pending recovery picks it up within ~30s.
func (wb *WriteBuffer) Submit(ctx context.Context, n *domain.Notification) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := wb.repo.Create(ctx, n); err != nil {
		return err
	}

	select {
	case wb.incoming <- n:
	default:
		wb.logger.Warn("write buffer channel full, scheduler will recover",
			"notification_id", n.ID,
		)
	}

	return nil
}

func (wb *WriteBuffer) Stop() {
	close(wb.incoming)
	wb.wg.Wait()
}

func (wb *WriteBuffer) run() {
	defer wb.wg.Done()

	batch := make([]*domain.Notification, 0, wb.flushSize)
	ticker := time.NewTicker(wb.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case n, ok := <-wb.incoming:
			if !ok {
				wb.flush(batch)
				return
			}
			batch = append(batch, n)
			if len(batch) >= wb.flushSize {
				wb.flush(batch)
				batch = make([]*domain.Notification, 0, wb.flushSize)
				ticker.Reset(wb.flushInterval)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				wb.flush(batch)
				batch = make([]*domain.Notification, 0, wb.flushSize)
			}
		}
	}
}

// flush only handles batched stream publish — the Redis HSET already happened
// in Submit. If publish fails, notifications stay "pending" and the scheduler
// recovers them.
func (wb *WriteBuffer) flush(batch []*domain.Notification) {
	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var immediate []*domain.Notification
	for _, n := range batch {
		if n.ScheduledAt == nil {
			immediate = append(immediate, n)
		}
	}

	if len(immediate) > 0 {
		for _, n := range immediate {
			wb.repo.UpdateStatus(ctx, n.ID, domain.StatusPending, domain.StatusQueued)
			n.Status = domain.StatusQueued
		}
		if pubErr := wb.publisher.PublishBatch(ctx, immediate); pubErr != nil {
			for _, n := range immediate {
				wb.repo.UpdateStatus(ctx, n.ID, domain.StatusQueued, domain.StatusPending)
				n.Status = domain.StatusPending
			}
			wb.logger.Warn("write buffer publish failed, scheduler will retry",
				"count", len(immediate),
				"error", pubErr,
			)
		}
	}

	wb.logger.Debug("write buffer flushed", "count", len(batch))
}
