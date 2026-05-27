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

type writeRequest struct {
	notification *domain.Notification
	resultCh     chan error
}

type WriteBuffer struct {
	repo          repository.NotificationRepository
	publisher     queue.Publisher
	logger        *slog.Logger
	incoming      chan *writeRequest
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
		incoming:      make(chan *writeRequest, flushSize*2),
		flushSize:     flushSize,
		flushInterval: flushInterval,
	}
	wb.wg.Add(1)
	go wb.run()
	return wb
}

func (wb *WriteBuffer) Submit(ctx context.Context, n *domain.Notification) error {
	req := &writeRequest{
		notification: n,
		resultCh:     make(chan error, 1),
	}

	select {
	case wb.incoming <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (wb *WriteBuffer) Stop() {
	close(wb.incoming)
	wb.wg.Wait()
}

func (wb *WriteBuffer) run() {
	defer wb.wg.Done()

	batch := make([]*writeRequest, 0, wb.flushSize)
	ticker := time.NewTicker(wb.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case req, ok := <-wb.incoming:
			if !ok {
				wb.flush(batch)
				return
			}
			batch = append(batch, req)
			if len(batch) >= wb.flushSize {
				wb.flush(batch)
				batch = make([]*writeRequest, 0, wb.flushSize)
				ticker.Reset(wb.flushInterval)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				wb.flush(batch)
				batch = make([]*writeRequest, 0, wb.flushSize)
			}
		}
	}
}

func (wb *WriteBuffer) flush(batch []*writeRequest) {
	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	notifications := make([]*domain.Notification, len(batch))
	for i, req := range batch {
		notifications[i] = req.notification
	}

	err := wb.repo.CreateBatch(ctx, notifications)

	if err != nil {
		wb.logger.Error("write buffer flush failed", "count", len(batch), "error", err)
		for _, req := range batch {
			req.resultCh <- err
		}
		return
	}

	wb.logger.Debug("write buffer flushed", "count", len(batch))

	var immediate []*domain.Notification
	for _, n := range notifications {
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

	for _, req := range batch {
		req.resultCh <- nil
	}
}
