package writer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/repository"
	"github.com/sertacyildirim/notification-system/shared/tracing"
)

const (
	persistStream = "persist:queue"
	groupName     = "dbwriter-group"
)

type Writer struct {
	redis         *redis.Client
	repo          repository.NotificationRepository
	batchSize     int
	flushInterval time.Duration
	logger        *slog.Logger
	wg            sync.WaitGroup
}

func New(
	redisClient *redis.Client,
	repo repository.NotificationRepository,
	batchSize int,
	flushInterval time.Duration,
	logger *slog.Logger,
) *Writer {
	if batchSize <= 0 {
		batchSize = 500
	}
	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond
	}
	return &Writer{
		redis:         redisClient,
		repo:          repo,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		logger:        logger,
	}
}

func (w *Writer) Start(ctx context.Context) {
	w.ensureGroup(ctx)

	consumerName := fmt.Sprintf("dbwriter-%s", uuid.New().String()[:8])

	// Reclaim pending messages from crashed instances before entering main loop
	w.reclaimPending(ctx, consumerName)

	w.wg.Add(1)
	go w.run(ctx, consumerName)
}

func (w *Writer) Stop() {
	w.wg.Wait()
	w.logger.Info("dbwriter stopped")
}

func (w *Writer) ensureGroup(ctx context.Context) {
	err := w.redis.XGroupCreateMkStream(ctx, persistStream, groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		w.logger.Error("failed to create consumer group", "error", err)
	}
}

func (w *Writer) run(ctx context.Context, consumerName string) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	reclaimTicker := time.NewTicker(60 * time.Second)
	defer reclaimTicker.Stop()

	var pending []pendingMsg

	for {
		select {
		case <-ctx.Done():
			if err := w.flush(ctx, pending); err == nil {
				w.ackMessages(ctx, pending)
			}
			return
		case <-reclaimTicker.C:
			w.reclaimPending(ctx, consumerName)
		case <-ticker.C:
			msgs := w.readBatch(ctx, consumerName)
			pending = append(pending, msgs...)

			if len(pending) >= w.batchSize || (len(pending) > 0 && len(msgs) == 0) {
				if err := w.flush(ctx, pending); err == nil {
					w.ackMessages(ctx, pending)
				} else {
					w.logger.Warn("flush failed, messages will be re-delivered", "pending", len(pending), "error", err)
				}
				pending = nil
			}
		}
	}
}

type pendingMsg struct {
	streamID string
	event    *repository.PersistEvent
}

func (w *Writer) readBatch(ctx context.Context, consumer string) []pendingMsg {
	results, err := w.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumer,
		Streams:  []string{persistStream, ">"},
		Count:    int64(w.batchSize),
		Block:    50 * time.Millisecond,
	}).Result()

	if err != nil {
		if err != redis.Nil {
			w.logger.Error("failed to read from persist stream", "error", err)
		}
		return nil
	}

	var msgs []pendingMsg
	for _, result := range results {
		for _, msg := range result.Messages {
			evt, err := repository.ParsePersistEvent(msg.Values)
			if err != nil {
				w.logger.Warn("invalid persist event", "id", msg.ID, "error", err)
				msgs = append(msgs, pendingMsg{streamID: msg.ID})
				continue
			}
			msgs = append(msgs, pendingMsg{streamID: msg.ID, event: evt})
		}
	}

	return msgs
}

func (w *Writer) flush(ctx context.Context, msgs []pendingMsg) error {
	if len(msgs) == 0 {
		return nil
	}

	ctx, span := tracing.StartSpan(ctx, "dbwriter.Flush")
	defer span.End()
	tracing.SetIntAttr(span, "flush.message_count", len(msgs))

	var events []*repository.PersistEvent
	for _, m := range msgs {
		if m.event != nil {
			events = append(events, m.event)
		}
	}

	creates, updates := repository.SplitPersistActions(events)

	var flushErr error

	if len(creates) > 0 {
		if err := w.flushCreates(ctx, creates); err != nil {
			flushErr = err
		}
	}

	if len(updates) > 0 {
		if err := w.flushUpdates(ctx, updates); err != nil {
			flushErr = err
		}
	}

	w.logger.Debug("flushed to postgres", "creates", len(creates), "updates", len(updates))
	return flushErr
}

func (w *Writer) flushCreates(ctx context.Context, notifications []*domain.Notification) error {
	ctx, span := tracing.StartSpan(ctx, "dbwriter.FlushCreates")
	defer span.End()
	tracing.SetIntAttr(span, "postgres.insert_count", len(notifications))

	var lastErr error
	for i := 0; i < len(notifications); i += w.batchSize {
		end := i + w.batchSize
		if end > len(notifications) {
			end = len(notifications)
		}
		batch := notifications[i:end]

		if err := w.repo.CreateBatch(ctx, batch); err != nil {
			w.logger.Error("failed to batch insert to postgres", "count", len(batch), "error", err)
			lastErr = err
			continue
		}

		// Mark each notification as persisted in Redis so cleanup won't evict unpersisted data
		pipe := w.redis.Pipeline()
		for _, n := range batch {
			pipe.Set(ctx, repository.KeyPersisted+n.ID.String(), "1", repository.PersistedTTL)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			w.logger.Error("failed to mark notifications as persisted", "count", len(batch), "error", err)
		}
	}
	return lastErr
}

func (w *Writer) flushUpdates(ctx context.Context, updates []map[string]string) error {
	ctx, span := tracing.StartSpan(ctx, "dbwriter.FlushUpdates")
	defer span.End()
	tracing.SetIntAttr(span, "postgres.update_count", len(updates))

	var lastErr error
	for _, u := range updates {
		action := u["action"]
		idStr := u["id"]
		id, err := uuid.Parse(idStr)
		if err != nil {
			w.logger.Warn("invalid id in update event", "id", idStr)
			continue
		}

		switch action {
		case "update_status":
			from := domain.Status(u["from"])
			to := domain.Status(u["to"])
			if _, err := w.repo.UpdateStatus(ctx, id, from, to); err != nil {
				w.logger.Error("failed to update status in postgres", "id", idStr, "error", err)
				lastErr = err
			}

		case "update_status_details":
			from := domain.Status(u["from"])
			to := domain.Status(u["to"])
			var pmid *string
			if v, ok := u["provider_msg_id"]; ok {
				pmid = &v
			}
			var emsg *string
			if v, ok := u["error_message"]; ok {
				emsg = &v
			}
			if _, err := w.repo.UpdateStatusWithDetails(ctx, id, from, to, pmid, emsg); err != nil {
				w.logger.Error("failed to update status details in postgres", "id", idStr, "error", err)
				lastErr = err
			}

		case "increment_retry":
			nextRetryAt, _ := time.Parse(time.RFC3339Nano, u["next_retry_at"])
			errMsg := u["error_message"]
			if err := w.repo.IncrementRetry(ctx, id, nextRetryAt, errMsg); err != nil {
				w.logger.Error("failed to increment retry in postgres", "id", idStr, "error", err)
				lastErr = err
			}

		case "move_to_dlq":
			n, err := w.repo.GetByID(ctx, id)
			if err != nil || n == nil {
				w.logger.Error("failed to get notification for DLQ", "id", idStr, "error", err)
				lastErr = err
				continue
			}
			errMsg := u["error_message"]
			if err := w.repo.MoveToDLQ(ctx, n, errMsg); err != nil {
				w.logger.Error("failed to move to DLQ in postgres", "id", idStr, "error", err)
				lastErr = err
			}
		}
	}
	return lastErr
}

func (w *Writer) reclaimPending(ctx context.Context, consumerName string) {
	for {
		result, _, err := w.redis.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   persistStream,
			Group:    groupName,
			Consumer: consumerName,
			MinIdle:  30 * time.Second,
			Start:    "0-0",
			Count:    int64(w.batchSize),
		}).Result()

		if err != nil {
			if err != redis.Nil {
				w.logger.Error("reclaim pending failed", "error", err)
			}
			return
		}

		if len(result) == 0 {
			return
		}

		var msgs []pendingMsg
		for _, msg := range result {
			evt, err := repository.ParsePersistEvent(msg.Values)
			if err != nil {
				msgs = append(msgs, pendingMsg{streamID: msg.ID})
				continue
			}
			msgs = append(msgs, pendingMsg{streamID: msg.ID, event: evt})
		}

		if err := w.flush(ctx, msgs); err != nil {
			w.logger.Warn("flush failed for reclaimed messages, will retry", "count", len(msgs), "error", err)
			return
		}
		w.ackMessages(ctx, msgs)
		w.logger.Info("reclaimed pending messages", "count", len(msgs))

		if len(result) < w.batchSize {
			return
		}
	}
}

func (w *Writer) ackMessages(ctx context.Context, msgs []pendingMsg) {
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.streamID)
	}
	if len(ids) > 0 {
		w.redis.XAck(ctx, persistStream, groupName, ids...)
	}
}
