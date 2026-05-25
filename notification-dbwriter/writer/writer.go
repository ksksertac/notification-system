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

	w.wg.Add(1)
	go w.run(ctx)
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

func (w *Writer) run(ctx context.Context) {
	defer w.wg.Done()

	consumerName := fmt.Sprintf("dbwriter-%s", uuid.New().String()[:8])
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	var pending []pendingMsg

	for {
		select {
		case <-ctx.Done():
			w.flush(pending)
			return
		case <-ticker.C:
			msgs := w.readBatch(ctx, consumerName)
			pending = append(pending, msgs...)

			if len(pending) >= w.batchSize || (len(pending) > 0 && len(msgs) == 0) {
				w.flush(pending)
				w.ackMessages(ctx, pending)
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

	if err == redis.Nil || err != nil {
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

func (w *Writer) flush(msgs []pendingMsg) {
	if len(msgs) == 0 {
		return
	}

	var events []*repository.PersistEvent
	for _, m := range msgs {
		if m.event != nil {
			events = append(events, m.event)
		}
	}

	creates, updates := repository.SplitPersistActions(events)

	if len(creates) > 0 {
		w.flushCreates(creates)
	}

	if len(updates) > 0 {
		w.flushUpdates(updates)
	}

	w.logger.Debug("flushed to postgres", "creates", len(creates), "updates", len(updates))
}

func (w *Writer) flushCreates(notifications []*domain.Notification) {
	for i := 0; i < len(notifications); i += w.batchSize {
		end := i + w.batchSize
		if end > len(notifications) {
			end = len(notifications)
		}
		batch := notifications[i:end]

		if err := w.repo.CreateBatch(context.Background(), batch); err != nil {
			w.logger.Error("failed to batch insert to postgres", "count", len(batch), "error", err)
		}
	}
}

func (w *Writer) flushUpdates(updates []map[string]string) {
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
			if _, err := w.repo.UpdateStatus(context.Background(), id, from, to); err != nil {
				w.logger.Error("failed to update status in postgres", "id", idStr, "error", err)
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
			if _, err := w.repo.UpdateStatusWithDetails(context.Background(), id, from, to, pmid, emsg); err != nil {
				w.logger.Error("failed to update status details in postgres", "id", idStr, "error", err)
			}

		case "increment_retry":
			nextRetryAt, _ := time.Parse(time.RFC3339Nano, u["next_retry_at"])
			errMsg := u["error_message"]
			if err := w.repo.IncrementRetry(context.Background(), id, nextRetryAt, errMsg); err != nil {
				w.logger.Error("failed to increment retry in postgres", "id", idStr, "error", err)
			}

		case "move_to_dlq":
			n, err := w.repo.GetByID(context.Background(), id)
			if err != nil || n == nil {
				w.logger.Error("failed to get notification for DLQ", "id", idStr, "error", err)
				continue
			}
			errMsg := u["error_message"]
			if err := w.repo.MoveToDLQ(context.Background(), n, errMsg); err != nil {
				w.logger.Error("failed to move to DLQ in postgres", "id", idStr, "error", err)
			}
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
