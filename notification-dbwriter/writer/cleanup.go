package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/shared/repository"
)

const (
	cleanupInterval = 1 * time.Minute
	hotWindow       = 1 * time.Hour
	cleanupBatch    = 500
)

func (w *Writer) StartCleanup(ctx context.Context) {
	w.wg.Add(1)
	go w.runCleanup(ctx)
}

func (w *Writer) runCleanup(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.evictOldEntries(ctx)
		}
	}
}

func (w *Writer) evictOldEntries(ctx context.Context) {
	cutoff := time.Now().UTC().Add(-hotWindow)
	cutoffScore := fmt.Sprintf("%d", cutoff.UnixNano())

	total := 0

	for {
		if ctx.Err() != nil {
			return
		}

		ids, err := w.redis.ZRangeByScore(ctx, repository.KeyIdxCreatedAt, &redis.ZRangeBy{
			Min:   "-inf",
			Max:   cutoffScore,
			Count: cleanupBatch,
		}).Result()
		if err != nil {
			w.logger.Error("cleanup: failed to get old entries", "error", err)
			return
		}

		if len(ids) == 0 {
			break
		}

		w.evictBatch(ctx, ids)
		total += len(ids)

		if len(ids) < cleanupBatch {
			break
		}
	}

	if total > 0 {
		w.logger.Info("cleanup: evicted old entries from Redis", "count", total)
	}
}

func (w *Writer) evictBatch(ctx context.Context, ids []string) {
	// Phase 1: Pipeline all Exists and HGetAll lookups to avoid per-ID round-trips
	lookupPipe := w.redis.Pipeline()

	existsCmds := make(map[string]*redis.IntCmd, len(ids))
	hgetCmds := make(map[string]*redis.MapStringStringCmd, len(ids))

	for _, idStr := range ids {
		existsCmds[idStr] = lookupPipe.Exists(ctx, repository.KeyPersisted+idStr)
		hgetCmds[idStr] = lookupPipe.HGetAll(ctx, repository.KeyNotification+idStr)
	}

	if _, err := lookupPipe.Exec(ctx); err != nil {
		w.logger.Error("cleanup: lookup pipeline exec failed", "error", err, "batch_size", len(ids))
		return
	}

	// Phase 2: Build eviction pipeline based on lookup results
	evictPipe := w.redis.Pipeline()

	for _, idStr := range ids {
		persisted, _ := existsCmds[idStr].Result()
		if persisted == 0 {
			continue
		}

		nKey := repository.KeyNotification + idStr

		vals, err := hgetCmds[idStr].Result()
		if err != nil || len(vals) == 0 {
			evictPipe.ZRem(ctx, repository.KeyIdxCreatedAt, idStr)
			evictPipe.Del(ctx, repository.KeyPersisted+idStr)
			continue
		}

		status := vals["status"]
		channel := vals["channel"]
		batchID := vals["batch_id"]

		evictPipe.Del(ctx, nKey)
		evictPipe.ZRem(ctx, repository.KeyIdxCreatedAt, idStr)
		if status != "" {
			evictPipe.ZRem(ctx, repository.KeyIdxStatus+status, idStr)
		}
		if channel != "" {
			evictPipe.ZRem(ctx, repository.KeyIdxChannel+channel, idStr)
		}
		if batchID != "" {
			evictPipe.SRem(ctx, repository.KeyIdxBatch+batchID, idStr)
		}

		evictPipe.Del(ctx, repository.KeyDLQ+idStr)
		evictPipe.Del(ctx, repository.KeyPersisted+idStr)
	}

	if _, err := evictPipe.Exec(ctx); err != nil {
		w.logger.Error("cleanup: eviction pipeline exec failed", "error", err, "batch_size", len(ids))
	}
}

