package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/tracing"
)

const (
	KeyNotification = "notification:"
	KeyIdxStatus    = "idx:status:"
	KeyIdxChannel   = "idx:channel:"
	KeyIdxCreatedAt = "idx:created_at"
	KeyIdxBatch     = "idx:batch:"
	KeyIdxIdemKey   = "idx:idempotency:"
	KeyIdxRetry     = "idx:retry"
	KeySchedule     = "schedule:pending"
	KeyPersistQueue = "persist:queue"
	KeyDLQ          = "dlq:"
	KeyPersisted    = "persisted:"

	IdemKeyTTL   = 24 * time.Hour
	PersistedTTL = 2 * time.Hour
)

type redisNotificationRepo struct {
	client *redis.Client
}

func NewRedisNotificationRepo(client *redis.Client) NotificationRepository {
	return &redisNotificationRepo{client: client}
}

func notificationKey(id uuid.UUID) string {
	return KeyNotification + id.String()
}

func notificationToMap(n *domain.Notification) map[string]interface{} {
	m := map[string]interface{}{
		"id":          n.ID.String(),
		"recipient":   n.Recipient,
		"channel":     string(n.Channel),
		"content":     n.Content,
		"priority":    n.Priority.String(),
		"status":      string(n.Status),
		"retry_count": strconv.Itoa(n.RetryCount),
		"max_retries":   strconv.Itoa(n.MaxRetries),
		"requeue_count": strconv.Itoa(n.RequeueCount),
		"created_at":    n.CreatedAt.Format(time.RFC3339Nano),
		"updated_at":  n.UpdatedAt.Format(time.RFC3339Nano),
	}
	if n.IdempotencyKey != nil {
		m["idempotency_key"] = *n.IdempotencyKey
	}
	if n.BatchID != nil {
		m["batch_id"] = n.BatchID.String()
	}
	if n.ProviderMsgID != nil {
		m["provider_msg_id"] = *n.ProviderMsgID
	}
	if n.NextRetryAt != nil {
		m["next_retry_at"] = n.NextRetryAt.Format(time.RFC3339Nano)
	}
	if n.ScheduledAt != nil {
		m["scheduled_at"] = n.ScheduledAt.Format(time.RFC3339Nano)
	}
	if n.Metadata != nil {
		m["metadata"] = string(n.Metadata)
	}
	if n.ErrorMessage != nil {
		m["error_message"] = *n.ErrorMessage
	}
	return m
}

func mapToNotification(vals map[string]string) (*domain.Notification, error) {
	id, err := uuid.Parse(vals["id"])
	if err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}

	priority, pErr := domain.PriorityFromString(vals["priority"])
	if pErr != nil {
		slog.Warn("mapToNotification: failed to parse priority, defaulting to 0", "id", vals["id"], "value", vals["priority"], "error", pErr)
	}
	retryCount, rcErr := strconv.Atoi(vals["retry_count"])
	if rcErr != nil {
		slog.Warn("mapToNotification: failed to parse retry_count, defaulting to 0", "id", vals["id"], "value", vals["retry_count"], "error", rcErr)
	}
	maxRetries, mrErr := strconv.Atoi(vals["max_retries"])
	if mrErr != nil {
		slog.Warn("mapToNotification: failed to parse max_retries, defaulting to 0", "id", vals["id"], "value", vals["max_retries"], "error", mrErr)
	}
	requeueCount, rqErr := strconv.Atoi(vals["requeue_count"])
	if rqErr != nil && vals["requeue_count"] != "" {
		slog.Warn("mapToNotification: failed to parse requeue_count, defaulting to 0", "id", vals["id"], "value", vals["requeue_count"], "error", rqErr)
	}

	createdAt, caErr := time.Parse(time.RFC3339Nano, vals["created_at"])
	if caErr != nil {
		slog.Warn("mapToNotification: failed to parse created_at, defaulting to zero time", "id", vals["id"], "value", vals["created_at"], "error", caErr)
	}
	updatedAt, uaErr := time.Parse(time.RFC3339Nano, vals["updated_at"])
	if uaErr != nil {
		slog.Warn("mapToNotification: failed to parse updated_at, defaulting to zero time", "id", vals["id"], "value", vals["updated_at"], "error", uaErr)
	}

	n := &domain.Notification{
		ID:         id,
		Recipient:  vals["recipient"],
		Channel:    domain.Channel(vals["channel"]),
		Content:    vals["content"],
		Priority:   priority,
		Status:     domain.Status(vals["status"]),
		RetryCount:   retryCount,
		MaxRetries:   maxRetries,
		RequeueCount: requeueCount,
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}

	if v, ok := vals["idempotency_key"]; ok && v != "" {
		n.IdempotencyKey = &v
	}
	if v, ok := vals["batch_id"]; ok && v != "" {
		bid, bidErr := uuid.Parse(v)
		if bidErr != nil {
			slog.Warn("mapToNotification: failed to parse batch_id", "id", vals["id"], "value", v, "error", bidErr)
		} else {
			n.BatchID = &bid
		}
	}
	if v, ok := vals["provider_msg_id"]; ok && v != "" {
		n.ProviderMsgID = &v
	}
	if v, ok := vals["next_retry_at"]; ok && v != "" {
		t, tErr := time.Parse(time.RFC3339Nano, v)
		if tErr != nil {
			slog.Warn("mapToNotification: failed to parse next_retry_at", "id", vals["id"], "value", v, "error", tErr)
		} else {
			n.NextRetryAt = &t
		}
	}
	if v, ok := vals["scheduled_at"]; ok && v != "" {
		t, tErr := time.Parse(time.RFC3339Nano, v)
		if tErr != nil {
			slog.Warn("mapToNotification: failed to parse scheduled_at", "id", vals["id"], "value", v, "error", tErr)
		} else {
			n.ScheduledAt = &t
		}
	}
	if v, ok := vals["metadata"]; ok && v != "" {
		n.Metadata = []byte(v)
	}
	if v, ok := vals["error_message"]; ok && v != "" {
		n.ErrorMessage = &v
	}

	return n, nil
}

// createScript atomically checks existence, writes the hash, and updates all indexes.
// KEYS: [1]=notification key, [2]=idx:status:<status>, [3]=idx:channel:<channel>,
//        [4]=idx:created_at, [5]=persist:queue
// Optional KEYS (positional, empty string if unused):
//        [6]=idx:idempotency:<key>, [7]=idx:batch:<batchID>, [8]=schedule:pending
// ARGV: [1]=member (id string), [2]=score (created_at nanos), [3]=idemKeyTTL seconds (0 if none),
//        [4]=scheduledAtScore (0 if none), [5]=persistEvent JSON,
//        [6..N]=field/value pairs for HSET
var createScript = redis.NewScript(`
local key = KEYS[1]

local member = ARGV[1]
local score = tonumber(ARGV[2])
local idemTTL = tonumber(ARGV[3])
local schedScore = tonumber(ARGV[4])
local persistEvt = ARGV[5]

-- Atomic idempotency check before any writes
if KEYS[6] ~= '' and idemTTL > 0 then
    local existing = redis.call('GET', KEYS[6])
    if existing then
        return 'IDEMPOTENCY_HIT:' .. existing
    end
end

local exists = redis.call('EXISTS', key)
if exists == 1 then
    return redis.error_reply('notification already exists')
end

-- Write hash fields
for i = 6, #ARGV, 2 do
    redis.call('HSET', key, ARGV[i], ARGV[i+1])
end

-- Status index
redis.call('ZADD', KEYS[2], score, member)
-- Channel index
redis.call('ZADD', KEYS[3], score, member)
-- Created_at index
redis.call('ZADD', KEYS[4], score, member)

-- Idempotency key (if provided)
if KEYS[6] ~= '' and idemTTL > 0 then
    redis.call('SET', KEYS[6], member, 'EX', idemTTL)
end

-- Batch index (if provided)
if KEYS[7] ~= '' then
    redis.call('SADD', KEYS[7], member)
end

-- Schedule index (if provided)
if KEYS[8] ~= '' and schedScore > 0 then
    redis.call('ZADD', KEYS[8], schedScore, member)
end

-- Publish persist event
redis.call('XADD', KEYS[5], 'MAXLEN', '~', '100000', '*', 'event', persistEvt)

return 1
`)

func (r *redisNotificationRepo) Create(ctx context.Context, n *domain.Notification) error {
	ctx, span := tracing.StartSpan(ctx, "redis.Create")
	defer span.End()
	tracing.SetNotificationAttrs(span, n.ID.String(), string(n.Channel), string(n.Status))

	fields := notificationToMap(n)
	score := float64(n.CreatedAt.UnixNano())
	idStr := n.ID.String()
	key := notificationKey(n.ID)

	// Build persist event JSON
	evt := PersistEvent{
		Action:       "create",
		Notification: n,
		Timestamp:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	persistData, err := json.Marshal(evt)
	if err != nil {
		slog.Error("publishPersistEvent: failed to marshal event", "error", err)
		return fmt.Errorf("marshal persist event: %w", err)
	}

	// Build KEYS
	idemKey := ""
	if n.IdempotencyKey != nil {
		idemKey = KeyIdxIdemKey + *n.IdempotencyKey
	}
	batchKey := ""
	if n.BatchID != nil {
		batchKey = KeyIdxBatch + n.BatchID.String()
	}
	scheduleKey := ""
	scheduleScore := float64(0)
	if n.ScheduledAt != nil {
		scheduleKey = KeySchedule
		scheduleScore = float64(n.ScheduledAt.UnixNano())
	}

	keys := []string{
		key,                                   // KEYS[1]
		KeyIdxStatus + string(n.Status),       // KEYS[2]
		KeyIdxChannel + string(n.Channel),     // KEYS[3]
		KeyIdxCreatedAt,                       // KEYS[4]
		KeyPersistQueue,                       // KEYS[5]
		idemKey,                               // KEYS[6]
		batchKey,                              // KEYS[7]
		scheduleKey,                           // KEYS[8]
	}

	idemTTLSeconds := int64(0)
	if n.IdempotencyKey != nil {
		idemTTLSeconds = int64(IdemKeyTTL.Seconds())
	}

	args := make([]interface{}, 0, 5+len(fields)*2)
	args = append(args, idStr, score, idemTTLSeconds, scheduleScore, string(persistData))
	for k, v := range fields {
		args = append(args, k, v)
	}

	result, err := createScript.Run(ctx, r.client, keys, args...).Result()
	if err != nil {
		tracing.RecordError(span, err)
		return fmt.Errorf("create notification: %w", err)
	}

	if str, ok := result.(string); ok && strings.HasPrefix(str, "IDEMPOTENCY_HIT:") {
		tracing.SetAttr(span, "idempotency", "hit")
		return ErrIdempotencyConflict
	}

	return nil
}

func (r *redisNotificationRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	ctx, span := tracing.StartSpan(ctx, "redis.CreateBatch")
	defer span.End()
	tracing.SetIntAttr(span, "batch.size", len(notifications))

	if err := createScript.Load(ctx, r.client).Err(); err != nil {
		slog.Warn("CreateBatch: script load failed, falling back to EVAL", "error", err)
	}

	const concurrency = 50
	sem := make(chan struct{}, concurrency)
	var mu sync.Mutex
	var firstErr error

	var wg sync.WaitGroup
	for _, n := range notifications {
		wg.Add(1)
		sem <- struct{}{}
		go func(n *domain.Notification) {
			defer wg.Done()
			defer func() { <-sem }()

			fields := notificationToMap(n)
			key := notificationKey(n.ID)
			score := float64(n.CreatedAt.UnixNano())
			idStr := n.ID.String()

			evt := PersistEvent{
				Action:       "create",
				Notification: n,
				Timestamp:    time.Now().UTC().Format(time.RFC3339Nano),
			}
			persistData, err := json.Marshal(evt)
			if err != nil {
				slog.Error("CreateBatch: failed to marshal persist event", "id", idStr, "error", err)
				return
			}

			idemKey := ""
			if n.IdempotencyKey != nil {
				idemKey = KeyIdxIdemKey + *n.IdempotencyKey
			}
			batchKey := ""
			if n.BatchID != nil {
				batchKey = KeyIdxBatch + n.BatchID.String()
			}
			scheduleKey := ""
			scheduleScore := float64(0)
			if n.ScheduledAt != nil {
				scheduleKey = KeySchedule
				scheduleScore = float64(n.ScheduledAt.UnixNano())
			}

			keys := []string{
				key,
				KeyIdxStatus + string(n.Status),
				KeyIdxChannel + string(n.Channel),
				KeyIdxCreatedAt,
				KeyPersistQueue,
				idemKey,
				batchKey,
				scheduleKey,
			}

			idemTTLSeconds := int64(0)
			if n.IdempotencyKey != nil {
				idemTTLSeconds = int64(IdemKeyTTL.Seconds())
			}

			args := make([]interface{}, 0, 5+len(fields)*2)
			args = append(args, idStr, score, idemTTLSeconds, scheduleScore, string(persistData))
			for k, v := range fields {
				args = append(args, k, v)
			}

			_, err = createScript.Run(ctx, r.client, keys, args...).Result()
			if err != nil {
				if strings.Contains(err.Error(), "notification already exists") {
					slog.Warn("CreateBatch: skipping duplicate notification", "id", idStr)
					return
				}
				if strings.Contains(err.Error(), "IDEMPOTENCY_HIT") {
					slog.Warn("CreateBatch: idempotency hit, skipping", "id", idStr)
					return
				}
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("create batch notification %s: %w", idStr, err)
				}
				mu.Unlock()
			}
		}(n)
	}
	wg.Wait()

	return firstErr
}

func (r *redisNotificationRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	ctx, span := tracing.StartSpan(ctx, "redis.GetByID")
	defer span.End()
	tracing.SetAttr(span, "notification.id", id.String())

	vals, err := r.client.HGetAll(ctx, notificationKey(id)).Result()
	if err != nil {
		tracing.RecordError(span, err)
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil
	}
	return mapToNotification(vals)
}

func (r *redisNotificationRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	ids, err := r.client.SMembers(ctx, KeyIdxBatch+batchID.String()).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	notifications := make([]*domain.Notification, 0, len(ids))
	pipe := r.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))

	for i, idStr := range ids {
		cmds[i] = pipe.HGetAll(ctx, KeyNotification+idStr)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	for _, cmd := range cmds {
		vals, err := cmd.Result()
		if err != nil || len(vals) == 0 {
			continue
		}
		n, err := mapToNotification(vals)
		if err != nil {
			continue
		}
		notifications = append(notifications, n)
	}

	return notifications, nil
}

func (r *redisNotificationRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	idStr, err := r.client.Get(ctx, KeyIdxIdemKey+key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("invalid notification id for idempotency key: %w", err)
	}

	return r.GetByID(ctx, id)
}

func (r *redisNotificationRepo) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	ctx, span := tracing.StartSpan(ctx, "redis.List")
	defer span.End()

	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	var sourceKey string
	needsIntersect := false
	filterKeys := []string{}

	if req.Status != nil {
		filterKeys = append(filterKeys, KeyIdxStatus+*req.Status)
	}
	if req.Channel != nil {
		filterKeys = append(filterKeys, KeyIdxChannel+*req.Channel)
	}

	switch len(filterKeys) {
	case 0:
		sourceKey = KeyIdxCreatedAt
	case 1:
		sourceKey = filterKeys[0]
	default:
		needsIntersect = true
		sourceKey = fmt.Sprintf("tmp:intersect:%s", uuid.New().String())
	}

	if needsIntersect {
		store := redis.ZStore{Keys: filterKeys, Aggregate: "MIN"}
		if err := r.client.ZInterStore(ctx, sourceKey, &store).Err(); err != nil {
			return nil, 0, fmt.Errorf("zinterstore: %w", err)
		}
		r.client.Expire(ctx, sourceKey, 30*time.Second)
		defer r.client.Del(ctx, sourceKey)
	}

	maxScore := "+inf"
	minScore := "-inf"

	if req.StartDate != nil {
		minScore = fmt.Sprintf("%d", req.StartDate.UnixNano())
	}
	if req.EndDate != nil {
		maxScore = fmt.Sprintf("%d", req.EndDate.UnixNano())
	}

	total, err := r.client.ZCount(ctx, sourceKey, minScore, maxScore).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("zcount: %w", err)
	}

	if req.Cursor != nil {
		cursorN, err := r.GetByID(ctx, *req.Cursor)
		if err == nil && cursorN != nil {
			cursorScore := fmt.Sprintf("(%d", cursorN.CreatedAt.UnixNano())
			maxScore = cursorScore
		}
	}

	ids, err := r.client.ZRevRangeByScore(ctx, sourceKey, &redis.ZRangeBy{
		Min:   minScore,
		Max:   maxScore,
		Count: int64(limit + 1),
	}).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("zrevrangebyscore: %w", err)
	}

	if len(ids) == 0 {
		return nil, total, nil
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, idStr := range ids {
		cmds[i] = pipe.HGetAll(ctx, KeyNotification+idStr)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, 0, fmt.Errorf("pipeline get: %w", err)
	}

	notifications := make([]*domain.Notification, 0, len(ids))
	for _, cmd := range cmds {
		vals, err := cmd.Result()
		if err != nil || len(vals) == 0 {
			continue
		}
		n, err := mapToNotification(vals)
		if err != nil {
			continue
		}
		notifications = append(notifications, n)
	}

	return notifications, total, nil
}

// updateStatusScript atomically checks current status, updates hash, and moves index entry.
// KEYS: [1]=notification key, [2]=idx:status:old, [3]=idx:status:new, [4]=idx:created_at
// ARGV: [1]=from status, [2]=to status, [3]=now timestamp, [4]=member (id string)
var updateStatusScript = redis.NewScript(`
local key = KEYS[1]
local idxOld = KEYS[2]
local idxNew = KEYS[3]
local idxCreatedAt = KEYS[4]
local from = ARGV[1]
local to = ARGV[2]
local now = ARGV[3]
local member = ARGV[4]
local current = redis.call('HGET', key, 'status')
if current ~= from then
    return 0
end
redis.call('HSET', key, 'status', to, 'updated_at', now)
-- Get score from the created_at index (always present)
local score = redis.call('ZSCORE', idxCreatedAt, member)
if not score then
    score = 0
end
redis.call('ZREM', idxOld, member)
redis.call('ZADD', idxNew, score, member)
return 1
`)

func (r *redisNotificationRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
	ctx, span := tracing.StartSpan(ctx, "redis.UpdateStatus")
	defer span.End()
	tracing.SetAttr(span, "notification.id", id.String())
	tracing.SetAttr(span, "status.from", string(from))
	tracing.SetAttr(span, "status.to", string(to))

	now := time.Now().UTC().Format(time.RFC3339Nano)
	key := notificationKey(id)
	idStr := id.String()

	idxOld := KeyIdxStatus + string(from)
	idxNew := KeyIdxStatus + string(to)

	result, err := updateStatusScript.Run(ctx, r.client,
		[]string{key, idxOld, idxNew, KeyIdxCreatedAt},
		string(from), string(to), now, idStr,
	).Int64()
	if err != nil {
		tracing.RecordError(span, err)
		return false, err
	}
	if result == 0 {
		return false, nil
	}

	pipe := r.client.Pipeline()
	r.publishPersistEvent(ctx, pipe, "update_status", nil, map[string]string{
		"id":   idStr,
		"from": string(from),
		"to":   string(to),
	})

	if _, err := pipe.Exec(ctx); err != nil {
		return true, fmt.Errorf("update status persist event: %w", err)
	}

	return true, nil
}

// updateStatusWithDetailsScript atomically checks status, updates hash with details, and moves index.
// KEYS: [1]=notification key, [2]=idx:status:old, [3]=idx:status:new, [4]=idx:created_at
// ARGV: [1]=from, [2]=to, [3]=now, [4]=pmid, [5]=emsg, [6]=member
var updateStatusWithDetailsScript = redis.NewScript(`
local key = KEYS[1]
local idxOld = KEYS[2]
local idxNew = KEYS[3]
local idxCreatedAt = KEYS[4]
local from = ARGV[1]
local to = ARGV[2]
local now = ARGV[3]
local pmid = ARGV[4]
local emsg = ARGV[5]
local member = ARGV[6]
local current = redis.call('HGET', key, 'status')
if current ~= from then
    return 0
end
redis.call('HSET', key, 'status', to, 'updated_at', now)
if pmid ~= '' then
    redis.call('HSET', key, 'provider_msg_id', pmid)
end
if emsg ~= '' then
    redis.call('HSET', key, 'error_message', emsg)
end
-- Get score from the created_at index
local score = redis.call('ZSCORE', idxCreatedAt, member)
if not score then
    score = 0
end
redis.call('ZREM', idxOld, member)
redis.call('ZADD', idxNew, score, member)
return 1
`)

func (r *redisNotificationRepo) UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
	ctx, span := tracing.StartSpan(ctx, "redis.UpdateStatusWithDetails")
	defer span.End()
	tracing.SetAttr(span, "notification.id", id.String())
	tracing.SetAttr(span, "status.from", string(from))
	tracing.SetAttr(span, "status.to", string(to))

	now := time.Now().UTC().Format(time.RFC3339Nano)
	key := notificationKey(id)
	idStr := id.String()

	pmid := ""
	if providerMsgID != nil {
		pmid = *providerMsgID
	}
	emsg := ""
	if errorMsg != nil {
		emsg = *errorMsg
	}

	idxOld := KeyIdxStatus + string(from)
	idxNew := KeyIdxStatus + string(to)

	result, err := updateStatusWithDetailsScript.Run(ctx, r.client,
		[]string{key, idxOld, idxNew, KeyIdxCreatedAt},
		string(from), string(to), now, pmid, emsg, idStr,
	).Int64()
	if err != nil {
		tracing.RecordError(span, err)
		return false, err
	}
	if result == 0 {
		return false, nil
	}

	pipe := r.client.Pipeline()
	extra := map[string]string{
		"id":   idStr,
		"from": string(from),
		"to":   string(to),
	}
	if pmid != "" {
		extra["provider_msg_id"] = pmid
	}
	if emsg != "" {
		extra["error_message"] = emsg
	}
	r.publishPersistEvent(ctx, pipe, "update_status_details", nil, extra)

	if _, err := pipe.Exec(ctx); err != nil {
		return true, fmt.Errorf("update status details persist event: %w", err)
	}

	return true, nil
}

// incrementRetryScript atomically increments retry, updates status indexes, and adds to retry index.
// KEYS: [1]=notification key, [2]=idx:status:processing, [3]=idx:status:failed,
//        [4]=idx:created_at, [5]=idx:retry, [6]=persist:queue
// ARGV: [1]=member, [2]=now, [3]=nextRetryAt, [4]=errorMsg, [5]=persistEvent JSON, [6]=retryScore (nanos)
var incrementRetryScript = redis.NewScript(`
local key = KEYS[1]
local idxProcessing = KEYS[2]
local idxFailed = KEYS[3]
local idxCreatedAt = KEYS[4]
local idxRetry = KEYS[5]
local persistQueue = KEYS[6]
local member = ARGV[1]
local now = ARGV[2]
local nextRetryAt = ARGV[3]
local errorMsg = ARGV[4]
local persistEvt = ARGV[5]

redis.call('HINCRBY', key, 'retry_count', 1)
redis.call('HSET', key, 'next_retry_at', nextRetryAt, 'error_message', errorMsg, 'status', 'failed', 'updated_at', now)

-- Get score from the created_at index
local score = redis.call('ZSCORE', idxCreatedAt, member)
if not score then
    score = 0
end

redis.call('ZREM', idxProcessing, member)
redis.call('ZADD', idxFailed, score, member)

-- Add to retry index with nextRetryAt as score
local retryScore = tonumber(ARGV[6])
redis.call('ZADD', idxRetry, retryScore, member)

-- Publish persist event
redis.call('XADD', persistQueue, 'MAXLEN', '~', '100000', '*', 'event', persistEvt)

return 1
`)

func (r *redisNotificationRepo) IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	ctx, span := tracing.StartSpan(ctx, "redis.IncrementRetry")
	defer span.End()
	tracing.SetAttr(span, "notification.id", id.String())

	key := notificationKey(id)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	idStr := id.String()

	evt := PersistEvent{
		Action: "increment_retry",
		Extra: map[string]string{
			"id":            idStr,
			"next_retry_at": nextRetryAt.Format(time.RFC3339Nano),
			"error_message": errorMsg,
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	persistData, err := json.Marshal(evt)
	if err != nil {
		slog.Error("IncrementRetry: failed to marshal persist event", "error", err)
		return fmt.Errorf("marshal persist event: %w", err)
	}

	keys := []string{
		key,
		KeyIdxStatus + string(domain.StatusProcessing),
		KeyIdxStatus + string(domain.StatusFailed),
		KeyIdxCreatedAt,
		KeyIdxRetry,
		KeyPersistQueue,
	}

	_, err = incrementRetryScript.Run(ctx, r.client, keys,
		idStr, now, nextRetryAt.Format(time.RFC3339Nano), errorMsg, string(persistData),
		float64(nextRetryAt.UnixNano()),
	).Result()
	if err != nil {
		tracing.RecordError(span, err)
		return fmt.Errorf("increment retry: %w", err)
	}

	return nil
}

// moveToDLQScript atomically updates notification status, creates DLQ entry,
// moves status indexes, and publishes persist event.
// KEYS: [1]=notification key, [2]=dlq key, [3]=idx:status:<from>, [4]=idx:status:failed, [5]=idx:created_at, [6]=persist:queue
// ARGV: [1]=member, [2]=now, [3]=errorMsg, [4]=dlqID, [5]=channel, [6]=recipient, [7]=content, [8]=retryCount, [9]=persistEvt
var moveToDLQScript = redis.NewScript(`
local nKey = KEYS[1]
local dlqKey = KEYS[2]
local fromIdx = KEYS[3]
local failedIdx = KEYS[4]
local idxCreatedAt = KEYS[5]
local persistQueue = KEYS[6]
local member = ARGV[1]
local now = ARGV[2]
local errorMsg = ARGV[3]
local dlqID = ARGV[4]
local channel = ARGV[5]
local recipient = ARGV[6]
local content = ARGV[7]
local retryCount = ARGV[8]
local persistEvt = ARGV[9]

redis.call('HSET', nKey, 'status', 'failed', 'error_message', errorMsg, 'updated_at', now)

redis.call('HSET', dlqKey, 'id', dlqID, 'notification_id', member, 'channel', channel,
    'recipient', recipient, 'content', content, 'error_message', errorMsg,
    'retry_count', retryCount, 'failed_at', now, 'reprocessed', 'false')

local score = redis.call('ZSCORE', idxCreatedAt, member)
if not score then
    score = 0
end

redis.call('ZREM', fromIdx, member)
redis.call('ZADD', failedIdx, score, member)

redis.call('XADD', persistQueue, 'MAXLEN', '~', '100000', '*', 'event', persistEvt)

return 1
`)

func (r *redisNotificationRepo) MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error {
	ctx, span := tracing.StartSpan(ctx, "redis.MoveToDLQ")
	defer span.End()
	tracing.SetNotificationAttrs(span, n.ID.String(), string(n.Channel), string(n.Status))

	now := time.Now().UTC()
	idStr := n.ID.String()
	nowStr := now.Format(time.RFC3339Nano)
	dlqID := uuid.New().String()

	evt := PersistEvent{
		Action:       "move_to_dlq",
		Notification: n,
		Extra:        map[string]string{"error_message": errorMsg},
		Timestamp:    nowStr,
	}
	persistData, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal persist event: %w", err)
	}

	keys := []string{
		notificationKey(n.ID),
		KeyDLQ + idStr,
		KeyIdxStatus + string(n.Status),
		KeyIdxStatus + string(domain.StatusFailed),
		KeyIdxCreatedAt,
		KeyPersistQueue,
	}

	_, err = moveToDLQScript.Run(ctx, r.client, keys,
		idStr, nowStr, errorMsg, dlqID,
		string(n.Channel), n.Recipient, n.Content,
		strconv.Itoa(n.RetryCount), string(persistData),
	).Result()
	if err != nil {
		tracing.RecordError(span, err)
		return fmt.Errorf("move to dlq: %w", err)
	}

	return nil
}

func (r *redisNotificationRepo) GetScheduledReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	now := float64(time.Now().UTC().UnixNano())

	ids, err := r.client.ZRangeByScore(ctx, KeySchedule, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", int64(now)),
		Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}

	return r.getNotificationsByIDs(ctx, ids)
}

var claimScheduledScript = redis.NewScript(`
local scheduleKey = KEYS[1]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local nowStr = ARGV[3]

local scheduled = redis.call('ZRANGEBYSCORE', scheduleKey, '-inf', now, 'LIMIT', 0, limit)

local claimed = {}
for _, id in ipairs(scheduled) do
    local nKey = 'notification:' .. id
    local status = redis.call('HGET', nKey, 'status')
    if status == 'pending' then
        redis.call('HSET', nKey, 'status', 'queued', 'updated_at', nowStr)
        redis.call('ZREM', scheduleKey, id)
        table.insert(claimed, id)
    end
end

return claimed
`)

func (r *redisNotificationRepo) ClaimScheduledBatch(ctx context.Context, limit int) ([]*domain.Notification, error) {
	now := time.Now().UTC()
	nowNano := now.UnixNano()
	nowStr := now.Format(time.RFC3339Nano)

	ids, err := claimScheduledScript.Run(ctx, r.client, []string{KeySchedule},
		nowNano, limit, nowStr,
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("claim scheduled: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	notifications, err := r.getNotificationsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}

	pipe := r.client.Pipeline()
	for _, n := range notifications {
		idStr := n.ID.String()
		score := float64(n.CreatedAt.UnixNano())
		pipe.ZRem(ctx, KeyIdxStatus+string(domain.StatusPending), idStr)
		pipe.ZAdd(ctx, KeyIdxStatus+string(domain.StatusQueued), redis.Z{Score: score, Member: idStr})

		r.publishPersistEvent(ctx, pipe, "update_status", nil, map[string]string{
			"id":   idStr,
			"from": string(domain.StatusPending),
			"to":   string(domain.StatusQueued),
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("claim scheduled indexes: %w", err)
	}

	return notifications, nil
}

// recoverStuckScript atomically finds stuck notifications, updates their status,
// and moves them in the status indexes. Filters by updated_at to avoid
// recovering recently re-queued notifications whose created_at is old.
// KEYS: [1]=idx:status:<from>, [2]=idx:status:<to>, [3]=idx:created_at, [4]=persist:queue
// ARGV: [1]=cutoffScore (created_at pre-filter), [2]=limit, [3]=now, [4]=fromStatus, [5]=toStatus, [6]=cutoffStr (RFC3339Nano)
var recoverStuckScript = redis.NewScript(`
local fromKey = KEYS[1]
local toKey = KEYS[2]
local idxCreatedAt = KEYS[3]
local persistQueue = KEYS[4]
local cutoffScore = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = ARGV[3]
local fromStatus = ARGV[4]
local toStatus = ARGV[5]
local cutoffStr = ARGV[6]

local candidates = redis.call('ZRANGEBYSCORE', fromKey, '-inf', cutoffScore, 'LIMIT', 0, limit)

local recovered = {}
for _, id in ipairs(candidates) do
    local nKey = 'notification:' .. id
    local updatedAt = redis.call('HGET', nKey, 'updated_at')
    if updatedAt and updatedAt < cutoffStr then
        redis.call('HSET', nKey, 'status', toStatus, 'updated_at', now)

        local score = redis.call('ZSCORE', idxCreatedAt, id)
        if not score then
            score = 0
        end

        redis.call('ZREM', fromKey, id)
        redis.call('ZADD', toKey, score, id)

        local evt = cjson.encode({action='update_status', extra={id=id, from=fromStatus, to=toStatus}, timestamp=now})
        redis.call('XADD', persistQueue, 'MAXLEN', '~', '100000', '*', 'event', evt)

        table.insert(recovered, id)
    end
end

return recovered
`)

func (r *redisNotificationRepo) RecoverStuckQueued(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	now := time.Now().UTC()
	cutoff := now.Add(-stuckThreshold)
	cutoffScore := float64(cutoff.UnixNano())
	nowStr := now.Format(time.RFC3339Nano)
	cutoffStr := cutoff.Format(time.RFC3339Nano)

	ids, err := recoverStuckScript.Run(ctx, r.client,
		[]string{
			KeyIdxStatus + string(domain.StatusQueued),
			KeyIdxStatus + string(domain.StatusPending),
			KeyIdxCreatedAt,
			KeyPersistQueue,
		},
		cutoffScore, limit, nowStr,
		string(domain.StatusQueued), string(domain.StatusPending),
		cutoffStr,
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("recover stuck: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	return r.getNotificationsByIDs(ctx, ids)
}

// getRetryReadyScript atomically claims retry-ready notifications with CAS check.
// Prevents race conditions when multiple scheduler instances run concurrently.
// KEYS: [1]=idx:retry, [2]=idx:status:failed, [3]=idx:status:queued, [4]=idx:created_at, [5]=persist:queue
// ARGV: [1]=nowNano, [2]=limit, [3]=now (RFC3339Nano)
var getRetryReadyScript = redis.NewScript(`
local retryKey = KEYS[1]
local failedIdx = KEYS[2]
local queuedIdx = KEYS[3]
local idxCreatedAt = KEYS[4]
local persistQueue = KEYS[5]
local nowNano = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = ARGV[3]

local ready = redis.call('ZRANGEBYSCORE', retryKey, '-inf', nowNano, 'LIMIT', 0, limit)

local claimed = {}
for _, id in ipairs(ready) do
    local nKey = 'notification:' .. id
    local status = redis.call('HGET', nKey, 'status')
    if status == 'failed' then
        redis.call('HSET', nKey, 'status', 'queued', 'updated_at', now)
        redis.call('ZREM', retryKey, id)

        local score = redis.call('ZSCORE', idxCreatedAt, id)
        if not score then
            score = 0
        end

        redis.call('ZREM', failedIdx, id)
        redis.call('ZADD', queuedIdx, score, id)

        local evt = cjson.encode({action='update_status', extra={id=id, from='failed', to='queued'}, timestamp=now})
        redis.call('XADD', persistQueue, 'MAXLEN', '~', '100000', '*', 'event', evt)

        table.insert(claimed, id)
    end
end

return claimed
`)

func (r *redisNotificationRepo) GetRetryReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	now := time.Now().UTC()
	nowNano := now.UnixNano()
	nowStr := now.Format(time.RFC3339Nano)

	ids, err := getRetryReadyScript.Run(ctx, r.client,
		[]string{
			KeyIdxRetry,
			KeyIdxStatus + string(domain.StatusFailed),
			KeyIdxStatus + string(domain.StatusQueued),
			KeyIdxCreatedAt,
			KeyPersistQueue,
		},
		nowNano, limit, nowStr,
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("get retry ready: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	return r.getNotificationsByIDs(ctx, ids)
}

// recoverStuckProcessingScript reuses the same pattern as recoverStuckScript.
// KEYS: [1]=idx:status:<from>, [2]=idx:status:<to>, [3]=idx:created_at, [4]=persist:queue
// ARGV: [1]=cutoffScore, [2]=limit, [3]=now, [4]=fromStatus, [5]=toStatus, [6]=cutoffStr (RFC3339Nano)
var recoverStuckProcessingScript = redis.NewScript(`
local fromKey = KEYS[1]
local toKey = KEYS[2]
local idxCreatedAt = KEYS[3]
local persistQueue = KEYS[4]
local cutoffScore = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = ARGV[3]
local fromStatus = ARGV[4]
local toStatus = ARGV[5]
local cutoffStr = ARGV[6]

local candidates = redis.call('ZRANGEBYSCORE', fromKey, '-inf', cutoffScore, 'LIMIT', 0, limit)

local recovered = {}
for _, id in ipairs(candidates) do
    local nKey = 'notification:' .. id
    local updatedAt = redis.call('HGET', nKey, 'updated_at')
    if updatedAt and updatedAt < cutoffStr then
        redis.call('HSET', nKey, 'status', toStatus, 'updated_at', now)

        local score = redis.call('ZSCORE', idxCreatedAt, id)
        if not score then
            score = 0
        end

        redis.call('ZREM', fromKey, id)
        redis.call('ZADD', toKey, score, id)

        local evt = cjson.encode({action='update_status', extra={id=id, from=fromStatus, to=toStatus}, timestamp=now})
        redis.call('XADD', persistQueue, 'MAXLEN', '~', '100000', '*', 'event', evt)

        table.insert(recovered, id)
    end
end

return recovered
`)

func (r *redisNotificationRepo) RecoverStuckProcessing(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	now := time.Now().UTC()
	cutoff := now.Add(-stuckThreshold)
	cutoffScore := float64(cutoff.UnixNano())
	nowStr := now.Format(time.RFC3339Nano)
	cutoffStr := cutoff.Format(time.RFC3339Nano)

	ids, err := recoverStuckProcessingScript.Run(ctx, r.client,
		[]string{
			KeyIdxStatus + string(domain.StatusProcessing),
			KeyIdxStatus + string(domain.StatusQueued),
			KeyIdxCreatedAt,
			KeyPersistQueue,
		},
		cutoffScore, limit, nowStr,
		string(domain.StatusProcessing), string(domain.StatusQueued),
		cutoffStr,
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("recover stuck processing: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	return r.getNotificationsByIDs(ctx, ids)
}

// recoverOrphanedPendingScript atomically finds stale pending notifications (not scheduled),
// updates their status, and moves them in status indexes.
// Filters by updated_at to avoid recovering recently created notifications.
// KEYS: [1]=idx:status:pending, [2]=schedule:pending, [3]=idx:status:queued, [4]=idx:created_at, [5]=persist:queue
// ARGV: [1]=cutoffScore, [2]=limit, [3]=now, [4]=cutoffStr (RFC3339Nano)
var recoverOrphanedPendingScript = redis.NewScript(`
local pendingKey = KEYS[1]
local scheduleKey = KEYS[2]
local queuedKey = KEYS[3]
local idxCreatedAt = KEYS[4]
local persistQueue = KEYS[5]
local cutoffScore = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = ARGV[3]
local cutoffStr = ARGV[4]

local candidates = redis.call('ZRANGEBYSCORE', pendingKey, '-inf', cutoffScore, 'LIMIT', 0, limit)

local recovered = {}
for _, id in ipairs(candidates) do
    local inSchedule = redis.call('ZSCORE', scheduleKey, id)
    if not inSchedule then
        local nKey = 'notification:' .. id
        local updatedAt = redis.call('HGET', nKey, 'updated_at')
        if updatedAt and updatedAt < cutoffStr then
            redis.call('HSET', nKey, 'status', 'queued', 'updated_at', now)

            local score = redis.call('ZSCORE', idxCreatedAt, id)
            if not score then
                score = 0
            end

            redis.call('ZREM', pendingKey, id)
            redis.call('ZADD', queuedKey, score, id)

            local evt = cjson.encode({action='update_status', extra={id=id, from='pending', to='queued'}, timestamp=now})
            redis.call('XADD', persistQueue, 'MAXLEN', '~', '100000', '*', 'event', evt)

            table.insert(recovered, id)
        end
    end
end

return recovered
`)

func (r *redisNotificationRepo) RecoverOrphanedPending(ctx context.Context, staleDuration time.Duration, limit int) ([]*domain.Notification, error) {
	now := time.Now().UTC()
	cutoff := now.Add(-staleDuration)
	cutoffScore := float64(cutoff.UnixNano())
	nowStr := now.Format(time.RFC3339Nano)
	cutoffStr := cutoff.Format(time.RFC3339Nano)

	ids, err := recoverOrphanedPendingScript.Run(ctx, r.client,
		[]string{
			KeyIdxStatus + string(domain.StatusPending),
			KeySchedule,
			KeyIdxStatus + string(domain.StatusQueued),
			KeyIdxCreatedAt,
			KeyPersistQueue,
		},
		cutoffScore, limit, nowStr, cutoffStr,
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("recover orphaned pending: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	return r.getNotificationsByIDs(ctx, ids)
}

func (r *redisNotificationRepo) getNotificationsByIDs(ctx context.Context, ids []string) ([]*domain.Notification, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, idStr := range ids {
		cmds[i] = pipe.HGetAll(ctx, KeyNotification+idStr)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	notifications := make([]*domain.Notification, 0, len(ids))
	for _, cmd := range cmds {
		vals, err := cmd.Result()
		if err != nil || len(vals) == 0 {
			continue
		}
		n, err := mapToNotification(vals)
		if err != nil {
			continue
		}
		notifications = append(notifications, n)
	}

	return notifications, nil
}

type PersistEvent struct {
	Action       string               `json:"action"`
	Notification *domain.Notification `json:"notification,omitempty"`
	Extra        map[string]string    `json:"extra,omitempty"`
	Timestamp    string               `json:"timestamp"`
}

func (r *redisNotificationRepo) publishPersistEvent(ctx context.Context, pipe redis.Pipeliner, action string, n *domain.Notification, extra map[string]string) {
	evt := PersistEvent{
		Action:       action,
		Notification: n,
		Extra:        extra,
		Timestamp:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		slog.Error("publishPersistEvent: failed to marshal event", "action", action, "error", err)
		return
	}

	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: KeyPersistQueue,
		MaxLen: 100000,
		Approx: true,
		Values: map[string]interface{}{
			"event": string(data),
		},
	})
}

func (r *redisNotificationRepo) UpdateRequeueCount(ctx context.Context, id uuid.UUID, count int) error {
	key := notificationKey(id)
	return r.client.HSet(ctx, key,
		"requeue_count", strconv.Itoa(count),
		"updated_at", time.Now().UTC().Format(time.RFC3339Nano),
	).Err()
}

func ParsePersistEvent(values map[string]interface{}) (*PersistEvent, error) {
	eventStr, ok := values["event"].(string)
	if !ok {
		return nil, fmt.Errorf("missing event field")
	}

	var evt PersistEvent
	if err := json.Unmarshal([]byte(eventStr), &evt); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w", err)
	}

	return &evt, nil
}

func SplitPersistActions(events []*PersistEvent) (creates []*domain.Notification, updates []map[string]string) {
	for _, evt := range events {
		switch {
		case evt.Action == "create" && evt.Notification != nil:
			creates = append(creates, evt.Notification)
		case strings.HasPrefix(evt.Action, "update_status") || evt.Action == "increment_retry" || evt.Action == "move_to_dlq":
			if evt.Extra["id"] == "" {
				continue
			}
			m := map[string]string{"action": evt.Action}
			for k, v := range evt.Extra {
				m[k] = v
			}
			updates = append(updates, m)
		}
	}
	return
}
