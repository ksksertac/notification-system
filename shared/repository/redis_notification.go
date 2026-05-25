package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

const (
	KeyNotification = "notification:"
	KeyIdxStatus    = "idx:status:"
	KeyIdxChannel   = "idx:channel:"
	KeyIdxCreatedAt = "idx:created_at"
	KeyIdxBatch     = "idx:batch:"
	KeyIdxIdemKey   = "idx:idempotency:"
	KeySchedule     = "schedule:pending"
	KeyPersistQueue = "persist:queue"
	KeyDLQ          = "dlq:"

	IdemKeyTTL = 24 * time.Hour
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
		"max_retries": strconv.Itoa(n.MaxRetries),
		"created_at":  n.CreatedAt.Format(time.RFC3339Nano),
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

	priority, _ := domain.PriorityFromString(vals["priority"])
	retryCount, _ := strconv.Atoi(vals["retry_count"])
	maxRetries, _ := strconv.Atoi(vals["max_retries"])

	createdAt, _ := time.Parse(time.RFC3339Nano, vals["created_at"])
	updatedAt, _ := time.Parse(time.RFC3339Nano, vals["updated_at"])

	n := &domain.Notification{
		ID:         id,
		Recipient:  vals["recipient"],
		Channel:    domain.Channel(vals["channel"]),
		Content:    vals["content"],
		Priority:   priority,
		Status:     domain.Status(vals["status"]),
		RetryCount: retryCount,
		MaxRetries: maxRetries,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}

	if v, ok := vals["idempotency_key"]; ok && v != "" {
		n.IdempotencyKey = &v
	}
	if v, ok := vals["batch_id"]; ok && v != "" {
		bid, _ := uuid.Parse(v)
		n.BatchID = &bid
	}
	if v, ok := vals["provider_msg_id"]; ok && v != "" {
		n.ProviderMsgID = &v
	}
	if v, ok := vals["next_retry_at"]; ok && v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v)
		n.NextRetryAt = &t
	}
	if v, ok := vals["scheduled_at"]; ok && v != "" {
		t, _ := time.Parse(time.RFC3339Nano, v)
		n.ScheduledAt = &t
	}
	if v, ok := vals["metadata"]; ok && v != "" {
		n.Metadata = []byte(v)
	}
	if v, ok := vals["error_message"]; ok && v != "" {
		n.ErrorMessage = &v
	}

	return n, nil
}

var createScript = redis.NewScript(`
local key = KEYS[1]
local exists = redis.call('EXISTS', key)
if exists == 1 then
    return redis.error_reply('notification already exists')
end
for i = 1, #ARGV, 2 do
    redis.call('HSET', key, ARGV[i], ARGV[i+1])
end
return 1
`)

func (r *redisNotificationRepo) Create(ctx context.Context, n *domain.Notification) error {
	fields := notificationToMap(n)
	score := float64(n.CreatedAt.UnixNano())
	idStr := n.ID.String()
	key := notificationKey(n.ID)

	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}

	pipe := r.client.Pipeline()

	createScript.Run(ctx, r.client, []string{key}, args...)

	pipe.ZAdd(ctx, KeyIdxStatus+string(n.Status), redis.Z{Score: score, Member: idStr})
	pipe.ZAdd(ctx, KeyIdxChannel+string(n.Channel), redis.Z{Score: score, Member: idStr})
	pipe.ZAdd(ctx, KeyIdxCreatedAt, redis.Z{Score: score, Member: idStr})

	if n.IdempotencyKey != nil {
		pipe.Set(ctx, KeyIdxIdemKey+*n.IdempotencyKey, idStr, IdemKeyTTL)
	}
	if n.BatchID != nil {
		pipe.SAdd(ctx, KeyIdxBatch+n.BatchID.String(), idStr)
	}
	if n.ScheduledAt != nil {
		pipe.ZAdd(ctx, KeySchedule, redis.Z{Score: float64(n.ScheduledAt.UnixNano()), Member: idStr})
	}

	r.publishPersistEvent(ctx, pipe, "create", n, nil)

	_, err := pipe.Exec(ctx)
	return err
}

func (r *redisNotificationRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	pipe := r.client.Pipeline()

	for _, n := range notifications {
		fields := notificationToMap(n)
		key := notificationKey(n.ID)
		score := float64(n.CreatedAt.UnixNano())
		idStr := n.ID.String()

		fieldPairs := make([]interface{}, 0, len(fields)*2)
		for k, v := range fields {
			fieldPairs = append(fieldPairs, k, v)
		}
		pipe.HSet(ctx, key, fieldPairs...)

		pipe.ZAdd(ctx, KeyIdxStatus+string(n.Status), redis.Z{Score: score, Member: idStr})
		pipe.ZAdd(ctx, KeyIdxChannel+string(n.Channel), redis.Z{Score: score, Member: idStr})
		pipe.ZAdd(ctx, KeyIdxCreatedAt, redis.Z{Score: score, Member: idStr})

		if n.IdempotencyKey != nil {
			pipe.Set(ctx, KeyIdxIdemKey+*n.IdempotencyKey, idStr, IdemKeyTTL)
		}
		if n.BatchID != nil {
			pipe.SAdd(ctx, KeyIdxBatch+n.BatchID.String(), idStr)
		}
		if n.ScheduledAt != nil {
			pipe.ZAdd(ctx, KeySchedule, redis.Z{Score: float64(n.ScheduledAt.UnixNano()), Member: idStr})
		}

		r.publishPersistEvent(ctx, pipe, "create", n, nil)
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (r *redisNotificationRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	vals, err := r.client.HGetAll(ctx, notificationKey(id)).Result()
	if err != nil {
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
		Count: int64(limit),
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

var updateStatusScript = redis.NewScript(`
local key = KEYS[1]
local from = ARGV[1]
local to = ARGV[2]
local now = ARGV[3]
local current = redis.call('HGET', key, 'status')
if current ~= from then
    return 0
end
redis.call('HSET', key, 'status', to, 'updated_at', now)
return 1
`)

func (r *redisNotificationRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	key := notificationKey(id)
	idStr := id.String()

	result, err := updateStatusScript.Run(ctx, r.client, []string{key}, string(from), string(to), now).Int64()
	if err != nil {
		return false, err
	}
	if result == 0 {
		return false, nil
	}

	pipe := r.client.Pipeline()
	pipe.ZRem(ctx, KeyIdxStatus+string(from), idStr)
	n, _ := r.GetByID(ctx, id)
	score := float64(0)
	if n != nil {
		score = float64(n.CreatedAt.UnixNano())
	}
	pipe.ZAdd(ctx, KeyIdxStatus+string(to), redis.Z{Score: score, Member: idStr})

	r.publishPersistEvent(ctx, pipe, "update_status", nil, map[string]string{
		"id":   idStr,
		"from": string(from),
		"to":   string(to),
	})

	pipe.Exec(ctx)

	return true, nil
}

var updateStatusWithDetailsScript = redis.NewScript(`
local key = KEYS[1]
local from = ARGV[1]
local to = ARGV[2]
local now = ARGV[3]
local current = redis.call('HGET', key, 'status')
if current ~= from then
    return 0
end
redis.call('HSET', key, 'status', to, 'updated_at', now)
if ARGV[4] ~= '' then
    redis.call('HSET', key, 'provider_msg_id', ARGV[4])
end
if ARGV[5] ~= '' then
    redis.call('HSET', key, 'error_message', ARGV[5])
end
return 1
`)

func (r *redisNotificationRepo) UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
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

	result, err := updateStatusWithDetailsScript.Run(ctx, r.client, []string{key},
		string(from), string(to), now, pmid, emsg,
	).Int64()
	if err != nil {
		return false, err
	}
	if result == 0 {
		return false, nil
	}

	pipe := r.client.Pipeline()
	pipe.ZRem(ctx, KeyIdxStatus+string(from), idStr)
	n, _ := r.GetByID(ctx, id)
	score := float64(0)
	if n != nil {
		score = float64(n.CreatedAt.UnixNano())
	}
	pipe.ZAdd(ctx, KeyIdxStatus+string(to), redis.Z{Score: score, Member: idStr})

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

	pipe.Exec(ctx)

	return true, nil
}

func (r *redisNotificationRepo) IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	key := notificationKey(id)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	pipe := r.client.Pipeline()
	pipe.HIncrBy(ctx, key, "retry_count", 1)
	pipe.HSet(ctx, key, map[string]interface{}{
		"next_retry_at": nextRetryAt.Format(time.RFC3339Nano),
		"error_message": errorMsg,
		"status":        string(domain.StatusFailed),
		"updated_at":    now,
	})

	idStr := id.String()
	pipe.ZRem(ctx, KeyIdxStatus+string(domain.StatusProcessing), idStr)
	n, _ := r.GetByID(ctx, id)
	score := float64(0)
	if n != nil {
		score = float64(n.CreatedAt.UnixNano())
	}
	pipe.ZAdd(ctx, KeyIdxStatus+string(domain.StatusFailed), redis.Z{Score: score, Member: idStr})

	r.publishPersistEvent(ctx, pipe, "increment_retry", nil, map[string]string{
		"id":            idStr,
		"next_retry_at": nextRetryAt.Format(time.RFC3339Nano),
		"error_message": errorMsg,
	})

	_, err := pipe.Exec(ctx)
	return err
}

func (r *redisNotificationRepo) MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error {
	now := time.Now().UTC()
	idStr := n.ID.String()

	dlqEntry := map[string]interface{}{
		"id":              uuid.New().String(),
		"notification_id": idStr,
		"channel":         string(n.Channel),
		"recipient":       n.Recipient,
		"content":         n.Content,
		"error_message":   errorMsg,
		"retry_count":     strconv.Itoa(n.RetryCount),
		"failed_at":       now.Format(time.RFC3339Nano),
		"reprocessed":     "false",
	}

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, KeyDLQ+idStr, dlqEntry)
	pipe.HSet(ctx, notificationKey(n.ID), map[string]interface{}{
		"status":        string(domain.StatusFailed),
		"error_message": errorMsg,
		"updated_at":    now.Format(time.RFC3339Nano),
	})

	pipe.ZRem(ctx, KeyIdxStatus+string(n.Status), idStr)
	pipe.ZAdd(ctx, KeyIdxStatus+string(domain.StatusFailed), redis.Z{
		Score:  float64(n.CreatedAt.UnixNano()),
		Member: idStr,
	})

	r.publishPersistEvent(ctx, pipe, "move_to_dlq", n, map[string]string{
		"error_message": errorMsg,
	})

	_, err := pipe.Exec(ctx)
	return err
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
local orphanThreshold = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

local scheduled = redis.call('ZRANGEBYSCORE', scheduleKey, '-inf', now, 'LIMIT', 0, limit)

local claimed = {}
for _, id in ipairs(scheduled) do
    local nKey = 'notification:' .. id
    local status = redis.call('HGET', nKey, 'status')
    if status == 'pending' then
        redis.call('HSET', nKey, 'status', 'queued', 'updated_at', ARGV[4])
        redis.call('ZREM', scheduleKey, id)
        table.insert(claimed, id)
    end
end

return claimed
`)

func (r *redisNotificationRepo) ClaimScheduledBatch(ctx context.Context, limit int) ([]*domain.Notification, error) {
	now := time.Now().UTC()
	nowNano := now.UnixNano()
	orphanThreshold := now.Add(-30 * time.Second).UnixNano()
	nowStr := now.Format(time.RFC3339Nano)

	ids, err := claimScheduledScript.Run(ctx, r.client, []string{KeySchedule},
		nowNano, orphanThreshold, limit, nowStr,
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("claim scheduled: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	for _, idStr := range ids {
		pipe.ZRem(ctx, KeyIdxStatus+string(domain.StatusPending), idStr)
		pipe.ZAdd(ctx, KeyIdxStatus+string(domain.StatusQueued), redis.Z{Score: float64(nowNano), Member: idStr})

		r.publishPersistEvent(ctx, pipe, "update_status", nil, map[string]string{
			"id":   idStr,
			"from": string(domain.StatusPending),
			"to":   string(domain.StatusQueued),
		})
	}
	pipe.Exec(ctx)

	return r.getNotificationsByIDs(ctx, ids)
}

var recoverStuckScript = redis.NewScript(`
local statusKey = KEYS[1]
local cutoffScore = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = ARGV[3]

local stuck = redis.call('ZRANGEBYSCORE', statusKey, '-inf', cutoffScore, 'LIMIT', 0, limit)

local recovered = {}
for _, id in ipairs(stuck) do
    local nKey = 'notification:' .. id
    local updatedAt = redis.call('HGET', nKey, 'updated_at')
    if updatedAt then
        redis.call('HSET', nKey, 'status', 'pending', 'updated_at', now)
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

	ids, err := recoverStuckScript.Run(ctx, r.client,
		[]string{KeyIdxStatus + string(domain.StatusQueued)},
		cutoffScore, limit, nowStr,
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("recover stuck: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	for _, idStr := range ids {
		pipe.ZRem(ctx, KeyIdxStatus+string(domain.StatusQueued), idStr)
		n, _ := r.GetByID(ctx, uuid.MustParse(idStr))
		score := float64(0)
		if n != nil {
			score = float64(n.CreatedAt.UnixNano())
		}
		pipe.ZAdd(ctx, KeyIdxStatus+string(domain.StatusPending), redis.Z{Score: score, Member: idStr})

		r.publishPersistEvent(ctx, pipe, "update_status", nil, map[string]string{
			"id":   idStr,
			"from": string(domain.StatusQueued),
			"to":   string(domain.StatusPending),
		})
	}
	pipe.Exec(ctx)

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
		return
	}

	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: KeyPersistQueue,
		Values: map[string]interface{}{
			"event": string(data),
		},
	})
}

func BuildPersistGroupName() string {
	return "dbwriter-group"
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
			m := map[string]string{"action": evt.Action}
			for k, v := range evt.Extra {
				m[k] = v
			}
			updates = append(updates, m)
		}
	}
	return
}
