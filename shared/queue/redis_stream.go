package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/tracing"
)

type redisStreamPublisher struct {
	client *redis.Client
}

func NewRedisPublisher(client *redis.Client) Publisher {
	return &redisStreamPublisher{client: client}
}

func (p *redisStreamPublisher) Publish(ctx context.Context, n *domain.Notification) error {
	ctx, span := tracing.StartSpan(ctx, "queue.Publish")
	defer span.End()
	tracing.SetNotificationAttrs(span, n.ID.String(), string(n.Channel), string(n.Status))
	tracing.SetAttr(span, "queue.stream", StreamForPriority(n.Priority))

	stream := StreamForPriority(n.Priority)

	values := map[string]interface{}{
		"notification_id": n.ID.String(),
		"channel":         string(n.Channel),
		"recipient":       n.Recipient,
		"content":         n.Content,
		"priority":        n.Priority.String(),
		"retry_count":     fmt.Sprintf("%d", n.RetryCount),
	}
	if cid := tracing.GetCorrelationID(ctx); cid != "" {
		values["correlation_id"] = cid
	}

	_, err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Result()
	if err != nil {
		tracing.RecordError(span, err)
	}
	return err
}

func (p *redisStreamPublisher) PublishBatch(ctx context.Context, notifications []*domain.Notification) error {
	ctx, span := tracing.StartSpan(ctx, "queue.PublishBatch")
	defer span.End()
	tracing.SetIntAttr(span, "batch.size", len(notifications))

	pipe := p.client.Pipeline()

	cid := tracing.GetCorrelationID(ctx)
	for _, n := range notifications {
		stream := StreamForPriority(n.Priority)
		values := map[string]interface{}{
			"notification_id": n.ID.String(),
			"channel":         string(n.Channel),
			"recipient":       n.Recipient,
			"content":         n.Content,
			"priority":        n.Priority.String(),
			"retry_count":     fmt.Sprintf("%d", n.RetryCount),
		}
		if cid != "" {
			values["correlation_id"] = cid
		}
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: values,
		})
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		tracing.RecordError(span, err)
	}
	return err
}

type redisStreamConsumer struct {
	client *redis.Client
}

func NewRedisConsumer(client *redis.Client) Consumer {
	return &redisStreamConsumer{client: client}
}

func (c *redisStreamConsumer) Read(ctx context.Context, stream string, group string, consumer string, count int64) ([]Message, error) {
	results, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    100 * time.Millisecond,
	}).Result()

	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("xreadgroup %s: %w", stream, err)
	}

	var messages []Message
	for _, result := range results {
		for _, msg := range result.Messages {
			m, err := ParseMessage(stream, msg.ID, msg.Values)
			if err != nil {
				continue
			}
			messages = append(messages, m)
		}
	}

	return messages, nil
}

func (c *redisStreamConsumer) Ack(ctx context.Context, stream string, group string, ids ...string) error {
	_, err := c.client.XAck(ctx, stream, group, ids...).Result()
	return err
}

func (c *redisStreamConsumer) ClaimStale(ctx context.Context, stream string, group string, consumer string, minIdle time.Duration, count int64) ([]Message, error) {
	pending, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  count,
		Idle:   minIdle,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("xpending %s: %w", stream, err)
	}

	if len(pending) == 0 {
		return nil, nil
	}

	ids := make([]string, len(pending))
	for i, p := range pending {
		ids[i] = p.ID
	}

	claimed, err := c.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("xclaim %s: %w", stream, err)
	}

	var messages []Message
	for _, msg := range claimed {
		m, err := ParseMessage(stream, msg.ID, msg.Values)
		if err != nil {
			continue
		}
		messages = append(messages, m)
	}

	return messages, nil
}

func (c *redisStreamConsumer) Len(ctx context.Context, stream string) (int64, error) {
	return c.client.XLen(ctx, stream).Result()
}

type redisStreamManager struct {
	client *redis.Client
}

func NewRedisStreamManager(client *redis.Client) StreamManager {
	return &redisStreamManager{client: client}
}

func (m *redisStreamManager) EnsureStreams(ctx context.Context, group string) error {
	streams := []string{StreamHigh, StreamNormal, StreamLow}

	for _, stream := range streams {
		err := m.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("creating consumer group for %s: %w", stream, err)
		}
	}

	return nil
}
