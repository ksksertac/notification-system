package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

const (
	StreamHigh   = "notifications:high"
	StreamNormal = "notifications:normal"
	StreamLow    = "notifications:low"
)

func StreamForPriority(p domain.Priority) string {
	switch p {
	case domain.PriorityHigh:
		return StreamHigh
	case domain.PriorityNormal:
		return StreamNormal
	case domain.PriorityLow:
		return StreamLow
	default:
		return StreamNormal
	}
}

type Message struct {
	ID             string
	StreamName     string
	NotificationID uuid.UUID
	Channel        domain.Channel
	Recipient      string
	Content        string
	Priority       domain.Priority
	RetryCount     int
}

type Publisher interface {
	Publish(ctx context.Context, n *domain.Notification) error
	PublishBatch(ctx context.Context, notifications []*domain.Notification) error
}

type Consumer interface {
	Read(ctx context.Context, stream string, group string, consumer string, count int64) ([]Message, error)
	Ack(ctx context.Context, stream string, group string, ids ...string) error
	ClaimStale(ctx context.Context, stream string, group string, consumer string, minIdle time.Duration, count int64) ([]Message, error)
	Len(ctx context.Context, stream string) (int64, error)
}

type StreamManager interface {
	EnsureStreams(ctx context.Context, group string) error
}

func ParseMessage(stream string, id string, values map[string]interface{}) (Message, error) {
	nid, ok := values["notification_id"].(string)
	if !ok {
		return Message{}, fmt.Errorf("missing notification_id")
	}
	parsedID, err := uuid.Parse(nid)
	if err != nil {
		return Message{}, fmt.Errorf("invalid notification_id: %w", err)
	}

	ch, _ := values["channel"].(string)
	recipient, _ := values["recipient"].(string)
	content, _ := values["content"].(string)
	priorityStr, _ := values["priority"].(string)
	retryStr, _ := values["retry_count"].(string)

	priority, _ := domain.PriorityFromString(priorityStr)

	retryCount := 0
	if retryStr != "" {
		fmt.Sscanf(retryStr, "%d", &retryCount)
	}

	return Message{
		ID:             id,
		StreamName:     stream,
		NotificationID: parsedID,
		Channel:        domain.Channel(ch),
		Recipient:      recipient,
		Content:        content,
		Priority:       priority,
		RetryCount:     retryCount,
	}, nil
}
