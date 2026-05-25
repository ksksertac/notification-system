package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

func (c Channel) IsValid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

func (c Channel) MaxContentLength() int {
	switch c {
	case ChannelSMS:
		return 160
	case ChannelEmail:
		return 10000
	case ChannelPush:
		return 256
	default:
		return 0
	}
}

type Priority int

const (
	PriorityHigh   Priority = 0
	PriorityNormal Priority = 1
	PriorityLow    Priority = 2
)

func (p Priority) IsValid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	default:
		return "unknown"
	}
}

func PriorityFromString(s string) (Priority, error) {
	switch s {
	case "high":
		return PriorityHigh, nil
	case "normal":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	default:
		return -1, fmt.Errorf("invalid priority: %s", s)
	}
}

type Status string

const (
	StatusPending    Status = "pending"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusDelivered  Status = "delivered"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

var validTransitions = map[Status][]Status{
	StatusPending:    {StatusQueued, StatusCancelled},
	StatusQueued:     {StatusProcessing, StatusCancelled},
	StatusProcessing: {StatusDelivered, StatusFailed},
	StatusFailed:     {StatusQueued},
}

func (s Status) CanTransitionTo(target Status) bool {
	allowed, ok := validTransitions[s]
	if !ok {
		return false
	}
	for _, a := range allowed {
		if a == target {
			return true
		}
	}
	return false
}

func (s Status) IsFinal() bool {
	return s == StatusDelivered || s == StatusCancelled
}

type Notification struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	IdempotencyKey *string    `db:"idempotency_key" json:"idempotency_key,omitempty"`
	BatchID        *uuid.UUID `db:"batch_id" json:"batch_id,omitempty"`
	Recipient      string     `db:"recipient" json:"recipient"`
	Channel        Channel    `db:"channel" json:"channel"`
	Content        string     `db:"content" json:"content"`
	Priority       Priority   `db:"priority" json:"priority"`
	Status         Status     `db:"status" json:"status"`
	ProviderMsgID  *string    `db:"provider_msg_id" json:"provider_msg_id,omitempty"`
	RetryCount     int        `db:"retry_count" json:"retry_count"`
	MaxRetries     int        `db:"max_retries" json:"max_retries"`
	NextRetryAt    *time.Time `db:"next_retry_at" json:"next_retry_at,omitempty"`
	ScheduledAt    *time.Time `db:"scheduled_at" json:"scheduled_at,omitempty"`
	Metadata       []byte     `db:"metadata" json:"metadata,omitempty"`
	ErrorMessage   *string    `db:"error_message" json:"error_message,omitempty"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}

type DeadLetterEntry struct {
	ID             uuid.UUID  `db:"id" json:"id"`
	NotificationID uuid.UUID  `db:"notification_id" json:"notification_id"`
	Channel        Channel    `db:"channel" json:"channel"`
	Recipient      string     `db:"recipient" json:"recipient"`
	Content        string     `db:"content" json:"content"`
	ErrorMessage   *string    `db:"error_message" json:"error_message,omitempty"`
	RetryCount     int        `db:"retry_count" json:"retry_count"`
	FailedAt       time.Time  `db:"failed_at" json:"failed_at"`
	Reprocessed    bool       `db:"reprocessed" json:"reprocessed"`
	ReprocessedAt  *time.Time `db:"reprocessed_at" json:"reprocessed_at,omitempty"`
}
