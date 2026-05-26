package domain

import (
	"fmt"
	"net/mail"
	"regexp"
	"time"

	"github.com/google/uuid"
)

var phoneRegex = regexp.MustCompile(`^\+[1-9]\d{6,14}$`)

type CreateNotificationRequest struct {
	Recipient   string          `json:"recipient"`
	Channel     string          `json:"channel"`
	Content     string          `json:"content"`
	Priority    string          `json:"priority"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	Metadata    map[string]any  `json:"metadata,omitempty"`
}

func (r *CreateNotificationRequest) Validate() error {
	if r.Recipient == "" {
		return fmt.Errorf("recipient is required")
	}
	if r.Content == "" {
		return fmt.Errorf("content is required")
	}

	ch := Channel(r.Channel)
	if !ch.IsValid() {
		return fmt.Errorf("invalid channel: %s (must be sms, email, or push)", r.Channel)
	}

	if len(r.Content) > ch.MaxContentLength() {
		return fmt.Errorf("content exceeds max length for %s channel (%d chars)", r.Channel, ch.MaxContentLength())
	}

	if r.Priority == "" {
		r.Priority = "normal"
	}
	if _, err := PriorityFromString(r.Priority); err != nil {
		return err
	}

	if err := validateRecipient(r.Recipient, ch); err != nil {
		return err
	}

	if r.ScheduledAt != nil && r.ScheduledAt.Before(time.Now()) {
		return fmt.Errorf("scheduled_at must be in the future")
	}

	return nil
}

func validateRecipient(recipient string, ch Channel) error {
	switch ch {
	case ChannelSMS:
		if !phoneRegex.MatchString(recipient) {
			return fmt.Errorf("invalid phone number format for SMS (expected E.164: +1234567890)")
		}
	case ChannelEmail:
		if _, err := mail.ParseAddress(recipient); err != nil {
			return fmt.Errorf("invalid email address: %s", recipient)
		}
	case ChannelPush:
		if len(recipient) < 10 {
			return fmt.Errorf("invalid push token: too short")
		}
	}
	return nil
}

type BatchCreateRequest struct {
	Notifications []CreateNotificationRequest `json:"notifications"`
}

func (r *BatchCreateRequest) Validate() error {
	if len(r.Notifications) == 0 {
		return fmt.Errorf("at least one notification is required")
	}
	if len(r.Notifications) > 1000 {
		return fmt.Errorf("batch size exceeds maximum of 1000")
	}
	for i := range r.Notifications {
		if err := r.Notifications[i].Validate(); err != nil {
			return fmt.Errorf("notification[%d]: %w", i, err)
		}
	}
	return nil
}

type NotificationResponse struct {
	ID             uuid.UUID       `json:"id"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	BatchID        *uuid.UUID      `json:"batch_id,omitempty"`
	Recipient      string          `json:"recipient"`
	Channel        string          `json:"channel"`
	Content        string          `json:"content"`
	Priority       string          `json:"priority"`
	Status         string          `json:"status"`
	ProviderMsgID  *string         `json:"provider_msg_id,omitempty"`
	RetryCount     int             `json:"retry_count"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
	ErrorMessage   *string         `json:"error_message,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

func ToNotificationResponse(n *Notification) NotificationResponse {
	return NotificationResponse{
		ID:             n.ID,
		IdempotencyKey: n.IdempotencyKey,
		BatchID:        n.BatchID,
		Recipient:      n.Recipient,
		Channel:        string(n.Channel),
		Content:        n.Content,
		Priority:       n.Priority.String(),
		Status:         string(n.Status),
		ProviderMsgID:  n.ProviderMsgID,
		RetryCount:     n.RetryCount,
		ScheduledAt:    n.ScheduledAt,
		ErrorMessage:   n.ErrorMessage,
		CreatedAt:      n.CreatedAt,
		UpdatedAt:      n.UpdatedAt,
	}
}

type BatchCreateResponse struct {
	BatchID       uuid.UUID              `json:"batch_id"`
	Total         int                    `json:"total"`
	Notifications []NotificationResponse `json:"notifications"`
}

type ListNotificationsRequest struct {
	Status    *string    `json:"status,omitempty"`
	Channel   *string    `json:"channel,omitempty"`
	StartDate *time.Time `json:"start_date,omitempty"`
	EndDate   *time.Time `json:"end_date,omitempty"`
	Cursor    *uuid.UUID `json:"cursor,omitempty"`
	Limit     int        `json:"limit"`
}

type ListNotificationsResponse struct {
	Notifications []NotificationResponse `json:"notifications"`
	NextCursor    *uuid.UUID             `json:"next_cursor,omitempty"`
	Total         int64                  `json:"total"`
}

type APIResponse struct {
	Success       bool   `json:"success"`
	Data          any    `json:"data,omitempty"`
	Error         *APIError `json:"error,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
