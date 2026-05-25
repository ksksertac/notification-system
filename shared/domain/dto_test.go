package domain

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCreateNotificationRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		req     CreateNotificationRequest
		wantErr string
	}{
		{
			name:    "empty recipient",
			req:     CreateNotificationRequest{Channel: "sms", Content: "hello"},
			wantErr: "recipient is required",
		},
		{
			name:    "empty content",
			req:     CreateNotificationRequest{Recipient: "+905551234567", Channel: "sms"},
			wantErr: "content is required",
		},
		{
			name:    "invalid channel",
			req:     CreateNotificationRequest{Recipient: "+905551234567", Channel: "telegram", Content: "hello"},
			wantErr: "invalid channel",
		},
		{
			name:    "sms content too long",
			req:     CreateNotificationRequest{Recipient: "+905551234567", Channel: "sms", Content: strings.Repeat("a", 161)},
			wantErr: "content exceeds max length",
		},
		{
			name:    "push content too long",
			req:     CreateNotificationRequest{Recipient: "abcdefghij1234567890", Channel: "push", Content: strings.Repeat("a", 257)},
			wantErr: "content exceeds max length",
		},
		{
			name:    "email content too long",
			req:     CreateNotificationRequest{Recipient: "test@example.com", Channel: "email", Content: strings.Repeat("a", 10001)},
			wantErr: "content exceeds max length",
		},
		{
			name:    "invalid priority",
			req:     CreateNotificationRequest{Recipient: "+905551234567", Channel: "sms", Content: "hello", Priority: "urgent"},
			wantErr: "invalid priority",
		},
		{
			name:    "invalid phone format",
			req:     CreateNotificationRequest{Recipient: "not-a-phone", Channel: "sms", Content: "hello"},
			wantErr: "invalid phone number",
		},
		{
			name:    "invalid email",
			req:     CreateNotificationRequest{Recipient: "not-an-email", Channel: "email", Content: "hello"},
			wantErr: "invalid email",
		},
		{
			name:    "push token too short",
			req:     CreateNotificationRequest{Recipient: "short", Channel: "push", Content: "hello"},
			wantErr: "invalid push token",
		},
		{
			name: "past scheduled_at",
			req: CreateNotificationRequest{
				Recipient:   "+905551234567",
				Channel:     "sms",
				Content:     "hello",
				ScheduledAt: timePtr(time.Now().Add(-1 * time.Hour)),
			},
			wantErr: "scheduled_at must be in the future",
		},
		{
			name:    "valid sms",
			req:     CreateNotificationRequest{Recipient: "+905551234567", Channel: "sms", Content: "hello"},
			wantErr: "",
		},
		{
			name:    "valid email",
			req:     CreateNotificationRequest{Recipient: "test@example.com", Channel: "email", Content: "hello"},
			wantErr: "",
		},
		{
			name:    "valid push",
			req:     CreateNotificationRequest{Recipient: "abcdefghij1234567890", Channel: "push", Content: "hello"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Errorf("expected error containing %q, got nil", tt.wantErr)
				return
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestBatchCreateRequest_Validate(t *testing.T) {
	t.Run("empty batch", func(t *testing.T) {
		req := BatchCreateRequest{}
		err := req.Validate()
		if err == nil || !strings.Contains(err.Error(), "at least one") {
			t.Errorf("expected 'at least one' error, got %v", err)
		}
	})

	t.Run("exceeds max batch size", func(t *testing.T) {
		req := BatchCreateRequest{
			Notifications: make([]CreateNotificationRequest, 1001),
		}
		err := req.Validate()
		if err == nil || !strings.Contains(err.Error(), "maximum of 1000") {
			t.Errorf("expected 'maximum of 1000' error, got %v", err)
		}
	})
}

func TestBatchCreateRequest_Validate_ItemError(t *testing.T) {
	t.Run("propagates individual item validation error", func(t *testing.T) {
		req := BatchCreateRequest{
			Notifications: []CreateNotificationRequest{
				{Recipient: "+905551234567", Channel: "sms", Content: "valid message"},
				{Recipient: "", Channel: "sms", Content: "missing recipient"},
			},
		}
		err := req.Validate()
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "notification[1]") {
			t.Errorf("expected error to reference notification[1], got %q", err.Error())
		}
		if !strings.Contains(err.Error(), "recipient is required") {
			t.Errorf("expected error to mention 'recipient is required', got %q", err.Error())
		}
	})
}

func TestToNotificationResponse(t *testing.T) {
	id := uuid.New()
	batchID := uuid.New()
	idempotencyKey := "idem-key-123"
	providerMsgID := "provider-msg-456"
	errorMsg := "delivery failed"
	now := time.Now().Truncate(time.Second)
	scheduledAt := now.Add(1 * time.Hour)

	n := &Notification{
		ID:             id,
		IdempotencyKey: &idempotencyKey,
		BatchID:        &batchID,
		Recipient:      "test@example.com",
		Channel:        ChannelEmail,
		Content:        "Hello world",
		Priority:       PriorityHigh,
		Status:         StatusFailed,
		ProviderMsgID:  &providerMsgID,
		RetryCount:     3,
		ScheduledAt:    &scheduledAt,
		ErrorMessage:   &errorMsg,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	resp := ToNotificationResponse(n)

	if resp.ID != id {
		t.Errorf("ID = %v, want %v", resp.ID, id)
	}
	if resp.IdempotencyKey == nil || *resp.IdempotencyKey != idempotencyKey {
		t.Errorf("IdempotencyKey = %v, want %v", resp.IdempotencyKey, &idempotencyKey)
	}
	if resp.BatchID == nil || *resp.BatchID != batchID {
		t.Errorf("BatchID = %v, want %v", resp.BatchID, &batchID)
	}
	if resp.Recipient != "test@example.com" {
		t.Errorf("Recipient = %q, want %q", resp.Recipient, "test@example.com")
	}
	if resp.Channel != "email" {
		t.Errorf("Channel = %q, want %q", resp.Channel, "email")
	}
	if resp.Content != "Hello world" {
		t.Errorf("Content = %q, want %q", resp.Content, "Hello world")
	}
	if resp.Priority != "high" {
		t.Errorf("Priority = %q, want %q", resp.Priority, "high")
	}
	if resp.Status != "failed" {
		t.Errorf("Status = %q, want %q", resp.Status, "failed")
	}
	if resp.ProviderMsgID == nil || *resp.ProviderMsgID != providerMsgID {
		t.Errorf("ProviderMsgID = %v, want %v", resp.ProviderMsgID, &providerMsgID)
	}
	if resp.RetryCount != 3 {
		t.Errorf("RetryCount = %d, want %d", resp.RetryCount, 3)
	}
	if resp.ScheduledAt == nil || !resp.ScheduledAt.Equal(scheduledAt) {
		t.Errorf("ScheduledAt = %v, want %v", resp.ScheduledAt, scheduledAt)
	}
	if resp.ErrorMessage == nil || *resp.ErrorMessage != errorMsg {
		t.Errorf("ErrorMessage = %v, want %v", resp.ErrorMessage, &errorMsg)
	}
	if !resp.CreatedAt.Equal(now) {
		t.Errorf("CreatedAt = %v, want %v", resp.CreatedAt, now)
	}
	if !resp.UpdatedAt.Equal(now) {
		t.Errorf("UpdatedAt = %v, want %v", resp.UpdatedAt, now)
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}
