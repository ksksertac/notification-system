package domain

import (
	"strings"
	"testing"
	"time"
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

func timePtr(t time.Time) *time.Time {
	return &t
}
