package queue

import (
	"testing"

	"github.com/sertacyildirim/notification-system/shared/domain"
)

func TestParseMessage_WithTraceContext(t *testing.T) {
	values := map[string]interface{}{
		"notification_id": "550e8400-e29b-41d4-a716-446655440000",
		"channel":         "email",
		"recipient":       "user@test.com",
		"content":         "hello",
		"priority":        "high",
		"retry_count":     "2",
		"correlation_id":  "corr-123",
		"traceparent":     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":      "vendor=value",
	}

	msg, err := ParseMessage("notifications:high", "1-0", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Traceparent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Errorf("Traceparent = %q, want W3C traceparent", msg.Traceparent)
	}
	if msg.Tracestate != "vendor=value" {
		t.Errorf("Tracestate = %q, want %q", msg.Tracestate, "vendor=value")
	}
	if msg.CorrelationID != "corr-123" {
		t.Errorf("CorrelationID = %q, want %q", msg.CorrelationID, "corr-123")
	}
	if msg.Channel != domain.ChannelEmail {
		t.Errorf("Channel = %q, want email", msg.Channel)
	}
	if msg.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2", msg.RetryCount)
	}
}

func TestParseMessage_WithoutTraceContext(t *testing.T) {
	values := map[string]interface{}{
		"notification_id": "550e8400-e29b-41d4-a716-446655440000",
		"channel":         "sms",
		"recipient":       "user@test.com",
		"content":         "hello",
		"priority":        "normal",
		"retry_count":     "0",
	}

	msg, err := ParseMessage("notifications:normal", "2-0", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Traceparent != "" {
		t.Errorf("Traceparent should be empty, got %q", msg.Traceparent)
	}
	if msg.Tracestate != "" {
		t.Errorf("Tracestate should be empty, got %q", msg.Tracestate)
	}
}

func TestParseMessage_MissingNotificationID(t *testing.T) {
	values := map[string]interface{}{
		"channel": "email",
	}

	_, err := ParseMessage("notifications:high", "3-0", values)
	if err == nil {
		t.Fatal("expected error for missing notification_id")
	}
}

func TestParseMessage_InvalidNotificationID(t *testing.T) {
	values := map[string]interface{}{
		"notification_id": "not-a-uuid",
	}

	_, err := ParseMessage("notifications:high", "4-0", values)
	if err == nil {
		t.Fatal("expected error for invalid notification_id")
	}
}

func TestStreamForPriority(t *testing.T) {
	tests := []struct {
		priority domain.Priority
		want     string
	}{
		{domain.PriorityHigh, StreamHigh},
		{domain.PriorityNormal, StreamNormal},
		{domain.PriorityLow, StreamLow},
		{domain.Priority(99), StreamNormal},
	}
	for _, tt := range tests {
		if got := StreamForPriority(tt.priority); got != tt.want {
			t.Errorf("StreamForPriority(%v) = %q, want %q", tt.priority, got, tt.want)
		}
	}
}
