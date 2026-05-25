package domain

import (
	"testing"
)

func TestPriority_String(t *testing.T) {
	tests := []struct {
		priority Priority
		want     string
	}{
		{PriorityHigh, "high"},
		{PriorityNormal, "normal"},
		{PriorityLow, "low"},
		{Priority(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.priority.String(); got != tt.want {
			t.Errorf("Priority(%d).String() = %q, want %q", tt.priority, got, tt.want)
		}
	}
}

func TestChannel_IsValid(t *testing.T) {
	tests := []struct {
		channel Channel
		want    bool
	}{
		{ChannelSMS, true},
		{ChannelEmail, true},
		{ChannelPush, true},
		{Channel("telegram"), false},
		{Channel(""), false},
	}

	for _, tt := range tests {
		if got := tt.channel.IsValid(); got != tt.want {
			t.Errorf("Channel(%q).IsValid() = %v, want %v", tt.channel, got, tt.want)
		}
	}
}

func TestChannel_MaxContentLength(t *testing.T) {
	tests := []struct {
		channel Channel
		want    int
	}{
		{ChannelSMS, 160},
		{ChannelEmail, 10000},
		{ChannelPush, 256},
		{Channel("invalid"), 0},
	}

	for _, tt := range tests {
		if got := tt.channel.MaxContentLength(); got != tt.want {
			t.Errorf("Channel(%q).MaxContentLength() = %d, want %d", tt.channel, got, tt.want)
		}
	}
}

func TestPriority_IsValid(t *testing.T) {
	tests := []struct {
		priority Priority
		want     bool
	}{
		{PriorityHigh, true},
		{PriorityNormal, true},
		{PriorityLow, true},
		{Priority(99), false},
	}

	for _, tt := range tests {
		if got := tt.priority.IsValid(); got != tt.want {
			t.Errorf("Priority(%d).IsValid() = %v, want %v", tt.priority, got, tt.want)
		}
	}
}

func TestPriorityFromString(t *testing.T) {
	tests := []struct {
		input   string
		want    Priority
		wantErr bool
	}{
		{"high", PriorityHigh, false},
		{"normal", PriorityNormal, false},
		{"low", PriorityLow, false},
		{"invalid", -1, true},
	}

	for _, tt := range tests {
		got, err := PriorityFromString(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("PriorityFromString(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
		}
		if got != tt.want {
			t.Errorf("PriorityFromString(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestStatus_CanTransitionTo(t *testing.T) {
	tests := []struct {
		from Status
		to   Status
		want bool
	}{
		{StatusPending, StatusQueued, true},
		{StatusPending, StatusCancelled, true},
		{StatusPending, StatusDelivered, false},
		{StatusQueued, StatusProcessing, true},
		{StatusQueued, StatusCancelled, true},
		{StatusQueued, StatusDelivered, false},
		{StatusProcessing, StatusDelivered, true},
		{StatusProcessing, StatusFailed, true},
		{StatusProcessing, StatusCancelled, false},
		{StatusFailed, StatusQueued, true},
		{StatusFailed, StatusDelivered, false},
		{StatusDelivered, StatusFailed, false},
		{StatusCancelled, StatusQueued, false},
	}

	for _, tt := range tests {
		if got := tt.from.CanTransitionTo(tt.to); got != tt.want {
			t.Errorf("Status(%q).CanTransitionTo(%q) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

func TestStatus_IsFinal(t *testing.T) {
	tests := []struct {
		status Status
		want   bool
	}{
		{StatusDelivered, true},
		{StatusCancelled, true},
		{StatusPending, false},
		{StatusQueued, false},
		{StatusProcessing, false},
		{StatusFailed, false},
	}

	for _, tt := range tests {
		if got := tt.status.IsFinal(); got != tt.want {
			t.Errorf("Status(%q).IsFinal() = %v, want %v", tt.status, got, tt.want)
		}
	}
}
