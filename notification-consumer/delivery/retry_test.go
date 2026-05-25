package delivery

import (
	"testing"
	"time"
)

func TestExponentialBackoff_NextDelay(t *testing.T) {
	eb := NewExponentialBackoff(2*time.Second, 60*time.Second)

	prev := time.Duration(0)
	for attempt := 1; attempt <= 5; attempt++ {
		delay := eb.NextDelay(attempt)

		if delay <= 0 {
			t.Errorf("attempt %d: delay should be positive, got %v", attempt, delay)
		}

		if delay > 60*time.Second {
			t.Errorf("attempt %d: delay %v exceeds max 60s", attempt, delay)
		}

		if attempt > 1 && delay < prev/2 {
			t.Logf("attempt %d: delay %v (note: jitter may cause non-monotonic growth)", attempt, delay)
		}

		prev = delay
	}
}

func TestExponentialBackoff_ShouldRetry(t *testing.T) {
	eb := NewExponentialBackoff(2*time.Second, 60*time.Second)

	tests := []struct {
		attempt    int
		maxAttempts int
		want       bool
	}{
		{1, 5, true},
		{4, 5, true},
		{5, 5, false},
		{6, 5, false},
	}

	for _, tt := range tests {
		got := eb.ShouldRetry(tt.attempt, tt.maxAttempts)
		if got != tt.want {
			t.Errorf("ShouldRetry(%d, %d) = %v, want %v", tt.attempt, tt.maxAttempts, got, tt.want)
		}
	}
}

func TestExponentialBackoff_CapsAtMaxDelay(t *testing.T) {
	maxDelay := 10 * time.Second
	eb := NewExponentialBackoff(1*time.Second, maxDelay)

	// High attempt number should still be capped at maxDelay
	for attempt := 10; attempt <= 20; attempt++ {
		delay := eb.NextDelay(attempt)
		if delay > maxDelay {
			t.Errorf("attempt %d: delay %v exceeds maxDelay %v", attempt, delay, maxDelay)
		}
		if delay <= 0 {
			t.Errorf("attempt %d: delay should be positive, got %v", attempt, delay)
		}
	}
}

func TestExponentialBackoff_FirstAttemptDelay(t *testing.T) {
	baseDelay := 100 * time.Millisecond
	maxDelay := 10 * time.Second
	eb := NewExponentialBackoff(baseDelay, maxDelay)

	// First attempt: base delay * 2^0 = baseDelay, plus jitter up to baseDelay*1
	// So delay should be between baseDelay and 2*baseDelay
	for i := 0; i < 100; i++ {
		delay := eb.NextDelay(1)
		if delay < baseDelay {
			t.Errorf("first attempt delay %v should be >= baseDelay %v", delay, baseDelay)
		}
		if delay > 2*baseDelay {
			t.Errorf("first attempt delay %v should be <= 2*baseDelay %v", delay, 2*baseDelay)
		}
	}
}

func TestExponentialBackoff_ShouldRetryEdgeCases(t *testing.T) {
	eb := NewExponentialBackoff(1*time.Second, 60*time.Second)

	// Attempt 0 should retry
	if !eb.ShouldRetry(0, 5) {
		t.Error("ShouldRetry(0, 5) should be true")
	}

	// maxAttempts 0 means never retry
	if eb.ShouldRetry(0, 0) {
		t.Error("ShouldRetry(0, 0) should be false")
	}

	// maxAttempts 1 means only attempt 0 retries
	if !eb.ShouldRetry(0, 1) {
		t.Error("ShouldRetry(0, 1) should be true")
	}
	if eb.ShouldRetry(1, 1) {
		t.Error("ShouldRetry(1, 1) should be false")
	}
}
