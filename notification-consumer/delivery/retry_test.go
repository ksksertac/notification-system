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
