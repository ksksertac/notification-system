package delivery

import (
	"math"
	"testing"
	"time"
)

func TestExponentialBackoff_NextDelay_FullJitterRange(t *testing.T) {
	baseDelay := 1 * time.Second
	maxDelay := 60 * time.Second
	eb := NewExponentialBackoff(baseDelay, maxDelay)

	for attempt := 1; attempt <= 10; attempt++ {
		expectedCap := math.Min(float64(maxDelay), float64(baseDelay)*math.Pow(2, float64(attempt-1)))

		for i := 0; i < 100; i++ {
			delay := eb.NextDelay(attempt)

			if delay < 0 {
				t.Errorf("attempt %d: delay should be >= 0, got %v", attempt, delay)
			}

			if float64(delay) > expectedCap {
				t.Errorf("attempt %d: delay %v exceeds expected cap %v", attempt, delay, time.Duration(expectedCap))
			}
		}
	}
}

func TestExponentialBackoff_ShouldRetry(t *testing.T) {
	eb := NewExponentialBackoff(2*time.Second, 60*time.Second)

	tests := []struct {
		attempt     int
		maxAttempts int
		want        bool
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

	// High attempt numbers should still be capped at maxDelay
	for attempt := 10; attempt <= 20; attempt++ {
		for i := 0; i < 50; i++ {
			delay := eb.NextDelay(attempt)
			if delay > maxDelay {
				t.Errorf("attempt %d: delay %v exceeds maxDelay %v", attempt, delay, maxDelay)
			}
			if delay < 0 {
				t.Errorf("attempt %d: delay should be >= 0, got %v", attempt, delay)
			}
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
