package delivery

import (
	"math"
	"math/rand"
	"time"
)

type RetryStrategy interface {
	NextDelay(attempt int) time.Duration
	ShouldRetry(attempt int, maxAttempts int) bool
}

type exponentialBackoff struct {
	baseDelay time.Duration
	maxDelay  time.Duration
}

func NewExponentialBackoff(baseDelay, maxDelay time.Duration) RetryStrategy {
	return &exponentialBackoff{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
	}
}

func (e *exponentialBackoff) NextDelay(attempt int) time.Duration {
	delay := float64(e.baseDelay) * math.Pow(2, float64(attempt-1))

	jitter := rand.Float64() * float64(e.baseDelay) * float64(attempt)
	delay += jitter

	if delay > float64(e.maxDelay) {
		delay = float64(e.maxDelay)
	}

	return time.Duration(delay)
}

func (e *exponentialBackoff) ShouldRetry(attempt int, maxAttempts int) bool {
	return attempt < maxAttempts
}
