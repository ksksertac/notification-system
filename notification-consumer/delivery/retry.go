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
	cap := math.Min(float64(e.maxDelay), float64(e.baseDelay)*math.Pow(2, float64(attempt-1)))
	return time.Duration(rand.Float64() * cap)
}

func (e *exponentialBackoff) ShouldRetry(attempt int, maxAttempts int) bool {
	return attempt < maxAttempts
}
