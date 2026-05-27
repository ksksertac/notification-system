package delivery

import (
	"context"
	"time"
)

type SendResult struct {
	ProviderMsgID string
	Retryable     bool
	RetryAfter    time.Duration
}

type Provider interface {
	Send(ctx context.Context, recipient string, channel string, content string) (*SendResult, error)
}
