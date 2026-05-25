package delivery

import (
	"context"
)

type SendResult struct {
	ProviderMsgID string
	Retryable     bool
}

type Provider interface {
	Send(ctx context.Context, recipient string, channel string, content string) (*SendResult, error)
}
