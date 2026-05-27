package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type mockProvider struct {
	latency time.Duration
}

func NewMockProvider(latency time.Duration) Provider {
	return &mockProvider{latency: latency}
}

func (p *mockProvider) Send(ctx context.Context, recipient string, channel string, content string) (*SendResult, error) {
	if p.latency > 0 {
		select {
		case <-time.After(p.latency):
		case <-ctx.Done():
			return &SendResult{Retryable: true}, fmt.Errorf("mock provider context cancelled: %w", ctx.Err())
		}
	}

	return &SendResult{
		ProviderMsgID: uuid.New().String(),
		Retryable:     false,
	}, nil
}
