package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type webhookProvider struct {
	url    string
	client *http.Client
}

func NewWebhookProvider(url string, timeout time.Duration) Provider {
	return &webhookProvider{
		url: url,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

type webhookRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type webhookResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

func (p *webhookProvider) Send(ctx context.Context, recipient string, channel string, content string) (*SendResult, error) {
	body, err := json.Marshal(webhookRequest{
		To:      recipient,
		Channel: channel,
		Content: content,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return &SendResult{Retryable: true}, fmt.Errorf("sending request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 500 {
		return &SendResult{Retryable: true}, fmt.Errorf("provider returned %d: %s", resp.StatusCode, string(respBody))
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		return &SendResult{Retryable: true}, fmt.Errorf("provider rate limited (429): %s", string(respBody))
	}

	if resp.StatusCode >= 400 {
		return &SendResult{Retryable: false}, fmt.Errorf("provider returned %d: %s", resp.StatusCode, string(respBody))
	}

	var webhookResp webhookResponse
	if err := json.Unmarshal(respBody, &webhookResp); err != nil {
		return &SendResult{Retryable: false}, fmt.Errorf("parsing response: %w", err)
	}

	return &SendResult{
		ProviderMsgID: webhookResp.MessageID,
		Retryable:     false,
	}, nil
}
