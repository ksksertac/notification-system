package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewWebhookProvider(t *testing.T) {
	provider := NewWebhookProvider("http://example.com/webhook", 5*time.Second)
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
}

func TestWebhookSend_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
		}

		var req webhookRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}
		if req.To != "user@example.com" {
			t.Errorf("expected To=user@example.com, got %s", req.To)
		}
		if req.Channel != "email" {
			t.Errorf("expected Channel=email, got %s", req.Channel)
		}
		if req.Content != "Hello World" {
			t.Errorf("expected Content=Hello World, got %s", req.Content)
		}

		resp := webhookResponse{
			MessageID: "msg-123",
			Status:    "sent",
			Timestamp: time.Now().Format(time.RFC3339),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	provider := NewWebhookProvider(server.URL, 5*time.Second)
	result, err := provider.Send(context.Background(), "user@example.com", "email", "Hello World")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.ProviderMsgID != "msg-123" {
		t.Errorf("expected ProviderMsgID=msg-123, got %s", result.ProviderMsgID)
	}
	if result.Retryable {
		t.Error("expected Retryable=false for successful send")
	}
}

func TestWebhookSend_ServerError500(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	provider := NewWebhookProvider(server.URL, 5*time.Second)
	result, err := provider.Send(context.Background(), "user@example.com", "email", "Hello")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if !result.Retryable {
		t.Error("expected Retryable=true for 500 error")
	}
}

func TestWebhookSend_RateLimit429(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte("rate limited"))
	}))
	defer server.Close()

	provider := NewWebhookProvider(server.URL, 5*time.Second)
	result, err := provider.Send(context.Background(), "user@example.com", "sms", "Hello")
	if err == nil {
		t.Fatal("expected error for 429 response")
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if !result.Retryable {
		t.Error("expected Retryable=true for 429 rate limit")
	}
}

func TestWebhookSend_ClientError400(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad request"))
	}))
	defer server.Close()

	provider := NewWebhookProvider(server.URL, 5*time.Second)
	result, err := provider.Send(context.Background(), "user@example.com", "push", "Hello")
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Retryable {
		t.Error("expected Retryable=false for 400 client error")
	}
}

func TestWebhookSend_InvalidJSONResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not valid json{{{"))
	}))
	defer server.Close()

	provider := NewWebhookProvider(server.URL, 5*time.Second)
	result, err := provider.Send(context.Background(), "user@example.com", "email", "Hello")
	if err == nil {
		t.Fatal("expected error for invalid JSON response")
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Retryable {
		t.Error("expected Retryable=false for invalid JSON")
	}
}

func TestWebhookSend_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	provider := NewWebhookProvider(server.URL, 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	result, err := provider.Send(ctx, "user@example.com", "email", "Hello")
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if !result.Retryable {
		t.Error("expected Retryable=true for network error from context cancellation")
	}
}

func TestWebhookSend_ConnectionRefused(t *testing.T) {
	// Use a URL that will refuse connection
	provider := NewWebhookProvider("http://127.0.0.1:1", 1*time.Second)
	result, err := provider.Send(context.Background(), "user@example.com", "email", "Hello")
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if !result.Retryable {
		t.Error("expected Retryable=true for connection error")
	}
}
