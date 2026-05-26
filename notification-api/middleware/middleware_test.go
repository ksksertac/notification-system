package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/tracing"
)

// ---------------------------------------------------------------------------
// Auth middleware tests
// ---------------------------------------------------------------------------

func TestAPIKeyAuth_ValidKey(t *testing.T) {
	const apiKey = "test-secret-key"

	handler := APIKeyAuth(apiKey)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", apiKey)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if rr.Body.String() != "ok" {
		t.Fatalf("expected body 'ok', got %q", rr.Body.String())
	}
}

func TestAPIKeyAuth_InvalidKey(t *testing.T) {
	handler := APIKeyAuth("correct-key")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called with invalid key")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "wrong-key")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", rr.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if body["error"] != "invalid API key" {
		t.Fatalf("expected error 'invalid API key', got %q", body["error"])
	}

	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type 'application/json', got %q", ct)
	}
}

func TestAPIKeyAuth_MissingKey(t *testing.T) {
	handler := APIKeyAuth("some-key")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called with missing key")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	// No X-API-Key header set
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", rr.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if body["error"] != "missing API key" {
		t.Fatalf("expected error 'missing API key', got %q", body["error"])
	}
}

func TestAPIKeyAuth_EmptyKey(t *testing.T) {
	handler := APIKeyAuth("some-key")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called with empty key")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", rr.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if body["error"] != "missing API key" {
		t.Fatalf("expected error 'missing API key', got %q", body["error"])
	}
}

// ---------------------------------------------------------------------------
// Correlation ID middleware tests
// ---------------------------------------------------------------------------

func TestCorrelationID_GeneratesNewID(t *testing.T) {
	var capturedID string

	handler := CorrelationID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = GetCorrelationID(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	// No X-Correlation-ID header
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if capturedID == "" {
		t.Fatal("expected a generated correlation ID, got empty string")
	}
	// UUID format: 8-4-4-4-12 hex chars with hyphens = 36 chars
	if len(capturedID) != 36 {
		t.Fatalf("expected UUID-length correlation ID (36 chars), got %d chars: %q", len(capturedID), capturedID)
	}
}

func TestCorrelationID_UsesExistingID(t *testing.T) {
	const existingID = "my-custom-correlation-id-123"
	var capturedID string

	handler := CorrelationID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = GetCorrelationID(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Correlation-ID", existingID)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if capturedID != existingID {
		t.Fatalf("expected correlation ID %q, got %q", existingID, capturedID)
	}

	if responseID := rr.Header().Get("X-Correlation-ID"); responseID != existingID {
		t.Fatalf("expected response header X-Correlation-ID %q, got %q", existingID, responseID)
	}
}

func TestCorrelationID_RejectsInvalidID(t *testing.T) {
	invalidIDs := []string{
		"has spaces in it",
		"has@special!chars",
		"id_with_underscore",
		"id.with.dots",
		"id/with/slashes",
	}

	for _, invalidID := range invalidIDs {
		t.Run(invalidID, func(t *testing.T) {
			var capturedID string

			handler := CorrelationID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedID = GetCorrelationID(r.Context())
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("X-Correlation-ID", invalidID)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			if capturedID == invalidID {
				t.Fatalf("invalid correlation ID %q should have been rejected", invalidID)
			}
			if capturedID == "" {
				t.Fatal("expected a generated correlation ID, got empty string")
			}
			// Should be a generated UUID
			if len(capturedID) != 36 {
				t.Fatalf("expected UUID-length replacement (36 chars), got %d chars: %q", len(capturedID), capturedID)
			}

			_ = rr // avoid unused variable warning
		})
	}
}

func TestCorrelationID_RejectsTooLongID(t *testing.T) {
	// Create a valid-chars string that exceeds 64 characters
	longID := strings.Repeat("a", 65)
	var capturedID string

	handler := CorrelationID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = GetCorrelationID(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Correlation-ID", longID)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if capturedID == longID {
		t.Fatal("too-long correlation ID should have been rejected")
	}
	if capturedID == "" {
		t.Fatal("expected a generated correlation ID, got empty string")
	}
	if len(capturedID) != 36 {
		t.Fatalf("expected UUID-length replacement (36 chars), got %d chars: %q", len(capturedID), capturedID)
	}

	_ = rr
}

func TestCorrelationID_SetsResponseHeader(t *testing.T) {
	handler := CorrelationID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Test with no incoming header: should still set response header
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	responseID := rr.Header().Get("X-Correlation-ID")
	if responseID == "" {
		t.Fatal("expected X-Correlation-ID response header to be set")
	}
	if len(responseID) != 36 {
		t.Fatalf("expected UUID-format correlation ID in response, got %q", responseID)
	}

	// Test with valid incoming header: response should match
	const knownID = "known-valid-id"
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("X-Correlation-ID", knownID)
	rr2 := httptest.NewRecorder()

	handler.ServeHTTP(rr2, req2)

	if rr2.Header().Get("X-Correlation-ID") != knownID {
		t.Fatalf("expected response header to be %q, got %q", knownID, rr2.Header().Get("X-Correlation-ID"))
	}
}

func TestGetCorrelationID_FromContext(t *testing.T) {
	// Context with a correlation ID
	ctx := context.WithValue(context.Background(), tracing.CorrelationIDKey, "test-id-456")
	got := GetCorrelationID(ctx)
	if got != "test-id-456" {
		t.Fatalf("expected 'test-id-456', got %q", got)
	}

	// Context without a correlation ID
	emptyCtx := context.Background()
	got = GetCorrelationID(emptyCtx)
	if got != "" {
		t.Fatalf("expected empty string for context without correlation ID, got %q", got)
	}

	// Context with wrong type value
	wrongCtx := context.WithValue(context.Background(), tracing.CorrelationIDKey, 12345)
	got = GetCorrelationID(wrongCtx)
	if got != "" {
		t.Fatalf("expected empty string for context with wrong type value, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Body size middleware tests
// ---------------------------------------------------------------------------

func TestMaxBodySize_WithinLimit(t *testing.T) {
	const maxBytes int64 = 1024 // 1 KB

	var bodyRead []byte
	handler := MaxBodySize(maxBytes)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		bodyRead, err = io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	smallBody := bytes.Repeat([]byte("a"), 512) // 512 bytes, within limit
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(smallBody))
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if len(bodyRead) != 512 {
		t.Fatalf("expected to read 512 bytes, got %d", len(bodyRead))
	}
}

func TestMaxBodySize_ExceedsLimit(t *testing.T) {
	const maxBytes int64 = 1024 // 1 KB

	handler := MaxBodySize(maxBytes)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		if err != nil {
			// http.MaxBytesReader returns an error when the limit is exceeded.
			// The handler sees a read error. The exact HTTP status depends on the handler;
			// here we verify the error occurs.
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	largeBody := bytes.Repeat([]byte("a"), 2048) // 2 KB, exceeds limit
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(largeBody))
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// Recovery middleware tests
// ---------------------------------------------------------------------------

func TestRecovery_NoPanic(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	handler := Recovery(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("all good"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if rr.Body.String() != "all good" {
		t.Fatalf("expected body 'all good', got %q", rr.Body.String())
	}
}

func TestRecovery_CatchesPanic(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	handler := Recovery(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("something went terribly wrong")
	}))

	// Add a correlation ID to the context so we can verify it appears in the response
	ctx := context.WithValue(context.Background(), tracing.CorrelationIDKey, "panic-corr-id")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rr.Code)
	}

	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type 'application/json', got %q", ct)
	}

	var resp domain.APIResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if resp.Success {
		t.Fatal("expected Success to be false")
	}
	if resp.Error == nil {
		t.Fatal("expected Error to be non-nil")
	}
	if resp.Error.Code != "INTERNAL_ERROR" {
		t.Fatalf("expected error code 'INTERNAL_ERROR', got %q", resp.Error.Code)
	}
	if resp.Error.Message != "an unexpected error occurred" {
		t.Fatalf("expected error message 'an unexpected error occurred', got %q", resp.Error.Message)
	}
	if resp.CorrelationID != "panic-corr-id" {
		t.Fatalf("expected correlation ID 'panic-corr-id', got %q", resp.CorrelationID)
	}
}

func TestRecovery_CatchesPanic_WithoutCorrelationID(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	handler := Recovery(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rr.Code)
	}

	var resp domain.APIResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if resp.Success {
		t.Fatal("expected Success to be false")
	}
	if resp.Error == nil {
		t.Fatal("expected Error to be non-nil")
	}
	// CorrelationID should be empty string since no correlation ID was set
	if resp.CorrelationID != "" {
		t.Fatalf("expected empty correlation ID, got %q", resp.CorrelationID)
	}
}
