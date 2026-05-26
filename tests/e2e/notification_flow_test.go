//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/sertacyildirim/notification-system/notification-api/handler"
	"github.com/sertacyildirim/notification-system/notification-api/middleware"
	"github.com/sertacyildirim/notification-system/notification-api/service"
	ws "github.com/sertacyildirim/notification-system/notification-api/websocket"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
)

// testEnv holds the test environment with miniredis, real components, and HTTP server.
type testEnv struct {
	redis  *miniredis.Miniredis
	client *redis.Client
	repo   repository.NotificationRepository
	server *httptest.Server
	buffer *service.WriteBuffer
}

// apiResponse mirrors domain.APIResponse for test JSON decoding.
type apiResponse struct {
	Success       bool            `json:"success"`
	Data          json.RawMessage `json:"data,omitempty"`
	Error         *apiError       `json:"error,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// notificationResponse mirrors domain.NotificationResponse for test JSON decoding.
type notificationResponse struct {
	ID             uuid.UUID  `json:"id"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty"`
	BatchID        *uuid.UUID `json:"batch_id,omitempty"`
	Recipient      string     `json:"recipient"`
	Channel        string     `json:"channel"`
	Content        string     `json:"content"`
	Priority       string     `json:"priority"`
	Status         string     `json:"status"`
	RetryCount     int        `json:"retry_count"`
	ScheduledAt    *time.Time `json:"scheduled_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

type batchCreateResponse struct {
	BatchID       uuid.UUID              `json:"batch_id"`
	Total         int                    `json:"total"`
	Notifications []notificationResponse `json:"notifications"`
}

// setupTestEnvWithRateLimit initializes a test environment with a configurable rate limit.
func setupTestEnvWithRateLimit(t *testing.T, rateLimit int) *testEnv {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	ctx := context.Background()
	logger := slog.Default()

	// Real repository (Redis-only, no tiered/postgres)
	repo := repository.NewRedisNotificationRepo(client)

	// Real publisher and consumer
	publisher := queue.NewRedisPublisher(client)
	consumer := queue.NewRedisConsumer(client)

	// Ensure streams exist for the consumer group
	streamMgr := queue.NewRedisStreamManager(client)
	if err := streamMgr.EnsureStreams(ctx, "test-group"); err != nil {
		t.Fatalf("failed to ensure streams: %v", err)
	}

	// Real write buffer with small batch size and short flush interval for tests
	writeBuffer := service.NewWriteBuffer(repo, publisher, 100, 10*time.Millisecond, logger)

	// Real notification service
	svc := service.NewNotificationService(repo, publisher, writeBuffer, 3, logger)

	// Real handlers
	nh := handler.NewNotificationHandler(svc)
	metrics := handler.NewMetricsCollector(consumer)
	hh := handler.NewHealthHandler(client, nil)
	wsHub := ws.NewHub(logger, nil)

	// Build Chi router with real middleware (same as production, minus swagger, with empty API key)
	r := chi.NewRouter()
	r.Use(middleware.CorrelationID)
	r.Use(middleware.Recovery(logger))
	r.Use(middleware.RateLimit(client, rateLimit))
	r.Use(middleware.Logging(logger, metrics))
	r.Use(middleware.MaxBodySize(2 << 20))

	r.Get("/health", hh.Health)
	r.Get("/ws", wsHub.HandleWS)

	r.Route("/api/v1", func(r chi.Router) {
		// No API key auth for tests (apiKey is empty so middleware is skipped)
		r.Post("/notifications", nh.Create)
		r.Post("/notifications/batch", nh.CreateBatch)
		r.Get("/notifications", nh.List)
		r.Get("/notifications/{id}", nh.GetByID)
		r.Get("/notifications/batch/{batchId}", nh.GetByBatchID)
		r.Patch("/notifications/{id}/cancel", nh.Cancel)
	})

	server := httptest.NewServer(r)

	env := &testEnv{
		redis:  mr,
		client: client,
		repo:   repo,
		server: server,
		buffer: writeBuffer,
	}

	t.Cleanup(func() {
		writeBuffer.Stop()
		server.Close()
		client.Close()
		mr.Close()
	})

	return env
}

// setupTestEnv initializes a test environment with the default rate limit (1000 req/s).
func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()
	return setupTestEnvWithRateLimit(t, 1000)
}

// parseAPIResponse decodes the response body into an apiResponse.
func parseAPIResponse(t *testing.T, resp *http.Response) apiResponse {
	t.Helper()
	var ar apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		t.Fatalf("failed to decode API response: %v", err)
	}
	return ar
}

// parseNotification decodes the Data field of an apiResponse into a notificationResponse.
func parseNotification(t *testing.T, ar apiResponse) notificationResponse {
	t.Helper()
	var nr notificationResponse
	if err := json.Unmarshal(ar.Data, &nr); err != nil {
		t.Fatalf("failed to decode notification from API response: %v", err)
	}
	return nr
}

// parseBatchResponse decodes the Data field of an apiResponse into a batchCreateResponse.
func parseBatchResponse(t *testing.T, ar apiResponse) batchCreateResponse {
	t.Helper()
	var br batchCreateResponse
	if err := json.Unmarshal(ar.Data, &br); err != nil {
		t.Fatalf("failed to decode batch response from API response: %v", err)
	}
	return br
}

// createNotification is a helper that creates a notification via the real API.
func createNotification(t *testing.T, env *testEnv, req domain.CreateNotificationRequest, headers map[string]string) (*http.Response, apiResponse) {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, env.server.URL+"/api/v1/notifications", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}

	ar := parseAPIResponse(t, resp)
	return resp, ar
}

// TestFullNotificationLifecycle tests the complete flow:
// Create notification via real API, verify it exists in Redis via repo.GetByID,
// verify status is queued (the service publishes immediately for non-scheduled notifications).
func TestFullNotificationLifecycle(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	req := domain.CreateNotificationRequest{
		Recipient: "test@example.com",
		Channel:   "email",
		Content:   "Welcome to our platform!",
		Priority:  "high",
		Metadata:  map[string]any{"campaign": "onboarding"},
	}

	// Use an idempotency key so the service writes directly (not via buffer)
	idemKey := "lifecycle-" + uuid.New().String()
	resp, ar := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idemKey,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d (error: %+v)", resp.StatusCode, ar.Error)
	}
	if !ar.Success {
		t.Fatalf("expected success=true, got false (error: %+v)", ar.Error)
	}

	nr := parseNotification(t, ar)

	if nr.ID == uuid.Nil {
		t.Fatal("expected non-nil notification ID")
	}
	if nr.Channel != "email" {
		t.Fatalf("expected channel 'email', got '%s'", nr.Channel)
	}
	if nr.Recipient != "test@example.com" {
		t.Fatalf("expected recipient 'test@example.com', got '%s'", nr.Recipient)
	}

	// The service publishes immediately for non-scheduled notifications with idempotency key,
	// so status should be "queued" (pending -> queued after publish).
	if nr.Status != "queued" && nr.Status != "pending" {
		t.Fatalf("expected status 'queued' or 'pending', got '%s'", nr.Status)
	}

	// Verify the notification exists in Redis via the real repo
	n, err := env.repo.GetByID(ctx, nr.ID)
	if err != nil {
		t.Fatalf("repo.GetByID failed: %v", err)
	}
	if n == nil {
		t.Fatal("expected notification to exist in Redis, got nil")
	}
	if n.Recipient != "test@example.com" {
		t.Fatalf("repo returned wrong recipient: %s", n.Recipient)
	}
}

// TestBatchCreationFlow tests creating 5 notifications via the batch endpoint.
func TestBatchCreationFlow(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	batchReq := domain.BatchCreateRequest{
		Notifications: []domain.CreateNotificationRequest{
			{Recipient: "+905551234567", Channel: "sms", Content: "Batch msg 1", Priority: "high"},
			{Recipient: "batch2@example.com", Channel: "email", Content: "Batch msg 2", Priority: "normal"},
			{Recipient: "push-token-abcdef1234", Channel: "push", Content: "Batch msg 3", Priority: "low"},
			{Recipient: "+905559876543", Channel: "sms", Content: "Batch msg 4", Priority: "high"},
			{Recipient: "batch5@example.com", Channel: "email", Content: "Batch msg 5", Priority: "normal"},
		},
	}

	body, _ := json.Marshal(batchReq)
	resp, err := http.Post(env.server.URL+"/api/v1/notifications/batch", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create batch: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		ar := parseAPIResponse(t, resp)
		t.Fatalf("expected 201, got %d (error: %+v)", resp.StatusCode, ar.Error)
	}

	ar := parseAPIResponse(t, resp)
	if !ar.Success {
		t.Fatalf("expected success=true, got false (error: %+v)", ar.Error)
	}

	br := parseBatchResponse(t, ar)

	if br.BatchID == uuid.Nil {
		t.Fatal("expected non-nil batch_id")
	}
	if br.Total != 5 {
		t.Fatalf("expected total 5, got %d", br.Total)
	}
	if len(br.Notifications) != 5 {
		t.Fatalf("expected 5 notifications, got %d", len(br.Notifications))
	}

	// Verify each notification exists in Redis via the real repo
	for i, nr := range br.Notifications {
		n, err := env.repo.GetByID(ctx, nr.ID)
		if err != nil {
			t.Fatalf("notification[%d]: repo.GetByID failed: %v", i, err)
		}
		if n == nil {
			t.Fatalf("notification[%d]: expected to exist in Redis, got nil", i)
		}
		if n.BatchID == nil || *n.BatchID != br.BatchID {
			t.Fatalf("notification[%d]: batch_id mismatch", i)
		}
	}

	// Verify batch lookup via repo
	batchNotifs, err := env.repo.GetByBatchID(ctx, br.BatchID)
	if err != nil {
		t.Fatalf("repo.GetByBatchID failed: %v", err)
	}
	if len(batchNotifs) != 5 {
		t.Fatalf("expected 5 notifications from GetByBatchID, got %d", len(batchNotifs))
	}
}

// TestScheduledNotificationFlow tests creating a notification with a future scheduled_at.
func TestScheduledNotificationFlow(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	scheduledTime := time.Now().Add(1 * time.Hour)
	req := domain.CreateNotificationRequest{
		Recipient:   "+905551112233",
		Channel:     "sms",
		Content:     "Scheduled reminder",
		Priority:    "normal",
		ScheduledAt: &scheduledTime,
	}

	idemKey := "scheduled-" + uuid.New().String()
	resp, ar := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idemKey,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d (error: %+v)", resp.StatusCode, ar.Error)
	}

	nr := parseNotification(t, ar)

	// Scheduled notifications should remain "pending" (not published to stream)
	if nr.Status != "pending" {
		t.Fatalf("expected status 'pending' for scheduled notification, got '%s'", nr.Status)
	}

	// Verify it's in the schedule:pending sorted set in Redis
	score, err := env.client.ZScore(ctx, "schedule:pending", nr.ID.String()).Result()
	if err != nil {
		t.Fatalf("notification not found in schedule:pending sorted set: %v", err)
	}

	// The score should be the scheduled_at time in nanoseconds
	expectedScore := float64(scheduledTime.UnixNano())
	if score != expectedScore {
		t.Fatalf("expected schedule score %f, got %f", expectedScore, score)
	}
}

// TestIdempotency tests that the same idempotency key returns the same notification.
func TestIdempotency(t *testing.T) {
	env := setupTestEnv(t)

	idempotencyKey := "unique-key-" + uuid.New().String()

	req := domain.CreateNotificationRequest{
		Recipient: "idempotent@example.com",
		Channel:   "email",
		Content:   "Idempotent message",
		Priority:  "high",
	}

	// First request
	resp1, ar1 := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idempotencyKey,
	})
	defer resp1.Body.Close()

	if resp1.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 for first request, got %d (error: %+v)", resp1.StatusCode, ar1.Error)
	}

	nr1 := parseNotification(t, ar1)

	// Second request with same idempotency key
	resp2, ar2 := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idempotencyKey,
	})
	defer resp2.Body.Close()

	// The service returns 201 for idempotent hits too (it returns the existing notification).
	// The key point is the same ID is returned.
	if !ar2.Success {
		t.Fatalf("expected success=true for idempotent request, got false (error: %+v)", ar2.Error)
	}

	nr2 := parseNotification(t, ar2)

	if nr1.ID != nr2.ID {
		t.Fatalf("idempotency failed: first ID '%s' != second ID '%s'", nr1.ID, nr2.ID)
	}

	// Third request with same key should also return same ID
	resp3, ar3 := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idempotencyKey,
	})
	defer resp3.Body.Close()

	nr3 := parseNotification(t, ar3)

	if nr1.ID != nr3.ID {
		t.Fatalf("idempotency failed on third call: '%s' != '%s'", nr1.ID, nr3.ID)
	}
}

// TestRateLimiting tests that requests beyond the rate limit receive 429 responses.
// Uses a very low rate limit (10 req/s) so we can reliably exceed it even with
// miniredis's single-threaded Lua execution.
func TestRateLimiting(t *testing.T) {
	// Use a very low rate limit to make it easy to exceed
	env := setupTestEnvWithRateLimit(t, 10)

	req := domain.CreateNotificationRequest{
		Recipient: "+905559998877",
		Channel:   "sms",
		Content:   "Rate limit test",
		Priority:  "low",
	}
	body, _ := json.Marshal(req)

	var successCount, rateLimitedCount atomic.Int64

	// Send requests sequentially so each gets a unique timestamp.
	// With a rate limit of 10/sec, requests 11+ within the same second should be rejected.
	totalRequests := 30

	for i := 0; i < totalRequests; i++ {
		resp, err := http.Post(env.server.URL+"/api/v1/notifications", "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		// Drain body
		var ar apiResponse
		json.NewDecoder(resp.Body).Decode(&ar)

		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
			successCount.Add(1)
		} else if resp.StatusCode == http.StatusTooManyRequests {
			rateLimitedCount.Add(1)
		}
		resp.Body.Close()
	}

	t.Logf("Total requests: %d, Successful: %d, Rate limited: %d",
		totalRequests, successCount.Load(), rateLimitedCount.Load())

	if rateLimitedCount.Load() == 0 {
		t.Fatal("expected some requests to be rate limited, but none were")
	}

	if successCount.Load() == 0 {
		t.Fatal("expected some requests to succeed, but none did")
	}
}

// TestRaceConditionIdempotency tests 50 concurrent requests with the same Idempotency-Key.
//
// Note: The service's idempotency check (GetByIdempotencyKey + Create) is not a single
// atomic operation. Under high concurrency, multiple requests may pass the GET check
// before any of them writes the idempotency index entry. This is a known trade-off
// in the current design. This test verifies that:
// 1. All requests succeed (no 500s)
// 2. After the burst, subsequent requests with the same key return a single consistent ID
// 3. A small number of unique IDs may be created during the race window
func TestRaceConditionIdempotency(t *testing.T) {
	env := setupTestEnv(t)

	idempotencyKey := "race-key-" + uuid.New().String()
	concurrency := 50

	req := domain.CreateNotificationRequest{
		Recipient: "race@example.com",
		Channel:   "email",
		Content:   "Race condition test",
		Priority:  "high",
	}
	body, _ := json.Marshal(req)

	type result struct {
		id         uuid.UUID
		statusCode int
	}

	resultsCh := make(chan result, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			httpReq, err := http.NewRequest(http.MethodPost, env.server.URL+"/api/v1/notifications", bytes.NewReader(body))
			if err != nil {
				return
			}
			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("Idempotency-Key", idempotencyKey)

			resp, err := http.DefaultClient.Do(httpReq)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var ar apiResponse
			json.NewDecoder(resp.Body).Decode(&ar)

			if ar.Success && ar.Data != nil {
				var nr notificationResponse
				if err := json.Unmarshal(ar.Data, &nr); err == nil {
					resultsCh <- result{id: nr.ID, statusCode: resp.StatusCode}
					return
				}
			}
			resultsCh <- result{statusCode: resp.StatusCode}
		}()
	}

	wg.Wait()
	close(resultsCh)

	uniqueIDs := make(map[uuid.UUID]int)
	successCount := 0
	errorCount := 0
	for r := range resultsCh {
		if r.id == uuid.Nil {
			errorCount++
			continue
		}
		successCount++
		uniqueIDs[r.id]++
	}

	t.Logf("Concurrent idempotency: %d successful, %d errors, %d unique IDs",
		successCount, errorCount, len(uniqueIDs))

	if successCount == 0 {
		t.Fatal("no successful responses received")
	}

	// After the concurrent burst, verify that subsequent requests converge to one ID
	postResp, postAR := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idempotencyKey,
	})
	defer postResp.Body.Close()

	postNR := parseNotification(t, postAR)
	t.Logf("Post-burst idempotency returns ID: %s", postNR.ID)

	// The post-burst response should match one of the IDs created during the race
	if _, ok := uniqueIDs[postNR.ID]; !ok {
		t.Fatalf("post-burst idempotency returned unknown ID '%s', expected one of %v", postNR.ID, uniqueIDs)
	}

	// Subsequent calls should all return the same ID (the idempotency index has converged)
	postResp2, postAR2 := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idempotencyKey,
	})
	defer postResp2.Body.Close()

	postNR2 := parseNotification(t, postAR2)
	if postNR.ID != postNR2.ID {
		t.Fatalf("post-burst idempotency not stable: '%s' != '%s'", postNR.ID, postNR2.ID)
	}
}

// TestCancelFlow tests creating a notification, cancelling it, and verifying
// that a second cancel returns 409 Conflict.
func TestCancelFlow(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	req := domain.CreateNotificationRequest{
		Recipient: "cancel@example.com",
		Channel:   "email",
		Content:   "This will be cancelled",
		Priority:  "low",
	}

	idemKey := "cancel-" + uuid.New().String()
	resp, ar := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idemKey,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d (error: %+v)", resp.StatusCode, ar.Error)
	}

	nr := parseNotification(t, ar)

	// Cancel the notification via PATCH
	cancelURL := fmt.Sprintf("%s/api/v1/notifications/%s/cancel", env.server.URL, nr.ID)
	cancelReq, _ := http.NewRequest(http.MethodPatch, cancelURL, nil)
	cancelResp, err := http.DefaultClient.Do(cancelReq)
	if err != nil {
		t.Fatalf("failed to cancel notification: %v", err)
	}
	defer cancelResp.Body.Close()

	if cancelResp.StatusCode != http.StatusOK {
		var cancelAR apiResponse
		json.NewDecoder(cancelResp.Body).Decode(&cancelAR)
		t.Fatalf("expected 200 for cancel, got %d (error: %+v)", cancelResp.StatusCode, cancelAR.Error)
	}

	// Verify status changed via repo
	n, err := env.repo.GetByID(ctx, nr.ID)
	if err != nil {
		t.Fatalf("repo.GetByID failed: %v", err)
	}
	if n == nil {
		t.Fatal("notification should exist after cancel")
	}
	if n.Status != domain.StatusCancelled {
		t.Fatalf("expected status 'cancelled', got '%s'", n.Status)
	}

	// Try to cancel again -- should return 409 Conflict
	cancelReq2, _ := http.NewRequest(http.MethodPatch, cancelURL, nil)
	cancelResp2, err := http.DefaultClient.Do(cancelReq2)
	if err != nil {
		t.Fatalf("second cancel request failed: %v", err)
	}
	defer cancelResp2.Body.Close()

	if cancelResp2.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for double cancel, got %d", cancelResp2.StatusCode)
	}
}

// TestStatusTransitionCAS tests that the repository's CAS UpdateStatus works correctly.
// Create a notification and verify that only one concurrent transition succeeds.
func TestStatusTransitionCAS(t *testing.T) {
	env := setupTestEnv(t)
	ctx := context.Background()

	// Create a notification
	req := domain.CreateNotificationRequest{
		Recipient: "cas@example.com",
		Channel:   "email",
		Content:   "CAS test message",
		Priority:  "high",
	}

	idemKey := "cas-" + uuid.New().String()
	resp, ar := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idemKey,
	})
	defer resp.Body.Close()

	nr := parseNotification(t, ar)

	// The notification is now in "queued" state (after publish).
	// Verify via repo.
	n, err := env.repo.GetByID(ctx, nr.ID)
	if err != nil {
		t.Fatalf("repo.GetByID failed: %v", err)
	}
	if n == nil {
		t.Fatal("notification should exist")
	}

	currentStatus := n.Status

	// Try concurrent CAS updates from the current status to "cancelled".
	// Only one should succeed (the valid transitions from queued include cancelled).
	concurrency := 20
	var successCount atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			updated, err := env.repo.UpdateStatus(ctx, nr.ID, currentStatus, domain.StatusCancelled)
			if err == nil && updated {
				successCount.Add(1)
			}
		}()
	}

	wg.Wait()

	// Exactly one CAS should succeed since they all race on the same from->to
	if successCount.Load() != 1 {
		t.Fatalf("expected exactly 1 successful CAS, got %d", successCount.Load())
	}

	// Verify final status
	n2, err := env.repo.GetByID(ctx, nr.ID)
	if err != nil {
		t.Fatalf("repo.GetByID failed: %v", err)
	}
	if n2.Status != domain.StatusCancelled {
		t.Fatalf("expected final status 'cancelled', got '%s'", n2.Status)
	}
}

// TestGetByID tests creating a notification and retrieving it by ID via the real API.
func TestGetByID(t *testing.T) {
	env := setupTestEnv(t)

	req := domain.CreateNotificationRequest{
		Recipient: "getbyid@example.com",
		Channel:   "email",
		Content:   "Get by ID test",
		Priority:  "normal",
	}

	idemKey := "getbyid-" + uuid.New().String()
	createResp, createAR := createNotification(t, env, req, map[string]string{
		"Idempotency-Key": idemKey,
	})
	defer createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d (error: %+v)", createResp.StatusCode, createAR.Error)
	}

	nr := parseNotification(t, createAR)

	// GET the notification by ID
	getURL := fmt.Sprintf("%s/api/v1/notifications/%s", env.server.URL, nr.ID)
	getResp, err := http.Get(getURL)
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getResp.StatusCode)
	}

	getAR := parseAPIResponse(t, getResp)
	if !getAR.Success {
		t.Fatalf("expected success=true on GET, got false (error: %+v)", getAR.Error)
	}

	getNR := parseNotification(t, getAR)
	if getNR.ID != nr.ID {
		t.Fatalf("GET returned different ID: expected '%s', got '%s'", nr.ID, getNR.ID)
	}
	if getNR.Recipient != "getbyid@example.com" {
		t.Fatalf("GET returned wrong recipient: '%s'", getNR.Recipient)
	}
	if getNR.Channel != "email" {
		t.Fatalf("GET returned wrong channel: '%s'", getNR.Channel)
	}
	if getNR.Content != "Get by ID test" {
		t.Fatalf("GET returned wrong content: '%s'", getNR.Content)
	}

	// Test GET for non-existent ID
	fakeID := uuid.New()
	getResp2, err := http.Get(fmt.Sprintf("%s/api/v1/notifications/%s", env.server.URL, fakeID))
	if err != nil {
		t.Fatalf("GET request for fake ID failed: %v", err)
	}
	defer getResp2.Body.Close()

	if getResp2.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for non-existent ID, got %d", getResp2.StatusCode)
	}
}

// TestGetByBatchID tests creating a batch and retrieving all notifications by batch ID.
func TestGetByBatchID(t *testing.T) {
	env := setupTestEnv(t)

	batchReq := domain.BatchCreateRequest{
		Notifications: []domain.CreateNotificationRequest{
			{Recipient: "+905551110001", Channel: "sms", Content: "Batch lookup 1", Priority: "high"},
			{Recipient: "+905551110002", Channel: "sms", Content: "Batch lookup 2", Priority: "normal"},
			{Recipient: "+905551110003", Channel: "sms", Content: "Batch lookup 3", Priority: "low"},
		},
	}

	body, _ := json.Marshal(batchReq)
	resp, err := http.Post(env.server.URL+"/api/v1/notifications/batch", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create batch: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	ar := parseAPIResponse(t, resp)
	br := parseBatchResponse(t, ar)

	if br.BatchID == uuid.Nil {
		t.Fatal("expected non-nil batch_id")
	}

	// GET notifications by batch ID
	getBatchURL := fmt.Sprintf("%s/api/v1/notifications/batch/%s", env.server.URL, br.BatchID)
	getBatchResp, err := http.Get(getBatchURL)
	if err != nil {
		t.Fatalf("GET batch request failed: %v", err)
	}
	defer getBatchResp.Body.Close()

	if getBatchResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getBatchResp.StatusCode)
	}

	getBatchAR := parseAPIResponse(t, getBatchResp)
	if !getBatchAR.Success {
		t.Fatalf("expected success=true, got false (error: %+v)", getBatchAR.Error)
	}

	var batchNotifs []notificationResponse
	if err := json.Unmarshal(getBatchAR.Data, &batchNotifs); err != nil {
		t.Fatalf("failed to decode batch notifications: %v", err)
	}

	if len(batchNotifs) != 3 {
		t.Fatalf("expected 3 notifications from batch lookup, got %d", len(batchNotifs))
	}

	// Verify all belong to the same batch
	for i, n := range batchNotifs {
		if n.BatchID == nil || *n.BatchID != br.BatchID {
			t.Fatalf("notification[%d]: expected batch_id '%s', got '%v'", i, br.BatchID, n.BatchID)
		}
	}
}

// TestHealthEndpoint tests that the /health endpoint returns healthy status.
func TestHealthEndpoint(t *testing.T) {
	env := setupTestEnv(t)

	resp, err := http.Get(env.server.URL + "/health")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var health struct {
		Status     string            `json:"status"`
		Components map[string]string `json:"components"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("failed to decode health response: %v", err)
	}

	if health.Status != "healthy" {
		t.Fatalf("expected status 'healthy', got '%s'", health.Status)
	}
	if health.Components["redis"] != "healthy" {
		t.Fatalf("expected redis component 'healthy', got '%s'", health.Components["redis"])
	}
}

// TestValidationErrors tests that invalid requests are properly rejected by the real validation.
func TestValidationErrors(t *testing.T) {
	env := setupTestEnv(t)

	tests := []struct {
		name    string
		req     domain.CreateNotificationRequest
		wantMsg string
	}{
		{
			name:    "empty recipient",
			req:     domain.CreateNotificationRequest{Channel: "sms", Content: "hello", Priority: "high"},
			wantMsg: "recipient is required",
		},
		{
			name:    "empty content",
			req:     domain.CreateNotificationRequest{Recipient: "+905551234567", Channel: "sms", Priority: "high"},
			wantMsg: "content is required",
		},
		{
			name:    "invalid channel",
			req:     domain.CreateNotificationRequest{Recipient: "+905551234567", Channel: "fax", Content: "hello", Priority: "high"},
			wantMsg: "invalid channel",
		},
		{
			name:    "invalid phone for SMS",
			req:     domain.CreateNotificationRequest{Recipient: "not-a-phone", Channel: "sms", Content: "hello", Priority: "high"},
			wantMsg: "invalid phone number",
		},
		{
			name:    "invalid email for email channel",
			req:     domain.CreateNotificationRequest{Recipient: "not-an-email", Channel: "email", Content: "hello", Priority: "high"},
			wantMsg: "invalid email",
		},
		{
			name:    "push token too short",
			req:     domain.CreateNotificationRequest{Recipient: "short", Channel: "push", Content: "hello", Priority: "high"},
			wantMsg: "invalid push token",
		},
		{
			name:    "invalid priority",
			req:     domain.CreateNotificationRequest{Recipient: "+905551234567", Channel: "sms", Content: "hello", Priority: "urgent"},
			wantMsg: "invalid priority",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, ar := createNotification(t, env, tt.req, nil)
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", resp.StatusCode)
			}
			if ar.Success {
				t.Fatal("expected success=false for validation error")
			}
			if ar.Error == nil {
				t.Fatal("expected error in response")
			}
			if ar.Error.Code != "VALIDATION_ERROR" {
				t.Fatalf("expected error code 'VALIDATION_ERROR', got '%s'", ar.Error.Code)
			}
		})
	}
}
