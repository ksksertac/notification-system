package writer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/repository"
)

// ---------------------------------------------------------------------------
// Mock repository
// ---------------------------------------------------------------------------

type mockRepo struct {
	mu sync.Mutex

	createBatchCalls   [][]*domain.Notification
	updateStatusCalls  []updateStatusCall
	updateDetailsCalls []updateDetailsCall
	incrementRetryCalls []incrementRetryCall
	moveToDLQCalls     []moveToDLQCall
	getByIDResult      *domain.Notification
	getByIDErr         error
}

type updateStatusCall struct {
	ID   uuid.UUID
	From domain.Status
	To   domain.Status
}

type updateDetailsCall struct {
	ID            uuid.UUID
	From          domain.Status
	To            domain.Status
	ProviderMsgID *string
	ErrorMsg      *string
}

type incrementRetryCall struct {
	ID          uuid.UUID
	NextRetryAt time.Time
	ErrorMsg    string
}

type moveToDLQCall struct {
	Notification *domain.Notification
	ErrorMsg     string
}

func (m *mockRepo) Create(_ context.Context, _ *domain.Notification) error { return nil }

func (m *mockRepo) CreateBatch(_ context.Context, notifications []*domain.Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createBatchCalls = append(m.createBatchCalls, notifications)
	return nil
}

func (m *mockRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getByIDResult != nil {
		return m.getByIDResult, m.getByIDErr
	}
	return &domain.Notification{
		ID:        id,
		Recipient: "test@example.com",
		Channel:   domain.ChannelEmail,
		Content:   "test content",
		Status:    domain.StatusFailed,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}, m.getByIDErr
}

func (m *mockRepo) GetByBatchID(_ context.Context, _ uuid.UUID) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) GetByIdempotencyKey(_ context.Context, _ string) (*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) List(_ context.Context, _ domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	return nil, 0, nil
}

func (m *mockRepo) UpdateStatus(_ context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateStatusCalls = append(m.updateStatusCalls, updateStatusCall{ID: id, From: from, To: to})
	return true, nil
}

func (m *mockRepo) UpdateStatusWithDetails(_ context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateDetailsCalls = append(m.updateDetailsCalls, updateDetailsCall{
		ID: id, From: from, To: to, ProviderMsgID: providerMsgID, ErrorMsg: errorMsg,
	})
	return true, nil
}

func (m *mockRepo) IncrementRetry(_ context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incrementRetryCalls = append(m.incrementRetryCalls, incrementRetryCall{
		ID: id, NextRetryAt: nextRetryAt, ErrorMsg: errorMsg,
	})
	return nil
}

func (m *mockRepo) MoveToDLQ(_ context.Context, n *domain.Notification, errorMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.moveToDLQCalls = append(m.moveToDLQCalls, moveToDLQCall{Notification: n, ErrorMsg: errorMsg})
	return nil
}

func (m *mockRepo) GetScheduledReady(_ context.Context, _ int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) ClaimScheduledBatch(_ context.Context, _ int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) RecoverStuckQueued(_ context.Context, _ time.Duration, _ int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) GetRetryReady(_ context.Context, _ int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) RecoverStuckProcessing(_ context.Context, _ time.Duration, _ int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) RecoverOrphanedPending(_ context.Context, _ time.Duration, _ int) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *mockRepo) UpdateRequeueCount(_ context.Context, _ uuid.UUID, _ int) error {
	return nil
}
func (m *mockRepo) AddToRequeueSet(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}
func (m *mockRepo) GetRequeueReady(_ context.Context, _ int) ([]*domain.Notification, error) {
	return nil, nil
}

// Thread-safe getters for assertions

func (m *mockRepo) getCreateBatchCalls() [][]*domain.Notification {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([][]*domain.Notification, len(m.createBatchCalls))
	copy(cp, m.createBatchCalls)
	return cp
}

func (m *mockRepo) getUpdateStatusCalls() []updateStatusCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]updateStatusCall, len(m.updateStatusCalls))
	copy(cp, m.updateStatusCalls)
	return cp
}

func (m *mockRepo) getUpdateDetailsCalls() []updateDetailsCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]updateDetailsCall, len(m.updateDetailsCalls))
	copy(cp, m.updateDetailsCalls)
	return cp
}

func (m *mockRepo) getIncrementRetryCalls() []incrementRetryCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]incrementRetryCall, len(m.incrementRetryCalls))
	copy(cp, m.incrementRetryCalls)
	return cp
}

func (m *mockRepo) getMoveToDLQCalls() []moveToDLQCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]moveToDLQCall, len(m.moveToDLQCalls))
	copy(cp, m.moveToDLQCalls)
	return cp
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func setupMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	return mr, client
}

func testLogger() *slog.Logger {
	return slog.Default()
}

// addCreateEvent adds a "create" persist event to the stream.
func addCreateEvent(t *testing.T, client *redis.Client, n *domain.Notification) {
	t.Helper()
	evt := repository.PersistEvent{
		Action:       "create",
		Notification: n,
		Timestamp:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal create event: %v", err)
	}
	client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: persistStream,
		Values: map[string]interface{}{"event": string(data)},
	})
}

// addUpdateStatusEvent adds an "update_status" persist event to the stream.
func addUpdateStatusEvent(t *testing.T, client *redis.Client, id uuid.UUID, from, to domain.Status) {
	t.Helper()
	evt := repository.PersistEvent{
		Action: "update_status",
		Extra: map[string]string{
			"id":   id.String(),
			"from": string(from),
			"to":   string(to),
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal update_status event: %v", err)
	}
	client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: persistStream,
		Values: map[string]interface{}{"event": string(data)},
	})
}

// addUpdateStatusDetailsEvent adds an "update_status_details" persist event.
func addUpdateStatusDetailsEvent(t *testing.T, client *redis.Client, id uuid.UUID, from, to domain.Status, providerMsgID, errorMsg string) {
	t.Helper()
	extra := map[string]string{
		"id":   id.String(),
		"from": string(from),
		"to":   string(to),
	}
	if providerMsgID != "" {
		extra["provider_msg_id"] = providerMsgID
	}
	if errorMsg != "" {
		extra["error_message"] = errorMsg
	}
	evt := repository.PersistEvent{
		Action:    "update_status_details",
		Extra:     extra,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal update_status_details event: %v", err)
	}
	client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: persistStream,
		Values: map[string]interface{}{"event": string(data)},
	})
}

// addIncrementRetryEvent adds an "increment_retry" persist event.
func addIncrementRetryEvent(t *testing.T, client *redis.Client, id uuid.UUID, nextRetryAt time.Time, errorMsg string) {
	t.Helper()
	evt := repository.PersistEvent{
		Action: "increment_retry",
		Extra: map[string]string{
			"id":            id.String(),
			"next_retry_at": nextRetryAt.Format(time.RFC3339Nano),
			"error_message": errorMsg,
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal increment_retry event: %v", err)
	}
	client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: persistStream,
		Values: map[string]interface{}{"event": string(data)},
	})
}

// addMoveToDLQEvent adds a "move_to_dlq" persist event.
func addMoveToDLQEvent(t *testing.T, client *redis.Client, id uuid.UUID, errorMsg string) {
	t.Helper()
	evt := repository.PersistEvent{
		Action: "move_to_dlq",
		Extra: map[string]string{
			"id":            id.String(),
			"error_message": errorMsg,
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal move_to_dlq event: %v", err)
	}
	client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: persistStream,
		Values: map[string]interface{}{"event": string(data)},
	})
}

func makeNotification(id uuid.UUID) *domain.Notification {
	now := time.Now().UTC()
	return &domain.Notification{
		ID:        id,
		Recipient: "user@example.com",
		Channel:   domain.ChannelEmail,
		Content:   "Hello World",
		Priority:  domain.PriorityNormal,
		Status:    domain.StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// waitFor polls a condition with a timeout.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

// ---------------------------------------------------------------------------
// Tests: Writer creation
// ---------------------------------------------------------------------------

func TestNew_Defaults(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	w := New(client, repo, 0, 0, testLogger())

	if w.batchSize != 500 {
		t.Errorf("expected default batchSize=500, got %d", w.batchSize)
	}
	if w.flushInterval != 100*time.Millisecond {
		t.Errorf("expected default flushInterval=100ms, got %v", w.flushInterval)
	}
}

func TestNew_CustomValues(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	w := New(client, repo, 250, 5*time.Second, testLogger())

	if w.batchSize != 250 {
		t.Errorf("expected batchSize=250, got %d", w.batchSize)
	}
	if w.flushInterval != 5*time.Second {
		t.Errorf("expected flushInterval=5s, got %v", w.flushInterval)
	}
}

// ---------------------------------------------------------------------------
// Tests: Core functionality
// ---------------------------------------------------------------------------

func TestWriter_ProcessesMessages(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	// Use small batch and fast flush so messages are processed quickly.
	w := New(client, repo, 10, 50*time.Millisecond, testLogger())

	// Add a create event and an update_status event.
	notifID := uuid.New()
	n := makeNotification(notifID)
	addCreateEvent(t, client, n)

	updateID := uuid.New()
	addUpdateStatusEvent(t, client, updateID, domain.StatusQueued, domain.StatusProcessing)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// Wait for the writer to process messages.
	waitFor(t, 2*time.Second, func() bool {
		return len(repo.getCreateBatchCalls()) > 0 && len(repo.getUpdateStatusCalls()) > 0
	}, "create and update_status events processed")

	cancel()
	w.Stop()

	// Verify create was batched.
	creates := repo.getCreateBatchCalls()
	totalCreated := 0
	for _, batch := range creates {
		totalCreated += len(batch)
	}
	if totalCreated != 1 {
		t.Errorf("expected 1 create, got %d", totalCreated)
	}

	// Verify update_status was called.
	updates := repo.getUpdateStatusCalls()
	if len(updates) != 1 {
		t.Fatalf("expected 1 update_status call, got %d", len(updates))
	}
	if updates[0].ID != updateID {
		t.Errorf("expected update ID %s, got %s", updateID, updates[0].ID)
	}
	if updates[0].From != domain.StatusQueued || updates[0].To != domain.StatusProcessing {
		t.Errorf("expected queued->processing, got %s->%s", updates[0].From, updates[0].To)
	}
}

func TestWriter_BatchFlush(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	batchSize := 5
	// Use a long flush interval so only batch-size triggers the flush.
	w := New(client, repo, batchSize, 10*time.Second, testLogger())

	// Add exactly batchSize create events.
	for i := 0; i < batchSize; i++ {
		addCreateEvent(t, client, makeNotification(uuid.New()))
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// The writer's ticker fires at flushInterval (10s), but even at tick time
	// the batch should be flushed when pending >= batchSize.
	// We need at least one tick to read messages. Use a shorter interval.
	cancel()
	w.Stop()

	// Since the flush interval is 10s, the ticker won't have fired yet.
	// Recreate with shorter interval to actually test batching by size.
	repo2 := &mockRepo{}
	w2 := New(client, repo2, batchSize, 50*time.Millisecond, testLogger())

	// Re-add events (previous ones were consumed or the stream was read).
	for i := 0; i < batchSize; i++ {
		addCreateEvent(t, client, makeNotification(uuid.New()))
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	w2.Start(ctx2)

	waitFor(t, 3*time.Second, func() bool {
		creates := repo2.getCreateBatchCalls()
		total := 0
		for _, batch := range creates {
			total += len(batch)
		}
		return total >= batchSize
	}, "batch flush triggered by batch size")

	cancel2()
	w2.Stop()

	creates := repo2.getCreateBatchCalls()
	total := 0
	for _, batch := range creates {
		total += len(batch)
	}
	if total < batchSize {
		t.Errorf("expected at least %d creates, got %d", batchSize, total)
	}
}

func TestWriter_FlushOnInterval(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	// Large batch size so it won't trigger by count; short interval to trigger flush.
	w := New(client, repo, 1000, 50*time.Millisecond, testLogger())

	// Add only 2 messages -- well below batch size.
	addCreateEvent(t, client, makeNotification(uuid.New()))
	addCreateEvent(t, client, makeNotification(uuid.New()))

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	waitFor(t, 2*time.Second, func() bool {
		creates := repo.getCreateBatchCalls()
		total := 0
		for _, batch := range creates {
			total += len(batch)
		}
		return total >= 2
	}, "flush on interval with partial batch")

	cancel()
	w.Stop()

	creates := repo.getCreateBatchCalls()
	total := 0
	for _, batch := range creates {
		total += len(batch)
	}
	if total != 2 {
		t.Errorf("expected 2 creates flushed on interval, got %d", total)
	}
}

func TestWriter_HandleInvalidEvents(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	w := New(client, repo, 10, 50*time.Millisecond, testLogger())

	// Add an invalid event (not valid JSON in "event" field).
	client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: persistStream,
		Values: map[string]interface{}{"event": "this is not json"},
	})

	// Also add a valid create event after the invalid one.
	validID := uuid.New()
	addCreateEvent(t, client, makeNotification(validID))

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// The writer should process the valid event without crashing.
	waitFor(t, 2*time.Second, func() bool {
		creates := repo.getCreateBatchCalls()
		total := 0
		for _, batch := range creates {
			total += len(batch)
		}
		return total >= 1
	}, "valid event processed after invalid event")

	cancel()
	w.Stop()

	// Verify exactly the valid notification was persisted.
	creates := repo.getCreateBatchCalls()
	total := 0
	for _, batch := range creates {
		total += len(batch)
	}
	if total != 1 {
		t.Errorf("expected 1 create (skipping invalid), got %d", total)
	}
}

func TestWriter_GracefulShutdown(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	// Long flush interval so the only flush happens on shutdown.
	w := New(client, repo, 1000, 10*time.Second, testLogger())

	addCreateEvent(t, client, makeNotification(uuid.New()))

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// Give the writer time to read the message into its pending buffer.
	// The ticker fires at flushInterval so we need at least one tick.
	// Since flushInterval is 10s, the message gets read at the first tick.
	// We can't wait 10s, so let's use a shorter interval.
	cancel()
	w.Stop()

	// With a 10s flush interval the ticker likely hasn't fired before cancel.
	// The graceful shutdown path in run() calls flush on pending messages.
	// However, if the ticker hasn't fired, pending is empty since readBatch
	// is only called inside the ticker case.
	//
	// Let's redo with a fast interval to ensure the message is read first,
	// then test that cancel triggers a final flush.

	repo2 := &mockRepo{}
	w2 := New(client, repo2, 1000, 50*time.Millisecond, testLogger())

	addCreateEvent(t, client, makeNotification(uuid.New()))

	ctx2, cancel2 := context.WithCancel(context.Background())
	w2.Start(ctx2)

	// Let one tick elapse to read the message into pending.
	time.Sleep(100 * time.Millisecond)

	// Cancel -- this should trigger the final flush in the ctx.Done case.
	cancel2()
	w2.Stop()

	creates := repo2.getCreateBatchCalls()
	total := 0
	for _, batch := range creates {
		total += len(batch)
	}
	if total < 1 {
		t.Errorf("expected at least 1 create on graceful shutdown, got %d", total)
	}
}

// ---------------------------------------------------------------------------
// Tests: Different update event types
// ---------------------------------------------------------------------------

func TestWriter_ProcessesUpdateStatusDetails(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	w := New(client, repo, 10, 50*time.Millisecond, testLogger())

	id := uuid.New()
	addUpdateStatusDetailsEvent(t, client, id, domain.StatusProcessing, domain.StatusDelivered, "msg-123", "")

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	waitFor(t, 2*time.Second, func() bool {
		return len(repo.getUpdateDetailsCalls()) > 0
	}, "update_status_details processed")

	cancel()
	w.Stop()

	calls := repo.getUpdateDetailsCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 update_status_details call, got %d", len(calls))
	}
	if calls[0].ID != id {
		t.Errorf("expected ID %s, got %s", id, calls[0].ID)
	}
	if calls[0].From != domain.StatusProcessing || calls[0].To != domain.StatusDelivered {
		t.Errorf("unexpected status transition: %s->%s", calls[0].From, calls[0].To)
	}
	if calls[0].ProviderMsgID == nil || *calls[0].ProviderMsgID != "msg-123" {
		t.Errorf("expected provider_msg_id=msg-123, got %v", calls[0].ProviderMsgID)
	}
}

func TestWriter_ProcessesIncrementRetry(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	w := New(client, repo, 10, 50*time.Millisecond, testLogger())

	id := uuid.New()
	nextRetry := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Microsecond)
	addIncrementRetryEvent(t, client, id, nextRetry, "provider timeout")

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	waitFor(t, 2*time.Second, func() bool {
		return len(repo.getIncrementRetryCalls()) > 0
	}, "increment_retry processed")

	cancel()
	w.Stop()

	calls := repo.getIncrementRetryCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 increment_retry call, got %d", len(calls))
	}
	if calls[0].ID != id {
		t.Errorf("expected ID %s, got %s", id, calls[0].ID)
	}
	if calls[0].ErrorMsg != "provider timeout" {
		t.Errorf("expected error_message='provider timeout', got '%s'", calls[0].ErrorMsg)
	}
}

func TestWriter_ProcessesMoveToDLQ(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	w := New(client, repo, 10, 50*time.Millisecond, testLogger())

	id := uuid.New()
	addMoveToDLQEvent(t, client, id, "max retries exceeded")

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	waitFor(t, 2*time.Second, func() bool {
		return len(repo.getMoveToDLQCalls()) > 0
	}, "move_to_dlq processed")

	cancel()
	w.Stop()

	calls := repo.getMoveToDLQCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 move_to_dlq call, got %d", len(calls))
	}
	if calls[0].ErrorMsg != "max retries exceeded" {
		t.Errorf("expected error_message='max retries exceeded', got '%s'", calls[0].ErrorMsg)
	}
}

// ---------------------------------------------------------------------------
// Tests: Reclaim
// ---------------------------------------------------------------------------

func TestWriter_ReclaimsPendingMessages(t *testing.T) {
	// miniredis does not fully support XAUTOCLAIM idle-time tracking,
	// so this test validates that reclaimPending runs without error
	// and does not crash the writer. Full reclaim behavior is verified
	// via integration tests against a real Redis instance.
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	ctx := context.Background()

	client.XGroupCreateMkStream(ctx, persistStream, groupName, "0")

	n := makeNotification(uuid.New())
	addCreateEvent(t, client, n)

	// Read the message as a different consumer so it becomes pending.
	client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: "crashed-consumer",
		Streams:  []string{persistStream, ">"},
		Count:    10,
		Block:    50 * time.Millisecond,
	})

	// Start the writer — reclaimPending runs during Start.
	// With miniredis the claim won't actually transfer messages,
	// but we verify the code path doesn't panic or deadlock.
	w := New(client, repo, 10, 50*time.Millisecond, testLogger())

	ctx2, cancel := context.WithCancel(context.Background())
	w.Start(ctx2)

	// Give the writer time to run its reclaim + one tick cycle.
	time.Sleep(200 * time.Millisecond)

	cancel()
	w.Stop()
}

// ---------------------------------------------------------------------------
// Tests: Flush with mixed event types
// ---------------------------------------------------------------------------

func TestWriter_MixedEventTypes(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	w := New(client, repo, 10, 50*time.Millisecond, testLogger())

	// Add a mix of event types.
	createID := uuid.New()
	addCreateEvent(t, client, makeNotification(createID))

	updateID := uuid.New()
	addUpdateStatusEvent(t, client, updateID, domain.StatusQueued, domain.StatusProcessing)

	retryID := uuid.New()
	addIncrementRetryEvent(t, client, retryID, time.Now().Add(time.Minute), "timeout")

	dlqID := uuid.New()
	addMoveToDLQEvent(t, client, dlqID, "permanent failure")

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	waitFor(t, 3*time.Second, func() bool {
		return len(repo.getCreateBatchCalls()) > 0 &&
			len(repo.getUpdateStatusCalls()) > 0 &&
			len(repo.getIncrementRetryCalls()) > 0 &&
			len(repo.getMoveToDLQCalls()) > 0
	}, "all mixed event types processed")

	cancel()
	w.Stop()

	if total := countCreates(repo); total != 1 {
		t.Errorf("expected 1 create, got %d", total)
	}
	if n := len(repo.getUpdateStatusCalls()); n != 1 {
		t.Errorf("expected 1 update_status, got %d", n)
	}
	if n := len(repo.getIncrementRetryCalls()); n != 1 {
		t.Errorf("expected 1 increment_retry, got %d", n)
	}
	if n := len(repo.getMoveToDLQCalls()); n != 1 {
		t.Errorf("expected 1 move_to_dlq, got %d", n)
	}
}

func countCreates(repo *mockRepo) int {
	total := 0
	for _, batch := range repo.getCreateBatchCalls() {
		total += len(batch)
	}
	return total
}

// ---------------------------------------------------------------------------
// Tests: ackMessages
// ---------------------------------------------------------------------------

func TestWriter_AcksMessagesAfterFlush(t *testing.T) {
	_, client := setupMiniredis(t)
	repo := &mockRepo{}

	w := New(client, repo, 10, 50*time.Millisecond, testLogger())

	// Add a few events.
	for i := 0; i < 3; i++ {
		addCreateEvent(t, client, makeNotification(uuid.New()))
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	waitFor(t, 2*time.Second, func() bool {
		return countCreates(repo) >= 3
	}, "all 3 events processed")

	cancel()
	w.Stop()

	// After processing and acking, the pending list for the group should be empty.
	// Check via XPENDING.
	pending, err := client.XPending(context.Background(), persistStream, groupName).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 0 {
		t.Errorf("expected 0 pending messages after ack, got %d", pending.Count)
	}
}
