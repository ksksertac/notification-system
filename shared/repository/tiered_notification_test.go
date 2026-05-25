package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

// mockNotificationRepo is a mock implementation of NotificationRepository that
// records which methods were called and returns configurable responses.
type mockNotificationRepo struct {
	name string

	// call tracking
	createCalled             bool
	createBatchCalled        bool
	getByIDCalled            bool
	getByBatchIDCalled       bool
	getByIdempotencyKeyCalled bool
	listCalled               bool
	updateStatusCalled       bool
	updateStatusWithDetailsCalled bool
	incrementRetryCalled     bool
	moveToDLQCalled          bool
	getScheduledReadyCalled  bool
	claimScheduledBatchCalled bool
	recoverStuckQueuedCalled bool

	// return values
	createErr             error
	createBatchErr        error
	getByIDResult         *domain.Notification
	getByIDErr            error
	getByBatchIDResult    []*domain.Notification
	getByBatchIDErr       error
	getByIdempotencyKeyResult *domain.Notification
	getByIdempotencyKeyErr    error
	listResult            []*domain.Notification
	listTotal             int64
	listErr               error
	updateStatusResult    bool
	updateStatusErr       error
	updateStatusWithDetailsResult bool
	updateStatusWithDetailsErr    error
	incrementRetryErr     error
	moveToDLQErr          error
	getScheduledReadyResult []*domain.Notification
	getScheduledReadyErr    error
	claimScheduledBatchResult []*domain.Notification
	claimScheduledBatchErr    error
	recoverStuckQueuedResult []*domain.Notification
	recoverStuckQueuedErr    error
}

func (m *mockNotificationRepo) Create(ctx context.Context, n *domain.Notification) error {
	m.createCalled = true
	return m.createErr
}

func (m *mockNotificationRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	m.createBatchCalled = true
	return m.createBatchErr
}

func (m *mockNotificationRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	m.getByIDCalled = true
	return m.getByIDResult, m.getByIDErr
}

func (m *mockNotificationRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	m.getByBatchIDCalled = true
	return m.getByBatchIDResult, m.getByBatchIDErr
}

func (m *mockNotificationRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	m.getByIdempotencyKeyCalled = true
	return m.getByIdempotencyKeyResult, m.getByIdempotencyKeyErr
}

func (m *mockNotificationRepo) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	m.listCalled = true
	return m.listResult, m.listTotal, m.listErr
}

func (m *mockNotificationRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
	m.updateStatusCalled = true
	return m.updateStatusResult, m.updateStatusErr
}

func (m *mockNotificationRepo) UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
	m.updateStatusWithDetailsCalled = true
	return m.updateStatusWithDetailsResult, m.updateStatusWithDetailsErr
}

func (m *mockNotificationRepo) IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	m.incrementRetryCalled = true
	return m.incrementRetryErr
}

func (m *mockNotificationRepo) MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error {
	m.moveToDLQCalled = true
	return m.moveToDLQErr
}

func (m *mockNotificationRepo) GetScheduledReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	m.getScheduledReadyCalled = true
	return m.getScheduledReadyResult, m.getScheduledReadyErr
}

func (m *mockNotificationRepo) ClaimScheduledBatch(ctx context.Context, limit int) ([]*domain.Notification, error) {
	m.claimScheduledBatchCalled = true
	return m.claimScheduledBatchResult, m.claimScheduledBatchErr
}

func (m *mockNotificationRepo) RecoverStuckQueued(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	m.recoverStuckQueuedCalled = true
	return m.recoverStuckQueuedResult, m.recoverStuckQueuedErr
}

func newTestNotification() *domain.Notification {
	return &domain.Notification{
		ID:        uuid.New(),
		Recipient: "+1234567890",
		Channel:   domain.ChannelSMS,
		Content:   "test message",
		Priority:  domain.PriorityNormal,
		Status:    domain.StatusPending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

// --- GetByID tests ---

func TestGetByID_ReturnsFromHotWhenFound(t *testing.T) {
	expected := newTestNotification()
	hot := &mockNotificationRepo{name: "hot", getByIDResult: expected}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	result, err := repo.GetByID(context.Background(), expected.ID)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expected {
		t.Fatalf("expected result from hot, got different notification")
	}
	if !hot.getByIDCalled {
		t.Fatal("expected hot.GetByID to be called")
	}
	if cold.getByIDCalled {
		t.Fatal("cold.GetByID should not be called when hot returns a result")
	}
}

func TestGetByID_FallsToColdWhenHotReturnsNil(t *testing.T) {
	expected := newTestNotification()
	hot := &mockNotificationRepo{name: "hot", getByIDResult: nil}
	cold := &mockNotificationRepo{name: "cold", getByIDResult: expected}

	repo := NewTieredNotificationRepo(hot, cold)
	result, err := repo.GetByID(context.Background(), expected.ID)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expected {
		t.Fatalf("expected result from cold, got different notification")
	}
	if !hot.getByIDCalled {
		t.Fatal("expected hot.GetByID to be called")
	}
	if !cold.getByIDCalled {
		t.Fatal("expected cold.GetByID to be called as fallback")
	}
}

func TestGetByID_ReturnsErrorFromHotWithoutCallingCold(t *testing.T) {
	expectedErr := errors.New("hot storage error")
	hot := &mockNotificationRepo{name: "hot", getByIDErr: expectedErr}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	_, err := repo.GetByID(context.Background(), uuid.New())

	if err != expectedErr {
		t.Fatalf("expected error from hot, got: %v", err)
	}
	if cold.getByIDCalled {
		t.Fatal("cold.GetByID should not be called when hot returns an error")
	}
}

// --- GetByBatchID tests ---

func TestGetByBatchID_ReturnsFromHotWhenFound(t *testing.T) {
	expected := []*domain.Notification{newTestNotification(), newTestNotification()}
	hot := &mockNotificationRepo{name: "hot", getByBatchIDResult: expected}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	batchID := uuid.New()
	result, err := repo.GetByBatchID(context.Background(), batchID)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(result))
	}
	if !hot.getByBatchIDCalled {
		t.Fatal("expected hot.GetByBatchID to be called")
	}
	if cold.getByBatchIDCalled {
		t.Fatal("cold.GetByBatchID should not be called when hot returns results")
	}
}

func TestGetByBatchID_FallsToColdWhenHotReturnsEmpty(t *testing.T) {
	expected := []*domain.Notification{newTestNotification()}
	hot := &mockNotificationRepo{name: "hot", getByBatchIDResult: nil}
	cold := &mockNotificationRepo{name: "cold", getByBatchIDResult: expected}

	repo := NewTieredNotificationRepo(hot, cold)
	batchID := uuid.New()
	result, err := repo.GetByBatchID(context.Background(), batchID)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result from cold, got %d", len(result))
	}
	if !hot.getByBatchIDCalled {
		t.Fatal("expected hot.GetByBatchID to be called")
	}
	if !cold.getByBatchIDCalled {
		t.Fatal("expected cold.GetByBatchID to be called as fallback")
	}
}

func TestGetByBatchID_ReturnsErrorFromHotWithoutCallingCold(t *testing.T) {
	expectedErr := errors.New("batch lookup error")
	hot := &mockNotificationRepo{name: "hot", getByBatchIDErr: expectedErr}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	_, err := repo.GetByBatchID(context.Background(), uuid.New())

	if err != expectedErr {
		t.Fatalf("expected error from hot, got: %v", err)
	}
	if cold.getByBatchIDCalled {
		t.Fatal("cold.GetByBatchID should not be called when hot returns an error")
	}
}

// --- GetByIdempotencyKey tests ---

func TestGetByIdempotencyKey_ReturnsFromHotWhenFound(t *testing.T) {
	expected := newTestNotification()
	hot := &mockNotificationRepo{name: "hot", getByIdempotencyKeyResult: expected}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	result, err := repo.GetByIdempotencyKey(context.Background(), "test-key-123")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expected {
		t.Fatal("expected result from hot")
	}
	if !hot.getByIdempotencyKeyCalled {
		t.Fatal("expected hot.GetByIdempotencyKey to be called")
	}
	if cold.getByIdempotencyKeyCalled {
		t.Fatal("cold.GetByIdempotencyKey should not be called when hot returns a result")
	}
}

func TestGetByIdempotencyKey_FallsToColdWhenHotReturnsNil(t *testing.T) {
	expected := newTestNotification()
	hot := &mockNotificationRepo{name: "hot", getByIdempotencyKeyResult: nil}
	cold := &mockNotificationRepo{name: "cold", getByIdempotencyKeyResult: expected}

	repo := NewTieredNotificationRepo(hot, cold)
	result, err := repo.GetByIdempotencyKey(context.Background(), "test-key-456")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expected {
		t.Fatal("expected result from cold")
	}
	if !hot.getByIdempotencyKeyCalled {
		t.Fatal("expected hot.GetByIdempotencyKey to be called")
	}
	if !cold.getByIdempotencyKeyCalled {
		t.Fatal("expected cold.GetByIdempotencyKey to be called as fallback")
	}
}

// --- List tests ---

func TestList_RoutesToHotWhenStartDateWithinLastHour(t *testing.T) {
	expected := []*domain.Notification{newTestNotification()}
	hot := &mockNotificationRepo{name: "hot", listResult: expected, listTotal: 1}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)

	// StartDate 30 minutes ago is within the 1-hour hot window
	startDate := time.Now().UTC().Add(-30 * time.Minute)
	req := domain.ListNotificationsRequest{
		StartDate: &startDate,
		Limit:     10,
	}

	result, total, err := repo.List(context.Background(), req)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected total 1, got %d", total)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if !hot.listCalled {
		t.Fatal("expected hot.List to be called for recent data")
	}
	if cold.listCalled {
		t.Fatal("cold.List should not be called for recent data")
	}
}

func TestList_RoutesToColdWhenStartDateOlderThanOneHour(t *testing.T) {
	expected := []*domain.Notification{newTestNotification()}
	hot := &mockNotificationRepo{name: "hot"}
	cold := &mockNotificationRepo{name: "cold", listResult: expected, listTotal: 5}

	repo := NewTieredNotificationRepo(hot, cold)

	// StartDate 2 hours ago is outside the 1-hour hot window
	startDate := time.Now().UTC().Add(-2 * time.Hour)
	req := domain.ListNotificationsRequest{
		StartDate: &startDate,
		Limit:     10,
	}

	result, total, err := repo.List(context.Background(), req)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 5 {
		t.Fatalf("expected total 5, got %d", total)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if hot.listCalled {
		t.Fatal("hot.List should not be called for old data")
	}
	if !cold.listCalled {
		t.Fatal("expected cold.List to be called for old data")
	}
}

func TestList_RoutesToColdWhenStartDateIsNil(t *testing.T) {
	expected := []*domain.Notification{newTestNotification()}
	hot := &mockNotificationRepo{name: "hot"}
	cold := &mockNotificationRepo{name: "cold", listResult: expected, listTotal: 10}

	repo := NewTieredNotificationRepo(hot, cold)

	req := domain.ListNotificationsRequest{
		StartDate: nil,
		Limit:     20,
	}

	_, total, err := repo.List(context.Background(), req)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 10 {
		t.Fatalf("expected total 10, got %d", total)
	}
	if hot.listCalled {
		t.Fatal("hot.List should not be called when StartDate is nil")
	}
	if !cold.listCalled {
		t.Fatal("expected cold.List to be called when StartDate is nil")
	}
}

// --- Create tests ---

func TestCreate_AlwaysWritesToHot(t *testing.T) {
	n := newTestNotification()
	hot := &mockNotificationRepo{name: "hot"}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	err := repo.Create(context.Background(), n)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hot.createCalled {
		t.Fatal("expected hot.Create to be called")
	}
	if cold.createCalled {
		t.Fatal("cold.Create should not be called")
	}
}

func TestCreate_ReturnsErrorFromHot(t *testing.T) {
	expectedErr := errors.New("write failed")
	hot := &mockNotificationRepo{name: "hot", createErr: expectedErr}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	err := repo.Create(context.Background(), newTestNotification())

	if err != expectedErr {
		t.Fatalf("expected error from hot, got: %v", err)
	}
}

// --- CreateBatch tests ---

func TestCreateBatch_AlwaysWritesToHot(t *testing.T) {
	notifications := []*domain.Notification{newTestNotification(), newTestNotification()}
	hot := &mockNotificationRepo{name: "hot"}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	err := repo.CreateBatch(context.Background(), notifications)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hot.createBatchCalled {
		t.Fatal("expected hot.CreateBatch to be called")
	}
	if cold.createBatchCalled {
		t.Fatal("cold.CreateBatch should not be called")
	}
}

// --- UpdateStatus tests ---

func TestUpdateStatus_AlwaysGoesToHot(t *testing.T) {
	hot := &mockNotificationRepo{name: "hot", updateStatusResult: true}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	result, err := repo.UpdateStatus(context.Background(), uuid.New(), domain.StatusPending, domain.StatusQueued)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected true result from hot")
	}
	if !hot.updateStatusCalled {
		t.Fatal("expected hot.UpdateStatus to be called")
	}
	if cold.updateStatusCalled {
		t.Fatal("cold.UpdateStatus should not be called")
	}
}

func TestUpdateStatusWithDetails_AlwaysGoesToHot(t *testing.T) {
	hot := &mockNotificationRepo{name: "hot", updateStatusWithDetailsResult: true}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	providerID := "provider-123"
	result, err := repo.UpdateStatusWithDetails(context.Background(), uuid.New(), domain.StatusProcessing, domain.StatusDelivered, &providerID, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected true result from hot")
	}
	if !hot.updateStatusWithDetailsCalled {
		t.Fatal("expected hot.UpdateStatusWithDetails to be called")
	}
	if cold.updateStatusWithDetailsCalled {
		t.Fatal("cold.UpdateStatusWithDetails should not be called")
	}
}

// --- MoveToDLQ tests ---

func TestMoveToDLQ_AlwaysGoesToHot(t *testing.T) {
	n := newTestNotification()
	hot := &mockNotificationRepo{name: "hot"}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	err := repo.MoveToDLQ(context.Background(), n, "max retries exceeded")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hot.moveToDLQCalled {
		t.Fatal("expected hot.MoveToDLQ to be called")
	}
	if cold.moveToDLQCalled {
		t.Fatal("cold.MoveToDLQ should not be called")
	}
}

func TestMoveToDLQ_ReturnsErrorFromHot(t *testing.T) {
	expectedErr := errors.New("dlq write failed")
	hot := &mockNotificationRepo{name: "hot", moveToDLQErr: expectedErr}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	err := repo.MoveToDLQ(context.Background(), newTestNotification(), "error")

	if err != expectedErr {
		t.Fatalf("expected error from hot, got: %v", err)
	}
}

// --- IncrementRetry tests ---

func TestIncrementRetry_AlwaysGoesToHot(t *testing.T) {
	hot := &mockNotificationRepo{name: "hot"}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	err := repo.IncrementRetry(context.Background(), uuid.New(), time.Now().Add(5*time.Minute), "temporary failure")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hot.incrementRetryCalled {
		t.Fatal("expected hot.IncrementRetry to be called")
	}
	if cold.incrementRetryCalled {
		t.Fatal("cold.IncrementRetry should not be called")
	}
}

// --- GetScheduledReady tests ---

func TestGetScheduledReady_AlwaysGoesToHot(t *testing.T) {
	expected := []*domain.Notification{newTestNotification()}
	hot := &mockNotificationRepo{name: "hot", getScheduledReadyResult: expected}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	result, err := repo.GetScheduledReady(context.Background(), 100)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if !hot.getScheduledReadyCalled {
		t.Fatal("expected hot.GetScheduledReady to be called")
	}
	if cold.getScheduledReadyCalled {
		t.Fatal("cold.GetScheduledReady should not be called")
	}
}

// --- ClaimScheduledBatch tests ---

func TestClaimScheduledBatch_AlwaysGoesToHot(t *testing.T) {
	expected := []*domain.Notification{newTestNotification(), newTestNotification()}
	hot := &mockNotificationRepo{name: "hot", claimScheduledBatchResult: expected}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	result, err := repo.ClaimScheduledBatch(context.Background(), 50)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if !hot.claimScheduledBatchCalled {
		t.Fatal("expected hot.ClaimScheduledBatch to be called")
	}
	if cold.claimScheduledBatchCalled {
		t.Fatal("cold.ClaimScheduledBatch should not be called")
	}
}

// --- RecoverStuckQueued tests ---

func TestRecoverStuckQueued_AlwaysGoesToHot(t *testing.T) {
	expected := []*domain.Notification{newTestNotification()}
	hot := &mockNotificationRepo{name: "hot", recoverStuckQueuedResult: expected}
	cold := &mockNotificationRepo{name: "cold"}

	repo := NewTieredNotificationRepo(hot, cold)
	result, err := repo.RecoverStuckQueued(context.Background(), 5*time.Minute, 10)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if !hot.recoverStuckQueuedCalled {
		t.Fatal("expected hot.RecoverStuckQueued to be called")
	}
	if cold.recoverStuckQueuedCalled {
		t.Fatal("cold.RecoverStuckQueued should not be called")
	}
}
