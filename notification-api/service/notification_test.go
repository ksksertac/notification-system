package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

type mockRepo struct {
	notifications     map[uuid.UUID]*domain.Notification
	byIdemKey         map[string]*domain.Notification
	createErr         error
	createBatchErr    error
	getByIDErr        error
	getByBatchIDErr   error
	getByIdemKeyErr   error
	listErr           error
	updateStatusErr   error
	updateStatusResult *bool
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		notifications: make(map[uuid.UUID]*domain.Notification),
		byIdemKey:     make(map[string]*domain.Notification),
	}
}

func (m *mockRepo) Create(ctx context.Context, n *domain.Notification) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.notifications[n.ID] = n
	if n.IdempotencyKey != nil {
		m.byIdemKey[*n.IdempotencyKey] = n
	}
	return nil
}

func (m *mockRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	if m.createBatchErr != nil {
		return m.createBatchErr
	}
	for _, n := range notifications {
		m.notifications[n.ID] = n
	}
	return nil
}

func (m *mockRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	if m.getByIDErr != nil {
		return nil, m.getByIDErr
	}
	return m.notifications[id], nil
}

func (m *mockRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	if m.getByBatchIDErr != nil {
		return nil, m.getByBatchIDErr
	}
	var result []*domain.Notification
	for _, n := range m.notifications {
		if n.BatchID != nil && *n.BatchID == batchID {
			result = append(result, n)
		}
	}
	return result, nil
}

func (m *mockRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	if m.getByIdemKeyErr != nil {
		return nil, m.getByIdemKeyErr
	}
	return m.byIdemKey[key], nil
}

func (m *mockRepo) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	if m.listErr != nil {
		return nil, 0, m.listErr
	}
	var result []*domain.Notification
	for _, n := range m.notifications {
		result = append(result, n)
	}
	return result, int64(len(result)), nil
}

func (m *mockRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
	if m.updateStatusErr != nil {
		return false, m.updateStatusErr
	}
	if m.updateStatusResult != nil {
		return *m.updateStatusResult, nil
	}
	if n, ok := m.notifications[id]; ok && n.Status == from {
		n.Status = to
		n.UpdatedAt = time.Now()
		return true, nil
	}
	return false, nil
}

func (m *mockRepo) UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
	return m.UpdateStatus(ctx, id, from, to)
}

func (m *mockRepo) IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	if n, ok := m.notifications[id]; ok {
		n.RetryCount++
	}
	return nil
}

func (m *mockRepo) MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error {
	n.Status = domain.StatusFailed
	return nil
}

func (m *mockRepo) GetScheduledReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) ClaimScheduledBatch(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) RecoverStuckQueued(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) GetRetryReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) RecoverStuckProcessing(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	return nil, nil
}

func (m *mockRepo) RecoverOrphanedPending(ctx context.Context, staleDuration time.Duration, limit int) ([]*domain.Notification, error) {
	return nil, nil
}

type mockPublisher struct {
	published      []*domain.Notification
	publishErr     error
	publishBatchErr error
}

func (m *mockPublisher) Publish(ctx context.Context, n *domain.Notification) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published = append(m.published, n)
	return nil
}

func (m *mockPublisher) PublishBatch(ctx context.Context, notifications []*domain.Notification) error {
	if m.publishBatchErr != nil {
		return m.publishBatchErr
	}
	m.published = append(m.published, notifications...)
	return nil
}

func TestCreate_ValidSMS(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "Test message",
		Priority:  "high",
	}

	n, err := svc.Create(context.Background(), req, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n.Channel != domain.ChannelSMS {
		t.Errorf("expected sms channel, got %s", n.Channel)
	}
	if n.Priority != domain.PriorityHigh {
		t.Errorf("expected high priority, got %d", n.Priority)
	}
	if len(pub.published) != 1 {
		t.Errorf("expected 1 published message, got %d", len(pub.published))
	}
}

func TestCreate_IdempotencyReturnsExisting(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "Test",
	}

	first, err := svc.Create(context.Background(), req, "key-123")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	second, err := svc.Create(context.Background(), req, "key-123")
	if err != nil {
		t.Fatalf("second create: %v", err)
	}

	if first.ID != second.ID {
		t.Error("idempotent create should return same notification")
	}
	if len(pub.published) != 1 {
		t.Errorf("should only publish once, got %d", len(pub.published))
	}
}

func TestCreate_ValidationError(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Channel: "sms",
		Content: "Test",
	}

	_, err := svc.Create(context.Background(), req, "")
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation, got: %v", err)
	}
}

func TestCancel_Success(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "Test",
	}

	n, _ := svc.Create(context.Background(), req, "")

	repo.notifications[n.ID].Status = domain.StatusPending

	err := svc.Cancel(context.Background(), n.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated := repo.notifications[n.ID]
	if updated.Status != domain.StatusCancelled {
		t.Errorf("expected cancelled status, got %s", updated.Status)
	}
}

func TestCancel_AlreadyProcessing(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "Test",
	}

	n, _ := svc.Create(context.Background(), req, "")
	repo.notifications[n.ID].Status = domain.StatusProcessing

	err := svc.Cancel(context.Background(), n.ID)
	if err == nil {
		t.Fatal("expected error cancelling processing notification")
	}
	if !errors.Is(err, ErrConflict) {
		t.Errorf("expected ErrConflict, got: %v", err)
	}
}

func TestCreateBatch(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.BatchCreateRequest{
		Notifications: []domain.CreateNotificationRequest{
			{Recipient: "+905551234567", Channel: "sms", Content: "msg1"},
			{Recipient: "test@example.com", Channel: "email", Content: "msg2"},
		},
	}

	batchID, notifications, err := svc.CreateBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if batchID == nil {
		t.Fatal("batch ID should not be nil")
	}
	if len(notifications) != 2 {
		t.Errorf("expected 2 notifications, got %d", len(notifications))
	}
	if len(pub.published) != 2 {
		t.Errorf("expected 2 published, got %d", len(pub.published))
	}
}

// --- Tests for GetByID ---

func TestGetByID_Found(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	id := uuid.New()
	repo.notifications[id] = &domain.Notification{
		ID:      id,
		Channel: domain.ChannelSMS,
		Status:  domain.StatusPending,
	}

	n, err := svc.GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n == nil {
		t.Fatal("expected notification, got nil")
	}
	if n.ID != id {
		t.Errorf("expected ID %s, got %s", id, n.ID)
	}
}

func TestGetByID_NotFound(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	n, err := svc.GetByID(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != nil {
		t.Errorf("expected nil, got %v", n)
	}
}

func TestGetByID_RepoError(t *testing.T) {
	repo := newMockRepo()
	repo.getByIDErr = errors.New("db connection failed")
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	_, err := svc.GetByID(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- Tests for GetByBatchID ---

func TestGetByBatchID_Found(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	batchID := uuid.New()
	id1, id2 := uuid.New(), uuid.New()
	repo.notifications[id1] = &domain.Notification{ID: id1, BatchID: &batchID, Status: domain.StatusPending}
	repo.notifications[id2] = &domain.Notification{ID: id2, BatchID: &batchID, Status: domain.StatusPending}

	results, err := svc.GetByBatchID(context.Background(), batchID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 notifications, got %d", len(results))
	}
}

func TestGetByBatchID_Empty(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	results, err := svc.GetByBatchID(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 notifications, got %d", len(results))
	}
}

func TestGetByBatchID_RepoError(t *testing.T) {
	repo := newMockRepo()
	repo.getByBatchIDErr = errors.New("db error")
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	_, err := svc.GetByBatchID(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- Tests for List ---

func TestList_Success(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	id1 := uuid.New()
	repo.notifications[id1] = &domain.Notification{ID: id1, Status: domain.StatusPending}

	results, total, err := svc.List(context.Background(), domain.ListNotificationsRequest{Limit: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 1 {
		t.Errorf("expected total 1, got %d", total)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestList_Empty(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	results, total, err := svc.List(context.Background(), domain.ListNotificationsRequest{Limit: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 0 {
		t.Errorf("expected total 0, got %d", total)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestList_RepoError(t *testing.T) {
	repo := newMockRepo()
	repo.listErr = errors.New("db error")
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	_, _, err := svc.List(context.Background(), domain.ListNotificationsRequest{Limit: 10})
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- Additional Cancel tests ---

func TestCancel_NotFound(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	err := svc.Cancel(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error for not found")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestCancel_AlreadyDelivered(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	id := uuid.New()
	repo.notifications[id] = &domain.Notification{
		ID:     id,
		Status: domain.StatusDelivered,
	}

	err := svc.Cancel(context.Background(), id)
	if err == nil {
		t.Fatal("expected error cancelling delivered notification")
	}
	if !errors.Is(err, ErrConflict) {
		t.Errorf("expected ErrConflict, got: %v", err)
	}
}

func TestCancel_ConcurrentUpdateFailure(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	id := uuid.New()
	repo.notifications[id] = &domain.Notification{
		ID:     id,
		Status: domain.StatusPending,
	}
	// Force UpdateStatus to return false (simulating concurrent modification)
	falseVal := false
	repo.updateStatusResult = &falseVal

	err := svc.Cancel(context.Background(), id)
	if err == nil {
		t.Fatal("expected error for concurrent update failure")
	}
	if !errors.Is(err, ErrConcurrentModification) {
		t.Errorf("expected ErrConcurrentModification, got: %v", err)
	}
}

func TestCancel_RepoGetError(t *testing.T) {
	repo := newMockRepo()
	repo.getByIDErr = errors.New("db connection lost")
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	err := svc.Cancel(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "getting notification") {
		t.Errorf("expected 'getting notification' error, got: %v", err)
	}
}

func TestCancel_UpdateStatusError(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	id := uuid.New()
	repo.notifications[id] = &domain.Notification{
		ID:     id,
		Status: domain.StatusPending,
	}
	repo.updateStatusErr = errors.New("redis error")

	err := svc.Cancel(context.Background(), id)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "cancelling notification") {
		t.Errorf("expected 'cancelling notification' error, got: %v", err)
	}
}

// --- Additional Create tests ---

func TestCreate_EmailChannel(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "user@example.com",
		Channel:   "email",
		Content:   "Hello via email",
		Priority:  "normal",
	}

	n, err := svc.Create(context.Background(), req, "email-key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.Channel != domain.ChannelEmail {
		t.Errorf("expected email channel, got %s", n.Channel)
	}
}

func TestCreate_InvalidEmailRecipient(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "not-an-email",
		Channel:   "email",
		Content:   "Hello",
	}

	_, err := svc.Create(context.Background(), req, "")
	if err == nil {
		t.Fatal("expected validation error for invalid email")
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation, got: %v", err)
	}
}

func TestCreate_ScheduledNotification(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	future := time.Now().Add(24 * time.Hour)
	req := domain.CreateNotificationRequest{
		Recipient:   "+905551234567",
		Channel:     "sms",
		Content:     "Scheduled message",
		Priority:    "low",
		ScheduledAt: &future,
	}

	n, err := svc.Create(context.Background(), req, "sched-key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.ScheduledAt == nil {
		t.Fatal("expected scheduled_at to be set")
	}
	// Scheduled notifications should NOT be published immediately
	if len(pub.published) != 0 {
		t.Errorf("expected 0 published (scheduled), got %d", len(pub.published))
	}
	// Should remain pending (not queued)
	if n.Status != domain.StatusPending {
		t.Errorf("expected pending status for scheduled, got %s", n.Status)
	}
}

func TestCreate_ContentTooLong(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	longContent := strings.Repeat("a", 161) // SMS max is 160
	req := domain.CreateNotificationRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   longContent,
	}

	_, err := svc.Create(context.Background(), req, "")
	if err == nil {
		t.Fatal("expected validation error for content too long")
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation, got: %v", err)
	}
}

func TestCreate_WithMetadata(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "Test with metadata",
		Metadata:  map[string]any{"key": "value", "number": 42},
	}

	n, err := svc.Create(context.Background(), req, "meta-key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(n.Metadata) == "{}" {
		t.Error("expected metadata to be set")
	}
}

func TestCreate_IdempotencyKeyCheckError(t *testing.T) {
	repo := newMockRepo()
	repo.getByIdemKeyErr = errors.New("redis timeout")
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "Test",
	}

	_, err := svc.Create(context.Background(), req, "some-key")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "idempotency") {
		t.Errorf("expected idempotency error, got: %v", err)
	}
}

func TestCreate_RepoCreateError(t *testing.T) {
	repo := newMockRepo()
	repo.createErr = errors.New("write failed")
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "Test",
	}

	_, err := svc.Create(context.Background(), req, "create-err-key")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "creating notification") {
		t.Errorf("expected 'creating notification' error, got: %v", err)
	}
}

func TestCreate_PublishError_StillSucceeds(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{publishErr: errors.New("publish failed")}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "Test",
	}

	n, err := svc.Create(context.Background(), req, "pub-err-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// When publish fails, status stays pending
	if n.Status != domain.StatusPending {
		t.Errorf("expected pending status when publish fails, got %s", n.Status)
	}
}

// --- Additional CreateBatch tests ---

func TestCreateBatch_ValidationError_EmptyBatch(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.BatchCreateRequest{
		Notifications: []domain.CreateNotificationRequest{},
	}

	_, _, err := svc.CreateBatch(context.Background(), req)
	if err == nil {
		t.Fatal("expected validation error for empty batch")
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation, got: %v", err)
	}
}

func TestCreateBatch_ValidationError_InvalidItem(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.BatchCreateRequest{
		Notifications: []domain.CreateNotificationRequest{
			{Recipient: "+905551234567", Channel: "sms", Content: "valid"},
			{Recipient: "", Channel: "sms", Content: "invalid - no recipient"},
		},
	}

	_, _, err := svc.CreateBatch(context.Background(), req)
	if err == nil {
		t.Fatal("expected validation error for invalid item")
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation, got: %v", err)
	}
}

func TestCreateBatch_RepoError(t *testing.T) {
	repo := newMockRepo()
	repo.createBatchErr = errors.New("batch write failed")
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.BatchCreateRequest{
		Notifications: []domain.CreateNotificationRequest{
			{Recipient: "+905551234567", Channel: "sms", Content: "msg1"},
		},
	}

	_, _, err := svc.CreateBatch(context.Background(), req)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "creating batch") {
		t.Errorf("expected 'creating batch' error, got: %v", err)
	}
}

func TestCreateBatch_PublishBatchError_StillSucceeds(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{publishBatchErr: errors.New("publish failed")}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.BatchCreateRequest{
		Notifications: []domain.CreateNotificationRequest{
			{Recipient: "+905551234567", Channel: "sms", Content: "msg1"},
			{Recipient: "user@example.com", Channel: "email", Content: "msg2"},
		},
	}

	batchID, notifications, err := svc.CreateBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batchID == nil {
		t.Fatal("expected batch ID")
	}
	// All remain pending since publish failed
	for _, n := range notifications {
		if n.Status != domain.StatusPending {
			t.Errorf("expected pending status, got %s", n.Status)
		}
	}
}

func TestCreateBatch_WithScheduledItems(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	future := time.Now().Add(24 * time.Hour)
	req := domain.BatchCreateRequest{
		Notifications: []domain.CreateNotificationRequest{
			{Recipient: "+905551234567", Channel: "sms", Content: "immediate"},
			{Recipient: "+905551234568", Channel: "sms", Content: "scheduled", ScheduledAt: &future},
		},
	}

	_, notifications, err := svc.CreateBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only the immediate one should be published
	if len(pub.published) != 1 {
		t.Errorf("expected 1 published (only immediate), got %d", len(pub.published))
	}
	// The immediate one should be queued, the scheduled one should remain pending
	for _, n := range notifications {
		if n.ScheduledAt != nil {
			if n.Status != domain.StatusPending {
				t.Errorf("scheduled notification should remain pending, got %s", n.Status)
			}
		} else {
			if n.Status != domain.StatusQueued {
				t.Errorf("immediate notification should be queued, got %s", n.Status)
			}
		}
	}
}

func TestCreateBatch_WithMetadata(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.BatchCreateRequest{
		Notifications: []domain.CreateNotificationRequest{
			{
				Recipient: "+905551234567",
				Channel:   "sms",
				Content:   "msg with meta",
				Metadata:  map[string]any{"campaign": "spring2025"},
			},
		},
	}

	_, notifications, err := svc.CreateBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(notifications[0].Metadata) == "{}" {
		t.Error("expected metadata to be set")
	}
}

// --- WriteBuffer tests ---

func TestBuffer_Stop(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	buf := NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil)

	// Submit a notification, then stop
	n := &domain.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "buffered msg",
		Status:    domain.StatusPending,
	}

	err := buf.Submit(context.Background(), n)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Stop should flush remaining items and return
	buf.Stop()

	// Verify the notification was persisted
	if _, ok := repo.notifications[n.ID]; !ok {
		t.Error("expected notification to be flushed on stop")
	}
}

func TestBuffer_Submit_ContextCancelled(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	buf := NewWriteBuffer(repo, pub, 100, 1*time.Hour, nil)

	// Use an already-cancelled context so that the second select (waiting for resultCh)
	// or the first select (sending to incoming) times out immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	n := &domain.Notification{
		ID:      uuid.New(),
		Channel: domain.ChannelSMS,
		Content: "timeout",
		Status:  domain.StatusPending,
	}

	err := buf.Submit(ctx, n)
	if err == nil {
		t.Fatal("expected context error")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
	buf.Stop()
}

func TestBuffer_FlushOnSizeThreshold(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	flushSize := 3
	buf := NewWriteBuffer(repo, pub, flushSize, 1*time.Hour, nil) // very long interval to ensure flush is by size

	// Submit flushSize notifications concurrently (each Submit blocks until flush)
	errCh := make(chan error, flushSize)
	for i := 0; i < flushSize; i++ {
		go func() {
			n := &domain.Notification{
				ID:        uuid.New(),
				Recipient: "+905551234567",
				Channel:   domain.ChannelSMS,
				Content:   "batch flush test",
				Status:    domain.StatusPending,
			}
			errCh <- buf.Submit(context.Background(), n)
		}()
	}

	// Wait for all submits to complete
	for i := 0; i < flushSize; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}

	// All should be persisted since flushSize was reached
	if len(repo.notifications) != flushSize {
		t.Errorf("expected %d notifications after flush, got %d", flushSize, len(repo.notifications))
	}
	buf.Stop()
}

func TestBuffer_FlushError_PropagatedToSubmitters(t *testing.T) {
	repo := newMockRepo()
	repo.createBatchErr = errors.New("batch write failed")
	pub := &mockPublisher{}
	buf := NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil)

	n := &domain.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "will fail",
		Status:    domain.StatusPending,
	}

	err := buf.Submit(context.Background(), n)
	if err == nil {
		t.Fatal("expected error from flush failure")
	}
	if !strings.Contains(err.Error(), "batch write failed") {
		t.Errorf("expected 'batch write failed' error, got: %v", err)
	}
	buf.Stop()
}

func TestBuffer_FlushPublishError_StillReturnsNil(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{publishBatchErr: errors.New("publish failed")}
	buf := NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil)

	n := &domain.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "publish will fail",
		Status:    domain.StatusPending,
	}

	err := buf.Submit(context.Background(), n)
	if err != nil {
		t.Fatalf("unexpected error: %v (publish failure should not propagate)", err)
	}
	// Notification was still created in repo
	if _, ok := repo.notifications[n.ID]; !ok {
		t.Error("expected notification to be in repo despite publish failure")
	}
	buf.Stop()
}

func TestBuffer_FlushWithScheduledItems(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	buf := NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil)

	future := time.Now().Add(24 * time.Hour)
	n := &domain.Notification{
		ID:          uuid.New(),
		Recipient:   "+905551234567",
		Channel:     domain.ChannelSMS,
		Content:     "scheduled via buffer",
		Status:      domain.StatusPending,
		ScheduledAt: &future,
	}

	err := buf.Submit(context.Background(), n)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for flush
	time.Sleep(20 * time.Millisecond)

	// Scheduled notification should NOT be published
	if len(pub.published) != 0 {
		t.Errorf("expected 0 published (scheduled), got %d", len(pub.published))
	}
	buf.Stop()
}

func TestBuffer_DefaultValues(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	// Pass invalid flushSize and flushInterval to test defaults
	buf := NewWriteBuffer(repo, pub, 0, 0, nil)

	n := &domain.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "defaults test",
		Status:    domain.StatusPending,
	}

	err := buf.Submit(context.Background(), n)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	buf.Stop()

	if _, ok := repo.notifications[n.ID]; !ok {
		t.Error("expected notification to be persisted")
	}
}

func TestCreate_PushChannel(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	req := domain.CreateNotificationRequest{
		Recipient: "device-token-abcdef1234567890",
		Channel:   "push",
		Content:   "Push notification",
		Priority:  "high",
	}

	n, err := svc.Create(context.Background(), req, "push-key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n.Channel != domain.ChannelPush {
		t.Errorf("expected push channel, got %s", n.Channel)
	}
}

func TestCancel_AlreadyCancelled(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	id := uuid.New()
	repo.notifications[id] = &domain.Notification{
		ID:     id,
		Status: domain.StatusCancelled,
	}

	err := svc.Cancel(context.Background(), id)
	if err == nil {
		t.Fatal("expected error cancelling already cancelled notification")
	}
	if !errors.Is(err, ErrConflict) {
		t.Errorf("expected ErrConflict, got: %v", err)
	}
}

func TestCancel_FromQueuedStatus(t *testing.T) {
	repo := newMockRepo()
	pub := &mockPublisher{}
	svc := NewNotificationService(repo, pub, NewWriteBuffer(repo, pub, 10, 5*time.Millisecond, nil), 5, nil)

	id := uuid.New()
	repo.notifications[id] = &domain.Notification{
		ID:     id,
		Status: domain.StatusQueued,
	}

	err := svc.Cancel(context.Background(), id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.notifications[id].Status != domain.StatusCancelled {
		t.Errorf("expected cancelled, got %s", repo.notifications[id].Status)
	}
}
