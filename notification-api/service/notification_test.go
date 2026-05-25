package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

type mockRepo struct {
	notifications map[uuid.UUID]*domain.Notification
	byIdemKey     map[string]*domain.Notification
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		notifications: make(map[uuid.UUID]*domain.Notification),
		byIdemKey:     make(map[string]*domain.Notification),
	}
}

func (m *mockRepo) Create(ctx context.Context, n *domain.Notification) error {
	m.notifications[n.ID] = n
	if n.IdempotencyKey != nil {
		m.byIdemKey[*n.IdempotencyKey] = n
	}
	return nil
}

func (m *mockRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	for _, n := range notifications {
		m.notifications[n.ID] = n
	}
	return nil
}

func (m *mockRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return m.notifications[id], nil
}

func (m *mockRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	var result []*domain.Notification
	for _, n := range m.notifications {
		if n.BatchID != nil && *n.BatchID == batchID {
			result = append(result, n)
		}
	}
	return result, nil
}

func (m *mockRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	return m.byIdemKey[key], nil
}

func (m *mockRepo) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	var result []*domain.Notification
	for _, n := range m.notifications {
		result = append(result, n)
	}
	return result, int64(len(result)), nil
}

func (m *mockRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
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

type mockPublisher struct {
	published []*domain.Notification
}

func (m *mockPublisher) Publish(ctx context.Context, n *domain.Notification) error {
	m.published = append(m.published, n)
	return nil
}

func (m *mockPublisher) PublishBatch(ctx context.Context, notifications []*domain.Notification) error {
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
	if !strings.Contains(err.Error(), "validation") {
		t.Errorf("expected validation error, got: %v", err)
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
	if !strings.Contains(err.Error(), "cannot cancel") {
		t.Errorf("expected 'cannot cancel' error, got: %v", err)
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
