package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/notification-consumer/delivery"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
)

// --- Mock implementations ---

type mockConsumer struct {
	mu        sync.Mutex
	readCalls []readCall
	ackCalls  []ackCall
	claimFn   func(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]queue.Message, error)
}

type readCall struct {
	stream   string
	group    string
	consumer string
	count    int64
	msgs     []queue.Message
	err      error
}

type ackCall struct {
	stream string
	group  string
	ids    []string
}

func (m *mockConsumer) Read(ctx context.Context, stream, group, consumer string, count int64) ([]queue.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, c := range m.readCalls {
		if c.stream == stream && c.count == count {
			// consume the call so it's not reused
			m.readCalls = append(m.readCalls[:i], m.readCalls[i+1:]...)
			return c.msgs, c.err
		}
	}
	return nil, nil
}

func (m *mockConsumer) Ack(ctx context.Context, stream, group string, ids ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackCalls = append(m.ackCalls, ackCall{stream: stream, group: group, ids: ids})
	return nil
}

func (m *mockConsumer) ClaimStale(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]queue.Message, error) {
	if m.claimFn != nil {
		return m.claimFn(ctx, stream, group, consumer, minIdle, count)
	}
	return nil, nil
}

func (m *mockConsumer) Len(ctx context.Context, stream string) (int64, error) {
	return 0, nil
}

type mockPublisher struct {
	mu         sync.Mutex
	published  []*domain.Notification
	publishErr error
}

func (m *mockPublisher) Publish(ctx context.Context, n *domain.Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, n)
	return m.publishErr
}

func (m *mockPublisher) PublishBatch(ctx context.Context, notifications []*domain.Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, notifications...)
	return m.publishErr
}

type mockProvider struct {
	sendFn func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error)
}

func (m *mockProvider) Send(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
	if m.sendFn != nil {
		return m.sendFn(ctx, recipient, channel, content)
	}
	return &delivery.SendResult{ProviderMsgID: "msg-123"}, nil
}

type mockRateLimiter struct {
	allowed bool
	err     error
}

func (m *mockRateLimiter) Allow(ctx context.Context, channel string) (bool, error) {
	return m.allowed, m.err
}

type mockRetryStrategy struct {
	shouldRetry bool
	nextDelay   time.Duration
}

func (m *mockRetryStrategy) NextDelay(attempt int) time.Duration {
	return m.nextDelay
}

func (m *mockRetryStrategy) ShouldRetry(attempt int, maxAttempts int) bool {
	return m.shouldRetry
}

type mockRepo struct {
	mu             sync.Mutex
	notifications  map[uuid.UUID]*domain.Notification
	statusUpdates  []statusUpdate
	dlqEntries     []*domain.Notification
	retryIncrements []retryIncrement
}

type statusUpdate struct {
	id   uuid.UUID
	from domain.Status
	to   domain.Status
}

type retryIncrement struct {
	id          uuid.UUID
	nextRetryAt time.Time
	errorMsg    string
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		notifications: make(map[uuid.UUID]*domain.Notification),
	}
}

func (m *mockRepo) Create(ctx context.Context, n *domain.Notification) error { return nil }
func (m *mockRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	return nil
}
func (m *mockRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.notifications[id]
	if !ok {
		return nil, nil
	}
	return n, nil
}
func (m *mockRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *mockRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	return nil, nil
}
func (m *mockRepo) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	return nil, 0, nil
}
func (m *mockRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statusUpdates = append(m.statusUpdates, statusUpdate{id: id, from: from, to: to})
	if n, ok := m.notifications[id]; ok {
		n.Status = to
	}
	return true, nil
}
func (m *mockRepo) UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statusUpdates = append(m.statusUpdates, statusUpdate{id: id, from: from, to: to})
	if n, ok := m.notifications[id]; ok {
		n.Status = to
		n.ProviderMsgID = providerMsgID
	}
	return true, nil
}
func (m *mockRepo) IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retryIncrements = append(m.retryIncrements, retryIncrement{id: id, nextRetryAt: nextRetryAt, errorMsg: errorMsg})
	return nil
}
func (m *mockRepo) MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlqEntries = append(m.dlqEntries, n)
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

var _ repository.NotificationRepository = (*mockRepo)(nil)

type mockBroadcaster struct {
	mu         sync.Mutex
	broadcasts []broadcastEntry
}

type broadcastEntry struct {
	id     uuid.UUID
	status domain.Status
}

func (m *mockBroadcaster) Broadcast(notificationID uuid.UUID, status domain.Status) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broadcasts = append(m.broadcasts, broadcastEntry{id: notificationID, status: status})
}

type mockMetrics struct {
	mu                  sync.Mutex
	deliveries          int
	failures            int
	rateLimitHits       int
	circuitBreakerOpens int
}

func (m *mockMetrics) RecordDelivery(channel string, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliveries++
}
func (m *mockMetrics) RecordFailure(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures++
}
func (m *mockMetrics) RecordRateLimitHit() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rateLimitHits++
}
func (m *mockMetrics) RecordCircuitBreakerOpen() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.circuitBreakerOpens++
}

type mockTemplateEngine struct{}

func (m *mockTemplateEngine) Render(tmpl string, metadata []byte) (string, error) {
	return tmpl, nil
}

// --- Helper to build a WorkerPool with defaults ---

func newTestWorkerPool(opts ...func(*testPoolOpts)) *WorkerPool {
	o := &testPoolOpts{}
	for _, fn := range opts {
		fn(o)
	}

	cfg := WorkerPoolConfig{
		ConsumerGroup: "test-group",
		WorkerCount:   1,
		WeightHigh:    10,
		WeightNormal:  5,
		WeightLow:     2,
		ClaimMinIdle:  30 * time.Second,
		ClaimInterval: 10 * time.Second,
		MaxRetries:    3,
	}

	consumer := &mockConsumer{}
	if o.consumer != nil {
		consumer = o.consumer
	}
	publisher := &mockPublisher{}
	if o.publisher != nil {
		publisher = o.publisher
	}
	provider := &mockProvider{}
	if o.provider != nil {
		provider = o.provider
	}
	rateLimiter := &mockRateLimiter{allowed: true}
	if o.rateLimiter != nil {
		rateLimiter = o.rateLimiter
	}
	cbRegistry := delivery.NewCircuitBreakerRegistry(delivery.CircuitBreakerConfig{
		FailureThreshold: 5,
		OpenDuration:     30 * time.Second,
		HalfOpenMax:      2,
	})
	retry := &mockRetryStrategy{shouldRetry: true, nextDelay: 100 * time.Millisecond}
	if o.retry != nil {
		retry = o.retry
	}
	repo := newMockRepo()
	if o.repo != nil {
		repo = o.repo
	}
	broadcaster := &mockBroadcaster{}
	metrics := &mockMetrics{}
	tmplEngine := &mockTemplateEngine{}
	logger := slog.Default()

	return &WorkerPool{
		cfg:         cfg,
		consumer:    consumer,
		publisher:   publisher,
		provider:    provider,
		rateLimiter: rateLimiter,
		cbRegistry:  cbRegistry,
		retry:       retry,
		repo:        repo,
		broadcaster: broadcaster,
		metrics:     metrics,
		tmplEngine:  tmplEngine,
		logger:      logger,
	}
}

type testPoolOpts struct {
	consumer    *mockConsumer
	publisher   *mockPublisher
	provider    *mockProvider
	rateLimiter *mockRateLimiter
	retry       *mockRetryStrategy
	repo        *mockRepo
}

// --- Tests ---

func TestPollStreams_WeightedPollingRatios(t *testing.T) {
	t.Run("reads from each stream with correct count based on weight", func(t *testing.T) {
		var readCalls []struct {
			stream string
			count  int64
		}

		consumer := &mockConsumer{}
		// We set up read calls for each stream that will be consumed
		consumer.readCalls = []readCall{
			{stream: queue.StreamHigh, count: 10, msgs: nil, err: nil},
			{stream: queue.StreamNormal, count: 5, msgs: nil, err: nil},
			{stream: queue.StreamLow, count: 2, msgs: nil, err: nil},
		}

		// Override the consumer to also track what was read
		trackingConsumer := &trackingConsumer{
			inner: consumer,
			calls: &readCalls,
		}

		wp := newTestWorkerPool()
		wp.consumer = trackingConsumer

		ctx := context.Background()
		wp.pollStreams(ctx, "test-consumer")

		if len(readCalls) != 3 {
			t.Fatalf("expected 3 read calls, got %d", len(readCalls))
		}

		expectedWeights := map[string]int64{
			queue.StreamHigh:   10,
			queue.StreamNormal: 5,
			queue.StreamLow:    2,
		}

		for _, call := range readCalls {
			expected, ok := expectedWeights[call.stream]
			if !ok {
				t.Errorf("unexpected stream: %s", call.stream)
				continue
			}
			if call.count != expected {
				t.Errorf("stream %s: expected count %d, got %d", call.stream, expected, call.count)
			}
		}
	})

	t.Run("processes messages from high priority first", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:       nID,
			Status:   domain.StatusQueued,
			Channel:  domain.ChannelEmail,
			Priority: domain.PriorityHigh,
		}

		consumer := &mockConsumer{
			readCalls: []readCall{
				{
					stream: queue.StreamHigh,
					count:  10,
					msgs: []queue.Message{
						{
							ID:             "msg-1",
							StreamName:     queue.StreamHigh,
							NotificationID: nID,
							Channel:        domain.ChannelEmail,
							Recipient:      "test@example.com",
							Content:        "Hello",
							Priority:       domain.PriorityHigh,
						},
					},
				},
				{stream: queue.StreamNormal, count: 5, msgs: nil},
				{stream: queue.StreamLow, count: 2, msgs: nil},
			},
		}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.consumer = consumer
			o.repo = repo
		})

		ctx := context.Background()
		processed := wp.pollStreams(ctx, "test-consumer")

		if !processed {
			t.Error("expected pollStreams to return true when messages were processed")
		}
	})

	t.Run("returns false when no messages available", func(t *testing.T) {
		consumer := &mockConsumer{
			readCalls: []readCall{
				{stream: queue.StreamHigh, count: 10, msgs: nil},
				{stream: queue.StreamNormal, count: 5, msgs: nil},
				{stream: queue.StreamLow, count: 2, msgs: nil},
			},
		}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.consumer = consumer
		})

		ctx := context.Background()
		processed := wp.pollStreams(ctx, "test-consumer")

		if processed {
			t.Error("expected pollStreams to return false when no messages processed")
		}
	})
}

func TestProcessMessage_SuccessPath(t *testing.T) {
	t.Run("successful delivery updates status to delivered and acks", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:       nID,
			Status:   domain.StatusQueued,
			Channel:  domain.ChannelSMS,
			Priority: domain.PriorityNormal,
		}

		providerMsgID := "provider-abc-123"
		provider := &mockProvider{
			sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
				return &delivery.SendResult{ProviderMsgID: providerMsgID}, nil
			},
		}

		consumer := &mockConsumer{}
		broadcaster := &mockBroadcaster{}
		metrics := &mockMetrics{}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.provider = provider
			o.consumer = consumer
		})
		wp.broadcaster = broadcaster
		wp.metrics = metrics

		msg := queue.Message{
			ID:             "stream-msg-1",
			StreamName:     queue.StreamNormal,
			NotificationID: nID,
			Channel:        domain.ChannelSMS,
			Recipient:      "+1234567890",
			Content:        "Test notification",
			Priority:       domain.PriorityNormal,
		}

		ctx := context.Background()
		wp.processMessage(ctx, msg)

		// Verify status was updated to delivered
		repo.mu.Lock()
		foundDelivered := false
		for _, su := range repo.statusUpdates {
			if su.id == nID && su.to == domain.StatusDelivered {
				foundDelivered = true
				break
			}
		}
		repo.mu.Unlock()

		if !foundDelivered {
			t.Error("expected notification status to be updated to delivered")
		}

		// Verify message was acked
		consumer.mu.Lock()
		if len(consumer.ackCalls) == 0 {
			t.Error("expected message to be acknowledged")
		} else {
			ack := consumer.ackCalls[0]
			if ack.stream != queue.StreamNormal {
				t.Errorf("expected ack on stream %s, got %s", queue.StreamNormal, ack.stream)
			}
		}
		consumer.mu.Unlock()

		// Verify broadcaster was called
		broadcaster.mu.Lock()
		foundBroadcast := false
		for _, b := range broadcaster.broadcasts {
			if b.id == nID && b.status == domain.StatusDelivered {
				foundBroadcast = true
				break
			}
		}
		broadcaster.mu.Unlock()

		if !foundBroadcast {
			t.Error("expected broadcaster to be called with StatusDelivered")
		}

		// Verify metrics recorded
		metrics.mu.Lock()
		if metrics.deliveries != 1 {
			t.Errorf("expected 1 delivery metric, got %d", metrics.deliveries)
		}
		metrics.mu.Unlock()
	})

	t.Run("skips cancelled notification and acks", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:     nID,
			Status: domain.StatusCancelled,
		}

		consumer := &mockConsumer{}
		provider := &mockProvider{
			sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
				t.Error("provider.Send should not be called for cancelled notifications")
				return nil, nil
			},
		}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.consumer = consumer
			o.provider = provider
		})

		msg := queue.Message{
			ID:             "msg-cancelled",
			StreamName:     queue.StreamHigh,
			NotificationID: nID,
			Channel:        domain.ChannelEmail,
		}

		ctx := context.Background()
		wp.processMessage(ctx, msg)

		consumer.mu.Lock()
		if len(consumer.ackCalls) == 0 {
			t.Error("expected cancelled message to still be acknowledged")
		}
		consumer.mu.Unlock()
	})

	t.Run("skips already delivered notification and acks", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:     nID,
			Status: domain.StatusDelivered,
		}

		consumer := &mockConsumer{}
		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.consumer = consumer
		})

		msg := queue.Message{
			ID:             "msg-delivered",
			StreamName:     queue.StreamNormal,
			NotificationID: nID,
			Channel:        domain.ChannelPush,
		}

		ctx := context.Background()
		wp.processMessage(ctx, msg)

		consumer.mu.Lock()
		if len(consumer.ackCalls) == 0 {
			t.Error("expected already-delivered message to be acknowledged")
		}
		consumer.mu.Unlock()
	})
}

func TestProcessMessage_RetryableFailure(t *testing.T) {
	t.Run("retryable failure increments retry count and schedules retry", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:         nID,
			Status:     domain.StatusQueued,
			Channel:    domain.ChannelEmail,
			Priority:   domain.PriorityNormal,
			RetryCount: 0,
			MaxRetries: 3,
		}

		provider := &mockProvider{
			sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
				return &delivery.SendResult{Retryable: true}, errors.New("temporary network error")
			},
		}

		retryStrategy := &mockRetryStrategy{
			shouldRetry: true,
			nextDelay:   50 * time.Millisecond,
		}

		consumer := &mockConsumer{}
		metrics := &mockMetrics{}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.provider = provider
			o.retry = retryStrategy
			o.consumer = consumer
		})
		wp.metrics = metrics

		msg := queue.Message{
			ID:             "msg-retry",
			StreamName:     queue.StreamNormal,
			NotificationID: nID,
			Channel:        domain.ChannelEmail,
			Recipient:      "user@example.com",
			Content:        "Hello",
		}

		ctx := context.Background()
		wp.processMessage(ctx, msg)

		// Verify retry was incremented
		repo.mu.Lock()
		if len(repo.retryIncrements) == 0 {
			t.Fatal("expected retry to be incremented")
		}
		ri := repo.retryIncrements[0]
		if ri.id != nID {
			t.Errorf("expected retry increment for %s, got %s", nID, ri.id)
		}
		if ri.errorMsg != "temporary network error" {
			t.Errorf("expected error message 'temporary network error', got '%s'", ri.errorMsg)
		}
		repo.mu.Unlock()

		// Verify failure metric recorded
		metrics.mu.Lock()
		if metrics.failures != 1 {
			t.Errorf("expected 1 failure metric, got %d", metrics.failures)
		}
		metrics.mu.Unlock()

		// Verify NOT moved to DLQ
		repo.mu.Lock()
		if len(repo.dlqEntries) != 0 {
			t.Error("notification should not be moved to DLQ on retryable failure within limits")
		}
		repo.mu.Unlock()
	})

	t.Run("retryable failure exceeding max retries moves to DLQ", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:         nID,
			Status:     domain.StatusQueued,
			Channel:    domain.ChannelSMS,
			Priority:   domain.PriorityHigh,
			RetryCount: 3,
			MaxRetries: 3,
		}

		provider := &mockProvider{
			sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
				return &delivery.SendResult{Retryable: true}, errors.New("persistent error")
			},
		}

		retryStrategy := &mockRetryStrategy{
			shouldRetry: false, // exceed max retries
			nextDelay:   100 * time.Millisecond,
		}

		consumer := &mockConsumer{}
		broadcaster := &mockBroadcaster{}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.provider = provider
			o.retry = retryStrategy
			o.consumer = consumer
		})
		wp.broadcaster = broadcaster

		msg := queue.Message{
			ID:             "msg-max-retry",
			StreamName:     queue.StreamHigh,
			NotificationID: nID,
			Channel:        domain.ChannelSMS,
			Recipient:      "+1234567890",
			Content:        "Hello",
		}

		ctx := context.Background()
		wp.processMessage(ctx, msg)

		// Verify moved to DLQ
		repo.mu.Lock()
		if len(repo.dlqEntries) != 1 {
			t.Fatalf("expected 1 DLQ entry, got %d", len(repo.dlqEntries))
		}
		if repo.dlqEntries[0].ID != nID {
			t.Errorf("expected DLQ entry for %s, got %s", nID, repo.dlqEntries[0].ID)
		}
		repo.mu.Unlock()

		// Verify broadcaster notified of failure
		broadcaster.mu.Lock()
		foundFailed := false
		for _, b := range broadcaster.broadcasts {
			if b.id == nID && b.status == domain.StatusFailed {
				foundFailed = true
				break
			}
		}
		broadcaster.mu.Unlock()
		if !foundFailed {
			t.Error("expected broadcaster to be called with StatusFailed")
		}
	})
}

func TestProcessMessage_PermanentFailure(t *testing.T) {
	t.Run("non-retryable failure immediately moves to DLQ", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:         nID,
			Status:     domain.StatusQueued,
			Channel:    domain.ChannelPush,
			Priority:   domain.PriorityLow,
			RetryCount: 0,
			MaxRetries: 3,
		}

		provider := &mockProvider{
			sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
				return &delivery.SendResult{Retryable: false}, errors.New("invalid recipient token")
			},
		}

		consumer := &mockConsumer{}
		broadcaster := &mockBroadcaster{}
		metrics := &mockMetrics{}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.provider = provider
			o.consumer = consumer
		})
		wp.broadcaster = broadcaster
		wp.metrics = metrics

		msg := queue.Message{
			ID:             "msg-perm-fail",
			StreamName:     queue.StreamLow,
			NotificationID: nID,
			Channel:        domain.ChannelPush,
			Recipient:      "invalid-token-abc",
			Content:        "Push content",
		}

		ctx := context.Background()
		wp.processMessage(ctx, msg)

		// Verify moved to DLQ immediately (no retries)
		repo.mu.Lock()
		if len(repo.dlqEntries) != 1 {
			t.Fatalf("expected 1 DLQ entry for permanent failure, got %d", len(repo.dlqEntries))
		}
		if repo.dlqEntries[0].ID != nID {
			t.Errorf("expected DLQ for %s, got %s", nID, repo.dlqEntries[0].ID)
		}
		// Verify NO retry increments
		if len(repo.retryIncrements) != 0 {
			t.Error("permanent failure should not increment retries")
		}
		repo.mu.Unlock()

		// Verify broadcaster notified
		broadcaster.mu.Lock()
		foundFailed := false
		for _, b := range broadcaster.broadcasts {
			if b.id == nID && b.status == domain.StatusFailed {
				foundFailed = true
			}
		}
		broadcaster.mu.Unlock()
		if !foundFailed {
			t.Error("expected broadcaster to broadcast StatusFailed for permanent failure")
		}

		// Verify failure metric
		metrics.mu.Lock()
		if metrics.failures != 1 {
			t.Errorf("expected 1 failure metric, got %d", metrics.failures)
		}
		metrics.mu.Unlock()
	})
}

func TestStaleMessageRecovery(t *testing.T) {
	t.Run("claimer reclaims stale messages and processes them", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:       nID,
			Status:   domain.StatusQueued,
			Channel:  domain.ChannelEmail,
			Priority: domain.PriorityHigh,
		}

		claimCalled := make(chan struct{}, 3)

		consumer := &mockConsumer{
			claimFn: func(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]queue.Message, error) {
				select {
				case claimCalled <- struct{}{}:
				default:
				}
				if stream == queue.StreamHigh {
					return []queue.Message{
						{
							ID:             "stale-msg-1",
							StreamName:     queue.StreamHigh,
							NotificationID: nID,
							Channel:        domain.ChannelEmail,
							Recipient:      "stale@example.com",
							Content:        "Stale message",
							Priority:       domain.PriorityHigh,
						},
					}, nil
				}
				return nil, nil
			},
		}

		provider := &mockProvider{
			sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
				return &delivery.SendResult{ProviderMsgID: "recovered-msg"}, nil
			},
		}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.consumer = consumer
			o.provider = provider
		})
		wp.cfg.ClaimInterval = 50 * time.Millisecond
		wp.cfg.ClaimMinIdle = 30 * time.Second

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		wp.wg.Add(1)
		go wp.runClaimer(ctx)

		// Wait for at least one claim cycle
		select {
		case <-claimCalled:
		case <-ctx.Done():
			t.Fatal("timeout waiting for claim to be called")
		}

		// Give a brief moment for processMessage to complete
		time.Sleep(50 * time.Millisecond)
		cancel()
		wp.wg.Wait()

		// Verify the stale message was processed (status updated to delivered)
		repo.mu.Lock()
		foundDelivered := false
		for _, su := range repo.statusUpdates {
			if su.id == nID && su.to == domain.StatusDelivered {
				foundDelivered = true
				break
			}
		}
		repo.mu.Unlock()

		if !foundDelivered {
			t.Error("expected stale message to be processed and delivered")
		}
	})

	t.Run("claimer handles claim errors gracefully", func(t *testing.T) {
		claimCallCount := 0
		consumer := &mockConsumer{
			claimFn: func(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]queue.Message, error) {
				claimCallCount++
				return nil, fmt.Errorf("redis connection refused")
			},
		}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.consumer = consumer
		})
		wp.cfg.ClaimInterval = 50 * time.Millisecond

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		wp.wg.Add(1)
		go wp.runClaimer(ctx)

		<-ctx.Done()
		wp.wg.Wait()

		// Should have attempted at least one claim cycle without panicking
		if claimCallCount == 0 {
			t.Error("expected at least one claim attempt")
		}
	})
}

// --- Tracking consumer wrapper for verifying read parameters ---

type trackingConsumer struct {
	inner *mockConsumer
	mu    sync.Mutex
	calls *[]struct {
		stream string
		count  int64
	}
}

func (t *trackingConsumer) Read(ctx context.Context, stream, group, consumer string, count int64) ([]queue.Message, error) {
	t.mu.Lock()
	*t.calls = append(*t.calls, struct {
		stream string
		count  int64
	}{stream: stream, count: count})
	t.mu.Unlock()
	return t.inner.Read(ctx, stream, group, consumer, count)
}

func (t *trackingConsumer) Ack(ctx context.Context, stream, group string, ids ...string) error {
	return t.inner.Ack(ctx, stream, group, ids...)
}

func (t *trackingConsumer) ClaimStale(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]queue.Message, error) {
	return t.inner.ClaimStale(ctx, stream, group, consumer, minIdle, count)
}

func (t *trackingConsumer) Len(ctx context.Context, stream string) (int64, error) {
	return t.inner.Len(ctx, stream)
}
