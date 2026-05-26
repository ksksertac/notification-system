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
func (m *mockRepo) GetRetryReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *mockRepo) RecoverStuckProcessing(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *mockRepo) RecoverOrphanedPending(ctx context.Context, staleDuration time.Duration, limit int) ([]*domain.Notification, error) {
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
		requeueSem:  make(chan struct{}, cfg.WorkerCount*2),
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

// --- Additional test cases for coverage ---

func TestNewWorkerPool(t *testing.T) {
	cfg := WorkerPoolConfig{
		ConsumerGroup: "my-group",
		WorkerCount:   4,
		WeightHigh:    10,
		WeightNormal:  5,
		WeightLow:     2,
		ClaimMinIdle:  30 * time.Second,
		ClaimInterval: 10 * time.Second,
		MaxRetries:    5,
	}

	consumer := &mockConsumer{}
	publisher := &mockPublisher{}
	provider := &mockProvider{}
	rateLimiter := &mockRateLimiter{allowed: true}
	cbRegistry := delivery.NewCircuitBreakerRegistry(delivery.CircuitBreakerConfig{
		FailureThreshold: 5,
		OpenDuration:     30 * time.Second,
		HalfOpenMax:      2,
	})
	retry := &mockRetryStrategy{shouldRetry: true, nextDelay: 100 * time.Millisecond}
	repo := newMockRepo()
	broadcaster := &mockBroadcaster{}
	metrics := &mockMetrics{}
	tmplEngine := &mockTemplateEngine{}
	logger := slog.Default()

	wp := NewWorkerPool(cfg, consumer, publisher, provider, rateLimiter, cbRegistry, retry, repo, broadcaster, metrics, tmplEngine, logger)

	if wp == nil {
		t.Fatal("expected non-nil WorkerPool")
	}
	if wp.cfg.ConsumerGroup != "my-group" {
		t.Errorf("expected ConsumerGroup 'my-group', got %q", wp.cfg.ConsumerGroup)
	}
	if wp.cfg.WorkerCount != 4 {
		t.Errorf("expected WorkerCount 4, got %d", wp.cfg.WorkerCount)
	}
	if wp.cfg.MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", wp.cfg.MaxRetries)
	}
	if wp.consumer != consumer {
		t.Error("expected consumer to be set")
	}
	if wp.publisher != publisher {
		t.Error("expected publisher to be set")
	}
	if wp.provider != provider {
		t.Error("expected provider to be set")
	}
	if wp.rateLimiter != rateLimiter {
		t.Error("expected rateLimiter to be set")
	}
	if wp.cbRegistry != cbRegistry {
		t.Error("expected cbRegistry to be set")
	}
	if wp.repo != repo {
		t.Error("expected repo to be set")
	}
	if wp.tmplEngine != tmplEngine {
		t.Error("expected tmplEngine to be set")
	}
}

func TestWorkerPool_StartStop(t *testing.T) {
	nID := uuid.New()
	repo := newMockRepo()
	repo.notifications[nID] = &domain.Notification{
		ID:       nID,
		Status:   domain.StatusQueued,
		Channel:  domain.ChannelEmail,
		Priority: domain.PriorityHigh,
	}

	msgDelivered := make(chan struct{}, 1)

	consumer := &mockConsumer{
		readCalls: []readCall{
			{
				stream: queue.StreamHigh,
				count:  10,
				msgs: []queue.Message{
					{
						ID:             "start-stop-msg-1",
						StreamName:     queue.StreamHigh,
						NotificationID: nID,
						Channel:        domain.ChannelEmail,
						Recipient:      "user@test.com",
						Content:        "Hello",
						Priority:       domain.PriorityHigh,
					},
				},
			},
		},
	}

	provider := &mockProvider{
		sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
			select {
			case msgDelivered <- struct{}{}:
			default:
			}
			return &delivery.SendResult{ProviderMsgID: "start-stop-provider"}, nil
		},
	}

	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.consumer = consumer
		o.repo = repo
		o.provider = provider
	})
	wp.cfg.ClaimInterval = 5 * time.Second // long interval so claimer doesn't interfere

	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)

	// Wait for the message to be delivered or timeout
	select {
	case <-msgDelivered:
		// success
	case <-time.After(2 * time.Second):
		// Message might not have been delivered depending on timing, that's okay
	}

	cancel()
	wp.Stop()

	// If we got here without hanging, Start/Stop lifecycle works correctly
}

func TestProcessMessage_RateLimitDenied(t *testing.T) {
	nID := uuid.New()
	repo := newMockRepo()
	repo.notifications[nID] = &domain.Notification{
		ID:       nID,
		Status:   domain.StatusQueued,
		Channel:  domain.ChannelEmail,
		Priority: domain.PriorityNormal,
	}

	consumer := &mockConsumer{}
	publisher := &mockPublisher{}
	metrics := &mockMetrics{}

	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.repo = repo
		o.consumer = consumer
		o.publisher = publisher
		o.rateLimiter = &mockRateLimiter{allowed: false, err: nil}
	})
	wp.metrics = metrics

	msg := queue.Message{
		ID:             "msg-rate-limited",
		StreamName:     queue.StreamNormal,
		NotificationID: nID,
		Channel:        domain.ChannelEmail,
		Recipient:      "user@example.com",
		Content:        "Rate limited content",
	}

	ctx := context.Background()
	wp.processMessage(ctx, msg)

	// Verify message was acked
	consumer.mu.Lock()
	if len(consumer.ackCalls) == 0 {
		t.Error("expected message to be acknowledged after rate limit denial")
	}
	consumer.mu.Unlock()

	// Verify rate limit hit metric recorded
	metrics.mu.Lock()
	if metrics.rateLimitHits != 1 {
		t.Errorf("expected 1 rate limit hit metric, got %d", metrics.rateLimitHits)
	}
	metrics.mu.Unlock()

	// Verify status was reverted to queued
	repo.mu.Lock()
	foundRevert := false
	for _, su := range repo.statusUpdates {
		if su.id == nID && su.from == domain.StatusProcessing && su.to == domain.StatusQueued {
			foundRevert = true
			break
		}
	}
	repo.mu.Unlock()
	if !foundRevert {
		t.Error("expected status to be reverted to queued on rate limit denial")
	}

	// Verify re-enqueue was triggered (publisher will be called after 500ms delay)
	time.Sleep(700 * time.Millisecond)
	publisher.mu.Lock()
	if len(publisher.published) != 1 {
		t.Errorf("expected 1 re-enqueue, got %d", len(publisher.published))
	} else if publisher.published[0].ID != nID {
		t.Errorf("expected re-enqueued notification ID %s, got %s", nID, publisher.published[0].ID)
	}
	publisher.mu.Unlock()
}

func TestProcessMessage_RateLimitError(t *testing.T) {
	nID := uuid.New()
	repo := newMockRepo()
	repo.notifications[nID] = &domain.Notification{
		ID:       nID,
		Status:   domain.StatusQueued,
		Channel:  domain.ChannelSMS,
		Priority: domain.PriorityNormal,
	}

	consumer := &mockConsumer{}
	publisher := &mockPublisher{}
	metrics := &mockMetrics{}

	// Rate limiter returns an error AND allowed=false
	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.repo = repo
		o.consumer = consumer
		o.publisher = publisher
		o.rateLimiter = &mockRateLimiter{allowed: false, err: errors.New("redis timeout")}
	})
	wp.metrics = metrics

	msg := queue.Message{
		ID:             "msg-rate-error",
		StreamName:     queue.StreamNormal,
		NotificationID: nID,
		Channel:        domain.ChannelSMS,
		Recipient:      "+1234567890",
		Content:        "Rate error content",
	}

	ctx := context.Background()
	wp.processMessage(ctx, msg)

	// Verify message was acked (rate limiter error with !allowed leads to re-enqueue path)
	consumer.mu.Lock()
	if len(consumer.ackCalls) == 0 {
		t.Error("expected message to be acknowledged after rate limiter error")
	}
	consumer.mu.Unlock()

	// Verify rate limit hit metric recorded
	metrics.mu.Lock()
	if metrics.rateLimitHits != 1 {
		t.Errorf("expected 1 rate limit hit metric, got %d", metrics.rateLimitHits)
	}
	metrics.mu.Unlock()

	// Verify re-enqueue was triggered
	time.Sleep(700 * time.Millisecond)
	publisher.mu.Lock()
	if len(publisher.published) != 1 {
		t.Errorf("expected 1 re-enqueue after rate limiter error, got %d", len(publisher.published))
	}
	publisher.mu.Unlock()
}

func TestProcessMessage_CircuitBreakerOpen(t *testing.T) {
	nID := uuid.New()
	repo := newMockRepo()
	repo.notifications[nID] = &domain.Notification{
		ID:       nID,
		Status:   domain.StatusQueued,
		Channel:  domain.ChannelEmail,
		Priority: domain.PriorityNormal,
	}

	consumer := &mockConsumer{}
	publisher := &mockPublisher{}
	metrics := &mockMetrics{}

	// Create a circuit breaker registry with low failure threshold and trip it
	cbRegistry := delivery.NewCircuitBreakerRegistry(delivery.CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     1 * time.Hour, // long enough it won't transition back
		HalfOpenMax:      1,
	})
	// Trip the circuit breaker for "email" channel
	cb := cbRegistry.Get("email")
	cb.RecordFailure() // This should trip it since threshold is 1

	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.repo = repo
		o.consumer = consumer
		o.publisher = publisher
	})
	wp.cbRegistry = cbRegistry
	wp.metrics = metrics

	msg := queue.Message{
		ID:             "msg-cb-open",
		StreamName:     queue.StreamNormal,
		NotificationID: nID,
		Channel:        domain.ChannelEmail,
		Recipient:      "user@example.com",
		Content:        "CB open content",
	}

	ctx := context.Background()
	wp.processMessage(ctx, msg)

	// Verify message was acked
	consumer.mu.Lock()
	if len(consumer.ackCalls) == 0 {
		t.Error("expected message to be acknowledged when circuit breaker is open")
	}
	consumer.mu.Unlock()

	// Verify circuit breaker open metric recorded
	metrics.mu.Lock()
	if metrics.circuitBreakerOpens != 1 {
		t.Errorf("expected 1 circuit breaker open metric, got %d", metrics.circuitBreakerOpens)
	}
	metrics.mu.Unlock()

	// Verify re-enqueue was triggered
	time.Sleep(700 * time.Millisecond)
	publisher.mu.Lock()
	if len(publisher.published) != 1 {
		t.Errorf("expected 1 re-enqueue when CB open, got %d", len(publisher.published))
	} else if publisher.published[0].ID != nID {
		t.Errorf("expected re-enqueued notification ID %s, got %s", nID, publisher.published[0].ID)
	}
	publisher.mu.Unlock()
}

func TestProcessMessage_NotFoundInRepo(t *testing.T) {
	// Use a notification ID that doesn't exist in the repo
	nID := uuid.New()
	repo := newMockRepo()
	// Do NOT add any notification to repo - so GetByID returns nil

	consumer := &mockConsumer{}
	provider := &mockProvider{
		sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
			t.Error("provider.Send should not be called when notification not found")
			return nil, nil
		},
	}

	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.repo = repo
		o.consumer = consumer
		o.provider = provider
	})

	msg := queue.Message{
		ID:             "msg-not-found",
		StreamName:     queue.StreamHigh,
		NotificationID: nID,
		Channel:        domain.ChannelEmail,
		Recipient:      "nobody@example.com",
		Content:        "Should not be sent",
	}

	ctx := context.Background()
	wp.processMessage(ctx, msg)

	// Verify message was acked (nil notification -> ack and skip)
	consumer.mu.Lock()
	if len(consumer.ackCalls) == 0 {
		t.Error("expected message to be acknowledged when notification is not found")
	}
	consumer.mu.Unlock()
}

func TestProcessMessage_GetByIDError(t *testing.T) {
	nID := uuid.New()

	// Create a custom repo that returns an error from GetByID
	errorRepo := &errorMockRepo{
		getByIDErr: errors.New("database connection lost"),
	}

	consumer := &mockConsumer{}
	provider := &mockProvider{
		sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
			t.Error("provider.Send should not be called when GetByID returns error")
			return nil, nil
		},
	}

	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.consumer = consumer
		o.provider = provider
	})
	wp.repo = errorRepo

	msg := queue.Message{
		ID:             "msg-repo-error",
		StreamName:     queue.StreamNormal,
		NotificationID: nID,
		Channel:        domain.ChannelSMS,
		Recipient:      "+1234567890",
		Content:        "Should not be sent",
	}

	ctx := context.Background()
	wp.processMessage(ctx, msg)

	// Verify message was NOT acked (early return on error without ack)
	consumer.mu.Lock()
	if len(consumer.ackCalls) != 0 {
		t.Error("expected message to NOT be acknowledged when GetByID returns error")
	}
	consumer.mu.Unlock()
}

func TestProcessMessage_TemplateRendering(t *testing.T) {
	t.Run("template renders successfully with metadata", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:       nID,
			Status:   domain.StatusQueued,
			Channel:  domain.ChannelEmail,
			Priority: domain.PriorityNormal,
			Metadata: []byte(`{"name":"John"}`),
		}

		var sentContent string
		provider := &mockProvider{
			sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
				sentContent = content
				return &delivery.SendResult{ProviderMsgID: "tmpl-msg"}, nil
			},
		}

		consumer := &mockConsumer{}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.provider = provider
			o.consumer = consumer
		})
		// Use a template engine that actually transforms content
		wp.tmplEngine = &transformingTemplateEngine{rendered: "Hello John!"}

		msg := queue.Message{
			ID:             "msg-tmpl",
			StreamName:     queue.StreamNormal,
			NotificationID: nID,
			Channel:        domain.ChannelEmail,
			Recipient:      "john@example.com",
			Content:        "Hello {{.name}}!",
		}

		ctx := context.Background()
		wp.processMessage(ctx, msg)

		if sentContent != "Hello John!" {
			t.Errorf("expected rendered content 'Hello John!', got %q", sentContent)
		}
	})

	t.Run("template rendering failure uses raw content", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:       nID,
			Status:   domain.StatusQueued,
			Channel:  domain.ChannelEmail,
			Priority: domain.PriorityNormal,
			Metadata: []byte(`{"name":"John"}`),
		}

		var sentContent string
		provider := &mockProvider{
			sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
				sentContent = content
				return &delivery.SendResult{ProviderMsgID: "tmpl-fail-msg"}, nil
			},
		}

		consumer := &mockConsumer{}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.provider = provider
			o.consumer = consumer
		})
		// Use a template engine that returns an error
		wp.tmplEngine = &errorTemplateEngine{}

		msg := queue.Message{
			ID:             "msg-tmpl-fail",
			StreamName:     queue.StreamNormal,
			NotificationID: nID,
			Channel:        domain.ChannelEmail,
			Recipient:      "john@example.com",
			Content:        "Hello {{.name}}!",
		}

		ctx := context.Background()
		wp.processMessage(ctx, msg)

		// When template rendering fails, raw content should be used
		if sentContent != "Hello {{.name}}!" {
			t.Errorf("expected raw content 'Hello {{.name}}!', got %q", sentContent)
		}
	})

	t.Run("nil metadata skips template rendering", func(t *testing.T) {
		nID := uuid.New()
		repo := newMockRepo()
		repo.notifications[nID] = &domain.Notification{
			ID:       nID,
			Status:   domain.StatusQueued,
			Channel:  domain.ChannelEmail,
			Priority: domain.PriorityNormal,
			Metadata: nil, // nil metadata
		}

		var sentContent string
		provider := &mockProvider{
			sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
				sentContent = content
				return &delivery.SendResult{ProviderMsgID: "no-tmpl-msg"}, nil
			},
		}

		consumer := &mockConsumer{}

		wp := newTestWorkerPool(func(o *testPoolOpts) {
			o.repo = repo
			o.provider = provider
			o.consumer = consumer
		})
		// Template engine that would fail if called
		wp.tmplEngine = &errorTemplateEngine{}

		msg := queue.Message{
			ID:             "msg-nil-meta",
			StreamName:     queue.StreamNormal,
			NotificationID: nID,
			Channel:        domain.ChannelEmail,
			Recipient:      "user@example.com",
			Content:        "Raw content here",
		}

		ctx := context.Background()
		wp.processMessage(ctx, msg)

		if sentContent != "Raw content here" {
			t.Errorf("expected raw content 'Raw content here', got %q", sentContent)
		}
	})
}

func TestReEnqueue(t *testing.T) {
	publisher := &mockPublisher{}
	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.publisher = publisher
	})

	n := &domain.Notification{
		ID:       uuid.New(),
		Status:   domain.StatusQueued,
		Channel:  domain.ChannelEmail,
		Priority: domain.PriorityNormal,
	}

	ctx := context.Background()
	wp.reEnqueue(ctx, n)

	// reEnqueue publishes after a 500ms delay in a goroutine
	time.Sleep(700 * time.Millisecond)

	publisher.mu.Lock()
	if len(publisher.published) != 1 {
		t.Fatalf("expected 1 published notification, got %d", len(publisher.published))
	}
	if publisher.published[0].ID != n.ID {
		t.Errorf("expected published notification ID %s, got %s", n.ID, publisher.published[0].ID)
	}
	publisher.mu.Unlock()
}

func TestPollStreams_ReadError(t *testing.T) {
	consumer := &mockConsumer{
		readCalls: []readCall{
			{stream: queue.StreamHigh, count: 10, msgs: nil, err: errors.New("read error on high stream")},
			{stream: queue.StreamNormal, count: 5, msgs: nil, err: errors.New("read error on normal stream")},
			{stream: queue.StreamLow, count: 2, msgs: nil, err: errors.New("read error on low stream")},
		},
	}

	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.consumer = consumer
	})

	ctx := context.Background()
	processed := wp.pollStreams(ctx, "test-consumer")

	// When all reads return errors, no messages are processed
	if processed {
		t.Error("expected pollStreams to return false when all reads error")
	}
}

func TestProcessMessage_NilBroadcaster(t *testing.T) {
	nID := uuid.New()
	repo := newMockRepo()
	repo.notifications[nID] = &domain.Notification{
		ID:       nID,
		Status:   domain.StatusQueued,
		Channel:  domain.ChannelEmail,
		Priority: domain.PriorityNormal,
	}

	provider := &mockProvider{
		sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
			return &delivery.SendResult{ProviderMsgID: "nil-bc-msg"}, nil
		},
	}

	consumer := &mockConsumer{}

	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.repo = repo
		o.provider = provider
		o.consumer = consumer
	})
	wp.broadcaster = nil // explicitly nil

	msg := queue.Message{
		ID:             "msg-nil-broadcaster",
		StreamName:     queue.StreamNormal,
		NotificationID: nID,
		Channel:        domain.ChannelEmail,
		Recipient:      "user@example.com",
		Content:        "Hello",
	}

	ctx := context.Background()
	// Should not panic when broadcaster is nil
	wp.processMessage(ctx, msg)

	// Verify it still delivered
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
		t.Error("expected delivery even with nil broadcaster")
	}
}

func TestProcessMessage_NilMetrics(t *testing.T) {
	nID := uuid.New()
	repo := newMockRepo()
	repo.notifications[nID] = &domain.Notification{
		ID:       nID,
		Status:   domain.StatusQueued,
		Channel:  domain.ChannelSMS,
		Priority: domain.PriorityNormal,
	}

	provider := &mockProvider{
		sendFn: func(ctx context.Context, recipient, channel, content string) (*delivery.SendResult, error) {
			return &delivery.SendResult{ProviderMsgID: "nil-metrics-msg"}, nil
		},
	}

	consumer := &mockConsumer{}

	wp := newTestWorkerPool(func(o *testPoolOpts) {
		o.repo = repo
		o.provider = provider
		o.consumer = consumer
	})
	wp.metrics = nil // explicitly nil

	msg := queue.Message{
		ID:             "msg-nil-metrics",
		StreamName:     queue.StreamNormal,
		NotificationID: nID,
		Channel:        domain.ChannelSMS,
		Recipient:      "+1234567890",
		Content:        "Hello",
	}

	ctx := context.Background()
	// Should not panic when metrics is nil
	wp.processMessage(ctx, msg)
}

// --- Additional mock types ---

type errorMockRepo struct {
	getByIDErr error
}

func (m *errorMockRepo) Create(ctx context.Context, n *domain.Notification) error { return nil }
func (m *errorMockRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	return nil
}
func (m *errorMockRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return nil, m.getByIDErr
}
func (m *errorMockRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *errorMockRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	return nil, nil
}
func (m *errorMockRepo) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	return nil, 0, nil
}
func (m *errorMockRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
	return true, nil
}
func (m *errorMockRepo) UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
	return true, nil
}
func (m *errorMockRepo) IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	return nil
}
func (m *errorMockRepo) MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error {
	return nil
}
func (m *errorMockRepo) GetScheduledReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *errorMockRepo) ClaimScheduledBatch(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *errorMockRepo) RecoverStuckQueued(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *errorMockRepo) GetRetryReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *errorMockRepo) RecoverStuckProcessing(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *errorMockRepo) RecoverOrphanedPending(ctx context.Context, staleDuration time.Duration, limit int) ([]*domain.Notification, error) {
	return nil, nil
}

var _ repository.NotificationRepository = (*errorMockRepo)(nil)

type transformingTemplateEngine struct {
	rendered string
}

func (m *transformingTemplateEngine) Render(tmpl string, metadata []byte) (string, error) {
	return m.rendered, nil
}

type errorTemplateEngine struct{}

func (m *errorTemplateEngine) Render(tmpl string, metadata []byte) (string, error) {
	return "", errors.New("template parse error")
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
