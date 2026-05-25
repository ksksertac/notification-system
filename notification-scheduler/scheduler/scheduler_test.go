package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/repository"
)

// --- Mock implementations ---

type mockRepo struct {
	mu                   sync.Mutex
	claimBatchFn         func(ctx context.Context, limit int) ([]*domain.Notification, error)
	recoverStuckQueuedFn func(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error)
}

func (m *mockRepo) Create(ctx context.Context, n *domain.Notification) error { return nil }
func (m *mockRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	return nil
}
func (m *mockRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return nil, nil
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
	return true, nil
}
func (m *mockRepo) UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
	return true, nil
}
func (m *mockRepo) IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	return nil
}
func (m *mockRepo) MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error {
	return nil
}
func (m *mockRepo) GetScheduledReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return nil, nil
}
func (m *mockRepo) ClaimScheduledBatch(ctx context.Context, limit int) ([]*domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimBatchFn != nil {
		return m.claimBatchFn(ctx, limit)
	}
	return nil, nil
}
func (m *mockRepo) RecoverStuckQueued(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recoverStuckQueuedFn != nil {
		return m.recoverStuckQueuedFn(ctx, stuckThreshold, limit)
	}
	return nil, nil
}

var _ repository.NotificationRepository = (*mockRepo)(nil)

type mockPublisher struct {
	mu              sync.Mutex
	publishBatchFn  func(ctx context.Context, notifications []*domain.Notification) error
	publishedBatch  [][]*domain.Notification
	publishCount    int
}

func (m *mockPublisher) Publish(ctx context.Context, n *domain.Notification) error {
	return nil
}

func (m *mockPublisher) PublishBatch(ctx context.Context, notifications []*domain.Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedBatch = append(m.publishedBatch, notifications)
	m.publishCount += len(notifications)
	if m.publishBatchFn != nil {
		return m.publishBatchFn(ctx, notifications)
	}
	return nil
}

// --- Helper functions ---

func makeNotification(priority domain.Priority, scheduledAt time.Time) *domain.Notification {
	return &domain.Notification{
		ID:          uuid.New(),
		Recipient:   "user@example.com",
		Channel:     domain.ChannelEmail,
		Content:     "Test notification",
		Priority:    priority,
		Status:      domain.StatusPending,
		ScheduledAt: &scheduledAt,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
}

func newTestScheduler(repo repository.NotificationRepository, pub *mockPublisher) *Scheduler {
	return &Scheduler{
		repo:           repo,
		publisher:      pub,
		pollInterval:   50 * time.Millisecond,
		batchSize:      10,
		stuckThreshold: 2 * time.Minute,
		logger:         slog.Default(),
	}
}

// --- Tests ---

func TestScheduler_ProcessesReadyNotifications(t *testing.T) {
	t.Run("processes notifications with scheduled_at in the past", func(t *testing.T) {
		past := time.Now().Add(-5 * time.Minute)
		ready := []*domain.Notification{
			makeNotification(domain.PriorityHigh, past),
			makeNotification(domain.PriorityNormal, past),
		}

		callCount := 0
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				callCount++
				if callCount == 1 {
					return ready, nil
				}
				return nil, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		s.drainScheduled(ctx)

		pub.mu.Lock()
		defer pub.mu.Unlock()

		if pub.publishCount != 2 {
			t.Errorf("expected 2 notifications published, got %d", pub.publishCount)
		}

		if len(pub.publishedBatch) == 0 {
			t.Fatal("expected at least one PublishBatch call")
		}

		published := pub.publishedBatch[0]
		if len(published) != 2 {
			t.Errorf("expected batch of 2, got %d", len(published))
		}
	})

	t.Run("claims up to batchSize notifications per call", func(t *testing.T) {
		past := time.Now().Add(-1 * time.Minute)
		batch := make([]*domain.Notification, 10)
		for i := range batch {
			batch[i] = makeNotification(domain.PriorityNormal, past)
		}

		callCount := 0
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				if limit != 10 {
					t.Errorf("expected limit 10, got %d", limit)
				}
				callCount++
				if callCount == 1 {
					return batch, nil
				}
				// Second call returns empty to stop draining
				return nil, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		s.drainScheduled(ctx)

		// Since first batch was exactly batchSize (10), drainScheduled loops
		// and calls again, getting empty result
		if callCount != 2 {
			t.Errorf("expected 2 claim calls (first full batch triggers re-poll), got %d", callCount)
		}
	})
}

func TestScheduler_SkipsFutureNotifications(t *testing.T) {
	t.Run("repository filters future notifications so scheduler gets empty batch", func(t *testing.T) {
		// The scheduler relies on ClaimScheduledBatch to only return ready notifications.
		// When nothing is ready, it should process zero.
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				// Simulates the repo correctly filtering: nothing is ready
				return nil, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		count := s.processBatch(ctx)

		if count != 0 {
			t.Errorf("expected 0 processed when no ready notifications, got %d", count)
		}

		pub.mu.Lock()
		defer pub.mu.Unlock()
		if pub.publishCount != 0 {
			t.Errorf("expected 0 published, got %d", pub.publishCount)
		}
	})
}

func TestScheduler_RecoveryLoop(t *testing.T) {
	t.Run("recovers stuck queued notifications", func(t *testing.T) {
		stuckNotification := &domain.Notification{
			ID:        uuid.New(),
			Status:    domain.StatusQueued,
			Channel:   domain.ChannelSMS,
			Priority:  domain.PriorityNormal,
			CreatedAt: time.Now().Add(-5 * time.Minute),
			UpdatedAt: time.Now().Add(-5 * time.Minute),
		}

		recoverCalled := make(chan struct{}, 1)
		repo := &mockRepo{
			recoverStuckQueuedFn: func(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
				select {
				case recoverCalled <- struct{}{}:
				default:
				}
				if stuckThreshold != 2*time.Minute {
					t.Errorf("expected stuck threshold 2m, got %v", stuckThreshold)
				}
				return []*domain.Notification{stuckNotification}, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		s.recoverStuck(ctx)

		select {
		case <-recoverCalled:
			// ok
		default:
			t.Fatal("recoverStuckQueued was not called")
		}
	})

	t.Run("handles recovery errors gracefully", func(t *testing.T) {
		repo := &mockRepo{
			recoverStuckQueuedFn: func(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
				return nil, errors.New("database connection lost")
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		// Should not panic
		s.recoverStuck(ctx)
	})
}

func TestScheduler_BatchPublishing(t *testing.T) {
	t.Run("publishes multiple notifications in a single batch", func(t *testing.T) {
		past := time.Now().Add(-1 * time.Minute)
		notifications := make([]*domain.Notification, 5)
		for i := range notifications {
			notifications[i] = makeNotification(domain.PriorityNormal, past)
		}

		callCount := 0
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				callCount++
				if callCount == 1 {
					return notifications, nil
				}
				return nil, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		s.drainScheduled(ctx)

		pub.mu.Lock()
		defer pub.mu.Unlock()

		if len(pub.publishedBatch) != 1 {
			t.Fatalf("expected 1 PublishBatch call, got %d", len(pub.publishedBatch))
		}

		if len(pub.publishedBatch[0]) != 5 {
			t.Errorf("expected batch of 5 notifications, got %d", len(pub.publishedBatch[0]))
		}
	})

	t.Run("drains multiple batches when first is full", func(t *testing.T) {
		past := time.Now().Add(-2 * time.Minute)

		// First batch is full (10 items = batchSize), second has 3
		firstBatch := make([]*domain.Notification, 10)
		for i := range firstBatch {
			firstBatch[i] = makeNotification(domain.PriorityHigh, past)
		}
		secondBatch := make([]*domain.Notification, 3)
		for i := range secondBatch {
			secondBatch[i] = makeNotification(domain.PriorityLow, past)
		}

		callCount := 0
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				callCount++
				switch callCount {
				case 1:
					return firstBatch, nil
				case 2:
					return secondBatch, nil
				default:
					return nil, nil
				}
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		s.drainScheduled(ctx)

		pub.mu.Lock()
		defer pub.mu.Unlock()

		if pub.publishCount != 13 {
			t.Errorf("expected 13 total notifications published, got %d", pub.publishCount)
		}

		if len(pub.publishedBatch) != 2 {
			t.Errorf("expected 2 PublishBatch calls, got %d", len(pub.publishedBatch))
		}
	})

	t.Run("publish error stops processing and returns zero", func(t *testing.T) {
		past := time.Now().Add(-1 * time.Minute)
		notifications := []*domain.Notification{
			makeNotification(domain.PriorityNormal, past),
		}

		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				return notifications, nil
			},
		}

		pub := &mockPublisher{
			publishBatchFn: func(ctx context.Context, notifications []*domain.Notification) error {
				return errors.New("redis unavailable")
			},
		}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		count := s.processBatch(ctx)

		if count != 0 {
			t.Errorf("expected 0 when publish fails, got %d", count)
		}
	})
}

func TestScheduler_ConcurrentClaimSimulation(t *testing.T) {
	t.Run("concurrent claims do not cause double processing", func(t *testing.T) {
		past := time.Now().Add(-1 * time.Minute)
		sharedNotification := makeNotification(domain.PriorityHigh, past)

		// Simulate atomic claim: only the first caller gets the notification
		var claimed int32

		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				if atomic.CompareAndSwapInt32(&claimed, 0, 1) {
					return []*domain.Notification{sharedNotification}, nil
				}
				return nil, nil
			},
		}

		pub := &mockPublisher{}

		// Run multiple schedulers concurrently to simulate concurrent claims
		const numSchedulers = 5
		var wg sync.WaitGroup

		for i := 0; i < numSchedulers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := newTestScheduler(repo, pub)
				ctx := context.Background()
				s.processBatch(ctx)
			}()
		}

		wg.Wait()

		pub.mu.Lock()
		defer pub.mu.Unlock()

		// Only one scheduler should have published the notification
		if pub.publishCount != 1 {
			t.Errorf("expected exactly 1 notification published (no double processing), got %d", pub.publishCount)
		}
	})

	t.Run("claim with database contention returns no notifications gracefully", func(t *testing.T) {
		// Simulate database returning empty due to row-level locking
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				// Another scheduler already claimed everything
				return nil, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		count := s.processBatch(ctx)

		if count != 0 {
			t.Errorf("expected 0 when all claimed by another scheduler, got %d", count)
		}

		pub.mu.Lock()
		defer pub.mu.Unlock()
		if pub.publishCount != 0 {
			t.Errorf("expected no publishing, got %d", pub.publishCount)
		}
	})
}

func TestScheduler_EmptyQueueHandling(t *testing.T) {
	t.Run("processBatch returns 0 when no notifications ready", func(t *testing.T) {
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				return nil, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		count := s.processBatch(ctx)

		if count != 0 {
			t.Errorf("expected 0 from empty queue, got %d", count)
		}
	})

	t.Run("processBatch returns 0 when repo returns empty slice", func(t *testing.T) {
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				return []*domain.Notification{}, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		count := s.processBatch(ctx)

		if count != 0 {
			t.Errorf("expected 0 from empty slice, got %d", count)
		}

		pub.mu.Lock()
		defer pub.mu.Unlock()
		if len(pub.publishedBatch) != 0 {
			t.Error("should not call PublishBatch with empty slice")
		}
	})

	t.Run("drainScheduled returns immediately on empty queue", func(t *testing.T) {
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				return nil, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()

		start := time.Now()
		s.drainScheduled(ctx)
		elapsed := time.Since(start)

		// drainScheduled should return almost immediately when queue is empty
		if elapsed > 100*time.Millisecond {
			t.Errorf("drainScheduled took too long on empty queue: %v", elapsed)
		}
	})

	t.Run("claim error returns 0 without calling publisher", func(t *testing.T) {
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				return nil, errors.New("connection timeout")
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		ctx := context.Background()
		count := s.processBatch(ctx)

		if count != 0 {
			t.Errorf("expected 0 on claim error, got %d", count)
		}

		pub.mu.Lock()
		defer pub.mu.Unlock()
		if len(pub.publishedBatch) != 0 {
			t.Error("publisher should not be called when claim fails")
		}
	})
}

func TestScheduler_ContextCancellation(t *testing.T) {
	t.Run("drainScheduled respects context cancellation mid-drain", func(t *testing.T) {
		past := time.Now().Add(-1 * time.Minute)
		fullBatch := make([]*domain.Notification, 10)
		for i := range fullBatch {
			fullBatch[i] = makeNotification(domain.PriorityNormal, past)
		}

		callCount := 0
		ctx, cancel := context.WithCancel(context.Background())

		repo := &mockRepo{
			claimBatchFn: func(innerCtx context.Context, limit int) ([]*domain.Notification, error) {
				callCount++
				if callCount == 1 {
					// After first batch, cancel context to simulate shutdown
					cancel()
					return fullBatch, nil
				}
				return nil, nil
			},
		}

		pub := &mockPublisher{}
		s := newTestScheduler(repo, pub)

		s.drainScheduled(ctx)

		// Should have processed the first batch but stopped after context was cancelled
		pub.mu.Lock()
		defer pub.mu.Unlock()

		if pub.publishCount != 10 {
			t.Errorf("expected first batch (10) to be published before cancellation, got %d", pub.publishCount)
		}

		// Second claim should not happen since context is cancelled
		if callCount > 2 {
			t.Errorf("expected at most 2 claim calls, got %d", callCount)
		}
	})

	t.Run("scheduler loop stops on context cancellation", func(t *testing.T) {
		repo := &mockRepo{
			claimBatchFn: func(ctx context.Context, limit int) ([]*domain.Notification, error) {
				return nil, nil
			},
		}

		pub := &mockPublisher{}
		s := &Scheduler{
			repo:           repo,
			publisher:      pub,
			pollInterval:   10 * time.Millisecond,
			batchSize:      10,
			stuckThreshold: 2 * time.Minute,
			logger:         slog.Default(),
		}

		ctx, cancel := context.WithCancel(context.Background())

		s.wg.Add(1)
		go s.runScheduler(ctx)

		// Let it run a few cycles
		time.Sleep(50 * time.Millisecond)
		cancel()

		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Successfully stopped
		case <-time.After(2 * time.Second):
			t.Fatal("scheduler did not stop after context cancellation")
		}
	})
}
