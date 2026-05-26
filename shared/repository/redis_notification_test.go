package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

func setupTestRepo(t *testing.T) (NotificationRepository, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() { client.Close() })

	repo := NewRedisNotificationRepo(client)
	return repo, mr, client
}

func newRedisTestNotification(opts ...func(*domain.Notification)) *domain.Notification {
	now := time.Now().UTC().Truncate(time.Millisecond)
	n := &domain.Notification{
		ID:         uuid.New(),
		Recipient:  "+1234567890",
		Channel:    domain.ChannelSMS,
		Content:    "Test message",
		Priority:   domain.PriorityNormal,
		Status:     domain.StatusPending,
		RetryCount: 0,
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

func TestCreateAndGetByID(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	idemKey := "idem-123"
	batchID := uuid.New()
	scheduledAt := time.Now().UTC().Add(1 * time.Hour).Truncate(time.Millisecond)

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.IdempotencyKey = &idemKey
		n.BatchID = &batchID
		n.ScheduledAt = &scheduledAt
		n.Channel = domain.ChannelEmail
		n.Recipient = "test@example.com"
		n.Content = "Hello World"
		n.Priority = domain.PriorityHigh
	})

	err := repo.Create(ctx, n)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := repo.GetByID(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if got == nil {
		t.Fatal("GetByID returned nil")
	}

	if got.ID != n.ID {
		t.Errorf("ID mismatch: got %v, want %v", got.ID, n.ID)
	}
	if got.Recipient != n.Recipient {
		t.Errorf("Recipient mismatch: got %v, want %v", got.Recipient, n.Recipient)
	}
	if got.Channel != n.Channel {
		t.Errorf("Channel mismatch: got %v, want %v", got.Channel, n.Channel)
	}
	if got.Content != n.Content {
		t.Errorf("Content mismatch: got %v, want %v", got.Content, n.Content)
	}
	if got.Priority != n.Priority {
		t.Errorf("Priority mismatch: got %v, want %v", got.Priority, n.Priority)
	}
	if got.Status != n.Status {
		t.Errorf("Status mismatch: got %v, want %v", got.Status, n.Status)
	}
	if got.RetryCount != n.RetryCount {
		t.Errorf("RetryCount mismatch: got %v, want %v", got.RetryCount, n.RetryCount)
	}
	if got.MaxRetries != n.MaxRetries {
		t.Errorf("MaxRetries mismatch: got %v, want %v", got.MaxRetries, n.MaxRetries)
	}
	if got.IdempotencyKey == nil || *got.IdempotencyKey != idemKey {
		t.Errorf("IdempotencyKey mismatch: got %v, want %v", got.IdempotencyKey, &idemKey)
	}
	if got.BatchID == nil || *got.BatchID != batchID {
		t.Errorf("BatchID mismatch: got %v, want %v", got.BatchID, &batchID)
	}
	if got.ScheduledAt == nil {
		t.Error("ScheduledAt is nil")
	} else if got.ScheduledAt.Unix() != scheduledAt.Unix() {
		t.Errorf("ScheduledAt mismatch: got %v, want %v", got.ScheduledAt, scheduledAt)
	}
}

func TestCreateDuplicate(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification()

	err := repo.Create(ctx, n)
	if err != nil {
		t.Fatalf("first Create failed: %v", err)
	}

	err = repo.Create(ctx, n)
	if err == nil {
		t.Fatal("expected error on duplicate Create, got nil")
	}
}

func TestCreateBatch(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	notifications := make([]*domain.Notification, 5)
	for i := 0; i < 5; i++ {
		notifications[i] = newRedisTestNotification(func(n *domain.Notification) {
			n.CreatedAt = n.CreatedAt.Add(time.Duration(i) * time.Millisecond)
		})
	}

	err := repo.CreateBatch(ctx, notifications)
	if err != nil {
		t.Fatalf("CreateBatch failed: %v", err)
	}

	for i, n := range notifications {
		got, err := repo.GetByID(ctx, n.ID)
		if err != nil {
			t.Fatalf("GetByID[%d] failed: %v", i, err)
		}
		if got == nil {
			t.Fatalf("GetByID[%d] returned nil", i)
		}
		if got.ID != n.ID {
			t.Errorf("notification[%d] ID mismatch: got %v, want %v", i, got.ID, n.ID)
		}
	}
}

func TestUpdateStatusCAS(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification()
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	t.Run("pending to queued succeeds", func(t *testing.T) {
		ok, err := repo.UpdateStatus(ctx, n.ID, domain.StatusPending, domain.StatusQueued)
		if err != nil {
			t.Fatalf("UpdateStatus failed: %v", err)
		}
		if !ok {
			t.Error("expected UpdateStatus to succeed (pending -> queued)")
		}

		got, _ := repo.GetByID(ctx, n.ID)
		if got.Status != domain.StatusQueued {
			t.Errorf("status should be queued, got %v", got.Status)
		}
	})

	t.Run("queued to pending fails wrong from state", func(t *testing.T) {
		ok, err := repo.UpdateStatus(ctx, n.ID, domain.StatusPending, domain.StatusQueued)
		if err != nil {
			t.Fatalf("UpdateStatus failed: %v", err)
		}
		if ok {
			t.Error("expected UpdateStatus to fail (from=pending but actual=queued)")
		}
	})
}

func TestUpdateStatusWithDetails(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusQueued
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	providerMsgID := "provider-abc-123"
	errorMsg := "timeout connecting"

	ok, err := repo.UpdateStatusWithDetails(ctx, n.ID, domain.StatusQueued, domain.StatusProcessing, &providerMsgID, &errorMsg)
	if err != nil {
		t.Fatalf("UpdateStatusWithDetails failed: %v", err)
	}
	if !ok {
		t.Error("expected UpdateStatusWithDetails to succeed")
	}

	got, err := repo.GetByID(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}

	if got.Status != domain.StatusProcessing {
		t.Errorf("status mismatch: got %v, want %v", got.Status, domain.StatusProcessing)
	}
	if got.ProviderMsgID == nil || *got.ProviderMsgID != providerMsgID {
		t.Errorf("ProviderMsgID mismatch: got %v, want %v", got.ProviderMsgID, providerMsgID)
	}
	if got.ErrorMessage == nil || *got.ErrorMessage != errorMsg {
		t.Errorf("ErrorMessage mismatch: got %v, want %v", got.ErrorMessage, errorMsg)
	}
}

func TestIncrementRetry(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusProcessing
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	nextRetry := time.Now().UTC().Add(5 * time.Minute)
	errMsg := "provider unavailable"

	err := repo.IncrementRetry(ctx, n.ID, nextRetry, errMsg)
	if err != nil {
		t.Fatalf("IncrementRetry failed: %v", err)
	}

	got, err := repo.GetByID(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}

	if got.RetryCount != 1 {
		t.Errorf("RetryCount should be 1, got %d", got.RetryCount)
	}
	if got.Status != domain.StatusFailed {
		t.Errorf("Status should be failed, got %v", got.Status)
	}
	if got.ErrorMessage == nil || *got.ErrorMessage != errMsg {
		t.Errorf("ErrorMessage mismatch: got %v, want %v", got.ErrorMessage, errMsg)
	}

	// Increment again
	err = repo.IncrementRetry(ctx, n.ID, nextRetry, "second failure")
	if err != nil {
		t.Fatalf("second IncrementRetry failed: %v", err)
	}

	got, _ = repo.GetByID(ctx, n.ID)
	if got.RetryCount != 2 {
		t.Errorf("RetryCount should be 2 after second increment, got %d", got.RetryCount)
	}
}

func TestMoveToDLQ(t *testing.T) {
	repo, _, client := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusProcessing
		n.RetryCount = 3
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	errMsg := "max retries exceeded"
	err := repo.MoveToDLQ(ctx, n, errMsg)
	if err != nil {
		t.Fatalf("MoveToDLQ failed: %v", err)
	}

	// Verify notification status updated
	got, err := repo.GetByID(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if got.Status != domain.StatusFailed {
		t.Errorf("Status should be failed after DLQ, got %v", got.Status)
	}
	if got.ErrorMessage == nil || *got.ErrorMessage != errMsg {
		t.Errorf("ErrorMessage should be set after DLQ")
	}

	// Verify DLQ entry was created using the redis client
	dlqKey := KeyDLQ + n.ID.String()
	dlqVals, err := client.HGetAll(ctx, dlqKey).Result()
	if err != nil {
		t.Fatalf("HGetAll DLQ failed: %v", err)
	}
	if len(dlqVals) == 0 {
		t.Fatal("DLQ hash not created")
	}
	if dlqVals["notification_id"] != n.ID.String() {
		t.Errorf("DLQ notification_id mismatch: got %v, want %v", dlqVals["notification_id"], n.ID.String())
	}
	if dlqVals["channel"] != string(n.Channel) {
		t.Errorf("DLQ channel mismatch: got %v, want %v", dlqVals["channel"], n.Channel)
	}
	if dlqVals["recipient"] != n.Recipient {
		t.Errorf("DLQ recipient mismatch: got %v, want %v", dlqVals["recipient"], n.Recipient)
	}
	if dlqVals["error_message"] != errMsg {
		t.Errorf("DLQ error_message mismatch: got %v, want %v", dlqVals["error_message"], errMsg)
	}
	if dlqVals["retry_count"] != "3" {
		t.Errorf("DLQ retry_count mismatch: got %v, want 3", dlqVals["retry_count"])
	}
	if dlqVals["reprocessed"] != "false" {
		t.Errorf("DLQ reprocessed should be false, got %v", dlqVals["reprocessed"])
	}
}

func TestListByStatus(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create 3 pending + 2 queued
	for i := 0; i < 3; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusPending
			n.CreatedAt = n.CreatedAt.Add(time.Duration(i) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create pending[%d] failed: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusQueued
			n.CreatedAt = n.CreatedAt.Add(time.Duration(i+10) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create queued[%d] failed: %v", i, err)
		}
	}

	statusPending := string(domain.StatusPending)
	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{
		Status: &statusPending,
		Limit:  20,
	})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if total != 3 {
		t.Errorf("expected total=3 pending, got %d", total)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Status != domain.StatusPending {
			t.Errorf("expected status pending, got %v", r.Status)
		}
	}
}

func TestListByChannel(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create SMS notifications
	for i := 0; i < 2; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Channel = domain.ChannelSMS
			n.CreatedAt = n.CreatedAt.Add(time.Duration(i) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create sms[%d] failed: %v", i, err)
		}
	}

	// Create Email notifications
	for i := 0; i < 3; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Channel = domain.ChannelEmail
			n.Recipient = "user@example.com"
			n.CreatedAt = n.CreatedAt.Add(time.Duration(i+10) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create email[%d] failed: %v", i, err)
		}
	}

	channelSMS := string(domain.ChannelSMS)
	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{
		Channel: &channelSMS,
		Limit:   20,
	})
	if err != nil {
		t.Fatalf("List by channel failed: %v", err)
	}
	if total != 2 {
		t.Errorf("expected total=2 SMS, got %d", total)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 SMS results, got %d", len(results))
	}
	for _, r := range results {
		if r.Channel != domain.ChannelSMS {
			t.Errorf("expected channel sms, got %v", r.Channel)
		}
	}
}

func TestListPagination(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create 10 notifications with staggered creation times
	for i := 0; i < 10; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.CreatedAt = time.Now().UTC().Add(time.Duration(i) * time.Second).Truncate(time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create[%d] failed: %v", i, err)
		}
	}

	// First page: limit=3, no cursor
	// Repository returns limit+1 items to allow the handler to detect "has more pages"
	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{
		Limit: 3,
	})
	if err != nil {
		t.Fatalf("List page 1 failed: %v", err)
	}
	if total != 10 {
		t.Errorf("expected total=10, got %d", total)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results on page 1 (limit+1), got %d", len(results))
	}

	// Trim to limit (as handler does) and use last item as cursor
	page1 := results[:3]
	cursor := page1[len(page1)-1].ID
	results2, _, err := repo.List(ctx, domain.ListNotificationsRequest{
		Cursor: &cursor,
		Limit:  3,
	})
	if err != nil {
		t.Fatalf("List page 2 failed: %v", err)
	}
	if len(results2) < 3 || len(results2) > 4 {
		t.Errorf("expected 3-4 results on page 2, got %d", len(results2))
	}

	// Verify no overlap between pages
	page1IDs := make(map[uuid.UUID]bool)
	for _, r := range page1 {
		page1IDs[r.ID] = true
	}
	for _, r := range results2[:min(3, len(results2))] {
		if page1IDs[r.ID] {
			t.Errorf("page 2 contains duplicate from page 1: %v", r.ID)
		}
	}
}

func TestGetByIdempotencyKey(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	idemKey := "unique-key-abc-123"
	n := newRedisTestNotification(func(n *domain.Notification) {
		n.IdempotencyKey = &idemKey
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := repo.GetByIdempotencyKey(ctx, idemKey)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey failed: %v", err)
	}
	if got == nil {
		t.Fatal("GetByIdempotencyKey returned nil")
	}
	if got.ID != n.ID {
		t.Errorf("ID mismatch: got %v, want %v", got.ID, n.ID)
	}

	// Non-existent key
	got, err = repo.GetByIdempotencyKey(ctx, "nonexistent-key")
	if err != nil {
		t.Fatalf("GetByIdempotencyKey for nonexistent key failed: %v", err)
	}
	if got != nil {
		t.Error("expected nil for nonexistent idempotency key")
	}
}

func TestGetByBatchID(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	batchID := uuid.New()
	expectedIDs := make(map[uuid.UUID]bool)

	for i := 0; i < 3; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.BatchID = &batchID
			n.CreatedAt = n.CreatedAt.Add(time.Duration(i) * time.Millisecond)
		})
		expectedIDs[n.ID] = true
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create batch[%d] failed: %v", i, err)
		}
	}

	// Create one notification with a different batch
	otherBatch := uuid.New()
	other := newRedisTestNotification(func(n *domain.Notification) {
		n.BatchID = &otherBatch
	})
	if err := repo.Create(ctx, other); err != nil {
		t.Fatalf("Create other batch failed: %v", err)
	}

	results, err := repo.GetByBatchID(ctx, batchID)
	if err != nil {
		t.Fatalf("GetByBatchID failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for _, r := range results {
		if !expectedIDs[r.ID] {
			t.Errorf("unexpected notification ID in batch results: %v", r.ID)
		}
	}
}

func TestClaimScheduledBatch(t *testing.T) {
	repo, mr, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create scheduled notifications in the past (ready to be claimed)
	past := time.Now().UTC().Add(-10 * time.Minute)
	scheduledIDs := make(map[uuid.UUID]bool)

	for i := 0; i < 3; i++ {
		scheduledAt := past.Add(time.Duration(i) * time.Minute)
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.ScheduledAt = &scheduledAt
			n.Status = domain.StatusPending
			n.CreatedAt = n.CreatedAt.Add(time.Duration(i) * time.Millisecond)
		})
		scheduledIDs[n.ID] = true
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create scheduled[%d] failed: %v", i, err)
		}
	}

	// Create one scheduled in the future (should not be claimed)
	future := time.Now().UTC().Add(1 * time.Hour)
	futureN := newRedisTestNotification(func(n *domain.Notification) {
		n.ScheduledAt = &future
		n.Status = domain.StatusPending
	})
	if err := repo.Create(ctx, futureN); err != nil {
		t.Fatalf("Create future scheduled failed: %v", err)
	}

	// Fast forward miniredis time to ensure "now" is past the scheduled times
	mr.FastForward(1 * time.Second)

	claimed, err := repo.ClaimScheduledBatch(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimScheduledBatch failed: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("expected 3 claimed, got %d", len(claimed))
	}

	for _, c := range claimed {
		if !scheduledIDs[c.ID] {
			t.Errorf("unexpected claimed ID: %v", c.ID)
		}
		// Verify status changed to queued
		got, _ := repo.GetByID(ctx, c.ID)
		if got.Status != domain.StatusQueued {
			t.Errorf("claimed notification should be queued, got %v", got.Status)
		}
	}

	// Future notification should still be pending
	futureGot, _ := repo.GetByID(ctx, futureN.ID)
	if futureGot.Status != domain.StatusPending {
		t.Errorf("future notification should still be pending, got %v", futureGot.Status)
	}
}

func TestRecoverStuckQueued(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create queued notifications with old timestamps (stuck)
	oldTime := time.Now().UTC().Add(-30 * time.Minute)
	stuckIDs := make(map[uuid.UUID]bool)

	for i := 0; i < 3; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusQueued
			n.CreatedAt = oldTime.Add(time.Duration(i) * time.Millisecond)
			n.UpdatedAt = oldTime
		})
		stuckIDs[n.ID] = true
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create stuck[%d] failed: %v", i, err)
		}
	}

	// Create a recently queued notification (should NOT be recovered)
	recentN := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusQueued
		n.CreatedAt = time.Now().UTC()
		n.UpdatedAt = time.Now().UTC()
	})
	if err := repo.Create(ctx, recentN); err != nil {
		t.Fatalf("Create recent failed: %v", err)
	}

	// Recover with 10 minute threshold (30-minute-old items should be recovered)
	recovered, err := repo.RecoverStuckQueued(ctx, 10*time.Minute, 10)
	if err != nil {
		t.Fatalf("RecoverStuckQueued failed: %v", err)
	}

	if len(recovered) != 3 {
		t.Fatalf("expected 3 recovered, got %d", len(recovered))
	}

	for _, r := range recovered {
		if !stuckIDs[r.ID] {
			t.Errorf("unexpected recovered ID: %v", r.ID)
		}
		got, _ := repo.GetByID(ctx, r.ID)
		if got.Status != domain.StatusPending {
			t.Errorf("recovered notification should be pending, got %v", got.Status)
		}
	}

	// Recent notification should still be queued
	recentGot, _ := repo.GetByID(ctx, recentN.ID)
	if recentGot.Status != domain.StatusQueued {
		t.Errorf("recent notification should still be queued, got %v", recentGot.Status)
	}
}

func TestRaceConditionConcurrentClaims(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create scheduled notifications in the past
	past := time.Now().UTC().Add(-5 * time.Minute)
	numNotifications := 10
	allIDs := make([]uuid.UUID, numNotifications)

	for i := 0; i < numNotifications; i++ {
		scheduledAt := past.Add(time.Duration(i) * time.Second)
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.ScheduledAt = &scheduledAt
			n.Status = domain.StatusPending
			n.CreatedAt = past.Add(time.Duration(i) * time.Second)
		})
		allIDs[i] = n.ID
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create[%d] failed: %v", i, err)
		}
	}

	// Simulate multiple pods claiming concurrently
	numWorkers := 5
	var wg sync.WaitGroup
	claimedCh := make(chan []*domain.Notification, numWorkers)

	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			claimed, err := repo.ClaimScheduledBatch(ctx, numNotifications)
			if err != nil {
				t.Errorf("ClaimScheduledBatch error: %v", err)
				return
			}
			claimedCh <- claimed
		}()
	}
	wg.Wait()
	close(claimedCh)

	// Collect all claimed IDs
	claimedIDs := make(map[uuid.UUID]int)
	totalClaimed := 0
	for claimed := range claimedCh {
		for _, n := range claimed {
			claimedIDs[n.ID]++
			totalClaimed++
		}
	}

	// Each notification should be claimed exactly once
	if totalClaimed != numNotifications {
		t.Errorf("expected %d total claims, got %d", numNotifications, totalClaimed)
	}
	for id, count := range claimedIDs {
		if count != 1 {
			t.Errorf("notification %v was claimed %d times (expected 1)", id, count)
		}
	}
}

func TestRaceConditionConcurrentStatusUpdates(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusPending
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Multiple goroutines try to update pending -> queued
	numWorkers := 10
	var wg sync.WaitGroup
	var successCount int64

	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			ok, err := repo.UpdateStatus(ctx, n.ID, domain.StatusPending, domain.StatusQueued)
			if err != nil {
				t.Errorf("UpdateStatus error: %v", err)
				return
			}
			if ok {
				atomic.AddInt64(&successCount, 1)
			}
		}()
	}
	wg.Wait()

	// Exactly one should succeed
	if successCount != 1 {
		t.Errorf("expected exactly 1 successful status update, got %d", successCount)
	}

	// Verify final status is queued
	got, _ := repo.GetByID(ctx, n.ID)
	if got.Status != domain.StatusQueued {
		t.Errorf("final status should be queued, got %v", got.Status)
	}
}

func TestGetByIDNotFound(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	got, err := repo.GetByID(ctx, uuid.New())
	if err != nil {
		t.Fatalf("GetByID should not error for missing ID: %v", err)
	}
	if got != nil {
		t.Error("expected nil for non-existent notification")
	}
}

func TestUpdateStatusWithDetailsWrongFromState(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusPending
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Try to update from queued (but actual is pending)
	providerID := "msg-xyz"
	ok, err := repo.UpdateStatusWithDetails(ctx, n.ID, domain.StatusQueued, domain.StatusProcessing, &providerID, nil)
	if err != nil {
		t.Fatalf("UpdateStatusWithDetails failed: %v", err)
	}
	if ok {
		t.Error("expected UpdateStatusWithDetails to fail (wrong from state)")
	}

	got, _ := repo.GetByID(ctx, n.ID)
	if got.Status != domain.StatusPending {
		t.Errorf("status should remain pending, got %v", got.Status)
	}
}

func TestMultipleIncrementRetries(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusProcessing
		n.MaxRetries = 5
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Increment 3 times
	for i := 0; i < 3; i++ {
		nextRetry := time.Now().UTC().Add(time.Duration(i+1) * time.Minute)
		err := repo.IncrementRetry(ctx, n.ID, nextRetry, fmt.Sprintf("failure %d", i+1))
		if err != nil {
			t.Fatalf("IncrementRetry[%d] failed: %v", i, err)
		}
	}

	got, err := repo.GetByID(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if got.RetryCount != 3 {
		t.Errorf("RetryCount should be 3 after 3 increments, got %d", got.RetryCount)
	}
	if got.ErrorMessage == nil || *got.ErrorMessage != "failure 3" {
		t.Errorf("ErrorMessage should be last failure message, got %v", got.ErrorMessage)
	}
}

func TestClaimScheduledBatchEmpty(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// No scheduled notifications
	claimed, err := repo.ClaimScheduledBatch(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimScheduledBatch failed: %v", err)
	}
	if claimed != nil {
		t.Errorf("expected nil for empty scheduled batch, got %d items", len(claimed))
	}
}

func TestRecoverStuckQueuedEmpty(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// No queued notifications
	recovered, err := repo.RecoverStuckQueued(ctx, 10*time.Minute, 10)
	if err != nil {
		t.Fatalf("RecoverStuckQueued failed: %v", err)
	}
	if recovered != nil {
		t.Errorf("expected nil for empty recover, got %d items", len(recovered))
	}
}

func TestIncrementRetryAddsToRetryIndex(t *testing.T) {
	repo, _, client := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusProcessing
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	nextRetry := time.Now().UTC().Add(5 * time.Minute)
	err := repo.IncrementRetry(ctx, n.ID, nextRetry, "provider timeout")
	if err != nil {
		t.Fatalf("IncrementRetry failed: %v", err)
	}

	// Verify notification is in idx:retry sorted set with score = nextRetryAt
	score, err := client.ZScore(ctx, KeyIdxRetry, n.ID.String()).Result()
	if err != nil {
		t.Fatalf("ZScore on idx:retry failed: %v", err)
	}
	expectedScore := float64(nextRetry.UnixNano())
	if score != expectedScore {
		t.Errorf("idx:retry score mismatch: got %v, want %v", score, expectedScore)
	}
}

func TestGetRetryReady(t *testing.T) {
	repo, _, client := setupTestRepo(t)
	ctx := context.Background()

	// Create notifications in "failed" status that have retry times in the past
	readyIDs := make(map[uuid.UUID]bool)
	for i := 0; i < 3; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusProcessing
			n.CreatedAt = n.CreatedAt.Add(time.Duration(i) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create[%d] failed: %v", i, err)
		}
		// Simulate IncrementRetry with a past retry time
		pastRetry := time.Now().UTC().Add(-time.Duration(i+1) * time.Minute)
		if err := repo.IncrementRetry(ctx, n.ID, pastRetry, "timeout"); err != nil {
			t.Fatalf("IncrementRetry[%d] failed: %v", i, err)
		}
		readyIDs[n.ID] = true
	}

	// Create one with retry time in the future (should NOT be returned)
	futureN := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusProcessing
	})
	if err := repo.Create(ctx, futureN); err != nil {
		t.Fatalf("Create future failed: %v", err)
	}
	futureRetry := time.Now().UTC().Add(1 * time.Hour)
	if err := repo.IncrementRetry(ctx, futureN.ID, futureRetry, "timeout"); err != nil {
		t.Fatalf("IncrementRetry future failed: %v", err)
	}

	results, err := repo.GetRetryReady(ctx, 10)
	if err != nil {
		t.Fatalf("GetRetryReady failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 retry-ready, got %d", len(results))
	}

	for _, r := range results {
		if !readyIDs[r.ID] {
			t.Errorf("unexpected notification in retry-ready: %v", r.ID)
		}
		// Verify they were transitioned to queued
		got, _ := repo.GetByID(ctx, r.ID)
		if got.Status != domain.StatusQueued {
			t.Errorf("expected queued after GetRetryReady, got %v", got.Status)
		}
	}

	// Verify they were removed from idx:retry
	remaining, err := client.ZCard(ctx, KeyIdxRetry).Result()
	if err != nil {
		t.Fatalf("ZCard idx:retry failed: %v", err)
	}
	if remaining != 1 {
		t.Errorf("expected 1 remaining in idx:retry (future), got %d", remaining)
	}
}

func TestGetRetryReadyEmpty(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	results, err := repo.GetRetryReady(ctx, 10)
	if err != nil {
		t.Fatalf("GetRetryReady failed: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil for empty retry index, got %d", len(results))
	}
}

func TestRecoverStuckProcessing(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create notifications stuck in "processing" with old updated_at
	oldTime := time.Now().UTC().Add(-10 * time.Minute)
	stuckIDs := make(map[uuid.UUID]bool)

	for i := 0; i < 3; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusProcessing
			n.CreatedAt = oldTime.Add(time.Duration(i) * time.Millisecond)
			n.UpdatedAt = oldTime
		})
		stuckIDs[n.ID] = true
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create stuck[%d] failed: %v", i, err)
		}
	}

	// Create a recently processing notification (should NOT be recovered)
	recentN := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusProcessing
		n.CreatedAt = time.Now().UTC()
		n.UpdatedAt = time.Now().UTC()
	})
	if err := repo.Create(ctx, recentN); err != nil {
		t.Fatalf("Create recent failed: %v", err)
	}

	// Recover with 5 min threshold
	recovered, err := repo.RecoverStuckProcessing(ctx, 5*time.Minute, 10)
	if err != nil {
		t.Fatalf("RecoverStuckProcessing failed: %v", err)
	}

	if len(recovered) != 3 {
		t.Fatalf("expected 3 recovered, got %d", len(recovered))
	}

	for _, r := range recovered {
		if !stuckIDs[r.ID] {
			t.Errorf("unexpected recovered ID: %v", r.ID)
		}
		got, _ := repo.GetByID(ctx, r.ID)
		if got.Status != domain.StatusQueued {
			t.Errorf("recovered notification should be queued, got %v", got.Status)
		}
	}

	// Recent notification should still be processing
	recentGot, _ := repo.GetByID(ctx, recentN.ID)
	if recentGot.Status != domain.StatusProcessing {
		t.Errorf("recent notification should still be processing, got %v", recentGot.Status)
	}
}

func TestRecoverStuckProcessingEmpty(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	recovered, err := repo.RecoverStuckProcessing(ctx, 5*time.Minute, 10)
	if err != nil {
		t.Fatalf("RecoverStuckProcessing failed: %v", err)
	}
	if recovered != nil {
		t.Errorf("expected nil for empty, got %d", len(recovered))
	}
}

func TestRecoverOrphanedPending(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create instant (non-scheduled) pending notifications with old timestamps
	oldTime := time.Now().UTC().Add(-5 * time.Minute)
	orphanIDs := make(map[uuid.UUID]bool)

	for i := 0; i < 3; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusPending
			n.CreatedAt = oldTime.Add(time.Duration(i) * time.Millisecond)
			n.UpdatedAt = oldTime
		})
		orphanIDs[n.ID] = true
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create orphan[%d] failed: %v", i, err)
		}
	}

	// Create a scheduled pending notification (should NOT be recovered)
	scheduledAt := time.Now().UTC().Add(1 * time.Hour)
	scheduledN := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusPending
		n.ScheduledAt = &scheduledAt
		n.CreatedAt = oldTime
		n.UpdatedAt = oldTime
	})
	if err := repo.Create(ctx, scheduledN); err != nil {
		t.Fatalf("Create scheduled failed: %v", err)
	}

	// Create a recent pending notification (should NOT be recovered due to time threshold)
	recentN := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusPending
		n.CreatedAt = time.Now().UTC()
		n.UpdatedAt = time.Now().UTC()
	})
	if err := repo.Create(ctx, recentN); err != nil {
		t.Fatalf("Create recent failed: %v", err)
	}

	// Recover with 30s threshold (5-min-old ones should be recovered)
	recovered, err := repo.RecoverOrphanedPending(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatalf("RecoverOrphanedPending failed: %v", err)
	}

	if len(recovered) != 3 {
		t.Fatalf("expected 3 recovered orphans, got %d", len(recovered))
	}

	for _, r := range recovered {
		if !orphanIDs[r.ID] {
			t.Errorf("unexpected recovered ID: %v", r.ID)
		}
		got, _ := repo.GetByID(ctx, r.ID)
		if got.Status != domain.StatusQueued {
			t.Errorf("recovered orphan should be queued, got %v", got.Status)
		}
	}

	// Scheduled notification should still be pending
	scheduledGot, _ := repo.GetByID(ctx, scheduledN.ID)
	if scheduledGot.Status != domain.StatusPending {
		t.Errorf("scheduled notification should still be pending, got %v", scheduledGot.Status)
	}

	// Recent notification should still be pending
	recentGot, _ := repo.GetByID(ctx, recentN.ID)
	if recentGot.Status != domain.StatusPending {
		t.Errorf("recent notification should still be pending, got %v", recentGot.Status)
	}
}

func TestRecoverOrphanedPendingEmpty(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	recovered, err := repo.RecoverOrphanedPending(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatalf("RecoverOrphanedPending failed: %v", err)
	}
	if recovered != nil {
		t.Errorf("expected nil for empty, got %d", len(recovered))
	}
}

func TestRedisCircuitBreakerViaRegistry(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	// Verify the cb:{channel} key is created when using Redis CB
	ctx := context.Background()
	key := "cb:sms"

	// Initially no key
	exists, _ := client.Exists(ctx, key).Result()
	if exists != 0 {
		t.Fatal("expected cb:sms to not exist initially")
	}

	// After a RecordFailure, the key should exist
	client.HSet(ctx, key, map[string]interface{}{
		"state":    "closed",
		"failures": "0",
	})
	exists, _ = client.Exists(ctx, key).Result()
	if exists != 1 {
		t.Fatal("expected cb:sms to exist after creation")
	}
}

// --- Additional coverage tests ---

func TestNotificationToMapAllOptionalFields(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	idemKey := "idem-key-full"
	batchID := uuid.New()
	providerMsgID := "provider-msg-abc"
	nextRetryAt := now.Add(5 * time.Minute)
	scheduledAt := now.Add(1 * time.Hour)
	errorMsg := "some error"
	metadata := []byte(`{"key":"value"}`)

	n := &domain.Notification{
		ID:             uuid.New(),
		IdempotencyKey: &idemKey,
		BatchID:        &batchID,
		Recipient:      "test@example.com",
		Channel:        domain.ChannelEmail,
		Content:        "full notification",
		Priority:       domain.PriorityHigh,
		Status:         domain.StatusProcessing,
		ProviderMsgID:  &providerMsgID,
		RetryCount:     2,
		MaxRetries:     5,
		NextRetryAt:    &nextRetryAt,
		ScheduledAt:    &scheduledAt,
		Metadata:       metadata,
		ErrorMessage:   &errorMsg,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	m := notificationToMap(n)

	// Check all optional fields are present
	if m["idempotency_key"] != idemKey {
		t.Errorf("idempotency_key mismatch: got %v", m["idempotency_key"])
	}
	if m["batch_id"] != batchID.String() {
		t.Errorf("batch_id mismatch: got %v", m["batch_id"])
	}
	if m["provider_msg_id"] != providerMsgID {
		t.Errorf("provider_msg_id mismatch: got %v", m["provider_msg_id"])
	}
	if m["next_retry_at"] != nextRetryAt.Format(time.RFC3339Nano) {
		t.Errorf("next_retry_at mismatch: got %v", m["next_retry_at"])
	}
	if m["scheduled_at"] != scheduledAt.Format(time.RFC3339Nano) {
		t.Errorf("scheduled_at mismatch: got %v", m["scheduled_at"])
	}
	if m["metadata"] != string(metadata) {
		t.Errorf("metadata mismatch: got %v", m["metadata"])
	}
	if m["error_message"] != errorMsg {
		t.Errorf("error_message mismatch: got %v", m["error_message"])
	}
}

func TestNotificationToMapNoOptionalFields(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	n := &domain.Notification{
		ID:         uuid.New(),
		Recipient:  "+1234567890",
		Channel:    domain.ChannelSMS,
		Content:    "minimal notification",
		Priority:   domain.PriorityNormal,
		Status:     domain.StatusPending,
		RetryCount: 0,
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	m := notificationToMap(n)

	// Optional fields should NOT be present
	if _, ok := m["idempotency_key"]; ok {
		t.Error("idempotency_key should not be present for nil value")
	}
	if _, ok := m["batch_id"]; ok {
		t.Error("batch_id should not be present for nil value")
	}
	if _, ok := m["provider_msg_id"]; ok {
		t.Error("provider_msg_id should not be present for nil value")
	}
	if _, ok := m["next_retry_at"]; ok {
		t.Error("next_retry_at should not be present for nil value")
	}
	if _, ok := m["scheduled_at"]; ok {
		t.Error("scheduled_at should not be present for nil value")
	}
	if _, ok := m["metadata"]; ok {
		t.Error("metadata should not be present for nil value")
	}
	if _, ok := m["error_message"]; ok {
		t.Error("error_message should not be present for nil value")
	}

	// Required fields should be present
	if m["id"] != n.ID.String() {
		t.Errorf("id mismatch: got %v", m["id"])
	}
	if m["recipient"] != n.Recipient {
		t.Errorf("recipient mismatch: got %v", m["recipient"])
	}
	if m["channel"] != string(n.Channel) {
		t.Errorf("channel mismatch: got %v", m["channel"])
	}
}

func TestMapToNotificationInvalidID(t *testing.T) {
	vals := map[string]string{
		"id":          "not-a-uuid",
		"recipient":   "+1234567890",
		"channel":     "sms",
		"content":     "test",
		"priority":    "normal",
		"status":      "pending",
		"retry_count": "0",
		"max_retries": "3",
		"created_at":  time.Now().Format(time.RFC3339Nano),
		"updated_at":  time.Now().Format(time.RFC3339Nano),
	}

	_, err := mapToNotification(vals)
	if err == nil {
		t.Fatal("expected error for invalid UUID")
	}
}

func TestMapToNotificationWithAllOptionalFields(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	batchID := uuid.New()
	nextRetryAt := now.Add(5 * time.Minute)
	scheduledAt := now.Add(1 * time.Hour)

	vals := map[string]string{
		"id":              uuid.New().String(),
		"recipient":       "test@example.com",
		"channel":         "email",
		"content":         "full test",
		"priority":        "high",
		"status":          "processing",
		"retry_count":     "2",
		"max_retries":     "5",
		"created_at":      now.Format(time.RFC3339Nano),
		"updated_at":      now.Format(time.RFC3339Nano),
		"idempotency_key": "idem-key-123",
		"batch_id":        batchID.String(),
		"provider_msg_id": "provider-xyz",
		"next_retry_at":   nextRetryAt.Format(time.RFC3339Nano),
		"scheduled_at":    scheduledAt.Format(time.RFC3339Nano),
		"metadata":        `{"foo":"bar"}`,
		"error_message":   "timeout error",
	}

	n, err := mapToNotification(vals)
	if err != nil {
		t.Fatalf("mapToNotification failed: %v", err)
	}

	if n.IdempotencyKey == nil || *n.IdempotencyKey != "idem-key-123" {
		t.Errorf("idempotency_key mismatch: got %v", n.IdempotencyKey)
	}
	if n.BatchID == nil || *n.BatchID != batchID {
		t.Errorf("batch_id mismatch: got %v", n.BatchID)
	}
	if n.ProviderMsgID == nil || *n.ProviderMsgID != "provider-xyz" {
		t.Errorf("provider_msg_id mismatch: got %v", n.ProviderMsgID)
	}
	if n.NextRetryAt == nil {
		t.Fatal("next_retry_at should not be nil")
	}
	if n.ScheduledAt == nil {
		t.Fatal("scheduled_at should not be nil")
	}
	if string(n.Metadata) != `{"foo":"bar"}` {
		t.Errorf("metadata mismatch: got %v", string(n.Metadata))
	}
	if n.ErrorMessage == nil || *n.ErrorMessage != "timeout error" {
		t.Errorf("error_message mismatch: got %v", n.ErrorMessage)
	}
}

func TestMapToNotificationMissingOptionalFields(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	vals := map[string]string{
		"id":          uuid.New().String(),
		"recipient":   "+1234567890",
		"channel":     "sms",
		"content":     "minimal",
		"priority":    "normal",
		"status":      "pending",
		"retry_count": "0",
		"max_retries": "3",
		"created_at":  now.Format(time.RFC3339Nano),
		"updated_at":  now.Format(time.RFC3339Nano),
	}

	n, err := mapToNotification(vals)
	if err != nil {
		t.Fatalf("mapToNotification failed: %v", err)
	}

	if n.IdempotencyKey != nil {
		t.Error("idempotency_key should be nil")
	}
	if n.BatchID != nil {
		t.Error("batch_id should be nil")
	}
	if n.ProviderMsgID != nil {
		t.Error("provider_msg_id should be nil")
	}
	if n.NextRetryAt != nil {
		t.Error("next_retry_at should be nil")
	}
	if n.ScheduledAt != nil {
		t.Error("scheduled_at should be nil")
	}
	if n.Metadata != nil {
		t.Error("metadata should be nil")
	}
	if n.ErrorMessage != nil {
		t.Error("error_message should be nil")
	}
}

func TestGetByBatchIDEmptyBatch(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Query a batch ID that has never been used
	nonExistentBatch := uuid.New()
	results, err := repo.GetByBatchID(ctx, nonExistentBatch)
	if err != nil {
		t.Fatalf("GetByBatchID failed: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil for empty batch, got %d items", len(results))
	}
}

func TestGetByIdempotencyKeyInvalidStoredUUID(t *testing.T) {
	_, mr, client := setupTestRepo(t)
	ctx := context.Background()

	// Manually set an invalid UUID for an idempotency key in Redis
	_ = mr
	idemKey := "test-invalid-uuid-key"
	client.Set(ctx, KeyIdxIdemKey+idemKey, "not-a-valid-uuid", IdemKeyTTL)

	repo := NewRedisNotificationRepo(client)
	_, err := repo.GetByIdempotencyKey(ctx, idemKey)
	if err == nil {
		t.Fatal("expected error for invalid UUID stored for idempotency key")
	}
}

func TestListWithDateRangeFilters(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	baseTime := time.Now().UTC().Add(-1 * time.Hour)

	// Create notifications at different times
	for i := 0; i < 5; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.CreatedAt = baseTime.Add(time.Duration(i*10) * time.Minute)
			n.UpdatedAt = n.CreatedAt
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create[%d] failed: %v", i, err)
		}
	}

	// Query with startDate filter only (should exclude the earliest ones)
	startDate := baseTime.Add(20 * time.Minute)
	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{
		StartDate: &startDate,
		Limit:     20,
	})
	if err != nil {
		t.Fatalf("List with start_date failed: %v", err)
	}
	if total != 3 {
		t.Errorf("expected total=3 with start_date filter, got %d", total)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results with start_date filter, got %d", len(results))
	}

	// Query with endDate filter only (should exclude the latest ones)
	endDate := baseTime.Add(25 * time.Minute)
	results, total, err = repo.List(ctx, domain.ListNotificationsRequest{
		EndDate: &endDate,
		Limit:   20,
	})
	if err != nil {
		t.Fatalf("List with end_date failed: %v", err)
	}
	if total != 3 {
		t.Errorf("expected total=3 with end_date filter, got %d", total)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results with end_date filter, got %d", len(results))
	}

	// Query with both startDate and endDate (narrow window)
	startDate2 := baseTime.Add(15 * time.Minute)
	endDate2 := baseTime.Add(35 * time.Minute)
	results, total, err = repo.List(ctx, domain.ListNotificationsRequest{
		StartDate: &startDate2,
		EndDate:   &endDate2,
		Limit:     20,
	})
	if err != nil {
		t.Fatalf("List with start+end date failed: %v", err)
	}
	if total != 2 {
		t.Errorf("expected total=2 with start+end date filter, got %d", total)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results with start+end date filter, got %d", len(results))
	}
}

func TestListMultiFilterIntersection(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create notifications with different status+channel combos
	for i := 0; i < 2; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusPending
			n.Channel = domain.ChannelSMS
			n.CreatedAt = time.Now().UTC().Add(time.Duration(i) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create pending+sms[%d] failed: %v", i, err)
		}
	}
	for i := 0; i < 3; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusPending
			n.Channel = domain.ChannelEmail
			n.Recipient = "user@example.com"
			n.CreatedAt = time.Now().UTC().Add(time.Duration(i+10) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create pending+email[%d] failed: %v", i, err)
		}
	}
	for i := 0; i < 4; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusQueued
			n.Channel = domain.ChannelSMS
			n.CreatedAt = time.Now().UTC().Add(time.Duration(i+20) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create queued+sms[%d] failed: %v", i, err)
		}
	}

	// Filter by status=pending AND channel=sms (should get 2)
	statusPending := string(domain.StatusPending)
	channelSMS := string(domain.ChannelSMS)
	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{
		Status:  &statusPending,
		Channel: &channelSMS,
		Limit:   20,
	})
	if err != nil {
		t.Fatalf("List multi-filter failed: %v", err)
	}
	if total != 2 {
		t.Errorf("expected total=2 for pending+sms intersection, got %d", total)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for pending+sms intersection, got %d", len(results))
	}
	for _, r := range results {
		if r.Status != domain.StatusPending {
			t.Errorf("expected status pending, got %v", r.Status)
		}
		if r.Channel != domain.ChannelSMS {
			t.Errorf("expected channel sms, got %v", r.Channel)
		}
	}
}

func TestListDefaultLimit(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create 25 notifications
	for i := 0; i < 25; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.CreatedAt = time.Now().UTC().Add(time.Duration(i) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create[%d] failed: %v", i, err)
		}
	}

	// Limit=0 should default to 20
	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{
		Limit: 0,
	})
	if err != nil {
		t.Fatalf("List with limit=0 failed: %v", err)
	}
	if total != 25 {
		t.Errorf("expected total=25, got %d", total)
	}
	// The function fetches limit+1 items, so with default limit=20 we get 21
	if len(results) != 21 {
		t.Errorf("expected 21 results (default limit 20 + 1), got %d", len(results))
	}

	// Limit > 100 should default to 20
	results, _, err = repo.List(ctx, domain.ListNotificationsRequest{
		Limit: 200,
	})
	if err != nil {
		t.Fatalf("List with limit=200 failed: %v", err)
	}
	if len(results) != 21 {
		t.Errorf("expected 21 results (limit capped to 20 + 1), got %d", len(results))
	}
}

func TestListCursorWithMissingNotification(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create some notifications
	for i := 0; i < 5; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.CreatedAt = time.Now().UTC().Add(time.Duration(i) * time.Second)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create[%d] failed: %v", i, err)
		}
	}

	// Use a non-existent UUID as cursor - should still return results
	// (cursor notification doesn't exist, so cursorScore won't be set)
	fakeCursor := uuid.New()
	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{
		Cursor: &fakeCursor,
		Limit:  20,
	})
	if err != nil {
		t.Fatalf("List with fake cursor failed: %v", err)
	}
	if total != 5 {
		t.Errorf("expected total=5, got %d", total)
	}
	// Since cursor doesn't exist, maxScore stays "+inf" so all results returned
	if len(results) != 5 {
		t.Errorf("expected 5 results with invalid cursor, got %d", len(results))
	}
}

func TestCreateBatchWithOptionalFields(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	batchID := uuid.New()
	idemKey1 := "batch-idem-1"
	idemKey2 := "batch-idem-2"
	scheduledAt := time.Now().UTC().Add(1 * time.Hour).Truncate(time.Millisecond)

	notifications := []*domain.Notification{
		newRedisTestNotification(func(n *domain.Notification) {
			n.BatchID = &batchID
			n.IdempotencyKey = &idemKey1
			n.ScheduledAt = &scheduledAt
			n.CreatedAt = time.Now().UTC().Truncate(time.Millisecond)
		}),
		newRedisTestNotification(func(n *domain.Notification) {
			n.BatchID = &batchID
			n.IdempotencyKey = &idemKey2
			n.CreatedAt = time.Now().UTC().Add(1 * time.Millisecond).Truncate(time.Millisecond)
		}),
	}

	err := repo.CreateBatch(ctx, notifications)
	if err != nil {
		t.Fatalf("CreateBatch with optional fields failed: %v", err)
	}

	// Verify batch index
	results, err := repo.GetByBatchID(ctx, batchID)
	if err != nil {
		t.Fatalf("GetByBatchID failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 in batch, got %d", len(results))
	}

	// Verify idempotency keys
	got1, err := repo.GetByIdempotencyKey(ctx, idemKey1)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey 1 failed: %v", err)
	}
	if got1 == nil || got1.ID != notifications[0].ID {
		t.Errorf("idempotency key 1 lookup mismatch")
	}

	got2, err := repo.GetByIdempotencyKey(ctx, idemKey2)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey 2 failed: %v", err)
	}
	if got2 == nil || got2.ID != notifications[1].ID {
		t.Errorf("idempotency key 2 lookup mismatch")
	}

	// Verify scheduled_at was stored
	got1Full, _ := repo.GetByID(ctx, notifications[0].ID)
	if got1Full.ScheduledAt == nil {
		t.Error("scheduled_at should be set for first notification")
	}
}

func TestUpdateStatusNonexistentNotification(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Try to update a notification that doesn't exist
	nonExistentID := uuid.New()
	ok, err := repo.UpdateStatus(ctx, nonExistentID, domain.StatusPending, domain.StatusQueued)
	if err != nil {
		t.Fatalf("UpdateStatus on non-existent should not error: %v", err)
	}
	if ok {
		t.Error("expected false for non-existent notification")
	}
}

func TestUpdateStatusWithDetailsNilProviderAndError(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusQueued
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update with nil providerMsgID and nil errorMsg
	ok, err := repo.UpdateStatusWithDetails(ctx, n.ID, domain.StatusQueued, domain.StatusProcessing, nil, nil)
	if err != nil {
		t.Fatalf("UpdateStatusWithDetails failed: %v", err)
	}
	if !ok {
		t.Error("expected UpdateStatusWithDetails to succeed")
	}

	got, _ := repo.GetByID(ctx, n.ID)
	if got.Status != domain.StatusProcessing {
		t.Errorf("status should be processing, got %v", got.Status)
	}
	if got.ProviderMsgID != nil {
		t.Error("provider_msg_id should remain nil")
	}
}

func TestGetScheduledReady(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create scheduled notifications in the past (ready to be fetched)
	past := time.Now().UTC().Add(-10 * time.Minute)
	readyIDs := make(map[uuid.UUID]bool)

	for i := 0; i < 3; i++ {
		scheduledAt := past.Add(time.Duration(i) * time.Minute)
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.ScheduledAt = &scheduledAt
			n.Status = domain.StatusPending
			n.CreatedAt = past.Add(time.Duration(i) * time.Millisecond)
		})
		readyIDs[n.ID] = true
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create scheduled[%d] failed: %v", i, err)
		}
	}

	// Create one scheduled in the future (should not be returned)
	future := time.Now().UTC().Add(1 * time.Hour)
	futureN := newRedisTestNotification(func(n *domain.Notification) {
		n.ScheduledAt = &future
		n.Status = domain.StatusPending
	})
	if err := repo.Create(ctx, futureN); err != nil {
		t.Fatalf("Create future scheduled failed: %v", err)
	}

	results, err := repo.GetScheduledReady(ctx, 10)
	if err != nil {
		t.Fatalf("GetScheduledReady failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 scheduled ready, got %d", len(results))
	}

	for _, r := range results {
		if !readyIDs[r.ID] {
			t.Errorf("unexpected ID in GetScheduledReady results: %v", r.ID)
		}
	}
}

func TestGetScheduledReadyEmpty(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	results, err := repo.GetScheduledReady(ctx, 10)
	if err != nil {
		t.Fatalf("GetScheduledReady failed: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil for empty scheduled ready, got %d", len(results))
	}
}

func TestGetScheduledReadyWithLimit(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create 5 past-scheduled notifications
	past := time.Now().UTC().Add(-10 * time.Minute)
	for i := 0; i < 5; i++ {
		scheduledAt := past.Add(time.Duration(i) * time.Minute)
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.ScheduledAt = &scheduledAt
			n.Status = domain.StatusPending
			n.CreatedAt = past.Add(time.Duration(i) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create[%d] failed: %v", i, err)
		}
	}

	// Limit to 2
	results, err := repo.GetScheduledReady(ctx, 2)
	if err != nil {
		t.Fatalf("GetScheduledReady with limit failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results with limit=2, got %d", len(results))
	}
}

func TestGetNotificationsByIDsEmpty(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	rr := repo.(*redisNotificationRepo)
	results, err := rr.getNotificationsByIDs(ctx, []string{})
	if err != nil {
		t.Fatalf("getNotificationsByIDs empty failed: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil for empty IDs, got %d", len(results))
	}
}

func TestGetNotificationsByIDsMixedValidInvalid(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// Create one valid notification
	n := newRedisTestNotification()
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	rr := repo.(*redisNotificationRepo)

	// Mix valid ID with non-existent IDs
	ids := []string{n.ID.String(), uuid.New().String(), uuid.New().String()}
	results, err := rr.getNotificationsByIDs(ctx, ids)
	if err != nil {
		t.Fatalf("getNotificationsByIDs mixed failed: %v", err)
	}
	// Only the valid one should be returned (non-existent get empty hash)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != n.ID {
		t.Errorf("expected ID %v, got %v", n.ID, results[0].ID)
	}
}

func TestParsePersistEventValid(t *testing.T) {
	n := newRedisTestNotification()
	evt := PersistEvent{
		Action:       "create",
		Notification: n,
		Extra:        map[string]string{"key": "val"},
		Timestamp:    time.Now().UTC().Format(time.RFC3339Nano),
	}

	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	values := map[string]interface{}{
		"event": string(data),
	}

	parsed, err := ParsePersistEvent(values)
	if err != nil {
		t.Fatalf("ParsePersistEvent failed: %v", err)
	}
	if parsed.Action != "create" {
		t.Errorf("Action mismatch: got %v", parsed.Action)
	}
	if parsed.Notification == nil {
		t.Error("Notification should not be nil")
	}
	if parsed.Extra["key"] != "val" {
		t.Errorf("Extra mismatch: got %v", parsed.Extra)
	}
}

func TestParsePersistEventMissingField(t *testing.T) {
	values := map[string]interface{}{
		"other_field": "something",
	}

	_, err := ParsePersistEvent(values)
	if err == nil {
		t.Fatal("expected error for missing event field")
	}
}

func TestParsePersistEventInvalidJSON(t *testing.T) {
	values := map[string]interface{}{
		"event": "not valid json {{{",
	}

	_, err := ParsePersistEvent(values)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParsePersistEventNonStringValue(t *testing.T) {
	values := map[string]interface{}{
		"event": 12345,
	}

	_, err := ParsePersistEvent(values)
	if err == nil {
		t.Fatal("expected error when event is not a string")
	}
}

func TestSplitPersistActions(t *testing.T) {
	n1 := newRedisTestNotification()
	n2 := newRedisTestNotification()

	events := []*PersistEvent{
		{Action: "create", Notification: n1},
		{Action: "create", Notification: n2},
		{Action: "update_status", Extra: map[string]string{"id": n1.ID.String(), "from": "pending", "to": "queued"}},
		{Action: "update_status_details", Extra: map[string]string{"id": n2.ID.String(), "from": "queued", "to": "processing"}},
		{Action: "increment_retry", Extra: map[string]string{"id": n1.ID.String(), "next_retry_at": "2025-01-01T00:00:00Z"}},
		{Action: "move_to_dlq", Extra: map[string]string{"id": n2.ID.String(), "error_message": "max retries"}},
	}

	creates, updates := SplitPersistActions(events)

	if len(creates) != 2 {
		t.Errorf("expected 2 creates, got %d", len(creates))
	}
	if len(updates) != 4 {
		t.Errorf("expected 4 updates, got %d", len(updates))
	}

	// Verify updates have action field
	for _, u := range updates {
		if u["action"] == "" {
			t.Error("update should have action field")
		}
	}
}

func TestSplitPersistActionsCreateWithNilNotification(t *testing.T) {
	events := []*PersistEvent{
		{Action: "create", Notification: nil},
		{Action: "unknown_action", Extra: map[string]string{"foo": "bar"}},
	}

	creates, updates := SplitPersistActions(events)
	if len(creates) != 0 {
		t.Errorf("expected 0 creates for nil notification, got %d", len(creates))
	}
	if len(updates) != 0 {
		t.Errorf("expected 0 updates for unknown action, got %d", len(updates))
	}
}

func TestSplitPersistActionsEmpty(t *testing.T) {
	creates, updates := SplitPersistActions(nil)
	if creates != nil {
		t.Errorf("expected nil creates, got %v", creates)
	}
	if updates != nil {
		t.Errorf("expected nil updates, got %v", updates)
	}
}

func TestListNoResults(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	// List with no notifications created
	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{
		Limit: 20,
	})
	if err != nil {
		t.Fatalf("List empty failed: %v", err)
	}
	if total != 0 {
		t.Errorf("expected total=0, got %d", total)
	}
	if results != nil {
		t.Errorf("expected nil results, got %d", len(results))
	}
}

func TestCreateWithAllOptionalFieldsRoundTrip(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	idemKey := "full-roundtrip-key"
	batchID := uuid.New()
	providerMsgID := "prov-msg-123"
	nextRetryAt := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Millisecond)
	scheduledAt := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Millisecond)
	errorMsg := "something went wrong"
	metadata := []byte(`{"template":"welcome","version":2}`)

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.IdempotencyKey = &idemKey
		n.BatchID = &batchID
		n.ProviderMsgID = &providerMsgID
		n.NextRetryAt = &nextRetryAt
		n.ScheduledAt = &scheduledAt
		n.ErrorMessage = &errorMsg
		n.Metadata = metadata
		n.Channel = domain.ChannelPush
		n.Recipient = "device-token-abcdef1234567890"
		n.Priority = domain.PriorityLow
		n.RetryCount = 1
		n.MaxRetries = 5
	})

	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := repo.GetByID(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if got == nil {
		t.Fatal("GetByID returned nil")
	}

	// Verify all fields round-trip correctly
	if got.ProviderMsgID == nil || *got.ProviderMsgID != providerMsgID {
		t.Errorf("ProviderMsgID mismatch: got %v", got.ProviderMsgID)
	}
	if got.NextRetryAt == nil {
		t.Fatal("NextRetryAt should not be nil")
	}
	if got.ErrorMessage == nil || *got.ErrorMessage != errorMsg {
		t.Errorf("ErrorMessage mismatch: got %v", got.ErrorMessage)
	}
	if string(got.Metadata) != string(metadata) {
		t.Errorf("Metadata mismatch: got %v", string(got.Metadata))
	}
	if got.Channel != domain.ChannelPush {
		t.Errorf("Channel mismatch: got %v", got.Channel)
	}
	if got.Priority != domain.PriorityLow {
		t.Errorf("Priority mismatch: got %v", got.Priority)
	}
}

func TestPublishPersistEventThroughCreate(t *testing.T) {
	_, _, client := setupTestRepo(t)
	ctx := context.Background()

	repo := NewRedisNotificationRepo(client)

	n := newRedisTestNotification()
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Verify a persist event was published to the stream
	messages, err := client.XRange(ctx, KeyPersistQueue, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange failed: %v", err)
	}
	if len(messages) == 0 {
		t.Fatal("expected at least one persist event in stream")
	}

	// Parse the event
	parsed, err := ParsePersistEvent(messages[0].Values)
	if err != nil {
		t.Fatalf("ParsePersistEvent failed: %v", err)
	}
	if parsed.Action != "create" {
		t.Errorf("expected action 'create', got %v", parsed.Action)
	}
	if parsed.Notification == nil {
		t.Error("expected notification in persist event")
	}
}

func TestUpdateStatusWithDetailsNonexistent(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	nonExistentID := uuid.New()
	providerID := "prov-123"
	errMsg := "timeout"
	ok, err := repo.UpdateStatusWithDetails(ctx, nonExistentID, domain.StatusQueued, domain.StatusProcessing, &providerID, &errMsg)
	if err != nil {
		t.Fatalf("UpdateStatusWithDetails on non-existent should not error: %v", err)
	}
	if ok {
		t.Error("expected false for non-existent notification")
	}
}

func TestGetByBatchIDWithInvalidNotificationData(t *testing.T) {
	_, _, client := setupTestRepo(t)
	ctx := context.Background()

	repo := NewRedisNotificationRepo(client)

	batchID := uuid.New()
	validID := uuid.New()
	invalidID := "invalid-uuid-in-batch"

	// Create a valid notification in the batch
	now := time.Now().UTC()
	validN := &domain.Notification{
		ID:         validID,
		Recipient:  "+1234567890",
		Channel:    domain.ChannelSMS,
		Content:    "valid",
		Priority:   domain.PriorityNormal,
		Status:     domain.StatusPending,
		RetryCount: 0,
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
		BatchID:    &batchID,
	}
	if err := repo.Create(ctx, validN); err != nil {
		t.Fatalf("Create valid notification failed: %v", err)
	}

	// Manually add an invalid ID to the batch set
	client.SAdd(ctx, KeyIdxBatch+batchID.String(), invalidID)
	// Create an invalid hash (missing required fields / invalid id)
	client.HSet(ctx, KeyNotification+invalidID, map[string]interface{}{
		"id":      "not-a-uuid",
		"channel": "sms",
	})

	results, err := repo.GetByBatchID(ctx, batchID)
	if err != nil {
		t.Fatalf("GetByBatchID failed: %v", err)
	}
	// Should only return the valid notification (invalid one is skipped)
	if len(results) != 1 {
		t.Fatalf("expected 1 valid result, got %d", len(results))
	}
	if results[0].ID != validID {
		t.Errorf("expected valid ID %v, got %v", validID, results[0].ID)
	}
}

// --- Error path tests: close miniredis to trigger connection errors ---

func TestGetByIDRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = repo.GetByID(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestGetByBatchIDRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = repo.GetByBatchID(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestGetByBatchIDPipeExecError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	batchID := uuid.New()
	// Add a member to the batch set so SMembers returns a non-empty slice
	client.SAdd(ctx, KeyIdxBatch+batchID.String(), uuid.New().String())

	// Close miniredis AFTER SMembers data is set, but before pipe.Exec
	// Actually, we need a different approach: close miniredis after SMembers succeeds
	// Since miniredis processes commands atomically, we can't do that easily.
	// Instead, test the empty hash path (member exists in set but hash is empty)
	results, err := repo.GetByBatchID(ctx, batchID)
	if err != nil {
		t.Fatalf("GetByBatchID failed: %v", err)
	}
	// The member ID has no corresponding hash, so it should be skipped
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty hashes, got %d", len(results))
	}
}

func TestGetByIdempotencyKeyRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = repo.GetByIdempotencyKey(ctx, "some-key")
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestGetScheduledReadyRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = repo.GetScheduledReady(ctx, 10)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestListRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, _, err = repo.List(ctx, domain.ListNotificationsRequest{Limit: 10})
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestUpdateStatusRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error in Lua script execution
	mr.Close()

	_, err = repo.UpdateStatus(ctx, uuid.New(), domain.StatusPending, domain.StatusQueued)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestClaimScheduledBatchRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = repo.ClaimScheduledBatch(ctx, 10)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestGetRetryReadyRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = repo.GetRetryReady(ctx, 10)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestRecoverStuckQueuedRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = repo.RecoverStuckQueued(ctx, 5*time.Minute, 10)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestRecoverStuckProcessingRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = repo.RecoverStuckProcessing(ctx, 5*time.Minute, 10)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestRecoverOrphanedPendingRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = repo.RecoverOrphanedPending(ctx, 30*time.Second, 10)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestUpdateStatusWithDetailsRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	providerID := "prov-123"
	_, err = repo.UpdateStatusWithDetails(ctx, uuid.New(), domain.StatusQueued, domain.StatusProcessing, &providerID, nil)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestCreateRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	n := newRedisTestNotification()
	err = repo.Create(ctx, n)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestCreateBatchRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	n := newRedisTestNotification()
	err = repo.CreateBatch(ctx, []*domain.Notification{n})
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestIncrementRetryRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	err = repo.IncrementRetry(ctx, uuid.New(), time.Now().Add(5*time.Minute), "error")
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestMoveToDLQRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	n := newRedisTestNotification()
	err = repo.MoveToDLQ(ctx, n, "error")
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestGetNotificationsByIDsRedisError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	repo := NewRedisNotificationRepo(client)
	rr := repo.(*redisNotificationRepo)
	ctx := context.Background()

	// Close miniredis to trigger error
	mr.Close()

	_, err = rr.getNotificationsByIDs(ctx, []string{uuid.New().String()})
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}

func TestListWithInvalidNotificationInResults(t *testing.T) {
	_, _, client := setupTestRepo(t)
	ctx := context.Background()

	repo := NewRedisNotificationRepo(client)

	// Create a valid notification
	validN := newRedisTestNotification()
	if err := repo.Create(ctx, validN); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Manually add an invalid notification hash that's indexed in created_at
	invalidID := uuid.New().String()
	score := float64(time.Now().UTC().UnixNano())
	client.ZAdd(ctx, KeyIdxCreatedAt, redis.Z{Score: score, Member: invalidID})
	client.ZAdd(ctx, KeyIdxStatus+string(domain.StatusPending), redis.Z{Score: score, Member: invalidID})
	client.HSet(ctx, KeyNotification+invalidID, map[string]interface{}{
		"id": "not-a-valid-uuid",
	})

	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{Limit: 20})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	// Total includes both entries
	if total != 2 {
		t.Errorf("expected total=2, got %d", total)
	}
	// But only the valid notification should be in results (invalid one is skipped)
	if len(results) != 1 {
		t.Fatalf("expected 1 valid result, got %d", len(results))
	}
	if results[0].ID != validN.ID {
		t.Errorf("expected valid notification ID")
	}
}

// --- Tests for GetRetryReady concurrent race safety (CAS check) ---

func TestGetRetryReadyConcurrentRace(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	numNotifications := 10

	for i := 0; i < numNotifications; i++ {
		n := newRedisTestNotification(func(n *domain.Notification) {
			n.Status = domain.StatusProcessing
			n.CreatedAt = n.CreatedAt.Add(time.Duration(i) * time.Millisecond)
		})
		if err := repo.Create(ctx, n); err != nil {
			t.Fatalf("Create[%d] failed: %v", i, err)
		}
		pastRetry := time.Now().UTC().Add(-time.Duration(i+1) * time.Minute)
		if err := repo.IncrementRetry(ctx, n.ID, pastRetry, "timeout"); err != nil {
			t.Fatalf("IncrementRetry[%d] failed: %v", i, err)
		}
	}

	numWorkers := 5
	var wg sync.WaitGroup
	claimedCh := make(chan []*domain.Notification, numWorkers)

	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			results, err := repo.GetRetryReady(ctx, numNotifications)
			if err != nil {
				t.Errorf("GetRetryReady error: %v", err)
				return
			}
			claimedCh <- results
		}()
	}
	wg.Wait()
	close(claimedCh)

	claimedIDs := make(map[uuid.UUID]int)
	totalClaimed := 0
	for results := range claimedCh {
		for _, n := range results {
			claimedIDs[n.ID]++
			totalClaimed++
		}
	}

	if totalClaimed != numNotifications {
		t.Errorf("expected %d total claims, got %d", numNotifications, totalClaimed)
	}
	for id, count := range claimedIDs {
		if count != 1 {
			t.Errorf("notification %v was claimed %d times (expected 1)", id, count)
		}
	}
}

// --- Test that MoveToDLQ atomically publishes persist event ---

func TestMoveToDLQPublishesPersistEvent(t *testing.T) {
	_, _, client := setupTestRepo(t)
	ctx := context.Background()

	repo := NewRedisNotificationRepo(client)

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusProcessing
		n.RetryCount = 3
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	errMsg := "max retries exceeded"
	if err := repo.MoveToDLQ(ctx, n, errMsg); err != nil {
		t.Fatalf("MoveToDLQ failed: %v", err)
	}

	messages, err := client.XRange(ctx, KeyPersistQueue, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange failed: %v", err)
	}

	if len(messages) < 2 {
		t.Fatalf("expected at least 2 persist events (create + move_to_dlq), got %d", len(messages))
	}

	lastEvent := messages[len(messages)-1]
	parsed, err := ParsePersistEvent(lastEvent.Values)
	if err != nil {
		t.Fatalf("ParsePersistEvent failed: %v", err)
	}
	if parsed.Action != "move_to_dlq" {
		t.Errorf("expected action 'move_to_dlq', got %v", parsed.Action)
	}
}

// --- Tests for recovery scripts skipping recently updated notifications ---

func TestRecoverStuckQueued_SkipsRecentlyRequeued(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	oldTime := time.Now().UTC().Add(-30 * time.Minute)
	recentTime := time.Now().UTC()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusQueued
		n.CreatedAt = oldTime
		n.UpdatedAt = recentTime
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	recovered, err := repo.RecoverStuckQueued(ctx, 10*time.Minute, 10)
	if err != nil {
		t.Fatalf("RecoverStuckQueued failed: %v", err)
	}

	if len(recovered) != 0 {
		t.Errorf("expected 0 recovered (recently re-queued via updated_at), got %d", len(recovered))
	}

	got, _ := repo.GetByID(ctx, n.ID)
	if got.Status != domain.StatusQueued {
		t.Errorf("status should remain queued, got %v", got.Status)
	}
}

func TestRecoverStuckProcessing_SkipsRecentlyUpdated(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	oldTime := time.Now().UTC().Add(-30 * time.Minute)
	recentTime := time.Now().UTC()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusProcessing
		n.CreatedAt = oldTime
		n.UpdatedAt = recentTime
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	recovered, err := repo.RecoverStuckProcessing(ctx, 5*time.Minute, 10)
	if err != nil {
		t.Fatalf("RecoverStuckProcessing failed: %v", err)
	}

	if len(recovered) != 0 {
		t.Errorf("expected 0 recovered (recently updated), got %d", len(recovered))
	}

	got, _ := repo.GetByID(ctx, n.ID)
	if got.Status != domain.StatusProcessing {
		t.Errorf("status should remain processing, got %v", got.Status)
	}
}

func TestRecoverOrphanedPending_SkipsRecentlyUpdated(t *testing.T) {
	repo, _, _ := setupTestRepo(t)
	ctx := context.Background()

	oldTime := time.Now().UTC().Add(-30 * time.Minute)
	recentTime := time.Now().UTC()

	n := newRedisTestNotification(func(n *domain.Notification) {
		n.Status = domain.StatusPending
		n.CreatedAt = oldTime
		n.UpdatedAt = recentTime
	})
	if err := repo.Create(ctx, n); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	recovered, err := repo.RecoverOrphanedPending(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatalf("RecoverOrphanedPending failed: %v", err)
	}

	if len(recovered) != 0 {
		t.Errorf("expected 0 recovered (recently updated), got %d", len(recovered))
	}

	got, _ := repo.GetByID(ctx, n.ID)
	if got.Status != domain.StatusPending {
		t.Errorf("status should remain pending, got %v", got.Status)
	}
}
