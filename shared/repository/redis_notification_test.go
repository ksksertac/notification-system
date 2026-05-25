package repository

import (
	"context"
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
	results, total, err := repo.List(ctx, domain.ListNotificationsRequest{
		Limit: 3,
	})
	if err != nil {
		t.Fatalf("List page 1 failed: %v", err)
	}
	if total != 10 {
		t.Errorf("expected total=10, got %d", total)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results on page 1, got %d", len(results))
	}

	// Second page using cursor from last result of page 1
	cursor := results[len(results)-1].ID
	results2, _, err := repo.List(ctx, domain.ListNotificationsRequest{
		Cursor: &cursor,
		Limit:  3,
	})
	if err != nil {
		t.Fatalf("List page 2 failed: %v", err)
	}
	if len(results2) != 3 {
		t.Errorf("expected 3 results on page 2, got %d", len(results2))
	}

	// Verify no overlap between pages
	page1IDs := make(map[uuid.UUID]bool)
	for _, r := range results {
		page1IDs[r.ID] = true
	}
	for _, r := range results2 {
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
