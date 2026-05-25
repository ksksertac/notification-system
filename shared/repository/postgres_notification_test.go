package repository

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

// notificationColumns defines the column names returned by SELECT * on the notifications table.
var notificationColumns = []string{
	"id", "idempotency_key", "batch_id", "recipient", "channel", "content",
	"priority", "status", "provider_msg_id", "retry_count", "max_retries",
	"next_retry_at", "scheduled_at", "metadata", "error_message", "created_at", "updated_at",
}

func setupPostgresTest(t *testing.T) (NotificationRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	sqlxDB := sqlx.NewDb(db, "postgres")
	repo := NewPostgresNotificationRepo(sqlxDB)
	cleanup := func() {
		db.Close()
	}
	return repo, mock, cleanup
}

func newPgTestNotification() *domain.Notification {
	now := time.Now().UTC().Truncate(time.Millisecond)
	idemKey := "idem-key-123"
	batchID := uuid.New()
	return &domain.Notification{
		ID:             uuid.New(),
		IdempotencyKey: &idemKey,
		BatchID:        &batchID,
		Recipient:      "+1234567890",
		Channel:        domain.ChannelSMS,
		Content:        "Hello test",
		Priority:       domain.PriorityNormal,
		Status:         domain.StatusPending,
		RetryCount:     0,
		MaxRetries:     3,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func notificationRow(n *domain.Notification) *sqlmock.Rows {
	return sqlmock.NewRows(notificationColumns).AddRow(
		n.ID, n.IdempotencyKey, n.BatchID, n.Recipient, n.Channel, n.Content,
		n.Priority, n.Status, n.ProviderMsgID, n.RetryCount, n.MaxRetries,
		n.NextRetryAt, n.ScheduledAt, n.Metadata, n.ErrorMessage, n.CreatedAt, n.UpdatedAt,
	)
}

func notificationRows(notifications []*domain.Notification) *sqlmock.Rows {
	rows := sqlmock.NewRows(notificationColumns)
	for _, n := range notifications {
		rows.AddRow(
			n.ID, n.IdempotencyKey, n.BatchID, n.Recipient, n.Channel, n.Content,
			n.Priority, n.Status, n.ProviderMsgID, n.RetryCount, n.MaxRetries,
			n.NextRetryAt, n.ScheduledAt, n.Metadata, n.ErrorMessage, n.CreatedAt, n.UpdatedAt,
		)
	}
	return rows
}

// ========== Create ==========

func TestPostgresCreate(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		mock.ExpectExec(regexp.QuoteMeta(
			`INSERT INTO notifications (id, idempotency_key, batch_id, recipient, channel, content, priority, status, max_retries, scheduled_at, metadata, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`)).
			WithArgs(n.ID, n.IdempotencyKey, n.BatchID, n.Recipient, n.Channel, n.Content,
				n.Priority, n.Status, n.MaxRetries, n.ScheduledAt, n.Metadata,
				n.CreatedAt, n.UpdatedAt).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err := repo.Create(context.Background(), n)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("db_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		mock.ExpectExec(regexp.QuoteMeta(
			`INSERT INTO notifications`)).
			WithArgs(n.ID, n.IdempotencyKey, n.BatchID, n.Recipient, n.Channel, n.Content,
				n.Priority, n.Status, n.MaxRetries, n.ScheduledAt, n.Metadata,
				n.CreatedAt, n.UpdatedAt).
			WillReturnError(errors.New("connection refused"))

		err := repo.Create(context.Background(), n)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== CreateBatch ==========

func TestPostgresCreateBatch(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n1 := newPgTestNotification()
		n2 := newPgTestNotification()
		notifications := []*domain.Notification{n1, n2}

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO notifications`)).
			WillReturnResult(sqlmock.NewResult(0, 2))
		mock.ExpectCommit()

		err := repo.CreateBatch(context.Background(), notifications)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("begin_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))

		err := repo.CreateBatch(context.Background(), []*domain.Notification{newPgTestNotification()})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("exec_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO notifications`)).
			WillReturnError(errors.New("insert failed"))
		mock.ExpectRollback()

		err := repo.CreateBatch(context.Background(), []*domain.Notification{newPgTestNotification()})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("commit_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO notifications`)).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))

		err := repo.CreateBatch(context.Background(), []*domain.Notification{newPgTestNotification()})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== GetByID ==========

func TestPostgresGetByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE id = $1`)).
			WithArgs(n.ID).
			WillReturnRows(notificationRow(n))

		result, err := repo.GetByID(context.Background(), n.ID)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if result == nil {
			t.Fatal("expected result, got nil")
		}
		if result.ID != n.ID {
			t.Fatalf("expected ID %v, got %v", n.ID, result.ID)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("not_found", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE id = $1`)).
			WithArgs(id).
			WillReturnError(sql.ErrNoRows)

		result, err := repo.GetByID(context.Background(), id)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if result != nil {
			t.Fatalf("expected nil result, got %v", result)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("db_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE id = $1`)).
			WithArgs(id).
			WillReturnError(errors.New("db error"))

		result, err := repo.GetByID(context.Background(), id)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if result == nil {
			t.Fatal("expected non-nil result on error path (returned &n)")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== GetByBatchID ==========

func TestPostgresGetByBatchID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		batchID := uuid.New()
		n1 := newPgTestNotification()
		n1.BatchID = &batchID
		n2 := newPgTestNotification()
		n2.BatchID = &batchID

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE batch_id = $1 ORDER BY created_at`)).
			WithArgs(batchID).
			WillReturnRows(notificationRows([]*domain.Notification{n1, n2}))

		results, err := repo.GetByBatchID(context.Background(), batchID)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		batchID := uuid.New()
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE batch_id = $1 ORDER BY created_at`)).
			WithArgs(batchID).
			WillReturnRows(sqlmock.NewRows(notificationColumns))

		results, err := repo.GetByBatchID(context.Background(), batchID)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("db_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		batchID := uuid.New()
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE batch_id = $1 ORDER BY created_at`)).
			WithArgs(batchID).
			WillReturnError(errors.New("query failed"))

		_, err := repo.GetByBatchID(context.Background(), batchID)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== GetByIdempotencyKey ==========

func TestPostgresGetByIdempotencyKey(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		key := "test-idem-key"
		n.IdempotencyKey = &key

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE idempotency_key = $1`)).
			WithArgs(key).
			WillReturnRows(notificationRow(n))

		result, err := repo.GetByIdempotencyKey(context.Background(), key)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if result == nil {
			t.Fatal("expected result, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("not_found", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE idempotency_key = $1`)).
			WithArgs("nonexistent").
			WillReturnError(sql.ErrNoRows)

		result, err := repo.GetByIdempotencyKey(context.Background(), "nonexistent")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if result != nil {
			t.Fatalf("expected nil result, got %v", result)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("db_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE idempotency_key = $1`)).
			WithArgs("some-key").
			WillReturnError(errors.New("connection error"))

		result, err := repo.GetByIdempotencyKey(context.Background(), "some-key")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if result == nil {
			t.Fatal("expected non-nil result on error path")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== List ==========

func TestPostgresList(t *testing.T) {
	t.Run("success_no_filters", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		req := domain.ListNotificationsRequest{Limit: 10}

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM notifications WHERE 1=1`)).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE 1=1 ORDER BY id DESC LIMIT $1`)).
			WithArgs(10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))

		results, total, err := repo.List(context.Background(), req)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if total != 1 {
			t.Fatalf("expected total 1, got %d", total)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("success_with_all_filters", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		status := "pending"
		channel := "sms"
		startDate := time.Now().Add(-24 * time.Hour)
		endDate := time.Now()
		cursor := uuid.New()

		req := domain.ListNotificationsRequest{
			Status:    &status,
			Channel:   &channel,
			StartDate: &startDate,
			EndDate:   &endDate,
			Cursor:    &cursor,
			Limit:     5,
		}

		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT COUNT(*) FROM notifications WHERE 1=1 AND status = $1 AND channel = $2 AND created_at >= $3 AND created_at <= $4 AND id < $5`)).
			WithArgs(status, channel, startDate, endDate, cursor).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(3)))

		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications WHERE 1=1 AND status = $1 AND channel = $2 AND created_at >= $3 AND created_at <= $4 AND id < $5 ORDER BY id DESC LIMIT $6`)).
			WithArgs(status, channel, startDate, endDate, cursor, 5).
			WillReturnRows(sqlmock.NewRows(notificationColumns))

		results, total, err := repo.List(context.Background(), req)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if total != 3 {
			t.Fatalf("expected total 3, got %d", total)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("default_limit", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		req := domain.ListNotificationsRequest{Limit: 0}

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM notifications WHERE 1=1`)).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE 1=1 ORDER BY id DESC LIMIT $1`)).
			WithArgs(20).
			WillReturnRows(sqlmock.NewRows(notificationColumns))

		results, total, err := repo.List(context.Background(), req)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if total != 0 {
			t.Fatalf("expected total 0, got %d", total)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("limit_exceeds_max", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		req := domain.ListNotificationsRequest{Limit: 200}

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM notifications WHERE 1=1`)).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE 1=1 ORDER BY id DESC LIMIT $1`)).
			WithArgs(20).
			WillReturnRows(sqlmock.NewRows(notificationColumns))

		_, _, err := repo.List(context.Background(), req)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("count_query_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		req := domain.ListNotificationsRequest{Limit: 10}

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM notifications WHERE 1=1`)).
			WillReturnError(errors.New("count error"))

		_, _, err := repo.List(context.Background(), req)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("list_query_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		req := domain.ListNotificationsRequest{Limit: 10}

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM notifications WHERE 1=1`)).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(5)))

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT * FROM notifications WHERE 1=1 ORDER BY id DESC LIMIT $1`)).
			WithArgs(10).
			WillReturnError(errors.New("list error"))

		_, _, err := repo.List(context.Background(), req)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== UpdateStatus ==========

func TestPostgresUpdateStatus(t *testing.T) {
	t.Run("success_updated", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = $3 AND status = $4`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), id, domain.StatusPending).
			WillReturnResult(sqlmock.NewResult(0, 1))

		updated, err := repo.UpdateStatus(context.Background(), id, domain.StatusPending, domain.StatusQueued)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !updated {
			t.Fatal("expected updated to be true")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("success_not_updated", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = $3 AND status = $4`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), id, domain.StatusPending).
			WillReturnResult(sqlmock.NewResult(0, 0))

		updated, err := repo.UpdateStatus(context.Background(), id, domain.StatusPending, domain.StatusQueued)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if updated {
			t.Fatal("expected updated to be false")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("db_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = $3 AND status = $4`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), id, domain.StatusPending).
			WillReturnError(errors.New("update failed"))

		updated, err := repo.UpdateStatus(context.Background(), id, domain.StatusPending, domain.StatusQueued)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if updated {
			t.Fatal("expected updated to be false on error")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== UpdateStatusWithDetails ==========

func TestPostgresUpdateStatusWithDetails(t *testing.T) {
	t.Run("success_with_details", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()
		providerMsgID := "provider-123"
		errorMsg := "timeout"

		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, provider_msg_id = COALESCE($2, provider_msg_id), error_message = COALESCE($3, error_message), updated_at = $4
			 WHERE id = $5 AND status = $6`)).
			WithArgs(domain.StatusDelivered, &providerMsgID, &errorMsg, sqlmock.AnyArg(), id, domain.StatusProcessing).
			WillReturnResult(sqlmock.NewResult(0, 1))

		updated, err := repo.UpdateStatusWithDetails(context.Background(), id, domain.StatusProcessing, domain.StatusDelivered, &providerMsgID, &errorMsg)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !updated {
			t.Fatal("expected updated to be true")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("success_with_nil_details", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()

		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, provider_msg_id = COALESCE($2, provider_msg_id), error_message = COALESCE($3, error_message), updated_at = $4
			 WHERE id = $5 AND status = $6`)).
			WithArgs(domain.StatusDelivered, nil, nil, sqlmock.AnyArg(), id, domain.StatusProcessing).
			WillReturnResult(sqlmock.NewResult(0, 1))

		updated, err := repo.UpdateStatusWithDetails(context.Background(), id, domain.StatusProcessing, domain.StatusDelivered, nil, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !updated {
			t.Fatal("expected updated to be true")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("not_updated", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()

		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, provider_msg_id = COALESCE($2, provider_msg_id), error_message = COALESCE($3, error_message), updated_at = $4
			 WHERE id = $5 AND status = $6`)).
			WithArgs(domain.StatusDelivered, nil, nil, sqlmock.AnyArg(), id, domain.StatusProcessing).
			WillReturnResult(sqlmock.NewResult(0, 0))

		updated, err := repo.UpdateStatusWithDetails(context.Background(), id, domain.StatusProcessing, domain.StatusDelivered, nil, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if updated {
			t.Fatal("expected updated to be false")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("db_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()

		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, provider_msg_id = COALESCE($2, provider_msg_id), error_message = COALESCE($3, error_message), updated_at = $4
			 WHERE id = $5 AND status = $6`)).
			WithArgs(domain.StatusDelivered, nil, nil, sqlmock.AnyArg(), id, domain.StatusProcessing).
			WillReturnError(errors.New("db error"))

		updated, err := repo.UpdateStatusWithDetails(context.Background(), id, domain.StatusProcessing, domain.StatusDelivered, nil, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if updated {
			t.Fatal("expected updated to be false on error")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== IncrementRetry ==========

func TestPostgresIncrementRetry(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()
		nextRetryAt := time.Now().Add(5 * time.Minute)
		errorMsg := "timeout error"

		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET retry_count = retry_count + 1, next_retry_at = $1, error_message = $2, status = 'failed', updated_at = $3
			 WHERE id = $4`)).
			WithArgs(nextRetryAt, errorMsg, sqlmock.AnyArg(), id).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err := repo.IncrementRetry(context.Background(), id, nextRetryAt, errorMsg)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("db_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		id := uuid.New()
		nextRetryAt := time.Now().Add(5 * time.Minute)

		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET retry_count = retry_count + 1, next_retry_at = $1, error_message = $2, status = 'failed', updated_at = $3
			 WHERE id = $4`)).
			WithArgs(nextRetryAt, "error", sqlmock.AnyArg(), id).
			WillReturnError(errors.New("update failed"))

		err := repo.IncrementRetry(context.Background(), id, nextRetryAt, "error")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== MoveToDLQ ==========

func TestPostgresMoveToDLQ(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.RetryCount = 3
		errorMsg := "max retries exceeded"

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(
			`INSERT INTO dead_letter_queue (notification_id, channel, recipient, content, error_message, retry_count)
			 VALUES ($1, $2, $3, $4, $5, $6)`)).
			WithArgs(n.ID, n.Channel, n.Recipient, n.Content, errorMsg, n.RetryCount).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = 'failed', error_message = $1, updated_at = $2 WHERE id = $3`)).
			WithArgs(errorMsg, sqlmock.AnyArg(), n.ID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err := repo.MoveToDLQ(context.Background(), n, errorMsg)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("begin_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))

		err := repo.MoveToDLQ(context.Background(), newPgTestNotification(), "error")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("dlq_insert_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		errorMsg := "max retries"

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(
			`INSERT INTO dead_letter_queue (notification_id, channel, recipient, content, error_message, retry_count)
			 VALUES ($1, $2, $3, $4, $5, $6)`)).
			WithArgs(n.ID, n.Channel, n.Recipient, n.Content, errorMsg, n.RetryCount).
			WillReturnError(errors.New("insert failed"))
		mock.ExpectRollback()

		err := repo.MoveToDLQ(context.Background(), n, errorMsg)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("update_status_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		errorMsg := "max retries"

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(
			`INSERT INTO dead_letter_queue (notification_id, channel, recipient, content, error_message, retry_count)
			 VALUES ($1, $2, $3, $4, $5, $6)`)).
			WithArgs(n.ID, n.Channel, n.Recipient, n.Content, errorMsg, n.RetryCount).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = 'failed', error_message = $1, updated_at = $2 WHERE id = $3`)).
			WithArgs(errorMsg, sqlmock.AnyArg(), n.ID).
			WillReturnError(errors.New("update failed"))
		mock.ExpectRollback()

		err := repo.MoveToDLQ(context.Background(), n, errorMsg)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("commit_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		errorMsg := "max retries"

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(
			`INSERT INTO dead_letter_queue (notification_id, channel, recipient, content, error_message, retry_count)
			 VALUES ($1, $2, $3, $4, $5, $6)`)).
			WithArgs(n.ID, n.Channel, n.Recipient, n.Content, errorMsg, n.RetryCount).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = 'failed', error_message = $1, updated_at = $2 WHERE id = $3`)).
			WithArgs(errorMsg, sqlmock.AnyArg(), n.ID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))

		err := repo.MoveToDLQ(context.Background(), n, errorMsg)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== GetScheduledReady ==========

func TestPostgresGetScheduledReady(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		scheduledAt := time.Now().Add(-1 * time.Minute)
		n := newPgTestNotification()
		n.Status = domain.StatusPending
		n.ScheduledAt = &scheduledAt

		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= $1
			 ORDER BY scheduled_at ASC LIMIT $2`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))

		results, err := repo.GetScheduledReady(context.Background(), 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= $1
			 ORDER BY scheduled_at ASC LIMIT $2`)).
			WithArgs(sqlmock.AnyArg(), 5).
			WillReturnRows(sqlmock.NewRows(notificationColumns))

		results, err := repo.GetScheduledReady(context.Background(), 5)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("db_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= $1
			 ORDER BY scheduled_at ASC LIMIT $2`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnError(errors.New("query failed"))

		_, err := repo.GetScheduledReady(context.Background(), 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== ClaimScheduledBatch ==========

func TestPostgresClaimScheduledBatch(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusPending

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending'
			   AND (
			     (scheduled_at IS NOT NULL AND scheduled_at <= $1)
			     OR
			     (scheduled_at IS NULL AND updated_at <= $2)
			   )
			 ORDER BY created_at ASC
			 LIMIT $3
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		results, err := repo.ClaimScheduledBatch(context.Background(), 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		if results[0].Status != domain.StatusQueued {
			t.Fatalf("expected status queued, got %s", results[0].Status)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending'
			   AND (
			     (scheduled_at IS NOT NULL AND scheduled_at <= $1)
			     OR
			     (scheduled_at IS NULL AND updated_at <= $2)
			   )
			 ORDER BY created_at ASC
			 LIMIT $3
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), 5).
			WillReturnRows(sqlmock.NewRows(notificationColumns))
		mock.ExpectRollback()

		results, err := repo.ClaimScheduledBatch(context.Background(), 5)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if results != nil {
			t.Fatalf("expected nil result, got %v", results)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("begin_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))

		results, err := repo.ClaimScheduledBatch(context.Background(), 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results on error")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("select_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending'
			   AND (
			     (scheduled_at IS NOT NULL AND scheduled_at <= $1)
			     OR
			     (scheduled_at IS NULL AND updated_at <= $2)
			   )
			 ORDER BY created_at ASC
			 LIMIT $3
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), 10).
			WillReturnError(errors.New("select error"))
		mock.ExpectRollback()

		results, err := repo.ClaimScheduledBatch(context.Background(), 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results on error")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("update_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusPending

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending'
			   AND (
			     (scheduled_at IS NOT NULL AND scheduled_at <= $1)
			     OR
			     (scheduled_at IS NULL AND updated_at <= $2)
			   )
			 ORDER BY created_at ASC
			 LIMIT $3
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(errors.New("update error"))
		mock.ExpectRollback()

		results, err := repo.ClaimScheduledBatch(context.Background(), 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results on error")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("commit_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusPending

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending'
			   AND (
			     (scheduled_at IS NOT NULL AND scheduled_at <= $1)
			     OR
			     (scheduled_at IS NULL AND updated_at <= $2)
			   )
			 ORDER BY created_at ASC
			 LIMIT $3
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("commit error"))

		results, err := repo.ClaimScheduledBatch(context.Background(), 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results on error")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== RecoverStuckQueued ==========

func TestPostgresRecoverStuckQueued(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusQueued

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'queued' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusPending, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		results, err := repo.RecoverStuckQueued(context.Background(), 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'queued' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(sqlmock.NewRows(notificationColumns))
		mock.ExpectRollback()

		results, err := repo.RecoverStuckQueued(context.Background(), 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if results != nil {
			t.Fatalf("expected nil results, got %v", results)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("begin_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))

		results, err := repo.RecoverStuckQueued(context.Background(), 5*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("select_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'queued' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnError(errors.New("select error"))
		mock.ExpectRollback()

		results, err := repo.RecoverStuckQueued(context.Background(), 5*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("update_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusQueued

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'queued' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusPending, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(errors.New("update error"))
		mock.ExpectRollback()

		results, err := repo.RecoverStuckQueued(context.Background(), 5*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("commit_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusQueued

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'queued' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusPending, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))

		results, err := repo.RecoverStuckQueued(context.Background(), 5*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== GetRetryReady ==========

func TestPostgresGetRetryReady(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		nextRetry := time.Now().Add(-1 * time.Minute)
		n := newPgTestNotification()
		n.Status = domain.StatusFailed
		n.NextRetryAt = &nextRetry

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		results, err := repo.GetRetryReady(context.Background(), 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		if results[0].Status != domain.StatusQueued {
			t.Fatalf("expected status queued, got %s", results[0].Status)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 5).
			WillReturnRows(sqlmock.NewRows(notificationColumns))
		mock.ExpectRollback()

		results, err := repo.GetRetryReady(context.Background(), 5)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if results != nil {
			t.Fatalf("expected nil results, got %v", results)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("begin_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))

		results, err := repo.GetRetryReady(context.Background(), 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("select_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnError(errors.New("select error"))
		mock.ExpectRollback()

		results, err := repo.GetRetryReady(context.Background(), 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("update_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		nextRetry := time.Now().Add(-1 * time.Minute)
		n := newPgTestNotification()
		n.Status = domain.StatusFailed
		n.NextRetryAt = &nextRetry

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(errors.New("update error"))
		mock.ExpectRollback()

		results, err := repo.GetRetryReady(context.Background(), 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("commit_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		nextRetry := time.Now().Add(-1 * time.Minute)
		n := newPgTestNotification()
		n.Status = domain.StatusFailed
		n.NextRetryAt = &nextRetry

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("commit error"))

		results, err := repo.GetRetryReady(context.Background(), 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== RecoverStuckProcessing ==========

func TestPostgresRecoverStuckProcessing(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusProcessing

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'processing' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		results, err := repo.RecoverStuckProcessing(context.Background(), 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		if results[0].Status != domain.StatusQueued {
			t.Fatalf("expected status queued, got %s", results[0].Status)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'processing' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(sqlmock.NewRows(notificationColumns))
		mock.ExpectRollback()

		results, err := repo.RecoverStuckProcessing(context.Background(), 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if results != nil {
			t.Fatalf("expected nil results, got %v", results)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("begin_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))

		results, err := repo.RecoverStuckProcessing(context.Background(), 5*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("select_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'processing' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnError(errors.New("select error"))
		mock.ExpectRollback()

		results, err := repo.RecoverStuckProcessing(context.Background(), 5*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("update_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusProcessing

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'processing' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(errors.New("update error"))
		mock.ExpectRollback()

		results, err := repo.RecoverStuckProcessing(context.Background(), 5*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("commit_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusProcessing

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'processing' AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("commit error"))

		results, err := repo.RecoverStuckProcessing(context.Background(), 5*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== RecoverOrphanedPending ==========

func TestPostgresRecoverOrphanedPending(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusPending
		n.ScheduledAt = nil

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending' AND scheduled_at IS NULL AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		results, err := repo.RecoverOrphanedPending(context.Background(), 10*time.Minute, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		if results[0].Status != domain.StatusQueued {
			t.Fatalf("expected status queued, got %s", results[0].Status)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending' AND scheduled_at IS NULL AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(sqlmock.NewRows(notificationColumns))
		mock.ExpectRollback()

		results, err := repo.RecoverOrphanedPending(context.Background(), 10*time.Minute, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if results != nil {
			t.Fatalf("expected nil results, got %v", results)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("begin_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))

		results, err := repo.RecoverOrphanedPending(context.Background(), 10*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("select_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending' AND scheduled_at IS NULL AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnError(errors.New("select error"))
		mock.ExpectRollback()

		results, err := repo.RecoverOrphanedPending(context.Background(), 10*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("update_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusPending
		n.ScheduledAt = nil

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending' AND scheduled_at IS NULL AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(errors.New("update error"))
		mock.ExpectRollback()

		results, err := repo.RecoverOrphanedPending(context.Background(), 10*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("commit_error", func(t *testing.T) {
		repo, mock, cleanup := setupPostgresTest(t)
		defer cleanup()

		n := newPgTestNotification()
		n.Status = domain.StatusPending
		n.ScheduledAt = nil

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(
			`SELECT * FROM notifications
			 WHERE status = 'pending' AND scheduled_at IS NULL AND updated_at <= $1
			 LIMIT $2
			 FOR UPDATE SKIP LOCKED`)).
			WithArgs(sqlmock.AnyArg(), 10).
			WillReturnRows(notificationRows([]*domain.Notification{n}))
		mock.ExpectExec(regexp.QuoteMeta(
			`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`)).
			WithArgs(domain.StatusQueued, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("commit error"))

		results, err := repo.RecoverOrphanedPending(context.Background(), 10*time.Minute, 10)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if results != nil {
			t.Fatal("expected nil results")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// ========== NewPostgresNotificationRepo ==========

func TestNewPostgresNotificationRepo(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "postgres")
	repo := NewPostgresNotificationRepo(sqlxDB)
	if repo == nil {
		t.Fatal("expected non-nil repo")
	}

	// Verify it implements the interface
	var _ NotificationRepository = repo
}

