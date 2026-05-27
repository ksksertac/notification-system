package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

type postgresNotificationRepo struct {
	db *sqlx.DB
}

func NewPostgresNotificationRepo(db *sqlx.DB) NotificationRepository {
	return &postgresNotificationRepo{db: db}
}

func (r *postgresNotificationRepo) Create(ctx context.Context, n *domain.Notification) error {
	query := `
		INSERT INTO notifications (id, idempotency_key, batch_id, recipient, channel, content, priority, status, max_retries, scheduled_at, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`

	_, err := r.db.ExecContext(ctx, query,
		n.ID, n.IdempotencyKey, n.BatchID, n.Recipient, n.Channel, n.Content,
		n.Priority, n.Status, n.MaxRetries, n.ScheduledAt, string(n.Metadata),
		n.CreatedAt, n.UpdatedAt,
	)
	return err
}

func (r *postgresNotificationRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		INSERT INTO notifications (id, idempotency_key, batch_id, recipient, channel, content, priority, status, max_retries, scheduled_at, metadata, created_at, updated_at)
		VALUES `

	valueStrings := make([]string, 0, len(notifications))
	valueArgs := make([]interface{}, 0, len(notifications)*13)

	for i, n := range notifications {
		base := i * 13
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7,
			base+8, base+9, base+10, base+11, base+12, base+13,
		))
		valueArgs = append(valueArgs,
			n.ID, n.IdempotencyKey, n.BatchID, n.Recipient, n.Channel, n.Content,
			n.Priority, n.Status, n.MaxRetries, n.ScheduledAt, string(n.Metadata),
			n.CreatedAt, n.UpdatedAt,
		)
	}

	query += strings.Join(valueStrings, ", ")

	if _, err := tx.ExecContext(ctx, query, valueArgs...); err != nil {
		return fmt.Errorf("batch insert: %w", err)
	}

	return tx.Commit()
}

func (r *postgresNotificationRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	var n domain.Notification
	err := r.db.GetContext(ctx, &n, "SELECT * FROM notifications WHERE id = $1", id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &n, err
}

func (r *postgresNotificationRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	var notifications []*domain.Notification
	err := r.db.SelectContext(ctx, &notifications, "SELECT * FROM notifications WHERE batch_id = $1 ORDER BY created_at", batchID)
	return notifications, err
}

func (r *postgresNotificationRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	var n domain.Notification
	err := r.db.GetContext(ctx, &n, "SELECT * FROM notifications WHERE idempotency_key = $1", key)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &n, err
}

func (r *postgresNotificationRepo) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	where := []string{"1=1"}
	args := []interface{}{}
	argIdx := 1

	if req.Status != nil {
		where = append(where, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, *req.Status)
		argIdx++
	}
	if req.Channel != nil {
		where = append(where, fmt.Sprintf("channel = $%d", argIdx))
		args = append(args, *req.Channel)
		argIdx++
	}
	if req.StartDate != nil {
		where = append(where, fmt.Sprintf("created_at >= $%d", argIdx))
		args = append(args, *req.StartDate)
		argIdx++
	}
	if req.EndDate != nil {
		where = append(where, fmt.Sprintf("created_at <= $%d", argIdx))
		args = append(args, *req.EndDate)
		argIdx++
	}
	if req.Cursor != nil {
		where = append(where, fmt.Sprintf("id < $%d", argIdx))
		args = append(args, *req.Cursor)
		argIdx++
	}

	whereClause := strings.Join(where, " AND ")

	var total int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM notifications WHERE %s", whereClause)
	if err := r.db.GetContext(ctx, &total, countQuery, args...); err != nil {
		return nil, 0, fmt.Errorf("counting notifications: %w", err)
	}

	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	listQuery := fmt.Sprintf(
		"SELECT * FROM notifications WHERE %s ORDER BY id DESC LIMIT $%d",
		whereClause, argIdx,
	)
	args = append(args, limit)

	var notifications []*domain.Notification
	if err := r.db.SelectContext(ctx, &notifications, listQuery, args...); err != nil {
		return nil, 0, fmt.Errorf("listing notifications: %w", err)
	}

	return notifications, total, nil
}

func (r *postgresNotificationRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
	result, err := r.db.ExecContext(ctx,
		"UPDATE notifications SET status = $1, updated_at = $2 WHERE id = $3 AND status = $4",
		to, time.Now().UTC(), id, from,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

func (r *postgresNotificationRepo) UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
	result, err := r.db.ExecContext(ctx,
		`UPDATE notifications SET status = $1, provider_msg_id = COALESCE($2, provider_msg_id), error_message = COALESCE($3, error_message), updated_at = $4
		 WHERE id = $5 AND status = $6`,
		to, providerMsgID, errorMsg, time.Now().UTC(), id, from,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

func (r *postgresNotificationRepo) IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE notifications SET retry_count = retry_count + 1, next_retry_at = $1, error_message = $2, status = 'failed', updated_at = $3
		 WHERE id = $4`,
		nextRetryAt, errorMsg, time.Now().UTC(), id,
	)
	return err
}

func (r *postgresNotificationRepo) MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO dead_letter_queue (notification_id, channel, recipient, content, error_message, retry_count)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		n.ID, n.Channel, n.Recipient, n.Content, errorMsg, n.RetryCount,
	)
	if err != nil {
		return fmt.Errorf("inserting into DLQ: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		"UPDATE notifications SET status = 'failed', error_message = $1, updated_at = $2 WHERE id = $3",
		errorMsg, time.Now().UTC(), n.ID,
	)
	if err != nil {
		return fmt.Errorf("updating notification status: %w", err)
	}

	return tx.Commit()
}

func (r *postgresNotificationRepo) GetScheduledReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	var notifications []*domain.Notification
	err := r.db.SelectContext(ctx, &notifications,
		`SELECT * FROM notifications
		 WHERE status = 'pending' AND scheduled_at IS NOT NULL AND scheduled_at <= $1
		 ORDER BY scheduled_at ASC LIMIT $2`,
		time.Now().UTC(), limit,
	)
	return notifications, err
}

func (r *postgresNotificationRepo) ClaimScheduledBatch(ctx context.Context, limit int) ([]*domain.Notification, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	var notifications []*domain.Notification
	err = tx.SelectContext(ctx, &notifications,
		`SELECT * FROM notifications
		 WHERE status = 'pending'
		   AND (
		     (scheduled_at IS NOT NULL AND scheduled_at <= $1)
		     OR
		     (scheduled_at IS NULL AND updated_at <= $2)
		   )
		 ORDER BY created_at ASC
		 LIMIT $3
		 FOR UPDATE SKIP LOCKED`,
		now, now.Add(-30*time.Second), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select claimable: %w", err)
	}

	if len(notifications) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, len(notifications))
	for i, n := range notifications {
		ids[i] = n.ID
	}

	_, err = tx.ExecContext(ctx,
		`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`,
		domain.StatusQueued, now, pq.Array(ids),
	)
	if err != nil {
		return nil, fmt.Errorf("batch update status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	for _, n := range notifications {
		n.Status = domain.StatusQueued
	}

	return notifications, nil
}

// RecoverStuckQueued finds notifications stuck in 'queued' status for longer
// than the threshold — meaning a pod claimed them but died before publishing
// to Redis. Resets them back to 'pending' so they get picked up again.
func (r *postgresNotificationRepo) RecoverStuckQueued(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	cutoff := time.Now().UTC().Add(-stuckThreshold)

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	var stuck []*domain.Notification
	err = tx.SelectContext(ctx, &stuck,
		`SELECT * FROM notifications
		 WHERE status = 'queued' AND updated_at <= $1
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`,
		cutoff, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select stuck queued: %w", err)
	}

	if len(stuck) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, len(stuck))
	for i, n := range stuck {
		ids[i] = n.ID
	}

	_, err = tx.ExecContext(ctx,
		`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`,
		domain.StatusPending, time.Now().UTC(), pq.Array(ids),
	)
	if err != nil {
		return nil, fmt.Errorf("reset stuck queued: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return stuck, nil
}

func (r *postgresNotificationRepo) GetRetryReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	now := time.Now().UTC()

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	var notifications []*domain.Notification
	err = tx.SelectContext(ctx, &notifications,
		`SELECT * FROM notifications
		 WHERE status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= $1
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`,
		now, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select retry ready: %w", err)
	}

	if len(notifications) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, len(notifications))
	for i, n := range notifications {
		ids[i] = n.ID
	}

	_, err = tx.ExecContext(ctx,
		`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`,
		domain.StatusQueued, now, pq.Array(ids),
	)
	if err != nil {
		return nil, fmt.Errorf("update retry ready: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	for _, n := range notifications {
		n.Status = domain.StatusQueued
	}

	return notifications, nil
}

func (r *postgresNotificationRepo) RecoverStuckProcessing(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	cutoff := time.Now().UTC().Add(-stuckThreshold)

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	var stuck []*domain.Notification
	err = tx.SelectContext(ctx, &stuck,
		`SELECT * FROM notifications
		 WHERE status = 'processing' AND updated_at <= $1
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`,
		cutoff, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select stuck processing: %w", err)
	}

	if len(stuck) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, len(stuck))
	for i, n := range stuck {
		ids[i] = n.ID
	}

	_, err = tx.ExecContext(ctx,
		`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`,
		domain.StatusQueued, time.Now().UTC(), pq.Array(ids),
	)
	if err != nil {
		return nil, fmt.Errorf("reset stuck processing: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	for _, n := range stuck {
		n.Status = domain.StatusQueued
	}

	return stuck, nil
}

func (r *postgresNotificationRepo) RecoverOrphanedPending(ctx context.Context, staleDuration time.Duration, limit int) ([]*domain.Notification, error) {
	cutoff := time.Now().UTC().Add(-staleDuration)

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	var orphaned []*domain.Notification
	err = tx.SelectContext(ctx, &orphaned,
		`SELECT * FROM notifications
		 WHERE status = 'pending' AND scheduled_at IS NULL AND updated_at <= $1
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`,
		cutoff, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select orphaned pending: %w", err)
	}

	if len(orphaned) == 0 {
		return nil, nil
	}

	ids := make([]uuid.UUID, len(orphaned))
	for i, n := range orphaned {
		ids[i] = n.ID
	}

	_, err = tx.ExecContext(ctx,
		`UPDATE notifications SET status = $1, updated_at = $2 WHERE id = ANY($3)`,
		domain.StatusQueued, time.Now().UTC(), pq.Array(ids),
	)
	if err != nil {
		return nil, fmt.Errorf("reset orphaned pending: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	for _, n := range orphaned {
		n.Status = domain.StatusQueued
	}

	return orphaned, nil
}

func (r *postgresNotificationRepo) UpdateRequeueCount(ctx context.Context, id uuid.UUID, count int) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE notifications SET requeue_count = $1, updated_at = $2 WHERE id = $3`,
		count, time.Now().UTC(), id,
	)
	return err
}

func (r *postgresNotificationRepo) AddToRequeueSet(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (r *postgresNotificationRepo) GetRequeueReady(_ context.Context, _ int) ([]*domain.Notification, error) {
	return nil, nil
}
