package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

type TieredNotificationRepo struct {
	hot  NotificationRepository
	cold NotificationRepository
}

func NewTieredNotificationRepo(hot, cold NotificationRepository) NotificationRepository {
	return &TieredNotificationRepo{hot: hot, cold: cold}
}

func (t *TieredNotificationRepo) Create(ctx context.Context, n *domain.Notification) error {
	return t.hot.Create(ctx, n)
}

func (t *TieredNotificationRepo) CreateBatch(ctx context.Context, notifications []*domain.Notification) error {
	return t.hot.CreateBatch(ctx, notifications)
}

func (t *TieredNotificationRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := t.hot.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if n != nil {
		return n, nil
	}
	return t.cold.GetByID(ctx, id)
}

func (t *TieredNotificationRepo) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	notifications, err := t.hot.GetByBatchID(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if len(notifications) > 0 {
		return notifications, nil
	}
	return t.cold.GetByBatchID(ctx, batchID)
}

func (t *TieredNotificationRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	n, err := t.hot.GetByIdempotencyKey(ctx, key)
	if err != nil {
		return nil, err
	}
	if n != nil {
		return n, nil
	}
	return t.cold.GetByIdempotencyKey(ctx, key)
}

func (t *TieredNotificationRepo) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	hotWindow := time.Now().UTC().Add(-1 * time.Hour)

	if req.StartDate != nil && req.StartDate.After(hotWindow) {
		return t.hot.List(ctx, req)
	}

	return t.cold.List(ctx, req)
}

func (t *TieredNotificationRepo) UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error) {
	return t.hot.UpdateStatus(ctx, id, from, to)
}

func (t *TieredNotificationRepo) UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error) {
	return t.hot.UpdateStatusWithDetails(ctx, id, from, to, providerMsgID, errorMsg)
}

func (t *TieredNotificationRepo) IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error {
	return t.hot.IncrementRetry(ctx, id, nextRetryAt, errorMsg)
}

func (t *TieredNotificationRepo) MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error {
	return t.hot.MoveToDLQ(ctx, n, errorMsg)
}

func (t *TieredNotificationRepo) GetScheduledReady(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return t.hot.GetScheduledReady(ctx, limit)
}

func (t *TieredNotificationRepo) ClaimScheduledBatch(ctx context.Context, limit int) ([]*domain.Notification, error) {
	return t.hot.ClaimScheduledBatch(ctx, limit)
}

func (t *TieredNotificationRepo) RecoverStuckQueued(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error) {
	return t.hot.RecoverStuckQueued(ctx, stuckThreshold, limit)
}
