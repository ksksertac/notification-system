package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

var ErrIdempotencyConflict = errors.New("idempotency key already exists")

type NotificationRepository interface {
	Create(ctx context.Context, n *domain.Notification) error
	CreateBatch(ctx context.Context, notifications []*domain.Notification) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error)
	List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, from, to domain.Status) (bool, error)
	UpdateStatusWithDetails(ctx context.Context, id uuid.UUID, from, to domain.Status, providerMsgID *string, errorMsg *string) (bool, error)
	IncrementRetry(ctx context.Context, id uuid.UUID, nextRetryAt time.Time, errorMsg string) error
	MoveToDLQ(ctx context.Context, n *domain.Notification, errorMsg string) error
	GetScheduledReady(ctx context.Context, limit int) ([]*domain.Notification, error)
	ClaimScheduledBatch(ctx context.Context, limit int) ([]*domain.Notification, error)
	RecoverStuckQueued(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error)
	GetRetryReady(ctx context.Context, limit int) ([]*domain.Notification, error)
	RecoverStuckProcessing(ctx context.Context, stuckThreshold time.Duration, limit int) ([]*domain.Notification, error)
	RecoverOrphanedPending(ctx context.Context, staleDuration time.Duration, limit int) ([]*domain.Notification, error)
}
