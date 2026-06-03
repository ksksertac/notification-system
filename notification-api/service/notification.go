package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
	"github.com/sertacyildirim/notification-system/shared/tracing"
)

// Sentinel errors for structured error handling in handlers.
var (
	ErrValidation             = errors.New("validation error")
	ErrNotFound               = errors.New("not found")
	ErrConflict               = errors.New("conflict")
	ErrConcurrentModification = errors.New("concurrent modification")
)

type NotificationService interface {
	Create(ctx context.Context, req domain.CreateNotificationRequest, idempotencyKey string) (*domain.Notification, error)
	CreateBatch(ctx context.Context, req domain.BatchCreateRequest) (*uuid.UUID, []*domain.Notification, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error)
}

type notificationService struct {
	repo       repository.NotificationRepository
	publisher  queue.Publisher
	buffer     *WriteBuffer
	maxRetries int
	logger     *slog.Logger
}

func NewNotificationService(repo repository.NotificationRepository, publisher queue.Publisher, buffer *WriteBuffer, maxRetries int, logger *slog.Logger) NotificationService {
	if logger == nil {
		logger = slog.Default()
	}
	return &notificationService{
		repo:       repo,
		publisher:  publisher,
		buffer:     buffer,
		maxRetries: maxRetries,
		logger:     logger,
	}
}

func (s *notificationService) Create(ctx context.Context, req domain.CreateNotificationRequest, idempotencyKey string) (*domain.Notification, error) {
	ctx, span := tracing.StartSpan(ctx, "service.Create")
	defer span.End()
	tracing.SetAttr(span, "notification.channel", req.Channel)

	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrValidation, err.Error())
	}

	if idempotencyKey != "" {
		existing, err := s.repo.GetByIdempotencyKey(ctx, idempotencyKey)
		if err != nil {
			return nil, fmt.Errorf("checking idempotency: %w", err)
		}
		if existing != nil {
			return existing, nil
		}
	}

	priority, _ := domain.PriorityFromString(req.Priority)

	metadata := []byte("{}")
	if req.Metadata != nil {
		var err error
		metadata, err = json.Marshal(req.Metadata)
		if err != nil {
			return nil, fmt.Errorf("marshaling metadata: %w", err)
		}
	}

	now := time.Now().UTC()
	n := &domain.Notification{
		ID:         uuid.New(),
		Recipient:  req.Recipient,
		Channel:    domain.Channel(req.Channel),
		Content:    req.Content,
		Priority:   priority,
		Status:     domain.StatusPending,
		MaxRetries: s.maxRetries,
		Metadata:   metadata,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if idempotencyKey != "" {
		n.IdempotencyKey = &idempotencyKey
	}

	if req.ScheduledAt != nil {
		n.ScheduledAt = req.ScheduledAt
	}

	if n.IdempotencyKey != nil {
		if err := s.repo.Create(ctx, n); err != nil {
			if errors.Is(err, repository.ErrIdempotencyConflict) {
				existing, getErr := s.repo.GetByIdempotencyKey(ctx, idempotencyKey)
				if getErr == nil && existing != nil {
					return existing, nil
				}
			}
			return nil, fmt.Errorf("creating notification: %w", err)
		}
		if n.ScheduledAt == nil {
			s.repo.UpdateStatus(ctx, n.ID, domain.StatusPending, domain.StatusQueued)
			n.Status = domain.StatusQueued
			if err := s.publisher.Publish(ctx, n); err != nil {
				s.repo.UpdateStatus(ctx, n.ID, domain.StatusQueued, domain.StatusPending)
				n.Status = domain.StatusPending
				s.logger.Warn("redis publish failed, scheduler will retry",
					"notification_id", n.ID,
					"error", err,
				)
			}
		}
	} else {
		if err := s.buffer.Submit(ctx, n); err != nil {
			return nil, fmt.Errorf("creating notification: %w", err)
		}
	}

	return n, nil
}

func (s *notificationService) CreateBatch(ctx context.Context, req domain.BatchCreateRequest) (*uuid.UUID, []*domain.Notification, error) {
	ctx, span := tracing.StartSpan(ctx, "service.CreateBatch")
	defer span.End()
	tracing.SetIntAttr(span, "batch.size", len(req.Notifications))

	if err := req.Validate(); err != nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrValidation, err.Error())
	}

	batchID := uuid.New()
	now := time.Now().UTC()

	var newNotifications []*domain.Notification
	var replayedNotifications []*domain.Notification
	for _, r := range req.Notifications {
		if r.IdempotencyKey != "" {
			existing, err := s.repo.GetByIdempotencyKey(ctx, r.IdempotencyKey)
			if err == nil && existing != nil {
				replayedNotifications = append(replayedNotifications, existing)
				continue
			}
		}

		priority, _ := domain.PriorityFromString(r.Priority)

		metadata := []byte("{}")
		if r.Metadata != nil {
			metadata, _ = json.Marshal(r.Metadata)
		}

		n := &domain.Notification{
			ID:         uuid.New(),
			BatchID:    &batchID,
			Recipient:  r.Recipient,
			Channel:    domain.Channel(r.Channel),
			Content:    r.Content,
			Priority:   priority,
			Status:     domain.StatusPending,
			MaxRetries: s.maxRetries,
			Metadata:   metadata,
			CreatedAt:  now,
			UpdatedAt:  now,
		}

		if r.IdempotencyKey != "" {
			n.IdempotencyKey = &r.IdempotencyKey
		}

		if r.ScheduledAt != nil {
			n.ScheduledAt = r.ScheduledAt
		}

		newNotifications = append(newNotifications, n)
	}

	if len(newNotifications) > 0 {
		if err := s.repo.CreateBatch(ctx, newNotifications); err != nil {
			return nil, nil, fmt.Errorf("creating batch: %w", err)
		}
	}

	notifications := append(newNotifications, replayedNotifications...)

	var immediate []*domain.Notification
	for _, n := range newNotifications {
		if n.ScheduledAt == nil {
			immediate = append(immediate, n)
		}
	}

	if len(immediate) > 0 {
		immediateIDs := make([]uuid.UUID, len(immediate))
		for i, n := range immediate {
			immediateIDs[i] = n.ID
		}
		if err := s.repo.UpdateStatusBatch(ctx, immediateIDs, domain.StatusPending, domain.StatusQueued); err != nil {
			s.logger.Error("failed to batch update status to queued", "error", err)
		} else {
			for _, n := range immediate {
				n.Status = domain.StatusQueued
			}
		}
		if err := s.publisher.PublishBatch(ctx, immediate); err != nil {
			s.repo.UpdateStatusBatch(ctx, immediateIDs, domain.StatusQueued, domain.StatusPending)
			for _, n := range immediate {
				n.Status = domain.StatusPending
			}
			s.logger.Warn("redis batch publish failed, scheduler will retry",
				"count", len(immediate),
				"error", err,
			)
		}
	}

	return &batchID, notifications, nil
}

func (s *notificationService) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *notificationService) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	return s.repo.GetByBatchID(ctx, batchID)
}

func (s *notificationService) Cancel(ctx context.Context, id uuid.UUID) error {
	ctx, span := tracing.StartSpan(ctx, "service.Cancel")
	defer span.End()
	tracing.SetAttr(span, "notification.id", id.String())

	n, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return fmt.Errorf("getting notification: %w", err)
	}
	if n == nil {
		return fmt.Errorf("%w: notification %s", ErrNotFound, id)
	}

	if !n.Status.CanTransitionTo(domain.StatusCancelled) {
		return fmt.Errorf("%w: cannot cancel notification in %s status", ErrConflict, n.Status)
	}

	updated, err := s.repo.UpdateStatus(ctx, id, n.Status, domain.StatusCancelled)
	if err != nil {
		return fmt.Errorf("cancelling notification: %w", err)
	}
	if !updated {
		return fmt.Errorf("%w: notification status changed concurrently", ErrConcurrentModification)
	}

	return nil
}

func (s *notificationService) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	return s.repo.List(ctx, req)
}
