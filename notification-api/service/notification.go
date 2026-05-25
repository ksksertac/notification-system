package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
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
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validation: %w", err)
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
			return nil, fmt.Errorf("creating notification: %w", err)
		}
		if n.ScheduledAt == nil {
			if err := s.publisher.Publish(ctx, n); err == nil {
				s.repo.UpdateStatus(ctx, n.ID, domain.StatusPending, domain.StatusQueued)
				n.Status = domain.StatusQueued
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
	if err := req.Validate(); err != nil {
		return nil, nil, fmt.Errorf("validation: %w", err)
	}

	batchID := uuid.New()
	now := time.Now().UTC()

	notifications := make([]*domain.Notification, len(req.Notifications))
	for i, r := range req.Notifications {
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

		if r.ScheduledAt != nil {
			n.ScheduledAt = r.ScheduledAt
		}

		notifications[i] = n
	}

	if err := s.repo.CreateBatch(ctx, notifications); err != nil {
		return nil, nil, fmt.Errorf("creating batch: %w", err)
	}

	var immediate []*domain.Notification
	for _, n := range notifications {
		if n.ScheduledAt == nil {
			immediate = append(immediate, n)
		}
	}

	if len(immediate) > 0 {
		if err := s.publisher.PublishBatch(ctx, immediate); err == nil {
			for _, n := range immediate {
				s.repo.UpdateStatus(ctx, n.ID, domain.StatusPending, domain.StatusQueued)
				n.Status = domain.StatusQueued
			}
		} else {
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
	n, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return fmt.Errorf("getting notification: %w", err)
	}
	if n == nil {
		return fmt.Errorf("notification not found")
	}

	if !n.Status.CanTransitionTo(domain.StatusCancelled) {
		return fmt.Errorf("cannot cancel notification in %s status", n.Status)
	}

	updated, err := s.repo.UpdateStatus(ctx, id, n.Status, domain.StatusCancelled)
	if err != nil {
		return fmt.Errorf("cancelling notification: %w", err)
	}
	if !updated {
		return fmt.Errorf("notification status changed concurrently")
	}

	return nil
}

func (s *notificationService) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	return s.repo.List(ctx, req)
}
