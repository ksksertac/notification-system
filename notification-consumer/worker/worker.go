package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/notification-consumer/delivery"
	"github.com/sertacyildirim/notification-system/notification-consumer/template"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/shared/queue"
	"github.com/sertacyildirim/notification-system/shared/repository"
	"github.com/sertacyildirim/notification-system/shared/tracing"
)

type WorkerPoolConfig struct {
	ConsumerGroup string
	WorkerCount   int
	WeightHigh    int
	WeightNormal  int
	WeightLow     int
	ClaimMinIdle  time.Duration
	ClaimInterval time.Duration
	MaxRetries    int
}

type StatusBroadcaster interface {
	Broadcast(notificationID uuid.UUID, status domain.Status)
}

type MetricsRecorder interface {
	RecordDelivery(channel string, latency time.Duration)
	RecordFailure(channel string)
	RecordRateLimitHit()
	RecordCircuitBreakerOpen()
}

type WorkerPool struct {
	cfg         WorkerPoolConfig
	consumer    queue.Consumer
	publisher   queue.Publisher
	provider    delivery.Provider
	rateLimiter delivery.RateLimiter
	cbRegistry  *delivery.CircuitBreakerRegistry
	retry       delivery.RetryStrategy
	repo        repository.NotificationRepository
	broadcaster StatusBroadcaster
	metrics     MetricsRecorder
	tmplEngine  template.Engine
	logger      *slog.Logger
	wg          sync.WaitGroup
	requeueSem  chan struct{}
}

func NewWorkerPool(
	cfg WorkerPoolConfig,
	consumer queue.Consumer,
	publisher queue.Publisher,
	provider delivery.Provider,
	rateLimiter delivery.RateLimiter,
	cbRegistry *delivery.CircuitBreakerRegistry,
	retry delivery.RetryStrategy,
	repo repository.NotificationRepository,
	broadcaster StatusBroadcaster,
	metrics MetricsRecorder,
	tmplEngine template.Engine,
	logger *slog.Logger,
) *WorkerPool {
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

func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.cfg.WorkerCount; i++ {
		wp.wg.Add(1)
		go wp.runWorker(ctx, i)
	}

	wp.wg.Add(1)
	go wp.runClaimer(ctx)

	wp.logger.Info("worker pool started",
		"workers", wp.cfg.WorkerCount,
		"group", wp.cfg.ConsumerGroup,
	)
}

func (wp *WorkerPool) Stop() {
	wp.wg.Wait()
	wp.logger.Info("worker pool stopped")
}

func (wp *WorkerPool) runWorker(ctx context.Context, id int) {
	defer wp.wg.Done()

	consumerName := uuid.New().String()
	logger := wp.logger.With("worker_id", id, "consumer", consumerName)

	logger.Info("worker started")

	for {
		select {
		case <-ctx.Done():
			logger.Info("worker shutting down")
			return
		default:
		}

		processed := wp.pollStreams(ctx, consumerName)
		if !processed {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (wp *WorkerPool) pollStreams(ctx context.Context, consumerName string) bool {
	processed := false

	streams := []struct {
		name   string
		weight int
	}{
		{queue.StreamHigh, wp.cfg.WeightHigh},
		{queue.StreamNormal, wp.cfg.WeightNormal},
		{queue.StreamLow, wp.cfg.WeightLow},
	}

	for _, s := range streams {
		msgs, err := wp.consumer.Read(ctx, s.name, wp.cfg.ConsumerGroup, consumerName, int64(s.weight))
		if err != nil {
			wp.logger.Error("failed to read stream", "stream", s.name, "error", err)
			continue
		}

		for _, msg := range msgs {
			wp.processMessage(ctx, msg)
			processed = true
		}
	}

	return processed
}

func (wp *WorkerPool) processMessage(ctx context.Context, msg queue.Message) {
	ctx, span := tracing.StartSpan(ctx, "consumer.ProcessMessage")
	defer span.End()
	tracing.SetNotificationAttrs(span, msg.NotificationID.String(), string(msg.Channel), "")
	tracing.SetAttr(span, "queue.stream", msg.StreamName)

	startTime := time.Now()
	logger := wp.logger.With(
		"notification_id", msg.NotificationID,
		"channel", msg.Channel,
		"stream", msg.StreamName,
		"correlation_id", msg.CorrelationID,
	)

	n, err := wp.repo.GetByID(ctx, msg.NotificationID)
	if err != nil {
		logger.Error("failed to get notification", "error", err)
		return
	}
	if n == nil || n.Status == domain.StatusCancelled || n.Status == domain.StatusDelivered {
		wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)
		return
	}

	updated, err := wp.repo.UpdateStatus(ctx, msg.NotificationID, n.Status, domain.StatusProcessing)
	if err != nil || !updated {
		logger.Warn("failed to transition to processing, skipping", "error", err, "updated", updated)
		wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)
		return
	}
	if wp.broadcaster != nil {
		wp.broadcaster.Broadcast(msg.NotificationID, domain.StatusProcessing)
	}

	cb := wp.cbRegistry.Get(string(msg.Channel))
	if !cb.Allow() {
		logger.Warn("circuit breaker open, re-enqueuing")
		if wp.metrics != nil {
			wp.metrics.RecordCircuitBreakerOpen()
		}
		wp.reEnqueue(ctx, n)
		wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)
		return
	}

	allowed, err := wp.rateLimiter.Allow(ctx, string(msg.Channel))
	if err != nil {
		logger.Error("rate limiter error", "error", err)
	}
	if !allowed {
		logger.Debug("rate limited, re-enqueuing")
		if wp.metrics != nil {
			wp.metrics.RecordRateLimitHit()
		}
		wp.repo.UpdateStatus(ctx, msg.NotificationID, domain.StatusProcessing, domain.StatusQueued)
		wp.reEnqueue(ctx, n)
		wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)
		return
	}

	content := msg.Content
	if wp.tmplEngine != nil && n.Metadata != nil {
		rendered, err := wp.tmplEngine.Render(content, n.Metadata)
		if err != nil {
			logger.Warn("template rendering failed, using raw content", "error", err)
		} else {
			content = rendered
		}
	}

	sendCtx, sendSpan := tracing.StartSpan(ctx, "provider.Send")
	tracing.SetAttr(sendSpan, "provider.channel", string(msg.Channel))
	result, sendErr := wp.provider.Send(sendCtx, msg.Recipient, string(msg.Channel), content)
	if sendErr != nil {
		tracing.RecordError(sendSpan, sendErr)
	}
	sendSpan.End()

	if sendErr != nil {
		cb.RecordFailure()
		if wp.metrics != nil {
			wp.metrics.RecordFailure(string(msg.Channel))
		}
		wp.handleFailure(ctx, n, sendErr, result, logger)
		wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)
		return
	}

	cb.RecordSuccess()

	latency := time.Since(startTime)
	if wp.metrics != nil {
		wp.metrics.RecordDelivery(string(msg.Channel), latency)
	}

	providerID := result.ProviderMsgID
	updated, updateErr := wp.repo.UpdateStatusWithDetails(ctx, msg.NotificationID, domain.StatusProcessing, domain.StatusDelivered, &providerID, nil)
	if updateErr != nil {
		logger.Error("failed to update status to delivered", "error", updateErr)
	} else if !updated {
		logger.Warn("status update to delivered failed, status may have changed concurrently")
	}

	if wp.broadcaster != nil {
		wp.broadcaster.Broadcast(msg.NotificationID, domain.StatusDelivered)
	}

	wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)

	logger.Info("notification delivered", "provider_msg_id", providerID, "latency_ms", latency.Milliseconds())
}

func (wp *WorkerPool) handleFailure(ctx context.Context, n *domain.Notification, sendErr error, result *delivery.SendResult, logger *slog.Logger) {
	errMsg := sendErr.Error()

	if result != nil && !result.Retryable {
		logger.Error("permanent failure, moving to DLQ", "error", errMsg)
		if err := wp.repo.MoveToDLQ(ctx, n, errMsg); err != nil {
			logger.Error("failed to move notification to DLQ", "error", err)
		}
		if wp.broadcaster != nil {
			wp.broadcaster.Broadcast(n.ID, domain.StatusFailed)
		}
		return
	}

	if !wp.retry.ShouldRetry(n.RetryCount+1, wp.cfg.MaxRetries) {
		logger.Error("max retries exceeded, moving to DLQ", "error", errMsg, "retry_count", n.RetryCount)
		if err := wp.repo.MoveToDLQ(ctx, n, errMsg); err != nil {
			logger.Error("failed to move notification to DLQ", "error", err)
		}
		if wp.broadcaster != nil {
			wp.broadcaster.Broadcast(n.ID, domain.StatusFailed)
		}
		return
	}

	delay := wp.retry.NextDelay(n.RetryCount + 1)
	nextRetry := time.Now().Add(delay)

	if err := wp.repo.IncrementRetry(ctx, n.ID, nextRetry, errMsg); err != nil {
		logger.Error("failed to increment retry count", "error", err)
	}

	logger.Warn("retry scheduled via persistence", "retry_count", n.RetryCount+1, "next_retry_at", nextRetry, "error", errMsg)
}

func (wp *WorkerPool) reEnqueue(ctx context.Context, n *domain.Notification) {
	select {
	case wp.requeueSem <- struct{}{}:
	default:
		wp.logger.Warn("requeue semaphore full, reverting status for recovery", "notification_id", n.ID)
		wp.repo.UpdateStatus(ctx, n.ID, domain.StatusProcessing, domain.StatusQueued)
		return
	}

	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		defer func() { <-wp.requeueSem }()

		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return
		}

		publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := wp.publisher.Publish(publishCtx, n); err != nil {
			wp.logger.Error("failed to re-enqueue notification", "notification_id", n.ID, "error", err)
		}
	}()
}

func (wp *WorkerPool) runClaimer(ctx context.Context) {
	defer wp.wg.Done()

	ticker := time.NewTicker(wp.cfg.ClaimInterval)
	defer ticker.Stop()

	consumerName := "claimer-" + uuid.New().String()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, stream := range []string{queue.StreamHigh, queue.StreamNormal, queue.StreamLow} {
				msgs, err := wp.consumer.ClaimStale(ctx, stream, wp.cfg.ConsumerGroup, consumerName, wp.cfg.ClaimMinIdle, 10)
				if err != nil {
					wp.logger.Error("claim stale failed", "stream", stream, "error", err)
					continue
				}
				for _, msg := range msgs {
					wp.processMessage(ctx, msg)
				}
			}
		}
	}
}
