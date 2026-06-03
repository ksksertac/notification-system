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
	logger *slog.Logger
	wg     sync.WaitGroup

	deficitMu sync.Mutex
	deficit   [3]int
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
		logger: logger,
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

	streams := [3]struct {
		name   string
		weight int
	}{
		{queue.StreamHigh, wp.cfg.WeightHigh},
		{queue.StreamNormal, wp.cfg.WeightNormal},
		{queue.StreamLow, wp.cfg.WeightLow},
	}

	wp.deficitMu.Lock()
	for i := range streams {
		wp.deficit[i] += streams[i].weight
	}
	wp.deficitMu.Unlock()

	for {
		maxIdx := -1
		maxDeficit := 0
		wp.deficitMu.Lock()
		for i := range streams {
			if wp.deficit[i] > maxDeficit {
				maxDeficit = wp.deficit[i]
				maxIdx = i
			}
		}
		wp.deficitMu.Unlock()

		if maxIdx < 0 || maxDeficit <= 0 {
			break
		}

		s := streams[maxIdx]
		msgs, err := wp.consumer.Read(ctx, s.name, wp.cfg.ConsumerGroup, consumerName, int64(maxDeficit))
		if err != nil {
			wp.logger.Error("failed to read stream", "stream", s.name, "error", err)
			wp.deficitMu.Lock()
			wp.deficit[maxIdx] = 0
			wp.deficitMu.Unlock()
			continue
		}

		wp.deficitMu.Lock()
		if len(msgs) > 0 {
			wp.deficit[maxIdx] -= len(msgs)
		} else {
			wp.deficit[maxIdx] = -1
		}
		wp.deficitMu.Unlock()

		for _, msg := range msgs {
			wp.processMessage(ctx, msg)
			processed = true
		}
	}

	return processed
}

func (wp *WorkerPool) processMessage(ctx context.Context, msg queue.Message) {
	if msg.Traceparent != "" {
		carrier := tracing.MapCarrier{
			"traceparent": msg.Traceparent,
			"tracestate":  msg.Tracestate,
		}
		ctx = tracing.ExtractTraceContext(ctx, carrier)
	}
	ctx, span := tracing.StartSpan(ctx, "consumer.ProcessMessage")
	defer span.End()
	tracing.SetNotificationAttrs(span, msg.NotificationID.String(), string(msg.Channel), "")
	tracing.SetAttr(span, "queue.stream", msg.StreamName)
	if msg.CorrelationID != "" {
		tracing.SetAttr(span, "correlation_id", msg.CorrelationID)
	}

	const maxDeliveryCount int64 = 10

	startTime := time.Now()
	logger := wp.logger.With(
		"notification_id", msg.NotificationID,
		"channel", msg.Channel,
		"stream", msg.StreamName,
		"correlation_id", msg.CorrelationID,
	)

	if msg.DeliveryCount >= maxDeliveryCount {
		logger.Error("max delivery count exceeded, force ACKing and moving to DLQ",
			"delivery_count", msg.DeliveryCount)
		n, _ := wp.repo.GetByID(ctx, msg.NotificationID)
		if n != nil && !n.Status.IsFinal() {
			_ = wp.repo.MoveToDLQ(ctx, n, "max stream delivery count exceeded")
		}
		wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)
		return
	}

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
	if err != nil {
		logger.Error("failed to transition to processing, not ACKing for redelivery", "error", err)
		return
	}
	if !updated {
		logger.Warn("CAS miss on processing transition, another consumer claimed it")
		wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)
		return
	}
	if wp.broadcaster != nil {
		wp.broadcaster.Broadcast(msg.NotificationID, domain.StatusProcessing)
	}

	cb := wp.cbRegistry.Get(string(msg.Channel))
	if !cb.Allow() {
		logger.Warn("circuit breaker open, re-enqueuing", "requeue_count", n.RequeueCount)
		if wp.metrics != nil {
			wp.metrics.RecordCircuitBreakerOpen()
		}
		if n.RequeueCount >= domain.MaxRequeueCount {
			logger.Error("max requeue count exceeded, moving to DLQ", "requeue_count", n.RequeueCount)
			if err := wp.repo.MoveToDLQ(ctx, n, "max requeue count exceeded (circuit breaker)"); err != nil {
				logger.Error("failed to move to DLQ, not ACKing for redelivery", "error", err)
				return
			}
			if wp.broadcaster != nil {
				wp.broadcaster.Broadcast(n.ID, domain.StatusFailed)
			}
			wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)
			return
		}
		n.RequeueCount++
		if err := wp.repo.UpdateRequeueCount(ctx, n.ID, n.RequeueCount); err != nil {
			logger.Error("failed to update requeue count, not ACKing for redelivery", "error", err)
			return
		}
		if _, err := wp.repo.UpdateStatus(ctx, msg.NotificationID, domain.StatusProcessing, domain.StatusQueued); err != nil {
			logger.Error("failed to revert status to queued, not ACKing for redelivery", "error", err)
			return
		}
		if err := wp.reEnqueue(ctx, n, cbBackoffDelay(n.RequeueCount)); err != nil {
			logger.Error("failed to add to requeue set, not ACKing for redelivery", "error", err)
			return
		}
		wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)
		return
	}

	allowed, err := wp.rateLimiter.Allow(ctx, string(msg.Channel))
	if err != nil {
		logger.Warn("rate limiter error, failing open", "error", err)
		allowed = true
	}
	if !allowed {
		logger.Debug("rate limited, re-enqueuing")
		if wp.metrics != nil {
			wp.metrics.RecordRateLimitHit()
		}
		if _, err := wp.repo.UpdateStatus(ctx, msg.NotificationID, domain.StatusProcessing, domain.StatusQueued); err != nil {
			logger.Error("failed to revert status to queued, not ACKing for redelivery", "error", err)
			return
		}
		if err := wp.reEnqueue(ctx, n, 500*time.Millisecond); err != nil {
			logger.Error("failed to add to requeue set, not ACKing for redelivery", "error", err)
			return
		}
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
	result, sendErr := wp.provider.Send(sendCtx, msg.NotificationID.String(), msg.Recipient, string(msg.Channel), content)
	if sendErr != nil {
		tracing.RecordError(sendSpan, sendErr)
	}
	sendSpan.End()

	if sendErr != nil {
		cb.RecordFailure()
		if wp.metrics != nil {
			wp.metrics.RecordFailure(string(msg.Channel))
		}
		if !wp.handleFailure(ctx, n, sendErr, result, logger) {
			logger.Error("side effect failed after send error, not ACKing for redelivery")
			return
		}
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
		logger.Error("failed to update status to delivered, not ACKing for redelivery", "error", updateErr)
		return
	}
	if !updated {
		current, err := wp.repo.GetByID(ctx, msg.NotificationID)
		if err != nil {
			logger.Error("failed to re-read notification after CAS miss, not ACKing", "error", err)
			return
		}
		if current != nil && !current.Status.IsFinal() {
			retryOk, retryErr := wp.repo.UpdateStatusWithDetails(ctx, msg.NotificationID, current.Status, domain.StatusDelivered, &providerID, nil)
			if retryErr != nil {
				logger.Error("failed to force status to delivered, not ACKing", "error", retryErr)
				return
			}
			if !retryOk {
				logger.Error("CAS retry also failed, status changed again, not ACKing")
				return
			}
			logger.Warn("forced status to delivered after CAS miss", "previous_status", current.Status)
		}
	}

	if wp.broadcaster != nil {
		wp.broadcaster.Broadcast(msg.NotificationID, domain.StatusDelivered)
	}

	wp.consumer.Ack(ctx, msg.StreamName, wp.cfg.ConsumerGroup, msg.ID)

	logger.Info("notification delivered", "provider_msg_id", providerID, "latency_ms", latency.Milliseconds())
}

func (wp *WorkerPool) handleFailure(ctx context.Context, n *domain.Notification, sendErr error, result *delivery.SendResult, logger *slog.Logger) bool {
	errMsg := sendErr.Error()

	if result != nil && !result.Retryable {
		logger.Error("permanent failure, moving to DLQ", "error", errMsg)
		if err := wp.repo.MoveToDLQ(ctx, n, errMsg); err != nil {
			logger.Error("failed to move notification to DLQ", "error", err)
			return false
		}
		if wp.broadcaster != nil {
			wp.broadcaster.Broadcast(n.ID, domain.StatusFailed)
		}
		return true
	}

	if !wp.retry.ShouldRetry(n.RetryCount+1, wp.cfg.MaxRetries) {
		logger.Error("max retries exceeded, moving to DLQ", "error", errMsg, "retry_count", n.RetryCount)
		if err := wp.repo.MoveToDLQ(ctx, n, errMsg); err != nil {
			logger.Error("failed to move notification to DLQ", "error", err)
			return false
		}
		if wp.broadcaster != nil {
			wp.broadcaster.Broadcast(n.ID, domain.StatusFailed)
		}
		return true
	}

	delay := wp.retry.NextDelay(n.RetryCount + 1)
	if result != nil && result.RetryAfter > 0 {
		delay = result.RetryAfter
	}
	nextRetry := time.Now().Add(delay)

	if err := wp.repo.IncrementRetry(ctx, n.ID, nextRetry, errMsg); err != nil {
		logger.Error("failed to increment retry count", "error", err)
		return false
	}

	if wp.broadcaster != nil {
		wp.broadcaster.Broadcast(n.ID, domain.StatusRetrying)
	}

	logger.Warn("retry scheduled via persistence", "retry_count", n.RetryCount+1, "next_retry_at", nextRetry, "error", errMsg)
	return true
}

func (wp *WorkerPool) reEnqueue(ctx context.Context, n *domain.Notification, delay time.Duration) error {
	requeueAt := time.Now().Add(delay)
	return wp.repo.AddToRequeueSet(ctx, n.ID, requeueAt)
}

func cbBackoffDelay(requeueCount int) time.Duration {
	const maxDelay = 30 * time.Second
	delay := 500 * time.Millisecond
	for i := 1; i < requeueCount; i++ {
		delay *= 2
		if delay >= maxDelay {
			return maxDelay
		}
	}
	return delay
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
