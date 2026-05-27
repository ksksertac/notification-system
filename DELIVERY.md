# Delivery & Retry Logic -- Design Document

This document explains the design decisions behind the notification system's delivery pipeline, retry strategy, failure handling, and recovery mechanisms.

---

## 1. Delivery Strategy Overview

The system delivers notifications through an external provider (webhook.site) using HTTP POST requests. Each delivery attempt sends a JSON payload and expects a structured response.

**Outbound request:**

```json
{
  "to": "<recipient>",
  "channel": "<sms|email|push>",
  "content": "<rendered content>"
}
```

**Expected response (HTTP 202 Accepted):**

```json
{
  "messageId": "<provider-assigned ID>",
  "status": "<provider status>",
  "timestamp": "<ISO timestamp>"
}
```

The webhook provider is implemented as a `Provider` interface (`delivery/provider.go`), making it straightforward to swap in a real SMS gateway, email service, or push notification provider without changing the delivery pipeline. The HTTP client is configured with connection pooling (100 max idle connections, 90s idle timeout) and a configurable request timeout to handle slow or unresponsive providers gracefully. Provider response bodies are capped at 1 MB via `io.LimitReader` to prevent memory exhaustion from oversized or malicious responses.

---

## 2. Retry Strategy

### Algorithm: Exponential Backoff with Jitter

Exponential backoff was chosen because it gives the external provider progressively more time to recover from transient failures (e.g., temporary overload, deploy in progress) without overwhelming it with rapid retry attempts.

**Formula:**

```
delay = baseDelay * 2^(attempt-1) + jitter
```

Where:

```
jitter = rand.Float64() * baseDelay * attempt
```

The delay is capped at `maxDelay` to prevent excessively long waits at high attempt counts.

**Example delay progression (with default 2s base):**

| Attempt | Base Delay | Jitter Range | Total Range |
|---------|-----------|--------------|-------------|
| 1       | 2s        | 0-2s         | 2s - 4s     |
| 2       | 4s        | 0-4s         | 4s - 8s     |
| 3       | 8s        | 0-6s         | 8s - 14s    |
| 4       | 16s       | 0-8s         | 16s - 24s   |
| 5       | 32s       | 0-10s        | 32s - 42s   |

### Why Jitter?

Without jitter, if 1,000 notifications all fail at the same moment, they would all retry at exactly the same times, creating synchronized retry storms. Jitter spreads these retries randomly across the delay window, smoothing the load on the provider. The jitter amount scales with the attempt number, providing wider distribution as retry pressure increases.

### Implementation

The retry strategy is defined behind a `RetryStrategy` interface with two methods:

- `NextDelay(attempt int) time.Duration` -- calculates the delay for a given attempt
- `ShouldRetry(attempt int, maxAttempts int) bool` -- determines if another retry is allowed

This interface allows the strategy to be swapped (e.g., to linear backoff or fixed delay) without changing the worker logic.

---

## 3. Retry Classification

Not all failures deserve retry attempts. The system classifies errors at the provider boundary to avoid wasting retries on permanent failures.

### Retryable Errors

These indicate transient conditions where a subsequent attempt may succeed:

- **5xx server errors** (500, 502, 503, 504) -- provider-side issues, typically temporary
- **429 Too Many Requests** -- provider rate limiting, will clear after a window
- **Network/connection errors** -- DNS resolution failures, connection refused, timeouts

### Non-Retryable (Permanent) Errors

These indicate client-side issues that will not resolve on retry:

- **4xx client errors** (except 429) -- malformed request, authentication failure, invalid recipient
- **Response parsing errors** -- provider returned a non-JSON body for a success status

### Failure Handling Flow

```
Provider returns error
        |
        v
   Retryable? ----NO----> MoveToDLQ (immediate, no retries wasted)
        |
       YES
        |
        v
   Attempts < MaxRetries? ----NO----> MoveToDLQ (retries exhausted)
        |
       YES
        |
        v
   IncrementRetry: bump retry_count, set next_retry_at,
   add to idx:retry sorted set, publish persist event
        |
        v
   Scheduler picks up when NextRetryAt arrives,
   re-publishes to priority stream
```

The `SendResult` struct returned by the provider carries a `Retryable` boolean, allowing the webhook implementation to classify errors at the source. The worker inspects this flag before deciding whether to retry or move directly to the DLQ.

---

## 4. Dead Letter Queue (DLQ)

Notifications enter the DLQ under two conditions:

1. **Retries exhausted** -- the notification has been retried `MaxRetries` times and still fails
2. **Permanent failure** -- the provider returned a non-retryable error (4xx except 429)

### Storage

DLQ entries are stored in two locations for durability:

- **Redis**: `dlq:{notification_id}` hash containing the original notification data, error message, retry count, and failure timestamp. The `MoveToDLQ` operation is a single atomic Lua script — it updates the notification status, creates the DLQ hash entry, moves status indexes, and publishes the persist event in one call, preventing inconsistent state on partial failure
- **PostgreSQL**: `dead_letter_queue` table, persisted asynchronously via the `persist:queue` stream

### DLQ Entry Fields

| Field             | Description                                  |
|-------------------|----------------------------------------------|
| `id`              | Unique DLQ entry ID (UUID)                   |
| `notification_id` | Original notification ID                     |
| `channel`         | Delivery channel (sms, email, push)          |
| `recipient`       | Target recipient                             |
| `content`         | Notification content                         |
| `error_message`   | Last error that caused the failure           |
| `retry_count`     | Number of retry attempts made                |
| `failed_at`       | Timestamp when the notification was moved to DLQ |
| `reprocessed`     | Whether the entry has been manually reprocessed |

### Side Effects

When a notification is moved to the DLQ:

1. The notification's status is set to `failed` in the Redis hash
2. The status index is updated (`idx:status:processing` -> `idx:status:failed`)
3. A persist event is published to `persist:queue` for PostgreSQL cold storage
4. A WebSocket broadcast is sent so connected clients see the status change in real time

---

## 5. Circuit Breaker (Per Channel)

The circuit breaker prevents the system from hammering a failing provider, giving it time to recover. Each delivery channel (sms, email, push) has its own independent circuit breaker, so a failure in one channel does not block delivery on others.

### State Machine

```
     Success
  +----------+
  |          |
  v          |
CLOSED --[failures >= threshold]--> OPEN --[openDuration elapsed]--> HALF-OPEN
  ^                                  ^                                  |
  |                                  |                                  |
  +-------[success in half-open]-----+--------[failure in half-open]----+
```

| State     | Behavior                                                       |
|-----------|----------------------------------------------------------------|
| Closed    | All requests pass through. Failures are counted.               |
| Open      | All requests are rejected. Messages are re-enqueued with a 500ms delay. |
| Half-Open | A limited number of test requests are allowed through. Success transitions to Closed; failure transitions back to Open. |

### Distributed State via Redis

In a multi-pod deployment, circuit breaker state must be shared. The system uses Redis-backed circuit breakers with Lua scripts for atomic state transitions:

- **State storage**: Redis Hash at `cb:{channel}` with fields `state`, `failures`, `opened_at`, `half_open_count`, `last_failure_at`
- **Atomicity**: All state reads and transitions happen inside Lua scripts to prevent race conditions between pods
- **Fail-open**: If Redis is unreachable, the circuit breaker defaults to allowing requests rather than blocking delivery

### When the Circuit Breaker is Open

Messages are not dropped. Instead:

1. The notification's status is reverted (processing -> queued via re-enqueue)
2. The message is re-published to its priority stream after a 500ms delay
3. The stream message is ACK'd to prevent the consumer group from re-delivering it
4. A Prometheus metric (`circuit_breaker_open_total`) is incremented

This ensures zero message loss while giving the provider breathing room.

---

## 6. Rate Limiting (Per Channel)

Rate limiting prevents the system from exceeding the external provider's throughput limits, even during burst traffic (e.g., a large batch notification).

### Algorithm: Redis Sliding Window

A sliding window counter implemented via a Redis Sorted Set tracks requests per channel per second. The algorithm runs as an atomic Lua script:

```lua
ZREMRANGEBYSCORE key '-inf' (now - window)   -- Remove expired entries
ZCARD key                                      -- Count current entries
if count < limit then
    ZADD key now (now + random_suffix)         -- Add new entry
    PEXPIRE key window                         -- Set TTL for cleanup
    return 1                                   -- Allowed
end
return 0                                       -- Denied
```

**Why sliding window over fixed window?** A fixed window can allow up to 2x the limit at window boundaries (e.g., 100 requests at 0:59 and 100 more at 1:01). The sliding window provides a smooth, accurate rate measurement with no boundary spikes.

**Why Lua script?** The ZREMRANGEBYSCORE + ZCARD + ZADD sequence must be atomic. Without Lua, a race condition between two pods could allow both to read `count = 99` and both add an entry, exceeding the limit.

### When Rate Limited

The notification is not lost or failed:

1. Status is reverted from `processing` back to `queued`
2. The message is re-enqueued to its priority stream after a 500ms delay
3. The stream message is ACK'd
4. A Prometheus metric (`rate_limit_hits_total`) is incremented

The 500ms delay prevents tight retry loops while keeping latency low.

---

## 7. Message Processing Flow

Each worker goroutine follows this pipeline for every message:

```
1. Read message from priority stream (high/normal/low with weighted polling)
         |
         v
2. Fetch notification from Redis (GetByID)
         |
         v
3. Skip if cancelled/delivered (ACK and move on)
         |
         v
4. CAS transition: current_status -> processing (atomic, prevents double-processing)
         |
         v
5. Circuit Breaker check
   |-- OPEN: re-enqueue with 500ms delay, ACK, return
         |
         v
6. Rate Limiter check
   |-- DENIED: revert to queued, re-enqueue with 500ms delay, ACK, return
         |
         v
7. Template rendering (if metadata present, uses html/template with sync.Map cache)
         |
         v
8. Provider.Send(recipient, channel, content)
   |
   |-- SUCCESS: RecordSuccess on circuit breaker, update status to delivered,
   |             store provider_msg_id, broadcast via WebSocket, ACK
   |
   |-- FAILURE:
       |-- RecordFailure on circuit breaker
       |-- Non-retryable? -> MoveToDLQ, broadcast failed, ACK
       |-- Retryable + attempts remaining? -> IncrementRetry (schedule next attempt), ACK
       |-- Retryable + attempts exhausted? -> MoveToDLQ, broadcast failed, ACK
```

**Key design principle**: The stream message is ACK'd only after all side effects (status update, DLQ write, persist event, WebSocket broadcast) are complete. This ensures that if a consumer crashes mid-processing, the message remains in the pending entries list and will be reclaimed by the XAUTOCLAIM mechanism.

---

## 8. Recovery Mechanisms (Multi-Layer Safety Net)

The system is designed with the assumption that any component can fail at any time. Nine layers of recovery ensure that no notification is permanently lost or stuck.

### Layer 1: Rate Limiter Re-enqueue

**Trigger:** Provider rate limit reached for a channel.
**Action:** Revert status to `queued`, re-enqueue after 500ms.
**Purpose:** Smooth out burst traffic without losing messages.

### Layer 2: Circuit Breaker Re-enqueue

**Trigger:** Circuit breaker is open for the target channel.
**Action:** Re-enqueue after 500ms (status remains `processing` -> re-queued).
**Purpose:** Stop sending to a failing provider while keeping messages in the pipeline.

### Layer 3: Exponential Backoff Retry via Scheduler

**Trigger:** Provider returns a retryable error and retries remain.
**Action:** `IncrementRetry` sets the notification status to `retrying` and adds it to the `idx:retry` sorted set with `next_retry_at` as score. The scheduler's `processRetryReady` loop polls every 10s, picks up notifications whose `next_retry_at` has arrived, transitions them from `retrying` to `queued`, and publishes them back to the priority stream.
**Purpose:** Give the provider time to recover with progressively longer waits.

### Layer 4: XAUTOCLAIM for Crashed Consumer Messages

**Trigger:** A consumer pod crashes or is killed while processing a message (message stays in PEL with idle time > 30s).
**Action:** A dedicated claimer goroutine runs `XAUTOCLAIM` every 15s on all three priority streams, reclaiming messages idle for more than 30s and reprocessing them.
**Purpose:** Recover messages that were mid-flight when a consumer died.

### Layer 5: Orphaned Pending Recovery

**Trigger:** Instant notifications (non-scheduled) stuck in `pending` status for more than 30s without being queued.
**Action:** Atomic Lua script checks `idx:status:pending`, excludes entries that exist in `schedule:pending` (those are legitimately waiting for their scheduled time), and transitions the rest to `queued` for re-publishing.
**Purpose:** Catch notifications that were created but never made it into the stream (e.g., API pod crashed after Redis write but before stream publish).

### Layer 6: Stuck Queued Recovery

**Trigger:** Notifications in `queued` status for more than 2 minutes without being picked up by a consumer.
**Action:** Lua script transitions them back to `pending` and re-publishes to the stream.
**Purpose:** Recover from stream delivery failures, consumer group issues, or prolonged consumer unavailability.

### Layer 7: Stuck Processing Recovery

**Trigger:** Notifications in `processing` status for more than 2 minutes without reaching a terminal state.
**Action:** Lua script transitions them to `queued` and re-publishes to the stream.
**Purpose:** Catch notifications where the consumer completed the provider call but crashed before updating the status.

### Layer 8: Dead Letter Queue

**Trigger:** Max retries exhausted or permanent provider failure.
**Action:** Notification is moved to `dlq:{id}` in Redis and persisted to the `dead_letter_queue` PostgreSQL table. Status is set to `failed`.
**Purpose:** Provide a permanent record of undeliverable notifications for investigation and potential manual reprocessing.

### Layer 9: PostgreSQL Cold Storage

**Trigger:** Every state change (create, status update, retry increment, DLQ move) publishes a persist event to the `persist:queue` Redis Stream.
**Action:** The `notification-dbwriter` service consumes these events and writes them to PostgreSQL in batches.
**Purpose:** Even if Redis data is lost entirely, the PostgreSQL tables contain the full history of every notification. This is the final safety net for data durability.

### Recovery Timing

| Layer | Mechanism                | Interval / Threshold   |
|-------|--------------------------|------------------------|
| 1     | Rate limiter re-enqueue  | 500ms delay            |
| 2     | Circuit breaker re-enqueue | 500ms delay          |
| 3     | Retry scheduler          | Polls every 10s        |
| 4     | XAUTOCLAIM (consumer)    | Every 15s, 30s idle    |
| 5     | Orphaned pending         | Every 30s, 30s threshold |
| 6     | Stuck queued             | Every 30s, 2min threshold |
| 7     | Stuck processing         | Every 30s, 2min threshold |
| 8     | DLQ                      | Immediate on trigger   |
| 9     | PostgreSQL persist       | Continuous stream consumption |

---

## 9. Design Decisions

### Why Redis-based retry scheduling instead of in-memory timers?

In-memory timers (e.g., `time.AfterFunc`) are lost when a pod restarts. With Redis, the `idx:retry` sorted set persists retry schedules across pod restarts and is visible to all consumer instances. Any scheduler pod can pick up due retries regardless of which consumer originally scheduled them.

### Why re-enqueue instead of sleep on rate limit / circuit breaker open?

Sleeping inside the worker goroutine would block it from processing other messages. With 5 workers per pod, sleeping on one means 20% throughput reduction. Re-enqueueing with a short delay frees the worker immediately to process the next message from the stream. The worker adds the notification to a persistent `idx:requeue` ZSET with a 500ms future timestamp. The scheduler polls this ZSET every 2s and republishes ready notifications to the priority streams. This approach is crash-safe — no notification is lost if a worker pod dies mid-requeue.

### Why per-channel circuit breaker instead of a global one?

If the SMS provider is down but the email provider is healthy, a global circuit breaker would block all channels. Per-channel isolation ensures that an SMS outage only affects SMS notifications; email and push continue uninterrupted. The `CircuitBreakerRegistry` lazily creates a breaker per channel on first access.

### Why ACK after all side effects instead of ACK-first?

ACK-first is simpler but creates a window where the message is acknowledged but the side effects (status update, DLQ write, persist event) haven't completed. If the consumer crashes in that window, the message is lost from the stream and the notification is stuck in an intermediate state. ACK-last ensures the message remains in the pending entries list until processing is fully complete, allowing XAUTOCLAIM to recover it.

### Why Compare-And-Swap (CAS) for status transitions?

Without CAS, two consumers could both read a notification as `queued`, both transition it to `processing`, and both attempt delivery, resulting in duplicate sends. The `updateStatusScript` Lua script atomically checks the current status before updating, ensuring exactly-once processing semantics. If the CAS fails, the worker skips the message (another consumer is handling it).

### Why 500ms for re-enqueue delay?

This value is a balance between responsiveness and backpressure:

- **Too low (< 100ms):** Creates a tight retry loop that wastes CPU and Redis operations
- **Too high (> 5s):** Adds noticeable latency to delivery when the rate limit or circuit breaker clears quickly
- **500ms:** Allows ~2 retry attempts per second per notification, which is fast enough for user experience while providing meaningful backpressure

---

## Configurable Parameters Summary

| Parameter                    | Environment Variable          | Default   | Description                                            |
|------------------------------|-------------------------------|-----------|--------------------------------------------------------|
| **Retry: Base Delay**        | `RETRY_BASE_DELAY`            | `2s`      | Initial delay before first retry                       |
| **Retry: Max Delay**         | `RETRY_MAX_DELAY`             | `60s`     | Maximum delay cap (prevents excessively long waits)    |
| **Retry: Max Attempts**      | `RETRY_MAX_ATTEMPTS`          | `5`       | Total attempts before moving to DLQ                    |
| **Circuit Breaker: Failure Threshold** | `CB_FAILURE_THRESHOLD` | `5`    | Consecutive failures to trip the breaker               |
| **Circuit Breaker: Open Duration**     | `CB_OPEN_DURATION`     | `30s`  | Time the breaker stays open before half-open           |
| **Circuit Breaker: Half-Open Max**     | `CB_HALF_OPEN_MAX_REQUESTS` | `1` | Test requests allowed in half-open state               |
| **Rate Limit: Per Second**   | `RATE_LIMIT_PER_SECOND`       | `100`     | Max messages per second per channel                    |
| **Provider: Webhook URL**    | `WEBHOOK_URL`                 | --        | External provider endpoint                             |
| **Provider: Timeout**        | `PROVIDER_TIMEOUT`            | `10s`     | HTTP request timeout per delivery attempt              |
| **Worker: Count**            | `QUEUE_WORKER_COUNT`          | `5`       | Number of concurrent worker goroutines per pod         |
| **Worker: Claim Min Idle**   | `QUEUE_CLAIM_MIN_IDLE`        | `30s`     | XAUTOCLAIM idle threshold for crashed consumers        |
| **Worker: Claim Interval**   | `QUEUE_CLAIM_INTERVAL`        | `15s`     | How often the claimer checks for stale messages        |
| **Scheduler: Poll Interval** | `SCHEDULER_POLL_INTERVAL`     | `5s`      | How often the scheduler checks for due notifications   |
| **Scheduler: Batch Size**    | `SCHEDULER_BATCH_SIZE`        | `500`     | Max notifications claimed per scheduler tick           |
| **Scheduler: Retry Interval**| `SCHEDULER_RETRY_INTERVAL`    | `10s`     | How often the scheduler checks `idx:retry` for due retries |
| **Scheduler: Stuck Threshold** | `SCHEDULER_STUCK_THRESHOLD` | `2m`      | Time before queued/processing notifications are considered stuck |
| **Scheduler: Recovery Interval** | `SCHEDULER_RECOVERY_INTERVAL` | `30s` | How often stuck recovery runs                          |
| **Scheduler: Orphan Threshold** | `SCHEDULER_ORPHAN_THRESHOLD` | `30s`   | Time before pending (non-scheduled) notifications are considered orphaned |
| **Queue: Weight High**       | `QUEUE_WEIGHT_HIGH`           | `10`      | Messages read per poll from the high-priority stream   |
| **Queue: Weight Normal**     | `QUEUE_WEIGHT_NORMAL`         | `5`       | Messages read per poll from the normal-priority stream |
| **Queue: Weight Low**        | `QUEUE_WEIGHT_LOW`            | `2`       | Messages read per poll from the low-priority stream    |

---

## 10. Distributed Tracing

The system propagates a **correlation ID** across all services to enable end-to-end request tracing through the asynchronous pipeline.

### Propagation Flow

```
Client → API (X-Correlation-ID header)
  → middleware sets tracing.CorrelationIDKey in context
    → Publisher embeds correlation_id in Redis Stream message
      → Consumer reads correlation_id from stream message
        → All worker logs include correlation_id field
```

### Implementation

- **Shared context key**: `shared/tracing/context.go` defines a typed context key used by all services — ensures type-safe context propagation without circular imports
- **API middleware**: Validates or generates a correlation ID (max 64 chars, alphanumeric + hyphens), stores in context
- **Publisher**: Extracts correlation ID from context, includes as `correlation_id` field in `XADD` values
- **Consumer**: Parses `correlation_id` from stream message, attaches to structured log output
- **Jaeger**: Available at `http://localhost:16686` for trace visualization (receives OTLP on port 4318)

### Querying Traces

Filter by correlation ID across services using Grafana Loki:
```
{job=~"notification-.*"} |= "abc123-correlation-id"
```

Or use Jaeger UI to search by service name and trace ID.
