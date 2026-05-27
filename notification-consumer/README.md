# notification-consumer

Queue worker microservice for the Event-Driven Notification System.

## Responsibilities

- Consume notifications from Redis Streams via weighted polling
- Read notification details from Redis Hash (`notification:{id}`)
- Deliver messages through external provider (webhook.site)
- Rate limiting per channel (sliding window, 100/s default)
- Circuit breaker per channel (5 failures -> open 30s -> half-open probe, Redis-backed distributed state with 500ms context timeouts)
- Exponential backoff with jitter for retries
- Dead Letter Queue for permanently failed notifications (stored as Redis Hash `dlq:{notification_id}`)
- Template rendering with `html/template` (XSS-safe) and `sync.Map` template cache
- **Ack-after-side-effects**: stream messages acknowledged only after all status updates and delivery side effects complete
- **CAS validation**: worker checks `UpdateStatus` return value — if CAS fails (concurrent update), message is acked and skipped
- **Goroutine safety**: `reEnqueue` goroutines tracked by WaitGroup with bounded publish context (5s timeout)
- Stale message recovery via XPENDING + XCLAIM
- Prometheus metrics (custom registry, delivery rate, failure rate, latency, CB/rate limit events)

## Weighted Polling

Prevents priority starvation by reading different amounts per cycle:

| Stream | Messages per cycle |
|--------|-------------------|
| notifications:high | 10 |
| notifications:normal | 5 |
| notifications:low | 2 |

Low-priority messages are never starved -- they always get 2 slots per cycle regardless of high-priority volume.

## Delivery Pipeline

```
Redis Stream -> Worker Pool -> Template Render -> Rate Limiter -> Circuit Breaker -> Provider
                                                                    |
                                                    +---------------+---------------+
                                                    v               v               v
                                               Delivered        Retryable       Permanent
                                               status=delivered  |              status=failed
                                                         status=retrying        -> DLQ
                                                         backoff + re-queue
```

## Status Updates — Redis-First

All status transitions are performed against Redis, not PostgreSQL:

- **Atomic CAS**: Lua script performs compare-and-swap on the `notification:{id}` Hash (e.g. `queued -> delivered`) to prevent race conditions
- **Sorted set updates**: on each status change, the notification is moved between `idx:status:{old}` and `idx:status:{new}` sorted sets
- **Async persistence**: every state change also publishes to the `persist:queue` Redis Stream so the dbwriter service can replicate the update to PostgreSQL

DLQ entries are stored as Redis Hashes (`dlq:{notification_id}`) containing the notification payload, failure reason, and retry history. These are also published to `persist:queue` for long-term storage.

## Safety Net — Multi-Layer Recovery

| # | Scenario | Mechanism | Recovery | Max Delay |
|---|----------|-----------|----------|-----------|
| 1 | Provider temporary failure | Exponential backoff retry | `IncrementRetry` → `idx:retry` sorted set → scheduler re-enqueues when delay expires | 2s–60s |
| 2 | Provider permanent failure | Dead Letter Queue | Moved to DLQ immediately, no retry | Immediate |
| 3 | Max retries exceeded (5 default) | Dead Letter Queue | Moved to DLQ after final attempt | Immediate |
| 4 | Consumer crash mid-processing | XPENDING + XCLAIM | Claimer goroutine claims unacknowledged messages after idle threshold | ~15s |
| 5 | Consumer crash (status stuck as `processing`) | Scheduler recovery loop | Resets to `queued` + re-publishes to stream | ~2min |
| 6 | Rate limited | Re-enqueue with delay | Status reset to `queued`, re-published after 500ms | 500ms |
| 7 | Circuit breaker open | Fast fail | Returns immediately without calling provider, consumer retries on next cycle | Varies |
| 8 | Provider temporarily down (all channels) | Circuit breaker per channel | Opens after 5 failures, half-open probe after 30s, closes on success | 30s |

### Retry Flow (Exponential Backoff)

```
Delivery failure (retryable)
    │
    ▼
IncrementRetry: retry_count++, status=retrying, next_retry_at = now + backoff(retry_count)
    │
    ▼
ZADD idx:retry <next_retry_at_unixnano> <notification_id>
    │
    ▼ (scheduler polls idx:retry every 10s)
    │
ZRANGEBYSCORE idx:retry -inf <now>  →  found!
    │
    ▼
Transition: retrying → queued, ZREM from idx:retry
    │
    ▼
PublishBatch to priority stream  →  consumer picks up again
```

Backoff delays: 2s → 4s → 8s → 16s → 32s → 60s (capped). With jitter to prevent thundering herd.

## Metrics (Prometheus on :9090)

| Metric | Type | Description |
|--------|------|-------------|
| `notifications_delivered_total` | Counter | Successful deliveries by channel |
| `notifications_failed_total` | Counter | Failed deliveries by channel |
| `notification_delivery_duration_seconds` | Histogram | End-to-end delivery latency by channel |
| `rate_limit_hits_total` | Counter | Rate limiter rejections |
| `circuit_breaker_open_total` | Counter | Circuit breaker open events |

## Scaling — Consumer Groups (Race-to-Claim)

Multiple consumer replicas share work automatically via Redis consumer groups (`XREADGROUP`). Each replica gets a unique consumer name. Adding replicas increases throughput linearly with no configuration changes.

No ring hash or partition assignment needed — Redis guarantees each stream message is delivered to exactly one consumer in the group:

```
notifications:high stream: [msg1, msg2, msg3, msg4, msg5, msg6]

Pod A (XREADGROUP): gets msg1, msg3, msg5
Pod B (XREADGROUP): gets msg2, msg4, msg6

Pod B dies? → msg4, msg6 unacknowledged → XPENDING+XCLAIM → Pod A takes over
New Pod C? → starts receiving messages immediately, no rebalance
```

## Kubernetes — KEDA Autoscaling

In the K3s deployment (`./k8s/setup.sh`), KEDA automatically scales consumer replicas based on Redis Stream lag:

| Stream | Lag Threshold | Behavior |
|--------|--------------|----------|
| `notifications:high` | 5 per replica | Aggressive scaling for high-priority messages |
| `notifications:normal` | 10 per replica | Moderate scaling |
| `notifications:low` | 20 per replica | Relaxed scaling |

- **Min replicas**: 1, **Max replicas**: 10
- **Polling interval**: 10s, **Cooldown**: 30s
- KEDA creates an HPA backed by external metrics from Redis Streams consumer group lag

```
Burst traffic → queue lag increases → KEDA scales 1 → 10 pods
  → more consumers drain the queue → lag drops → KEDA scales back to 1
```

## Run

```bash
go run .
```

## Build

```bash
docker build -t notification-consumer -f Dockerfile ..
```

## Environment Variables

See [.env.example](.env.example) for all configuration options.
