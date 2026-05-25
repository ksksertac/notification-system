# notification-consumer

Queue worker microservice for the Event-Driven Notification System.

## Responsibilities

- Consume notifications from Redis Streams via weighted polling
- Read notification details from Redis Hash (`notification:{id}`)
- Deliver messages through external provider (webhook.site)
- Rate limiting per channel (sliding window, 100/s default)
- Circuit breaker per channel (5 failures -> open 30s -> half-open probe)
- Exponential backoff with jitter for retries
- Dead Letter Queue for permanently failed notifications (stored as Redis Hash `dlq:{notification_id}`)
- Template rendering with Go text/template
- Stale message recovery via XPENDING + XCLAIM
- Prometheus metrics (delivery rate, failure rate, latency, CB/rate limit events)

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
                                                         backoff + re-queue     -> DLQ
```

## Status Updates — Redis-First

All status transitions are performed against Redis, not PostgreSQL:

- **Atomic CAS**: Lua script performs compare-and-swap on the `notification:{id}` Hash (e.g. `queued -> delivered`) to prevent race conditions
- **Sorted set updates**: on each status change, the notification is moved between `idx:status:{old}` and `idx:status:{new}` sorted sets
- **Async persistence**: every state change also publishes to the `persist:queue` Redis Stream so the dbwriter service can replicate the update to PostgreSQL

DLQ entries are stored as Redis Hashes (`dlq:{notification_id}`) containing the notification payload, failure reason, and retry history. These are also published to `persist:queue` for long-term storage.

## Crash Recovery

| Scenario | Mechanism | How |
|----------|-----------|-----|
| Consumer crash mid-processing | XPENDING + XCLAIM | Other consumers claim unacknowledged messages after idle threshold |
| Provider temporarily down | Circuit breaker | Opens after 5 failures, probes after 30s |
| Permanent delivery failure | Dead Letter Queue | After max retries, moves to DLQ Hash in Redis |

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
