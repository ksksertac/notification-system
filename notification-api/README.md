# notification-api

HTTP API microservice for the Event-Driven Notification System.

## Responsibilities

- REST API for notification CRUD, batch creation, cancellation, listing
- **API Key Authentication** (`X-API-Key` header, timing-safe `subtle.ConstantTimeCompare`)
- Idempotency via `Idempotency-Key` header (Redis key `idx:idempotency:{key}` with 24h TTL)
- Input validation (E.164 phone, email format, push token, content length)
- Cursor-based pagination with filters (status, channel, date range)
- Write buffer for batch coalescing under high load (30s flush timeout)
- Global rate limiting (Redis sliding window, 1000 req/s across all pods)
- **WebSocket** real-time status updates (origin validation, ping/pong heartbeat 30s/60s, max 1000 connections)
- Swagger/OpenAPI documentation
- Prometheus metrics (custom registry, route template labels) + structured JSON logging
- Health check endpoint (Redis + PostgreSQL)
- **Sentinel errors** (`ErrValidation`, `ErrNotFound`, `ErrConflict`, `ErrConcurrentModification`) with `errors.Is()`
- **Correlation ID validation** (max 64 chars, alphanumeric + hyphens)

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/notifications` | Create notification |
| POST | `/api/v1/notifications/batch` | Create batch (up to 1000) |
| GET | `/api/v1/notifications` | List with filters + pagination |
| GET | `/api/v1/notifications/{id}` | Get by ID |
| GET | `/api/v1/notifications/batch/{batchId}` | Get batch |
| PATCH | `/api/v1/notifications/{id}/cancel` | Cancel notification |
| GET | `/health` | Health check (Redis + PostgreSQL) |
| GET | `/metrics` | Prometheus metrics |
| GET | `/ws` | WebSocket status updates |
| GET | `/swagger/*` | Swagger UI |

## Storage Model — Tiered (Hot/Cold)

The API uses a **TieredNotificationRepo** — writes always go to Redis, reads check Redis first and fall back to PostgreSQL for data older than 1 hour.

**Hot tier (Redis — last 1 hour):**
- `notification:{id}` Redis Hash (all notification fields)
- `idx:status:{status}` sorted set (scored by created_at)
- `idx:channel:{channel}` sorted set (scored by created_at)
- `idx:created_at` sorted set (scored by unix timestamp)
- `idx:idempotency:{key}` Redis key with 24h TTL

**Cold tier (PostgreSQL — full history):**
- `notifications` table (read-only fallback for API queries)
- Fed asynchronously by `notification-dbwriter` via `persist:queue`
- Used when Redis doesn't have the data (evicted after 1 hour)

```
GET /api/v1/notifications/abc123
  → Redis HGETALL notification:abc123
    ├─ found (last 1h) → return (sub-ms)
    └─ not found → PostgreSQL SELECT (cold fallback, ~5ms)

GET /api/v1/notifications?start_date=2025-05-20
  → start_date > 1h ago? → PostgreSQL (cold)
  → start_date within 1h? → Redis sorted sets (hot)
```

## Data Consistency — Optimistic Publish

The API uses an **optimistic publish** pattern with Redis as the primary store:

```
1. Save notification to Redis Hash `notification:{id}` (status: pending)
   + update sorted set indexes                          <- source of truth
2. Publish to `persist:queue` stream                    <- async PostgreSQL persistence
3. Try Redis XADD to priority stream (optimistic, best-effort)
   +-- success -> update status to 'queued' in Redis Hash  <- fast path
   +-- fail -> stays 'pending', return 200 OK anyway       <- safe path
4. Client always gets success if Redis write succeeded
```

**Why this matters:**

- The client never sees a failure for a saved notification
- Pod crashes between steps cannot cause data loss -- notification stays `pending` in Redis
- The scheduler picks up any orphaned `pending` notifications within ~30 seconds
- Every write also publishes to the `persist:queue` Redis Stream, which the dbwriter service drains to PostgreSQL for cold storage, reporting, and compliance
- No intermediate `queued` state exists before stream publish, so there is no dangerous window

This is functionally equivalent to the **Transactional Outbox Pattern** -- Redis `pending` status acts as the outbox, and the scheduler acts as the relay. PostgreSQL is populated asynchronously by dbwriter and serves as the long-term archive.

## High Throughput — Write Buffer & Backpressure

Under burst traffic the API protects Redis through layered backpressure:

```
Request -> Rate Limiter (1000/s) -> Write Buffer -> Redis
              |                       |
              v                       v
         excess -> 429         500 single HSETs
         Too Many Requests    -> 1 batched pipeline
```

### Write Buffer (Batch Coalescing)

Single `POST /notifications` calls are collected in an in-memory buffer and flushed as one batched Redis pipeline:

- **Size trigger**: flushes when 500 items accumulate
- **Time trigger**: flushes every 50ms (whichever comes first)
- Handlers wait on a per-request channel and get their result when the batch completes
- Requests with `Idempotency-Key` bypass the buffer (need immediate visibility for duplicate detection)

```
Without buffer:  1000 requests -> 1000 HSET commands -> 1000 Redis round trips
With buffer:     1000 requests -> 2 batched pipelines (500 each) -> 2 Redis round trips
```

### Global Rate Limiter (Redis)

Redis sliding window algorithm (same Lua script as consumer-side rate limiter) at 1000 req/s **shared across all pods**. Unlike per-pod token buckets, this prevents 1000 pods from collectively overwhelming the system. When exceeded, returns `429 Too Many Requests` with `Retry-After: 1` header. If Redis is unreachable, requests pass through (fail-open) to avoid blocking the entire API.

## Metrics (Prometheus)

| Metric | Type | Description |
|--------|------|-------------|
| `queue_depth` | Gauge | Messages in each priority stream |
| `http_requests_total` | Counter | HTTP requests by method, path, status |
| `http_request_duration_seconds` | Histogram | Request latency by method, path |

## Run

```bash
go run .
```

## Build

```bash
docker build -t notification-api -f Dockerfile ..
```

## Scaling

API is fully stateless — any pod handles any request. No shared state, no coordination needed. Redis handles all data consistency via atomic Lua scripts:

- **Writes**: Redis HSET + sorted set index updates (all via pipeline, single round trip)
- **Hot reads**: Redis HGETALL / ZRANGEBYSCORE (sub-ms)
- **Cold reads**: PostgreSQL via PgBouncer (fallback for data older than 1 hour)

Rate limiting uses a global Redis Lua script (sliding window) — works correctly across all pods without per-pod counters.

### Kubernetes — KEDA Autoscaling

In the K3s deployment (`./k8s/setup.sh`), KEDA scales API replicas based on CPU utilization (threshold: 70%). Min 1, max 5 replicas. The API is a producer (writes to Redis Streams), not a consumer — so CPU-based scaling is more appropriate than queue-based scaling.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `API_KEY` | _(empty — auth disabled)_ | API key for `/api/v1/*` routes. When set, all API requests must include `X-API-Key` header |
| `WS_ALLOWED_ORIGINS` | _(empty — all origins)_ | Comma-separated list of allowed WebSocket origins (e.g., `https://example.com,https://app.example.com`) |

See [.env.example](.env.example) for all configuration options.
