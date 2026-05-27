# Event-Driven Notification System

Scalable, event-driven notification system built with Go. Processes and delivers messages through SMS, Email, and Push channels with reliable delivery guarantees. Uses a **hybrid Redis-first architecture** with hot/cold tiering — Redis keeps the last 1 hour of data for sub-ms access, while PostgreSQL stores the full history for reporting and analytics.

## Architecture

```
                           ┌──────────────────────────────────────────────────┐
                           │             notification-api                     │
Client ──→ Rate Limiter (1000/s) ──→ Validation ──→ Write Buffer             │
                           │              ↓                                  │
                           │  Batch HSET to Redis (500 items)                │
                           │  + Index updates (status, channel, created_at)  │
                           │              ↓                                  │
                           │  Optimistic Publish (XADD to Redis Stream)      │
                           │  ├─ success → status=queued (fast path)         │
                           │  └─ fail → stays pending in Redis               │
                           │  XADD persist:queue (async DB persistence)      │
                           │  WebSocket Hub (real-time updates)               │
                           │  /swagger, /health, /metrics                    │
                           └──────────────────────────────────────────────────┘
                                          ↓                        ↓
              ┌────────────────────────────────────────┐   ┌─────────────────┐
              │       Redis (Primary Data Store)       │   │  persist:queue  │
              │                                        │   │  (Redis Stream) │
              │  notification:{id}  — Hash (all fields)│   └────────┬────────┘
              │  idx:status:{s}     — Sorted Set       │            │
              │  idx:channel:{ch}   — Sorted Set       │            ▼
              │  idx:created_at     — Sorted Set       │   ┌─────────────────────────────┐
              │  idx:batch:{batchId}— Set              │   │  notification-dbwriter       │
              │  idx:idempotency:{k}— String (TTL 24h) │   │  (x N replicas)             │
              │  schedule:pending   — Sorted Set       │   │                             │
              │                                        │   │  Auto-migration (leader lock)│
              │  notifications:high │ :normal │ :low   │   │  Consumer group on           │
              │  (priority delivery streams)           │   │  persist:queue stream        │
              └────────────────────────────────────────┘   │        ↓                     │
                                  ↓                        │  Batch INSERT via PgBouncer  │
                                  ↓                        │  Cleanup (evict >1h entries) │
                                                           │        ↓                     │
                                                           │  PostgreSQL (cold storage)   │
┌───────────────────────────────────────────────────────┐  └─────────────────────────────┘
│              notification-consumer (x N replicas)      │            ↓
│                                                        │  ┌──────────────────────────┐
│  Weighted Polling ──→ Rate Limiter ──→ Circuit Breaker │  │       PgBouncer          │
│  high:10 / normal:5 / low:2  (100/s)   per channel    │  │  Connections → 50 PG     │
│                                    ↓                   │  │  Transaction pooling     │
│                              webhook.site              │  └──────────────────────────┘
│                                    ↓                   │            ↓
│                          Success → delivered           │  ┌──────────────────────────┐
│  Stale recovery (XPENDING+XCLAIM) Failure → retry/DLQ │  │  PostgreSQL (reporting)  │
│  Status updates → Redis Hash + idx:status              │  │  Cold storage, analytics │
└───────────────────────────────────────────────────────┘  └──────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────────┐
│                    notification-scheduler (x N replicas)                      │
│                                                                              │
│  Poll Redis schedule:pending (5s) ──→ Lua claim script ──→ PublishBatch      │
│  Each pod claims different items via atomic Lua ZREM — no locks              │
│  Recovery loop (30s):                                                        │
│    stuck 'queued' (>2min) → reset to 'pending' + re-publish to stream        │
│    stuck 'processing' (>2min) → reset to 'queued' + re-publish to stream     │
│    orphaned 'pending' (>30s, instant) → set to 'queued' + publish to stream  │
│  Retry recovery (10s):                                                       │
│    idx:retry sorted set (score=next_retry_at) → re-enqueue when delay expires│
│  Requeue recovery (2s):                                                      │
│    idx:requeue sorted set → re-publish CB/rate-limit delayed notifications   │
└───────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────────┐
│                          Observability Stack                                  │
│                                                                              │
│  Prometheus ──→ Grafana Dashboards (metrics, queue depth, latency)           │
│  Promtail ──→ Loki ──→ Grafana Logs (structured JSON, correlation ID)       │
│  Alertmanager ──→ Slack/Teams (critical alerts: failure rate, CB, downtime)  │
│  Jaeger ──→ Distributed Tracing (correlation ID propagation across services)│
└───────────────────────────────────────────────────────────────────────────────┘
```

## Security

| Feature | Implementation |
|---------|---------------|
| **API Key Authentication** | `X-API-Key` header with timing-safe comparison (`subtle.ConstantTimeCompare`), protects `/api/v1/*` routes |
| **WebSocket Origin Validation** | Per-request origin check against configurable allowlist (`WS_ALLOWED_ORIGINS`) |
| **WebSocket Heartbeat** | Ping/pong every 30s, 60s read deadline — detects and evicts stale connections |
| **WebSocket Connection Limit** | Max 1000 concurrent connections to prevent resource exhaustion |
| **Rate Limiting** | Global Redis sliding window (1000 req/s), returns `429` with `Retry-After` header |
| **Correlation ID Validation** | Max 64 chars, alphanumeric + hyphens only — prevents header injection |
| **Request Body Limit** | 2 MB max body size middleware |
| **Template Rendering** | `html/template` (not `text/template`) for XSS-safe output |
| **Provider Response Limit** | `io.LimitReader` caps provider response body at 1 MB — prevents memory exhaustion from malicious/broken providers |
| **Persistent Re-enqueue** | CB/rate-limit deferred notifications stored in persistent `idx:requeue` ZSET — scheduler polls every 2s, crash-safe, no goroutine leaks |
| **Requeue Count Limit** | Circuit breaker re-enqueue capped at 50 attempts per notification — moves to DLQ on exceeded limit, preventing infinite re-enqueue loops |
| **Prometheus Isolation** | Custom registry per service — no global metric conflicts between instances |

## Services

| Service | Description | Scaling | README |
|---------|-------------|---------|--------|
| [notification-api](notification-api/) | REST API, WebSocket, Swagger, tiered reads, API key auth | Horizontal (stateless) | [Details](notification-api/README.md) |
| [notification-consumer](notification-consumer/) | Queue workers, delivery, retry, circuit breaker | Horizontal (consumer groups) | [Details](notification-consumer/README.md) |
| [notification-scheduler](notification-scheduler/) | Scheduled + orphaned notification processing, configurable thresholds | Horizontal (Lua race-to-claim) | [Details](notification-scheduler/README.md) |
| [notification-dbwriter](notification-dbwriter/) | Async Redis-to-PostgreSQL persistence, auto-migration, cleanup | Horizontal (consumer groups) | [Details](notification-dbwriter/README.md) |
| [shared](shared/) | Domain models, config, Redis, queue, repository | Library (not deployed) | - |

## Quick Start

### 1. Configure Webhook Provider

The system uses [webhook.site](https://webhook.site) as a mock delivery provider. Get your unique URL:

1. Go to https://webhook.site — you'll get a unique UUID (e.g., `abc123-def456-...`)
2. Copy the `.env.example` to `.env` and set your webhook UUID:

```bash
cp .env.example .env
# Edit .env and set:
# PROVIDER_WEBHOOK_URL=https://webhook.site/YOUR-UUID-HERE
```

All SMS, Email, and Push deliveries will be sent to this URL. You can monitor deliveries in real-time on the webhook.site dashboard.

### 2. Configure Security

```bash
# API key authentication is enabled by default in docker-compose.yml.
# To change the key, edit the API_KEY environment variable:
# API_KEY=changeme-notification-secret                  # Protects /api/v1/* routes

# Optional: restrict WebSocket origins
# WS_ALLOWED_ORIGINS=https://example.com,https://app.example.com
```

All API requests to `/api/v1/*` require the `X-API-Key` header:
```bash
curl -H "X-API-Key: changeme-notification-secret" http://localhost:8080/api/v1/notifications
```

### 3. Start Services

```bash
# Start everything — builds 4 Docker images, runs all services + observability
docker-compose up --build
```

| URL | Description |
|-----|-------------|
| http://localhost:8080 | API |
| http://localhost:8080/swagger/index.html | Swagger UI |
| http://localhost:8080/health | Health check |
| http://localhost:8080/metrics | Prometheus metrics |
| ws://localhost:8080/ws | WebSocket |
| http://localhost:3000 | Grafana (admin/admin) |
| http://localhost:9094 | Prometheus |
| http://localhost:9093 | Alertmanager |
| http://localhost:16686 | Jaeger (distributed tracing) |

### Kubernetes — K3s + KEDA (Event-Driven Autoscaling)

Run the same services inside a real Kubernetes cluster with **KEDA** autoscaling — consumer and dbwriter pods scale automatically based on Redis Stream queue depth.

**Prerequisites:** Docker, k3d, kubectl, helm (`brew install k3d helm`)

```bash
# One-command setup: k3d cluster + KEDA + all services
./k8s/setup.sh

# Watch pods scale in real time
kubectl -n notification get pods -w

# Send 500 notifications to trigger autoscaling
./k8s/demo.sh

# Tear down
./k8s/teardown.sh
```

| Service | KEDA Trigger | Scale Metric | Min → Max |
|---------|-------------|--------------|-----------|
| **notification-consumer** | `redis-streams` | Stream lag on `notifications:high/normal/low` | 1 → 10 |
| **notification-dbwriter** | `redis-streams` | Stream lag on `persist:queue` | 1 → 8 |
| **notification-api** | `cpu` | CPU utilization > 70% | 1 → 5 |
| **notification-scheduler** | `redis` | Sorted set size (`schedule:pending`) > 100 | 1 → 3 |

Consumer scaling is priority-aware — high-priority queue lag triggers more aggressive scaling (threshold: 5) than low-priority (threshold: 20).

```
Load test → API receives burst traffic
  → notifications pile up in Redis Streams
    → KEDA detects stream lag increase
      → consumer pods scale 1 → 10
        → queue drains, lag drops
          → KEDA scales back down to 1 (cooldown: 30s)
```

**Docker Compose vs K3s + KEDA:**

| | Docker Compose | K3s + KEDA |
|---|---|---|
| Environment | Plain Docker | Real Kubernetes (k3s-in-Docker) |
| Scaling | Fixed replicas | Dynamic, event-driven (KEDA) |
| Autoscaler | None | KEDA (redis-streams + cpu triggers) |
| Production-like | No | Yes — same manifests work in cloud K8s |

## Safety Net — Self-Healing Recovery

Every notification state has an automatic recovery path. If any component crashes or a delivery fails, the system self-heals without manual intervention:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     9-Layer Safety Net                              │
│                                                                     │
│  Layer 1: Rate Limit Re-enqueue (500ms)                            │
│    └─ Rate limited? → re-publish to stream after 500ms             │
│                                                                     │
│  Layer 2: Exponential Backoff Retry (2s–60s)                       │
│    └─ Provider failure? → IncrementRetry → idx:retry sorted set    │
│                                                                     │
│  Layer 3: Retry Recovery (every 10s)                               │
│    └─ Scheduler polls idx:retry → re-enqueues when delay expires   │
│                                                                     │
│  Layer 4: XAUTOCLAIM Stale Recovery (every 15s)                    │
│    └─ Consumer claimer claims idle stream messages from dead pods   │
│                                                                     │
│  Layer 5: Orphaned Pending Recovery (every 30s)                    │
│    └─ Instant notifications stuck as 'pending' > 30s → re-publish  │
│                                                                     │
│  Layer 6: Stuck Queued Recovery (every 30s, threshold 2min)        │
│    └─ Notifications stuck as 'queued' → reset + re-publish         │
│                                                                     │
│  Layer 7: Stuck Processing Recovery (every 30s, threshold 2min)    │
│    └─ Notifications stuck as 'processing' → reset + re-publish     │
│                                                                     │
│  Layer 8: Circuit Breaker (per channel, real-time)                 │
│    └─ Provider down? → fast fail, half-open probe after 30s        │
│                                                                     │
│  Layer 9: Dead Letter Queue (immediate)                            │
│    └─ Permanent failure or max retries → DLQ for manual review     │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Consistency & Reliability

The system uses an **optimistic publish pattern** (outbox-like) to guarantee zero message loss, even during pod crashes or Redis downtime. Redis is the primary data store — all hot-path reads and writes go through Redis. PostgreSQL serves as cold storage for reporting and analytics, fed asynchronously via the `persist:queue` stream. A **tiered read** pattern ensures the API reads from Redis for recent data (last 1 hour) and falls back to PostgreSQL for older data.

### Atomic Lua Scripts

All critical Redis operations are performed via Lua scripts to ensure atomicity:

| Operation | What it does atomically |
|-----------|------------------------|
| **Create** | Idempotency key check + HSET + all index updates (status, channel, created_at, batch, idempotency, schedule) + persist event — single atomic operation, prevents duplicate creation under concurrent requests with same idempotency key |
| **UpdateStatus (CAS)** | Read current status + read score from idx:created_at + HSET + ZREM/ZADD status indexes + persist event |
| **IncrementRetry** | HINCRBY retry_count + HSET next fields + ZREM/ZADD status indexes + ZADD idx:retry + persist event |
| **MoveToDLQ** | Update notification status + create DLQ hash entry + ZREM/ZADD status indexes + persist event — all atomic |
| **GetRetryReady** | Scan idx:retry + CAS check (status must be 'retrying') + transition to 'queued' + ZREM from retry/retrying indexes + ZADD queued index + persist event — prevents double-claiming by multiple scheduler pods |
| **Recovery scripts** | Status reset + `updated_at` comparison against cutoff (not just `created_at` score) + ZREM/ZADD index updates + persist event — correctly skips recently re-queued notifications |

This eliminates TOCTOU (time-of-check-time-of-use) race conditions that existed when these operations were split across multiple Redis commands.

### Notification Lifecycle

```
POST /api/v1/notifications
         │
         ▼
 ┌─ Write Buffer collects requests (up to 500 or 50ms)
 │         │
 │         ▼
 │  Batch HSET to Redis (notification:{id} hashes + index updates)
 │         │
 │         ▼
 │  Redis Pipeline XADD (optimistic)
 │   ├─ success → update idx:status to 'queued' (fast path, ~ms)
 │   └─ fail → stays 'pending' in Redis, scheduler picks up within 30s
 │         │
 │         ▼
 │  XADD persist:queue (notification-dbwriter picks up asynchronously)
 │         │
 │         ▼
 │  notification-dbwriter: batch INSERT to PostgreSQL via PgBouncer
 │
 └─ Client always gets 200 OK if Redis write succeeded
```

### Failure Scenarios

| Scenario | What happens | Recovery | Max delay |
|----------|-------------|----------|-----------|
| Redis down | API cannot write — client gets error | Client retries with Idempotency-Key | Immediate |
| API pod killed before Redis publish | Notification stays `pending` in Redis | Scheduler picks up orphaned `pending` | ~30s |
| Scheduler pod killed after claim, before stream publish | Notification stuck as `queued` in Redis | Recovery loop resets to `pending` + re-publishes to stream | ~2min |
| Consumer pod killed mid-processing | Redis Stream message unacknowledged | XPENDING + XCLAIM recovery (claimer goroutine) | ~15s |
| Consumer pod killed mid-processing (status stuck) | Notification stuck as `processing` in Redis | Recovery loop resets to `queued` + re-publishes to stream | ~2min |
| Provider temporary failure | Delivery fails with retryable error, status set to `retrying` | Exponential backoff retry via `idx:retry` sorted set — scheduler re-enqueues when delay expires | 2s–60s |
| Provider permanent failure | Delivery fails with non-retryable error | Moved to DLQ immediately | Immediate |
| Max retries exceeded | All retry attempts exhausted | Moved to DLQ | Immediate |
| persist:queue consumer (dbwriter) down | Notifications remain in Redis (hot store) | dbwriter catches up on restart via consumer group | Minutes |
| PostgreSQL down | Hot path unaffected — API/consumer/scheduler use Redis only | dbwriter retries persist:queue entries on PG recovery | Minutes |
| Redis write fails | Client gets error, nothing persisted | Client retries with Idempotency-Key | Immediate |

### Why Redis-First? (Hot/Cold Tiering)

Redis provides sub-millisecond latency for all hot-path operations (writes, status updates, lookups). PostgreSQL is used only for cold storage, reporting, and analytics — fed asynchronously via the `persist:queue` stream. This eliminates PostgreSQL as a bottleneck on the write path while preserving durable long-term storage.

**Hot/cold tiering with 1-hour window:**

```
┌─────────────────────────────────┐  ┌──────────────────────────────────┐
│  Redis (hot — last 1 hour)      │  │  PostgreSQL (cold — full history) │
│  ~50K notifications, ~100MB RAM │  │  50M+ notifications, disk-based  │
│  sub-ms reads                   │  │  SQL analytics & reporting       │
└──────────────────┬──────────────┘  └──────────────────┬───────────────┘
                   │                                    │
                   └──── Tiered Read: Redis first ──────┘
                         if not found → PostgreSQL
```

- **Writes** always go to Redis (hot store) + `persist:queue` for async PostgreSQL persistence
- **Reads**: API uses a **TieredNotificationRepo** — checks Redis first (sub-ms), falls back to PostgreSQL for data older than 1 hour
- **Cleanup**: `notification-dbwriter` evicts Redis entries older than 1 hour every minute (Hash + all sorted set indexes)
- **RAM control**: Redis only holds the active window, keeping memory bounded regardless of total data volume

## High Throughput & Backpressure

The system handles burst traffic (1M+ notifications) through layered protection:

```
Client (1M req) → Rate Limiter (1000/s) → Write Buffer (batch 500)
                       ↓                         ↓
                 excess → 429             500 HSET → 1 pipeline HSET
                                                 ↓
                                            Redis (hot store)
                                                 ↓
                                  persist:queue → notification-dbwriter
                                                 ↓
                                     PgBouncer (dbwriter only)
                                                 ↓
                                     PostgreSQL (cold storage)
```

### Protection Layers

| Layer | What it does | Where |
|-------|-------------|-------|
| **API Rate Limiter** | Global Redis sliding window (Lua), 1000 req/s across all pods — excess gets `429 Too Many Requests` | Middleware |
| **Write Buffer** | Collects single creates, flushes as batch HSET pipeline every 50ms or 500 items — reduces Redis round trips ~500x | Service |
| **PgBouncer** | Connection pooler — used exclusively by notification-dbwriter to multiplex connections to PostgreSQL (transaction mode) | Infrastructure |
| **Connection Pool** | Per-dbwriter-pod max 25 connections, prevents single pod from exhausting DB | Go runtime |
| **Redis Sorted Set Indexes** | Scheduler and consumer queries use targeted sorted set lookups, O(log N) even on millions of entries | Redis |

### Why PgBouncer?

PgBouncer is shared by `notification-dbwriter` (writes) and `notification-api` (read-only fallback for cold data). Without PgBouncer: multiple pods each opening connections would exhaust PostgreSQL's `max_connections`. With PgBouncer: all connections are multiplexed through a small pool of real PostgreSQL connections in transaction mode. Consumer and scheduler services do not connect to PostgreSQL at all — they only use Redis.

### Write Buffer — Batch Coalescing

Individual `HSET` commands are the biggest Redis bottleneck under high load. The write buffer solves this:

```
Without buffer:  1000 requests → 1000 HSET commands → 1000 Redis round trips
With buffer:     1000 requests → 2 pipeline HSETs (500 each) → 2 Redis round trips
```

- `CreateBatch` pre-loads the Lua script SHA and runs up to 50 concurrent goroutines — 1000-item batch completes in ~20ms instead of ~500ms sequential
- Requests with `Idempotency-Key` check `idx:idempotency:{key}` (String with 24h TTL) for duplicate detection (supported on both single and batch create)
- Buffer flushes on **size threshold** (500 items) or **time threshold** (50ms) — whichever comes first
- Each waiting handler gets its result via a dedicated channel — no polling

## Redis Key Schema

| Key Pattern | Type | Purpose |
|-------------|------|---------|
| `notification:{id}` | Hash | All notification fields (primary data) |
| `idx:status:{status}` | Sorted Set | Index by status (score=created_at, member=ID) |
| `idx:channel:{channel}` | Sorted Set | Index by channel (score=created_at, member=ID) |
| `idx:created_at` | Sorted Set | Global time-ordered index |
| `idx:batch:{batchId}` | Set | Set of notification IDs in a batch |
| `idx:idempotency:{key}` | String (TTL 24h) | Idempotency deduplication |
| `schedule:pending` | Sorted Set | Scheduled notifications (score=scheduled_at) |
| `persist:queue` | Stream | Async persistence feed for dbwriter |
| `notifications:high` | Stream | High-priority delivery queue |
| `notifications:normal` | Stream | Normal-priority delivery queue |
| `idx:retry` | Sorted Set | Retry scheduling index (score=next_retry_at as UnixNano) |
| `idx:requeue` | Sorted Set | Re-enqueue scheduling index for CB/rate-limit deferred notifications |
| `cb:{channel}` | Hash | Redis-backed circuit breaker state per channel |
| `dlq:{notification_id}` | Hash | Dead letter queue entry (failed notification + error details) |
| `notifications:low` | Stream | Low-priority delivery queue |

## Horizontal Scaling — Race-to-Claim (No Ring Hash)

All services scale horizontally without distributed coordination (no Zookeeper, etcd, or ring hash):

```
Traditional (Ring Hash):     Ours (Race-to-Claim):
┌─────────────────────┐     ┌─────────────────────────────────────┐
│ Pod A → partition 0  │     │ Sorted set: [n1, n2, n3, n4, n5]   │
│ Pod B → partition 1  │     │                                     │
│ Pod C → partition 2  │     │ Pod A: Lua ZREM → n1, n2 (atomic)  │
│                      │     │ Pod B: Lua ZREM → n3, n4 (atomic)  │
│ Pod B dies?          │     │ Pod C: Lua ZREM → n5     (atomic)  │
│ → rebalance needed!  │     │                                     │
└─────────────────────┘     │ Pod B dies? → nothing changes       │
                             └─────────────────────────────────────┘
```

**How it works**: Redis is single-threaded — Lua scripts execute atomically. When a pod claims an item (ZREM from sorted set), no other pod can see it. First come, first served. No pre-assignment, no rebalance, no coordination.

| Service | Scaling mechanism | How pods avoid conflicts |
|---------|------------------|------------------------|
| **Scheduler** | `ZRANGEBYSCORE` + Lua `ZREM` on `schedule:pending` | Atomic removal — if ZREM returns 0, another pod took it |
| **Consumer** | Redis `XREADGROUP` consumer groups | Redis auto-distributes — same message never goes to 2 pods |
| **DBWriter** | Redis `XREADGROUP` on `persist:queue` | Same as consumer — consumer group guarantees |
| **API** | Stateless HTTP | No shared state — any pod handles any request |

**KEDA Autoscaling (K3s):** In the Kubernetes deployment (`./k8s/setup.sh`), KEDA watches Redis metrics and automatically adjusts replica counts. Consumer and dbwriter scale on stream lag (redis-streams trigger); scheduler scales on sorted set depth (redis trigger on `schedule:pending`); API scales on CPU utilization. See [Kubernetes — K3s + KEDA](#kubernetes--k3s--keda-event-driven-autoscaling).

### Redis Lua Scripts — Why?

Lua scripts run **inside Redis** (server-side), not in Go. Redis executes the entire script atomically — no other command can interleave:

```lua
-- This runs INSIDE Redis, atomically
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'pending' then
    redis.call('HSET', KEYS[1], 'status', 'queued')  -- no one can read 'pending' here
    redis.call('ZREM', 'schedule:pending', ARGV[1])
    return 1
end
return 0  -- another pod already claimed it
```

Used for: status transitions (CAS), scheduled claim, stuck recovery, rate limiting. Same guarantee as PostgreSQL's `SELECT ... FOR UPDATE`, but at Redis speed.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Go 1.25+ |
| HTTP | Chi router |
| Primary Data Store | Redis (Hashes, Sorted Sets, Streams, Sets) |
| Cold Storage | PostgreSQL 16 + PgBouncer (dbwriter only) |
| Queue | Redis Streams |
| Auth | API Key middleware (timing-safe `subtle.ConstantTimeCompare`) |
| Rate Limit | Redis Lua sliding window (inbound global + outbound per-channel) |
| Template | `html/template` with sync.Map cache (XSS-safe) |
| Migrations | golang-migrate (auto, Redis leader election via dbwriter) |
| API Docs | Swagger (swaggo) |
| Metrics | Prometheus (custom registries) + Grafana |
| Logging | Structured JSON + Loki + Promtail |
| Tracing | OpenTelemetry SDK (OTLP/HTTP exporter) + Jaeger UI — spans + correlation ID propagation |
| Alerting | Alertmanager (Slack/Teams webhook) |
| CI/CD | GitHub Actions (per service) |

## Key Design Decisions

- **Hybrid Redis-First with Hot/Cold Tiering**: Redis keeps last 1 hour of data (hot), PostgreSQL keeps full history (cold). API reads use tiered fallback — Redis first, PostgreSQL for older data. dbwriter evicts Redis entries older than 1 hour to keep RAM bounded
- **Redis Streams as queue**: 3 priority streams (high/normal/low), O(1) enqueue, native consumer groups, built-in crash recovery (XPENDING + XCLAIM)
- **Optimistic Publish**: Redis is source of truth for hot data, stream publish is best-effort — scheduler catches failures
- **Write Buffer**: batch coalescing turns 500 individual HSETs into 1 pipeline HSET (~500x throughput)
- **PgBouncer**: connection multiplexing shared by dbwriter (writes) and API (cold read fallback) to PostgreSQL
- **Race-to-Claim**: scheduler, consumer, dbwriter all scale via atomic Redis operations — no ring hash, no partition assignment, no rebalance. Lua scripts guarantee atomicity
- **Redis Lua Scripts**: server-side atomic operations for CAS (compare-and-swap), scheduled claim, recovery, rate limiting — same guarantee as `SELECT FOR UPDATE` at Redis speed
- **Multi-Layer Safety Net**: 9 recovery mechanisms ensure no notification is lost — from exponential backoff retry (2s) through XAUTOCLAIM (15s) to stuck recovery (2min). Every stuck state has an automatic recovery path that re-publishes to the delivery stream
- **Global Rate Limiting**: Redis sliding window (1000 req/s shared across all pods) protects the system from traffic bursts
- **Deficit Round-Robin Scheduling**: prevents priority starvation with fair weighted scheduling (high:10, normal:5, low:2). Each stream accumulates deficit credits; the highest-deficit stream is served first, ensuring all priorities get throughput proportional to their weight
- **Circuit Breaker**: per channel, 5 failures -> open 30s -> half-open probe. Redis-backed distributed state with 500ms context timeouts. Re-enqueue capped at 50 attempts — exceeding the limit moves to DLQ instead of infinite loops
- **Exponential Backoff + Jitter**: prevents thundering herd on provider recovery
- **Auto-Migration**: Redis SETNX leader election, no init container needed
- **Cursor-Based Pagination**: consistent performance regardless of offset depth
- **API Key Authentication**: timing-safe comparison via `subtle.ConstantTimeCompare`, optional per environment
- **WebSocket Security**: per-request origin validation, ping/pong heartbeat (30s/60s), max 1000 connections
- **Template Caching**: compiled templates cached in `sync.Map`, `html/template` for XSS safety
- **Atomic Idempotency**: Lua `createScript` checks the idempotency key (`GET`) before writing — prevents duplicate notifications under concurrent requests with the same key (eliminates TOCTOU race between `GetByIdempotencyKey` and `Create`)
- **Distributed Tracing**: Full OpenTelemetry SDK integration (OTLP/HTTP exporter → Jaeger). All 4 services initialize `tracing.InitTracer()` at startup. API uses `otelhttp` middleware for automatic span creation. Correlation ID propagated via Redis Stream messages for log-to-trace correlation
- **Persistent Re-enqueue**: Rate-limited and circuit-breaker-deferred notifications are added to a persistent `idx:requeue` ZSET instead of in-memory goroutines. The scheduler polls this ZSET every 2s and republishes ready notifications to streams — crash-safe, no goroutine leaks
- **Provider Response Limit**: `io.LimitReader(resp.Body, 1MB)` prevents memory exhaustion from oversized provider responses
- **Ack-After-Side-Effects**: consumer ACKs stream messages only after all status updates and side effects complete
- **Bounded Contexts**: all background operations use `context.WithTimeout` — no unbounded `context.Background()` calls
- **Sentinel Errors**: `errors.Is()` for error classification instead of string matching

## Alerting Rules

| Alert | Severity | Condition |
|-------|----------|-----------|
| HighFailureRate | critical | failure rate > 0.1/s for 2min |
| QueueDepthHigh | warning | queue depth > 1000 for 5min |
| DeliveryLatencyHigh | warning | p95 latency > 5s for 3min |
| CircuitBreakerOpen | critical | CB opened in last 5min |
| APIHighErrorRate | critical | 5xx rate > 5% for 2min |
| ServiceDown | critical | Prometheus target down > 1min |
| PersistQueueLag | warning | persist:queue consumer lag > 10000 for 5min |

## Project Structure

```
├── shared/                      # Shared Go module
│   ├── config/                  # Environment config
│   ├── redis/                   # Redis connection pool + key helpers
│   ├── domain/                  # Entities, DTOs, validation, state machine
│   ├── queue/                   # Redis Streams publisher/consumer/pipeline
│   ├── repository/              # Notification repository (Redis-backed)
│   └── tracing/                 # OpenTelemetry SDK init, HTTP middleware, correlation ID context
│
├── notification-api/            # API microservice
│   ├── Dockerfile
│   ├── handler/                 # HTTP handlers + Prometheus metrics
│   ├── middleware/              # Correlation ID, logging, recovery, rate limit
│   ├── service/                 # Business logic, write buffer, optimistic publish
│   └── websocket/               # Real-time updates
│
├── notification-consumer/       # Consumer microservice
│   ├── Dockerfile
│   ├── worker/                  # Weighted polling worker pool
│   ├── delivery/                # Provider, rate limiter, circuit breaker
│   ├── metrics/                 # Prometheus recorder
│   └── template/                # Go template engine
│
├── notification-scheduler/      # Scheduler microservice
│   ├── Dockerfile
│   └── scheduler/               # Lua race-to-claim on schedule:pending + recovery loop
│
├── notification-dbwriter/       # Async persistence microservice
│   ├── Dockerfile
│   ├── migrator/                # Auto-migration (Redis leader election)
│   ├── migrations/              # SQL migration files
│   └── writer/                  # persist:queue consumer, batch INSERT, cleanup
│
├── observability/               # Monitoring, alerting, and tracing configs
│   ├── prometheus/              # Scrape config + alert rules (all 4 services)
│   ├── alertmanager/            # Slack/Teams webhook routing
│   ├── grafana/                 # Dashboards + datasource provisioning
│   └── promtail/                # Docker log collection pipeline
│   # Jaeger runs as a Docker container (jaegertracing/all-in-one)
│
├── k8s/                         # Kubernetes (K3s + KEDA) local deployment
│   ├── setup.sh                 # One-command: k3d cluster + KEDA + deploy all
│   ├── teardown.sh              # Cleanup cluster + images
│   ├── demo.sh                  # Load test + watch autoscaling live
│   ├── infra/                   # Redis, PostgreSQL, PgBouncer manifests
│   ├── apps/                    # Service Deployments + ConfigMaps
│   └── autoscaling/             # KEDA ScaledObjects (redis-streams + cpu)
│
├── .github/workflows/           # Per-service CI/CD (paths filter)
└── docker-compose.yml           # One-command setup (4 images + PgBouncer + observability)
```
