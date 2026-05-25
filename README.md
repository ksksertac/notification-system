# Event-Driven Notification System

Scalable, event-driven notification system built with Go. Processes and delivers messages through SMS, Email, and Push channels with reliable delivery guarantees. Uses a **hybrid Redis-first architecture** with hot/cold tiering — Redis keeps the last 1 hour of data for sub-ms access, while PostgreSQL stores the full history for reporting and analytics.

## Architecture

```
                           ┌──────────────────────────────────────────────────┐
                           │             notification-api                     │
Client ──→ Rate Limiter (1000/s) ──→ Validation ──→ Write Buffer             │
                           │  Auto-migration (Redis leader election)          │
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
              │                                        │   │  Consumer group on           │
              │  notifications:high │ :normal │ :low   │   │  persist:queue stream        │
              │  (priority delivery streams)           │   │        ↓                     │
              └────────────────────────────────────────┘   │  Batch INSERT via PgBouncer  │
                                  ↓                        │        ↓                     │
                                  ↓                        │  PostgreSQL (cold storage)   │
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
│  Poll Redis schedule:pending (5s) ──→ ZRANGEBYSCORE ──→ PublishBatch         │
│  Each pod claims different items via atomic ZPOPMIN — no locks               │
│  Recovery loop (30s) ──→ stuck 'queued' in idx:status → reset to 'pending'   │
└───────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────────┐
│                          Observability Stack                                  │
│                                                                              │
│  Prometheus ──→ Grafana Dashboards (metrics, queue depth, latency)           │
│  Promtail ──→ Loki ──→ Grafana Logs (structured JSON, correlation ID)       │
│  Alertmanager ──→ Slack/Teams (critical alerts: failure rate, CB, downtime)  │
└───────────────────────────────────────────────────────────────────────────────┘
```

## Services

| Service | Description | Scaling | README |
|---------|-------------|---------|--------|
| [notification-api](notification-api/) | REST API, WebSocket, Swagger, auto-migration | Horizontal (stateless) | [Details](notification-api/README.md) |
| [notification-consumer](notification-consumer/) | Queue workers, delivery, retry, circuit breaker | Horizontal (consumer groups) | [Details](notification-consumer/README.md) |
| [notification-scheduler](notification-scheduler/) | Scheduled + orphaned notification processing | Horizontal (atomic ZPOPMIN) | [Details](notification-scheduler/README.md) |
| [notification-dbwriter](notification-dbwriter/) | Async Redis-to-PostgreSQL persistence | Horizontal (consumer groups) | [Details](notification-dbwriter/README.md) |
| [shared](shared/) | Domain models, config, Redis, queue, repository | Library (not deployed) | - |

## Quick Start

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
| http://localhost:9091 | Prometheus |
| http://localhost:9093 | Alertmanager |

## Data Consistency & Reliability

The system uses an **optimistic publish pattern** (outbox-like) to guarantee zero message loss, even during pod crashes or Redis downtime. Redis is the primary data store — all hot-path reads and writes go through Redis. PostgreSQL serves as cold storage for reporting and analytics, fed asynchronously via the `persist:queue` stream. A **tiered read** pattern ensures the API reads from Redis for recent data (last 1 hour) and falls back to PostgreSQL for older data.

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
| API pod killed before Redis publish | Notification stays `pending` in Redis | Scheduler picks up `pending` | ~30s |
| Scheduler pod killed after status update, before Redis publish | Notification stuck as `queued` in Redis | Recovery loop resets to `pending` | ~2min |
| Consumer pod killed mid-processing | Redis Stream message unacknowledged | XPENDING + XCLAIM recovery | ~30s |
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

- Requests with `Idempotency-Key` check `idx:idempotency:{key}` (String with 24h TTL) for duplicate detection
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
| Language | Go 1.23+ |
| HTTP | Chi router |
| Primary Data Store | Redis (Hashes, Sorted Sets, Streams, Sets) |
| Cold Storage | PostgreSQL 16 + PgBouncer (dbwriter only) |
| Queue | Redis Streams |
| Rate Limit | Redis Lua sliding window (inbound global + outbound per-channel) |
| Migrations | golang-migrate (auto, leader election) |
| API Docs | Swagger (swaggo) |
| Metrics | Prometheus + Grafana |
| Logging | Structured JSON + Loki + Promtail |
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
- **3-Layer Self-Healing**: optimistic publish keeps `pending` on failure (instant) → scheduler claims orphaned (30s) → recovery resets stuck `queued` (2min)
- **Global Rate Limiting**: Redis sliding window (1000 req/s shared across all pods) protects the system from traffic bursts
- **Weighted Polling**: prevents priority starvation (high:10, normal:5, low:2)
- **Circuit Breaker**: per channel, 5 failures -> open 30s -> half-open probe
- **Exponential Backoff + Jitter**: prevents thundering herd on provider recovery
- **Auto-Migration**: Redis SETNX leader election, no init container needed
- **Cursor-Based Pagination**: consistent performance regardless of offset depth

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
│   └── repository/              # Notification repository (Redis-backed)
│
├── notification-api/            # API microservice
│   ├── Dockerfile
│   ├── handler/                 # HTTP handlers + Prometheus metrics
│   ├── middleware/              # Correlation ID, logging, recovery, rate limit
│   ├── migrator/                # Auto-migration (leader election)
│   ├── service/                 # Business logic, write buffer, optimistic publish
│   ├── websocket/               # Real-time updates
│   └── migrations/              # SQL migrations (used by dbwriter)
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
│   └── scheduler/               # ZPOPMIN on schedule:pending + recovery loop
│
├── notification-dbwriter/       # Async persistence microservice
│   ├── Dockerfile
│   └── writer/                  # persist:queue consumer, batch INSERT via PgBouncer
│
├── observability/               # Monitoring & alerting configs
│   ├── prometheus/              # Scrape config + alert rules
│   ├── alertmanager/            # Slack/Teams webhook routing
│   ├── grafana/                 # Dashboards + datasource provisioning
│   └── promtail/                # Docker log collection pipeline
│
├── .github/workflows/           # Per-service CI/CD (paths filter)
└── docker-compose.yml           # One-command setup (4 images + PgBouncer + observability)
```
