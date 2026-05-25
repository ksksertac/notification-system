# Development Plan — Event-Driven Notification System

## Assessment Requirements

Build a scalable, event-driven notification system that:
- Supports SMS, Email, Push channels
- Handles high throughput (burst traffic)
- Guarantees reliable delivery with retry/DLQ
- Provides real-time status tracking
- Scales horizontally without coordination

## Architecture Decision: Hybrid Redis-First

### Options Considered

| Approach | Pros | Cons |
|----------|------|------|
| PostgreSQL only | Simple, ACID, familiar | Write bottleneck at scale, ~5ms latency |
| Redis only | Sub-ms latency, simple | No durability, RAM cost grows unbounded |
| **Hybrid Redis + PostgreSQL** | Sub-ms hot path, bounded RAM, durable cold storage | More complex, eventual consistency for cold data |

### Decision: Hybrid with 1-hour hot window

Redis handles all hot-path operations (writes, status updates, lookups for last 1 hour). PostgreSQL stores full history for reporting. A dedicated `notification-dbwriter` service drains events asynchronously.

**Why this works:**
- 99% of reads hit the last 1 hour (sub-ms from Redis)
- RAM stays bounded (~100MB vs 20GB unbounded)
- PostgreSQL is never on the write critical path
- Cold reads (older than 1h) fall back to PostgreSQL (~5ms, acceptable)

## Implementation Plan

### Phase 1: Core Infrastructure
- [x] Shared module: domain models, config, Redis connection, queue abstraction
- [x] Repository interface with Redis implementation (Hashes, Sorted Sets, Lua scripts)
- [x] PostgreSQL repository for cold storage reads
- [x] TieredNotificationRepo (hot Redis → cold PostgreSQL fallback)

### Phase 2: API Service
- [x] REST API with Chi router (CRUD, batch, cancel, list with pagination)
- [x] Input validation (E.164 phone, email format, content length)
- [x] Idempotency via Redis String with 24h TTL
- [x] Write buffer for batch coalescing (500 items / 50ms flush)
- [x] Global rate limiter (Redis Lua sliding window, 1000 req/s)
- [x] Optimistic publish pattern (Redis write → best-effort stream publish)
- [x] WebSocket hub for real-time status updates
- [x] Swagger/OpenAPI documentation
- [x] Prometheus metrics + structured JSON logging

### Phase 3: Consumer Service
- [x] Weighted polling from 3 priority streams (high:10, normal:5, low:2)
- [x] Rate limiter per channel (sliding window, 100/s)
- [x] Circuit breaker per channel (5 failures → open 30s → half-open probe)
- [x] Exponential backoff with jitter for retries
- [x] Dead Letter Queue (Redis Hash + persist to PostgreSQL)
- [x] Template rendering with Go text/template
- [x] Stale message recovery (XPENDING + XCLAIM)
- [x] Status updates via atomic Lua CAS scripts

### Phase 4: Scheduler Service
- [x] Scheduled notification processing (poll every 5s)
- [x] Race-to-claim via Lua scripts (no ring hash, no coordination)
- [x] Recovery loop for stuck 'queued' notifications (every 30s)
- [x] Batch publishing via Redis Pipeline

### Phase 5: DBWriter Service
- [x] Consumer group on `persist:queue` Redis Stream
- [x] Batch INSERT/UPDATE to PostgreSQL via PgBouncer
- [x] Event coalescing (multiple updates → single DB write)
- [x] Redis cleanup job (evict entries older than 1 hour)
- [x] Auto-migration with Redis leader election

### Phase 6: Observability
- [x] Prometheus scrape config for all services
- [x] Grafana dashboards (queue depth, delivery rate, latency)
- [x] Promtail → Loki for structured log aggregation
- [x] Alertmanager rules (failure rate, queue depth, CB open, service down)

### Phase 7: Hardening & Review
- [x] Code review: identified 35 issues across all services
- [x] Fixed critical bugs: createScript atomicity, pipe.Exec error handling, unused Lua params
- [x] Removed dead code: unused functions, stale migrator in API
- [x] Aligned Go versions (1.24) across go.mod, Dockerfiles, CI workflows
- [x] Documentation pass: all READMEs updated to match actual implementation

### Phase 8: Testing & Quality
- [x] Unit tests: domain models, handlers, service layer, circuit breaker, retry
- [x] Integration tests: Redis repository with miniredis (Lua scripts, CAS, indexes)
- [x] Race condition tests: concurrent claims, concurrent status updates, idempotency under concurrency
- [x] E2E tests: full notification lifecycle, batch, scheduled, retry/DLQ, rate limiting
- [x] Worker tests: weighted polling, message processing, stale recovery
- [x] Scheduler tests: claim batch, recovery loop, concurrent pod simulation
- [x] SonarCloud integration for continuous code quality and coverage tracking
- [x] TESTING.md strategy document with run commands and coverage targets

### Coverage Results
| Package | Coverage |
|---------|----------|
| domain | 82.8% |
| repository | 59.8% |
| service | 72.8% |
| worker | 63.0% |
| scheduler | 65.2% |
| template | 92.3% |
| delivery (CB, retry) | 54.6% |

## Scaling Strategy

```
                    No Coordination Needed
                    ┌─────────────────────┐
                    │                     │
   API (N pods) ──→ │  Redis (single)    │ ←── Scheduler (N pods)
                    │                     │         race-to-claim
   Consumer (N) ──→ │  Lua atomicity     │ ←── DBWriter (N pods)
                    │  Consumer groups    │         consumer groups
                    └─────────────────────┘
```

All services scale by adding pods. No ring hash, no Zookeeper, no partition rebalancing.
- **Scheduler**: Lua ZREM is atomic — whoever removes an item first wins
- **Consumer/DBWriter**: Redis XREADGROUP auto-distributes messages
- **API**: Stateless — any pod handles any request

## Trade-offs Accepted

| Trade-off | Why |
|-----------|-----|
| Eventual consistency for cold reads | PostgreSQL is 1-2 seconds behind Redis — acceptable for reporting |
| Redis as SPOF for hot path | Redis Sentinel/Cluster can mitigate; for this scope, single Redis is fine |
| No exactly-once delivery | At-least-once with idempotency is sufficient; exactly-once adds prohibitive complexity |
| Webhook.site as provider | Assessment scope — real providers (Twilio, SendGrid) are plug-and-play replacements |
| Single Redis instance | Assessment scope — production would use Redis Cluster for HA |
