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

### Phase 9: Security & Reliability Hardening (34 fixes)
- [x] **Critical — Lua Atomicity**: `createScript` extended to handle hash + all indexes + persist event atomically (8 KEYS)
- [x] **Critical — TOCTOU Elimination**: `UpdateStatus`/`UpdateStatusWithDetails` Lua scripts read score internally via ZSCORE — no separate GetByID
- [x] **Critical — Atomic IncrementRetry**: New Lua script handles HINCRBY + HSET + ZREM/ZADD + idx:retry + persist event atomically
- [x] **Critical — Ack-After-Side-Effects**: Consumer ACKs moved after all status updates and side effects (was before delivery check)
- [x] **Critical — Goroutine Safety**: `reEnqueue` goroutines tracked by WaitGroup, bounded publish context (5s timeout)
- [x] **Critical — Recovery Atomicity**: All recovery Lua scripts extended to handle ZREM/ZADD and persist events inside Lua
- [x] **Security — API Key Auth**: `X-API-Key` header with `subtle.ConstantTimeCompare`, protects `/api/v1/*` routes
- [x] **Security — WebSocket**: Origin validation against allowlist, ping/pong heartbeat (30s/60s), max 1000 connections
- [x] **Security — Template XSS**: Switched from `text/template` to `html/template` with sync.Map cache
- [x] **Security — Correlation ID**: Validated (max 64 chars, alphanumeric + hyphens only)
- [x] **Medium — Sentinel Errors**: Service uses `ErrValidation`/`ErrNotFound`/`ErrConflict`/`ErrConcurrentModification` with `errors.Is()`
- [x] **Medium — Prometheus Isolation**: Custom `prometheus.NewRegistry()` per service (no global metric conflicts)
- [x] **Medium — Route Label Cardinality**: Logging middleware uses `chi.RouteContext().RoutePattern()` instead of raw URL path
- [x] **Medium — Bounded Contexts**: All background operations use `context.WithTimeout` (write buffer 30s, circuit breaker Redis 500ms)
- [x] **Medium — Stream Capping**: XADD calls use `MaxLen: 100000, Approx: true` to prevent unbounded growth
- [x] **Medium — CAS Validation**: Worker checks `UpdateStatus` return value — if CAS fails, message is acked and skipped
- [x] **Medium — Error Logging**: `mapToNotification` parse errors, `publishPersistEvent` marshal errors now logged
- [x] **Medium — Tiered Read Routing**: `List()` defaults to hot tier when `StartDate` is nil, only routes to cold for older data
- [x] **Low — Health Check**: Now pings both Redis and PostgreSQL (when available)
- [x] **Low — Batch Validation**: Fixed range-by-value bug in DTO batch validation
- [x] **Low — Circuit Breaker Logging**: Dead `fmt.Sprintf` replaced with `slog.Info` for state changes
- [x] **Low — DBWriter Safety**: `flush()` returns error, ACK only on success; `readBatch()` logs real errors
- [x] **Low — DBWriter Cleanup**: Two-phase pipeline for eviction (phase 1: Exists+HGetAll, phase 2: eviction commands)
- [x] **Low — Scheduler Config**: Configurable thresholds via `Config` struct + env vars (stuckThreshold, recoveryInterval, etc.)
- [x] **Low — Domain Validation**: `ValidateTransition` method on Notification for state machine validation

### Coverage Results
| Package | Coverage |
|---------|----------|
| shared/domain | 98.4% |
| shared/repository | 97.7% |
| notification-api/handler | 100% |
| notification-api/service | 97.8% |
| notification-consumer/delivery | 95.2% |
| notification-consumer/template | 92.3% |
| notification-consumer/worker | 99.2% |
| notification-scheduler/scheduler | 97.4% |

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

### Phase 10: Kubernetes — K3s + KEDA Event-Driven Autoscaling
- [x] k3d (k3s-in-Docker) local Kubernetes cluster setup
- [x] Kubernetes manifests for all 4 services + infrastructure (Redis, PostgreSQL, PgBouncer)
- [x] KEDA ScaledObjects: `notification-consumer` scales on Redis Streams lag (`notifications:high/normal/low`)
- [x] KEDA ScaledObjects: `notification-dbwriter` scales on Redis Streams lag (`persist:queue`)
- [x] KEDA ScaledObjects: `notification-api` and `notification-scheduler` scale on CPU utilization
- [x] Priority-aware scaling thresholds: high=5, normal=10, low=20 lag per replica
- [x] One-command setup script (`./k8s/setup.sh`): cluster + KEDA + image build + deploy
- [x] Demo script (`./k8s/demo.sh`): sends 500 notifications and watches pod scaling live
- [x] Teardown script (`./k8s/teardown.sh`): cleanup cluster + Docker images
- [x] Documentation updated: README, PLAN, loadtest README, service READMEs

## Trade-offs Accepted

| Trade-off | Why |
|-----------|-----|
| Eventual consistency for cold reads | PostgreSQL is 1-2 seconds behind Redis — acceptable for reporting |
| Redis as SPOF for hot path | Redis Sentinel/Cluster can mitigate; for this scope, single Redis is fine |
| No exactly-once delivery | At-least-once with idempotency is sufficient; exactly-once adds prohibitive complexity |
| Webhook.site as provider | Assessment scope — real providers (Twilio, SendGrid) are plug-and-play replacements |
| Single Redis instance | Assessment scope — production would use Redis Cluster for HA |
| k3d for local K8s | Lightweight alternative to minikube/kind — k3s runs in Docker containers, same K8s API |
| KEDA vs native HPA | KEDA enables event-driven scaling from Redis Streams; native HPA only supports cpu/memory |
