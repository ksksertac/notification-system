# CLAUDE.md — AI-Assisted Development Context

This project was developed using Claude Code (Anthropic's AI coding assistant) as the primary development partner. This file documents the AI-assisted workflow, architectural decisions, and development strategy.

## Development Workflow

```
1. Requirements Analysis    → Claude analyzed the assessment brief
2. Architecture Planning    → Designed hybrid Redis-first system (see PLAN.md)
3. Implementation           → Iterative development with Claude Code
4. Code Review              → AI-assisted review found 35 issues, fixed critical bugs
5. Documentation            → All READMEs generated and maintained with AI
```

## How Claude Was Used

- **Architecture Design**: Evaluated trade-offs between pure PostgreSQL, pure Redis, and hybrid approaches. Chose Redis-first with hot/cold tiering based on latency and throughput requirements.
- **Code Generation**: All Go microservices written with Claude — domain models, Redis Lua scripts, pipeline optimizations, circuit breakers, rate limiters.
- **Code Review**: Two-pass review. First pass identified 35 issues; second pass found and fixed 34 additional issues (Lua atomicity, TOCTOU races, ack ordering, security hardening, bounded contexts).
- **Security Hardening**: API key auth, WebSocket origin validation + heartbeat + connection limits, html/template for XSS safety, correlation ID validation.
- **Documentation**: README files, architecture diagrams, and this plan document maintained throughout development.
- **Refactoring**: Migrator moved from API to dbwriter, dead code removed, go.mod versions aligned, sentinel errors, custom Prometheus registries — all AI-guided.
- **Kubernetes + KEDA**: Local K3s cluster via k3d, KEDA event-driven autoscaling on Redis Stream lag, priority-aware scaling thresholds, one-command setup/demo/teardown scripts.
- **Code Review Fixes**: Atomic idempotency (Lua script), bounded re-enqueue goroutines, provider response body limit (1MB), Prometheus full scrape coverage (all 4 services), API key enabled by default.
- **Distributed Tracing**: Full OpenTelemetry SDK integration (OTLP/HTTP → Jaeger), otelhttp middleware on API, correlation ID propagation via `shared/tracing` package.
- **Critical Bug Fixes (Post-Review)**: Recovery scripts now compare `updated_at` (not `created_at` score) to avoid recovering recently re-queued notifications. `MoveToDLQ` and `GetRetryReady` converted from Redis pipeline to atomic Lua scripts preventing race conditions. `CreateBatch` parallelized with 50 concurrent goroutines (500ms → ~20ms). Worker now checks `UpdateStatusWithDetails` return values. Batch create supports per-notification idempotency keys. List temporary intersection keys now have EXPIRE as safety net.
- **Priority Queue Fairness**: Replaced sequential stream polling with deficit round-robin scheduling to prevent low-priority starvation. Each priority stream accumulates deficit credits proportional to its weight; the stream with highest deficit is served first, ensuring all priorities get throughput.
- **Circuit Breaker Requeue Safety**: Added `RequeueCount` field and `MaxRequeueCount` (50) limit. Notifications re-enqueued due to circuit breaker open state now track requeue attempts; exceeding the limit moves the notification to DLQ instead of infinite re-enqueue loops.
- **WebSocket Swagger Docs**: Added Swagger annotations to `/ws` endpoint for OpenAPI spec completeness.
- **Retrying Status (Y1)**: Separated `retrying` from `failed` in the status model. `failed` now exclusively means permanent failure (DLQ). `retrying` indicates a transient failure with scheduled retry. Lua scripts (`incrementRetryScript`, `getRetryReadyScript`) and status indexes updated accordingly.
- **CB Path Status Reset (Y2)**: Circuit breaker open path now resets status `processing → queued` before re-enqueue, matching the rate-limiter path for consistency.
- **Persistent Requeue (Y3)**: Replaced in-memory goroutine-based `reEnqueue` with persistent `idx:requeue` ZSET. Scheduler polls this ZSET every 2s and republishes ready notifications to streams. Crash-safe: no notification lost if worker dies mid-requeue.
- **Migration Readiness (Y5)**: API startup now waits for the dbwriter's migration lock to be released before accepting traffic, preventing queries against unmigrated tables on fresh deployments.
- **Publish-Before-Queued Race Fix (C2)**: Create flow now transitions `pending→queued` BEFORE publishing to Redis Stream, preventing consumer CAS failures. On publish failure, status reverts to `pending` for scheduler recovery.
- **CB Re-enqueue Backoff (C1)**: Circuit breaker open path now uses exponential backoff (500ms→1s→2s→...→30s cap) based on `RequeueCount`, preventing re-enqueue storms. Rate-limiter path retains fixed 500ms delay.
- **Provider 429 Retry-After (C5)**: Webhook provider now parses `Retry-After` header (seconds or HTTP-date format) on 429 responses. Consumer uses provider-specified delay instead of default exponential backoff when available.
- **OTel Endpoint Fix (C4)**: Fixed double `http://` prefix in docker-compose OTLP endpoint values. `WithEndpoint()` expects `host:port` without scheme.
- **End-to-End Trace Propagation**: W3C Trace Context (`traceparent`/`tracestate`) now propagated through Redis Stream messages and persist events. Consumer extracts trace context from queue messages so Jaeger shows a single trace spanning API→Queue→Consumer→Webhook→DBWriter. Webhook HTTP client wrapped with `otelhttp.NewTransport` for automatic span creation on outbound calls. `shared/tracing/propagation.go` provides `MapCarrier`, `InjectTraceContext`, `ExtractTraceContext` helpers.
- **Observability Stack Enhancements**: Custom Loki config (TSDB v13, 7-day retention, embedded cache, ruler for log-based alerting), enhanced Promtail config (Docker stage, service labels, structured metadata, Promtail metrics), Jaeger datasource in Grafana with Loki↔Jaeger trace-log correlation via `correlation_id`, 7 log-based alert rules (HighErrorRate, CircuitBreakerOpen, DLQ, DB/Redis errors, NoLogs), enriched Log Explorer dashboard with stat panels, top errors table, and correlation ID filter.
- **Crash-Safe Write Buffer**: Buffer `Submit()` now persists each notification to Redis immediately via `repo.Create()` before returning 200 OK. Stream publish (XADD) is still batched for throughput. If pod crashes before stream publish, notification stays `pending` in Redis — scheduler recovers within ~30s. Eliminates the previous in-memory buffer crash-loss window. (Crash-safe yalnızca Redis ayakta ve veri kaybetmediği sürece geçerlidir — Redis persistence olmadan restart'ta HSET'ler kaybolur. Ayrıca scheduler'ın çalışıyor olması gerekir; tüm scheduler pod'lar çökmüşse orphaned pending recovery uygulanmaz.)
- **At-Least-Once Delivery Documentation**: Explicitly documented at-least-once delivery semantics in DELIVERY.md. Clarified that CAS provides exactly-once queue-level processing, not exactly-once end-to-end delivery.
- **Provider Dedup Key**: Webhook provider now includes `notification_id` in outbound JSON payload, enabling provider-side deduplication for at-least-once delivery scenarios.

- **Architectural Hardening (9 fixes)**:
  - (1) `claimScheduledScript` now performs index updates + persist events atomically inside Lua — eliminates crash-window between status change and index update.
  - (2) Consumer rate limiter changed to fail-open (was fail-closed on Redis error). API rate limiter now has in-memory token bucket fallback via `golang.org/x/time/rate` when Redis is down.
  - (3) Batch `UpdateStatus` via pipelined Lua scripts — replaces N individual calls with one pipeline.
  - (4) Idempotency TTL extended from 24h to 7 days. Batch `CreateBatch` now checks idempotency keys before insert. Idempotency hits return `200 OK` + `X-Idempotent-Replayed: true` header (was `201 Created`).
  - (5) Notification hash now gets 48h TTL (`EXPIRE`) set atomically in the create Lua script — prevents unbounded Redis memory growth.
  - (6) Deficit round-robin no longer breaks after first stream returns messages — all streams are served proportionally per poll cycle, preventing low-priority starvation under sustained high traffic.
  - (7) `getRetryReadyScript` and `getRequeueReadyScript` already atomic Lua — confirmed no race. `claimScheduledScript` was the gap (fixed in #1).
  - (8) Retry jitter changed from additive (`rand * base * attempt`) to AWS full jitter (`rand * min(cap, base * 2^attempt)`). Eliminates thundering herd at low attempts.
  - (9) API rate limiter: per-user in-memory fallback limiter added, Redis errors now logged via `slog.Warn`.
- **Scalability & Durability Hardening (7 fixes)**:
  - Redis Cluster hash tag: per-notification keys now use `n:{id}`, `dlq:{id}`, `p:{id}` format — all keys for a single notification land in the same hash slot. Global indexes remain un-tagged (single-shard throughput ceiling, documented as known limitation).
  - Deficit round-robin: empty-read no longer resets deficit to 0 — sets to -1 so accumulated credit is preserved for next poll cycle, ensuring mathematical fairness.
  - Circuit breaker fail-open: Redis CB errors now logged via `slog.Warn` (was silent). Design tradeoff documented: fail-open chosen for availability over strict protection; CB state shared across pods via Redis.
  - Tiered List cold fallback: `List()` with no `StartDate` filter now merges hot (Redis) + cold (PostgreSQL) results when hot returns fewer than requested, with deduplication by ID.
  - persist:queue MAXLEN increased from `~100000` to `~1000000` (10x) — reduces risk of silent event loss when dbwriter lags under sustained load.
  - Migration PgBouncer bypass: `DirectDSN()` method added to config; migrations now connect directly to PostgreSQL (`DB_DIRECT_HOST`) instead of through PgBouncer, avoiding advisory lock issues with transaction-pooling mode.
  - Design rationale documented: `idx:requeue`/`idx:retry` use persistent ZSET (crash-safe, no goroutine leak) instead of in-memory timers; scheduler uses Lua ZREM-claim (sorted sets have no consumer group) while consumer uses XREADGROUP (streams have native consumer groups).

- **Notification Overview Dashboard**: Grafana dashboard with today/week/month delivery counts, failure rates, success rate %, avg delivery time, p50/p95/p99 latency percentiles, channel breakdown (donut chart + bar gauge), rate limit hits, circuit breaker opens, cumulative totals. Filterable by channel (email/sms/push).
- **Redis Cluster Support**: All services migrated from `*redis.Client` to `redis.UniversalClient` interface. New `shared/redis/client.go` factory creates standalone or cluster client based on `REDIS_CLUSTER_ADDRS` env var. Zero code change needed to switch between modes — same Lua scripts, same logic. Global indexes remain single-shard by design (documented limitation with scaling guidance).
- **ACK Safety Hardening (5 fixes)**:
  - Consumer no longer ACKs stream messages when side effects fail. `UpdateStatusWithDetails` error after delivery → message stays unACKed for XCLAIM redelivery (was: ACK anyway, causing stuck `processing` status and eventual duplicate delivery via scheduler recovery).
  - `handleFailure` now returns `bool` — `MoveToDLQ` or `IncrementRetry` failure → not ACKed, message redelivered.
  - CB/rate-limit re-enqueue path: `UpdateStatus`, `UpdateRequeueCount`, and `AddToRequeueSet` failures each abort without ACK (was: fire-and-forget with ACK, causing silent notification loss).
  - `reEnqueue` now returns `error` so callers can gate ACK on success.
  - Test `TestProcessMessage_UpdateStatusWithDetailsError` updated to assert no-ACK on failure.
- **WebSocket Auth**: `/ws` endpoint moved behind `APIKeyAuth` middleware. Supports both `X-API-Key` header and `?api_key=` query parameter (browsers cannot set headers on WebSocket upgrade).
- **API Key Log Leak Fix**: Removed plaintext API key from rate limiter Redis fallback warning log (`ratelimit.go`).
- **K8s Secret Hygiene**: Moved `DB_PASSWORD`, `REDIS_PASSWORD`, `API_KEY` from ConfigMap to K8s Secret across all 4 service manifests. Deployments now mount both `configMapRef` and `secretRef`.

## Key Commands Used

```bash
# Claude Code CLI
claude                          # Interactive mode
claude "fix the Redis pipeline bugs"
claude "review all code and update docs"
claude "move migrator from API to dbwriter"
```

## Testing Strategy

- **Unit tests**: domain, handlers (sentinel errors), service, circuit breaker, retry, template (html/template + sync.Map cache), tracing propagation (MapCarrier, inject/extract round-trip, linked spans), queue message parsing (traceparent/tracestate fields)
- **Integration tests**: Redis repository via miniredis (atomic Lua scripts, sorted set indexes, CAS, IncrementRetry), dbwriter trace carrier extraction
- **Race condition tests**: concurrent pod claim simulation, concurrent status CAS, idempotency under concurrency
- **E2E tests (12 scenarios)**: real handlers, services, repositories, middleware wired against miniredis — no mocks. Tests: lifecycle, batch, scheduled, idempotency, rate limiting, race condition, cancel, CAS, getByID, getByBatchID, health, validation
- **SonarCloud**: continuous quality gate on every PR (see `sonar-project.properties`)
- **Run**: `go test ./...` per module, `go test -tags=e2e ./tests/e2e/...` for E2E
- **See**: `TESTING.md` for full strategy and commands

## Project Conventions

- Go 1.25, Chi router, go-redis/v9
- All services share code via `shared/` module with `replace` directive
- Redis is the primary data store; PostgreSQL is cold storage only (Redis persistence yapılandırılmamış — restart'ta tüm hot data kaybolur; production'da AOF/RDB aktif edilmeli)
- Every Redis write publishes to `persist:queue` for async PostgreSQL persistence
- Lua scripts for all atomic operations (CAS, claim, recovery, create, incrementRetry)
- No `.env` files in repo — only `.env.example` (ancak K8s ConfigMap'lerde ve docker-compose.yml'de DB_PASSWORD, API_KEY gibi credentials plaintext olarak bulunuyor — production'da K8s Secret veya Vault kullanılmalı)
- Structured JSON logging, Prometheus metrics (custom registries) on all services
- Sentinel errors with `errors.Is()` in API service (including `ErrIdempotencyConflict`)
- All background operations use `context.WithTimeout` (no unbounded contexts)
- Stream messages ACK'd only after all side effects complete
- Correlation ID propagated across services via `shared/tracing` context key and Redis Stream messages
- W3C Trace Context (`traceparent`/`tracestate`) propagated through Redis Stream messages and persist events for end-to-end Jaeger traces
- Webhook HTTP client instrumented with `otelhttp.NewTransport` for automatic outbound span creation
- Re-enqueue via persistent `idx:requeue` ZSET (scheduler-driven, crash-safe)
- Deficit round-robin scheduling for priority queues (all streams served proportionally per poll — no starvation)
- Circuit breaker re-enqueue capped at `MaxRequeueCount` (50) to prevent infinite loops
- Provider response bodies capped at 1 MB (`io.LimitReader`)
- Docker multi-stage builds, GitHub Actions CI per service
- Jaeger for distributed tracing (OTLP endpoint on 4317/4318, UI on 16686)
- Redis Cluster support: all services use `redis.UniversalClient` interface — set `REDIS_CLUSTER_ADDRS` for Cluster mode, otherwise `REDIS_ADDR` for standalone. Per-notification keys use `n:{id}`, `dlq:{id}`, `p:{id}` hash tags for slot co-location. Global indexes (`idx:*`) are single-shard by design (documented limitation)
- Migration bypasses PgBouncer via `DB_DIRECT_HOST` to avoid advisory lock issues with transaction-pooling
- Tiered List merges hot+cold results when no date filter is provided (deduplicates by ID)
- persist:queue capped at `~1000000` entries (MAXLEN) — monitor dbwriter lag to prevent silent trim loss
