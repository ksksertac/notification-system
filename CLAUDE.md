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

## Key Commands Used

```bash
# Claude Code CLI
claude                          # Interactive mode
claude "fix the Redis pipeline bugs"
claude "review all code and update docs"
claude "move migrator from API to dbwriter"
```

## Testing Strategy

- **Unit tests**: domain, handlers (sentinel errors), service, circuit breaker, retry, template (html/template + sync.Map cache)
- **Integration tests**: Redis repository via miniredis (atomic Lua scripts, sorted set indexes, CAS, IncrementRetry)
- **Race condition tests**: concurrent pod claim simulation, concurrent status CAS, idempotency under concurrency
- **E2E tests (12 scenarios)**: real handlers, services, repositories, middleware wired against miniredis — no mocks. Tests: lifecycle, batch, scheduled, idempotency, rate limiting, race condition, cancel, CAS, getByID, getByBatchID, health, validation
- **SonarCloud**: continuous quality gate on every PR (see `sonar-project.properties`)
- **Run**: `go test ./...` per module, `go test -tags=e2e ./tests/e2e/...` for E2E
- **See**: `TESTING.md` for full strategy and commands

## Project Conventions

- Go 1.25, Chi router, go-redis/v9
- All services share code via `shared/` module with `replace` directive
- Redis is the primary data store; PostgreSQL is cold storage only
- Every Redis write publishes to `persist:queue` for async PostgreSQL persistence
- Lua scripts for all atomic operations (CAS, claim, recovery, create, incrementRetry)
- No `.env` files in repo — only `.env.example`
- Structured JSON logging, Prometheus metrics (custom registries) on all services
- Sentinel errors with `errors.Is()` in API service (including `ErrIdempotencyConflict`)
- All background operations use `context.WithTimeout` (no unbounded contexts)
- Stream messages ACK'd only after all side effects complete
- Correlation ID propagated across services via `shared/tracing` context key and Redis Stream messages
- Re-enqueue via persistent `idx:requeue` ZSET (scheduler-driven, crash-safe)
- Deficit round-robin scheduling for priority queues (prevents low-priority starvation)
- Circuit breaker re-enqueue capped at `MaxRequeueCount` (50) to prevent infinite loops
- Provider response bodies capped at 1 MB (`io.LimitReader`)
- Docker multi-stage builds, GitHub Actions CI per service
- Jaeger for distributed tracing (OTLP endpoint on 4317/4318, UI on 16686)
