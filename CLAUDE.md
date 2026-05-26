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
- **Distributed Tracing**: Correlation ID propagation via `shared/tracing` package (API → Redis Streams → Consumer logs), Jaeger for trace visualization.

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

- Go 1.24, Chi router, go-redis/v9
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
- Re-enqueue goroutines bounded by semaphore channel to prevent leak under backpressure
- Provider response bodies capped at 1 MB (`io.LimitReader`)
- Docker multi-stage builds, GitHub Actions CI per service
- Jaeger for distributed tracing (OTLP endpoint on 4317/4318, UI on 16686)
