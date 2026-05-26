# Testing Strategy

This document covers the testing approach for the notification system, including how to run tests, what we test, and our quality targets.

---

## Test Pyramid

```
         /  E2E  \          <- Full lifecycle tests with Docker containers
        /----------\
       / Integration \      <- Service + Redis + provider mocks
      /----------------\
     /    Unit Tests     \  <- Pure logic, no I/O
    /______________________\
```

- **Unit tests**: Fast, isolated, no external dependencies. Cover domain logic, validation, serialization, and state machines.
- **Integration tests**: Test a single service with real Redis (miniredis or Docker). Cover stream operations, Lua scripts, consumer groups.
- **End-to-end tests**: Wire up real handlers, services, repositories, and middleware against miniredis. Test full notification lifecycle from HTTP request through to Redis state verification. No mocks — real code paths.

---

## How to Run Tests

### Unit Tests

```bash
# Run all unit tests per module (multi-module project)
cd shared && go test ./... && cd ..
cd notification-api && go test ./... && cd ..
cd notification-consumer && go test ./... && cd ..
cd notification-scheduler && go test ./... && cd ..

# Run with race detector
cd shared && go test -race ./... && cd ..

# Run with coverage
cd shared && go test -coverprofile=coverage.out ./... && cd ..
go tool cover -html=coverage.out -o coverage.html

# Run tests for a specific package
cd notification-api && go test ./handler/... && cd ..
cd notification-consumer && go test ./delivery/... && cd ..
cd notification-scheduler && go test ./scheduler/... && cd ..
```

### End-to-End Tests

```bash
# Run e2e tests (requires the e2e build tag)
cd tests/e2e
go test -tags=e2e -v ./...

# Run a specific e2e test
go test -tags=e2e -v -run TestFullNotificationLifecycle ./...

# Run with timeout (e2e tests may take longer)
go test -tags=e2e -v -timeout 5m ./...
```

### Full Integration with Docker

```bash
# Start all services + dependencies for integration testing
docker-compose -f docker-compose.test.yml up --build

# Run tests against Docker environment
REDIS_ADDR=localhost:6379 go test -tags=e2e -v ./tests/e2e/...

# Tear down after tests
docker-compose -f docker-compose.test.yml down -v
```

### Quick Check (CI-style)

```bash
# Unit tests + race detection across all modules
for dir in shared notification-api notification-consumer notification-scheduler; do
  (cd $dir && go test -race -count=1 ./...)
done
```

---

## Race Condition Testing Strategy

Race conditions are a first-class concern in a distributed notification system. We test for them explicitly.

### 1. Concurrent Pod Simulation

Multiple goroutines simulate multiple service instances competing for the same work:

```go
// Simulate 10 scheduler pods all trying to claim the same notification
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        claimed := scheduler.TryClaim(ctx, notificationID)
        // Only one should succeed
    }()
}
wg.Wait()
```

Run with `-race` flag to catch data races:
```bash
go test -race -count=100 -run TestConcurrentClaim ./internal/scheduler/...
```

### 2. Redis Lua Script Atomicity Verification

All critical Redis operations use Lua scripts to guarantee atomicity:

- **Claim script**: `WATCH` + conditional `SET` to prevent double-claim
- **Status transition**: Compare-and-swap (CAS) pattern
- **Rate limiter**: Atomic increment + expire

Tests verify that under concurrent load, invariants hold:
- A notification is claimed by exactly one pod
- Status transitions are monotonic (pending -> delivering -> delivered)
- Rate limit counters never exceed the window

### 3. Status Transition CAS Under Concurrency

Status transitions use a compare-and-swap pattern:

```lua
-- Only transition if current status matches expected
local current = redis.call('GET', key)
if current.status == expected_status then
    redis.call('SET', key, new_status)
    return 1  -- success
end
return 0  -- conflict
```

Tests launch N goroutines all attempting the same transition; exactly 1 must succeed.

### 4. XREADGROUP Consumer Group Guarantees

We verify that:
- Each message is delivered to exactly one consumer in the group
- Pending messages are recovered after consumer crash (XCLAIM)
- No messages are lost during rebalancing

```bash
go test -tags=e2e -v -run TestConsumerGroupRecovery ./tests/e2e/...
```

---

## Coverage Targets & Results

All packages with business logic target **90%+ coverage**:

| Package | Coverage | Test Approach |
|---------|----------|---------------|
| `shared/domain` | 98.4% | Pure unit tests (validation, DTOs, state machine, ValidateTransition) |
| `shared/repository` | 97.7% | miniredis (Redis Lua scripts, CAS, atomic indexes) + sqlmock (PostgreSQL) |
| `notification-api/handler` | 100% | httptest + mock service, sentinel error classification |
| `notification-api/service` | 97.8% | Mock repository + mock publisher, sentinel errors (ErrValidation, ErrNotFound, etc.) |
| `notification-consumer/delivery` | 95.2% | miniredis (rate limiter, CB Redis-backed with 500ms timeouts) + httptest (webhook) |
| `notification-consumer/template` | 92.3% | Pure unit tests (html/template with sync.Map cache) |
| `notification-consumer/worker` | 99.2% | Full mock stack — ack-after-side-effects, CAS validation, reEnqueue WaitGroup tracking |
| `notification-scheduler/scheduler` | 97.4% | Mock repo + mock publisher, configurable thresholds, MetricsRecorder interface |

Generate coverage report:
```bash
cd shared && go test -coverprofile=coverage.out -covermode=atomic ./...
go tool cover -func=coverage.out | grep total
```

---

## What We Test Per Service

### API Service

| Component | What We Test |
|-----------|-------------|
| Handlers | Request parsing, response format, status codes, sentinel error classification (`errors.Is`) |
| Validation | Required fields, channel enum, priority enum, template existence |
| Auth Middleware | API key presence, timing-safe comparison, 401 on invalid/missing key |
| Write Buffer | Batching behavior, flush with 30s context timeout, ordering |
| Rate Limiter | Per-user limits, global limits, sliding window accuracy, burst handling |
| Idempotency | Same key returns same response, expiry after 24h, concurrent same-key |
| WebSocket | Origin validation against allowlist, ping/pong heartbeat, connection limit |
| Correlation ID | Validation (max 64 chars, alphanumeric + hyphens), replacement of invalid IDs |
| Health | Redis + PostgreSQL ping (when available) |
| Metrics | Custom Prometheus registry (no global conflicts), route template labels |

### Consumer Service

| Component | What We Test |
|-----------|-------------|
| Circuit Breaker (in-memory) | Opens after N failures, half-open probe, closes on success, state callbacks, slog logging on state change |
| Circuit Breaker (Redis-backed) | Full lifecycle, shared state across instances, half-open max, fail-open on Redis down, 500ms context timeouts |
| Rate Limiter | Sliding window limits, Redis-backed state, fail-open on Redis down |
| Retry Logic | Exponential backoff timing, max retry count, jitter |
| DLQ | Messages move to DLQ after max retries, permanent failures go to DLQ immediately |
| Webhook Provider | HTTP delivery, timeout handling, non-2xx responses, retryable vs permanent errors |
| Worker Pool | Start/stop lifecycle, ack-after-side-effects, CAS validation, rate limit re-enqueue (WaitGroup tracked), CB open handling |
| Weighted Polling | high:10 / normal:5 / low:2, starvation prevention |
| Stale Recovery | XPENDING + XCLAIM for idle messages |
| Template | html/template rendering with sync.Map cache, XSS safety |

### Scheduler Service

| Component | What We Test |
|-----------|-------------|
| Claim Atomicity | Only one pod claims a notification, Lua script correctness |
| Stuck Queued Recovery | Notifications stuck >threshold as `queued` are reset and re-published to stream |
| Stuck Processing Recovery | Notifications stuck >threshold as `processing` are reset and re-published |
| Orphaned Pending Recovery | Instant notifications stuck >orphanThreshold as `pending` are published to stream |
| Retry Recovery | Failed notifications in `idx:retry` are re-enqueued when backoff delay expires |
| Batch Publishing | Notifications published in correct batch size via Redis Pipeline |
| Configurable Thresholds | `Config` struct with stuckThreshold, recoveryInterval, retryInterval, orphanThreshold |
| MetricsRecorder | RecordClaimed, RecordRecovered, RecordRetryReady interface |
| Start/Stop Lifecycle | Graceful shutdown, all goroutines exit cleanly |
| Error Handling | Recovery errors don't crash the scheduler, publish errors are logged |

### DBWriter Service

| Component | What We Test |
|-----------|-------------|
| Batch Coalescing | Multiple updates to same ID coalesce into one write |
| Cleanup | Two-phase pipeline eviction (phase 1: Exists+HGetAll, phase 2: eviction commands) |
| Consumer Group | Acknowledges only after successful flush (flush returns error), replays on failure |
| Ordering | Final state is correct regardless of event arrival order |
| Error Handling | `readBatch()` logs real errors, `flushUpdates` accepts and uses context |

---

## SonarCloud Integration

SonarCloud will be configured separately to provide:

- Automated code quality gates on every PR
- Coverage tracking and trend analysis
- Code smell and bug detection
- Security hotspot identification
- Duplication detection

Configuration will be added via `sonar-project.properties` at the repo root.

---

## CI Integration (GitHub Actions)

Tests run automatically on every pull request:

```yaml
# .github/workflows/test.yml (summary)
on:
  pull_request:
    branches: [main, develop]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.25'
      - run: go test -race -coverprofile=coverage.out ./...
      - uses: codecov/codecov-action@v4

  e2e-tests:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:7-alpine
        ports: ['6379:6379']
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.25'
      - run: go test -tags=e2e -v -timeout 5m ./tests/e2e/...
        env:
          REDIS_ADDR: localhost:6379

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: golangci/golangci-lint-action@v4
```

PRs are blocked from merging unless all checks pass.

---

## E2E Test Scenarios

The E2E tests (`tests/e2e/notification_flow_test.go`) use **real handlers, services, repositories, and middleware** wired up against miniredis — no mocks. Each test creates a full `testEnv` with a Chi router, real service layer, real Redis repository, and real middleware stack (correlation ID, rate limiting, logging, body size limit).

| # | Scenario | What It Validates |
|---|----------|-------------------|
| 1 | Full notification lifecycle | HTTP POST → Redis create (atomic Lua) → verify pending status → verify all indexes |
| 2 | Batch creation flow | POST /batch creates N notifications, all stored in Redis with batch index |
| 3 | Scheduled notification flow | scheduledAt in future → stored in schedule:pending sorted set with correct score |
| 4 | Idempotency | Same Idempotency-Key returns same notification ID, second request returns existing |
| 5 | Rate limiting | Send > limit requests, verify 429 responses with Retry-After header |
| 6 | Race condition (idempotency) | 50 goroutines with same key simultaneously, all get same ID, exactly 1 creates |
| 7 | Cancel flow | Create → cancel → verify cancelled status → double-cancel returns 409 Conflict |
| 8 | Status transition CAS | 20 concurrent CAS attempts on same notification, exactly 1 succeeds |
| 9 | Get by ID | Create notification, GET /notifications/{id}, verify all fields match |
| 10 | Get by batch ID | Create batch, GET /notifications/batch/{batchId}, verify all batch members returned |
| 11 | Health endpoint | GET /health returns 200 with Redis status |
| 12 | Validation errors | Invalid channel, missing recipient, bad phone format → proper 400 error codes |

### Running Individual Scenarios

```bash
# Run a single scenario
go test -tags=e2e -v -run TestFullNotificationLifecycle ./tests/e2e/...
go test -tags=e2e -v -run TestBatchCreationFlow ./tests/e2e/...
go test -tags=e2e -v -run TestScheduledNotificationFlow ./tests/e2e/...
go test -tags=e2e -v -run TestIdempotency ./tests/e2e/...
go test -tags=e2e -v -run TestRateLimiting ./tests/e2e/...
go test -tags=e2e -v -run TestRaceConditionIdempotency ./tests/e2e/...
go test -tags=e2e -v -run TestCancelFlow ./tests/e2e/...
go test -tags=e2e -v -run TestStatusTransitionCAS ./tests/e2e/...
go test -tags=e2e -v -run TestGetByID ./tests/e2e/...
go test -tags=e2e -v -run TestGetByBatchID ./tests/e2e/...
go test -tags=e2e -v -run TestHealthEndpoint ./tests/e2e/...
go test -tags=e2e -v -run TestValidationErrors ./tests/e2e/...
```

---

## Local Development Tips

1. **Use miniredis for fast iteration**: The e2e tests use miniredis by default, so no Docker needed for development.

2. **Run tests on save**: Use `watchexec` or similar:
   ```bash
   watchexec -e go -- go test -race ./internal/...
   ```

3. **Debug flaky tests**: Run with high count to expose race conditions:
   ```bash
   go test -race -count=100 -run TestSuspectTest ./...
   ```

4. **Profile slow tests**:
   ```bash
   go test -cpuprofile=cpu.prof -memprofile=mem.prof -bench=. ./...
   go tool pprof cpu.prof
   ```
