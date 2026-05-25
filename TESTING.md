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
- **End-to-end tests**: Spin up all services, test full notification lifecycle from API request to delivery confirmation.

---

## How to Run Tests

### Unit Tests

```bash
# Run all unit tests across all services
go test ./...

# Run with race detector
go test -race ./...

# Run with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

# Run tests for a specific service
go test ./internal/api/...
go test ./internal/consumer/...
go test ./internal/scheduler/...
go test ./internal/dbwriter/...
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
# Lint + unit tests + race detection
golangci-lint run ./...
go test -race -count=1 ./...
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

## Coverage Targets

| Layer | Target | Rationale |
|-------|--------|-----------|
| Domain logic (models, validation) | 90%+ | Core business rules must be well-tested |
| Business logic (handlers, services) | 80%+ | Critical paths fully covered |
| Infrastructure (Redis, HTTP) | 70%+ | Integration tests cover the gaps |
| Generated code (protobuf, mocks) | Excluded | Not meaningful to test |

Generate coverage report:
```bash
go test -coverprofile=coverage.out -covermode=atomic ./...
go tool cover -func=coverage.out | grep total
```

---

## What We Test Per Service

### API Service

| Component | What We Test |
|-----------|-------------|
| Handlers | Request parsing, response format, status codes, error responses |
| Validation | Required fields, channel enum, priority enum, template existence |
| Write Buffer | Batching behavior, flush on threshold, flush on interval, ordering |
| Rate Limiter | Per-user limits, global limits, sliding window accuracy, burst handling |
| Idempotency | Same key returns same response, expiry after 24h, concurrent same-key |

### Consumer Service

| Component | What We Test |
|-----------|-------------|
| Circuit Breaker | Opens after N failures, half-open probe, closes on success |
| Retry Logic | Exponential backoff timing, max retry count, jitter |
| Backoff | Correct intervals (1s, 2s, 4s, 8s...), cap at max |
| DLQ | Messages move to DLQ after max retries, DLQ format is correct |
| Weighted Polling | Critical > high > normal > low ordering, starvation prevention |

### Scheduler Service

| Component | What We Test |
|-----------|-------------|
| Claim Atomicity | Only one pod claims a notification, Lua script correctness |
| Recovery | Orphaned claims are reclaimed after timeout |
| Batch Publishing | Notifications published in correct batch size, ordering preserved |
| Cron Accuracy | Scheduled notifications fire within acceptable window (< 1s drift) |

### DBWriter Service

| Component | What We Test |
|-----------|-------------|
| Batch Coalescing | Multiple updates to same ID coalesce into one write |
| Cleanup | Old hot-tier data moves to cold after TTL |
| Consumer Group | Acknowledges only after successful write, replays on failure |
| Ordering | Final state is correct regardless of event arrival order |

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
          go-version: '1.24'
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
          go-version: '1.24'
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

The following scenarios are tested in `tests/e2e/notification_flow_test.go`:

| # | Scenario | What It Validates |
|---|----------|-------------------|
| 1 | Full notification lifecycle | API create -> scheduler claim -> consumer deliver -> status=delivered |
| 2 | Batch creation flow | POST /batch creates N notifications, all appear in pending stream |
| 3 | Scheduled notification flow | scheduledAt in future -> wait -> scheduler picks up -> delivers |
| 4 | Retry + DLQ flow | Provider failure -> 3 retries with backoff -> moved to DLQ |
| 5 | Idempotency | Same idempotency key returns same notification ID, no duplicates |
| 6 | Rate limiting | Send > 1000 requests/sec, verify 429 responses are returned |
| 7 | Race condition (idempotency) | 50 goroutines with same key simultaneously, all get same ID |
| 8 | Cancel flow | Create pending -> cancel -> verify cancelled -> double-cancel returns 409 |
| 9 | Tiered read | Create notification -> move to cold storage -> verify cold read works |
| 10 | Status transition CAS | 20 concurrent CAS attempts, exactly 1 succeeds |

### Running Individual Scenarios

```bash
# Run a single scenario
go test -tags=e2e -v -run TestFullNotificationLifecycle ./tests/e2e/...
go test -tags=e2e -v -run TestBatchCreationFlow ./tests/e2e/...
go test -tags=e2e -v -run TestScheduledNotificationFlow ./tests/e2e/...
go test -tags=e2e -v -run TestRetryAndDLQFlow ./tests/e2e/...
go test -tags=e2e -v -run TestIdempotency ./tests/e2e/...
go test -tags=e2e -v -run TestRateLimiting ./tests/e2e/...
go test -tags=e2e -v -run TestRaceConditionIdempotency ./tests/e2e/...
go test -tags=e2e -v -run TestCancelFlow ./tests/e2e/...
go test -tags=e2e -v -run TestTieredRead ./tests/e2e/...
go test -tags=e2e -v -run TestStatusTransitionCAS ./tests/e2e/...
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
