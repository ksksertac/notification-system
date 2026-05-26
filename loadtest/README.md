# Load Test

Go-based load testing tool for the Notification System.

## Scenarios

| Scenario | Description | Defaults |
|----------|-------------|----------|
| `single` | Individual notification creation | 100K notifications, 50 workers |
| `batch` | Bulk creation via batch endpoint | 1M notifications, 1000/batch, 20 workers |
| `mixed` | Mixed traffic (create 70%, list 15%, get 10%, cancel 5%) | 60s duration, 50 workers |

## Usage

```bash
cd loadtest

# Help
go run . 

# 100K individual notifications
go run . -scenario single

# 1M batch notifications
go run . -scenario batch

# 2 minutes of mixed traffic
go run . -scenario mixed -duration 2m

# Custom settings
go run . -scenario single -total 50000 -concurrency 100 -url http://localhost:8080
go run . -scenario batch -total 500000 -batch-size 500 -concurrency 30
go run . -scenario mixed -duration 5m -concurrency 200
```

## Output

Each scenario prints a detailed report at the end:
- Total duration, success/failure counts
- Req/sec throughput
- Latency: avg, p50, p95, p99
- Rate limit (429) count

## Kubernetes (K3s + KEDA) Load Test

Run load tests against the K3s cluster to observe KEDA autoscaling in real time:

```bash
# 1. Set up K3s cluster + deploy services
./k8s/setup.sh

# 2. Watch pods in a separate terminal
kubectl -n notification get pods -w

# 3. Send 500 notifications via demo script (triggers autoscaling)
./k8s/demo.sh

# 4. Or point the Go load test tool at the K3s API
cd loadtest
go run . -scenario single -total 10000 -url http://localhost:30080

# 5. Watch HPA metrics
kubectl -n notification get hpa -w
```

KEDA autoscaling behavior:
- As Redis Stream lag increases → consumer pods scale from 1 → 10
- As `persist:queue` lag increases → dbwriter pods scale from 1 → 8
- After cooldown (30s), once the queue drains, pods scale back down to 1

## Notes

- Rate limiter defaults to 1000 req/s — increasing concurrency will produce 429 responses (expected behavior)
- Batch endpoint accepts max 1000 notifications per batch
- Docker Compose: `docker compose up -d` | K3s: `./k8s/setup.sh`
