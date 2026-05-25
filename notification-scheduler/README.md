# notification-scheduler

Scheduled and orphaned notification processor for the Event-Driven Notification System.

## Responsibilities

- Poll Redis sorted sets for notifications ready to be published to Redis Streams
- Process both **scheduled** (`scheduled_at <= NOW()`) and **orphaned** (`pending` + stale) notifications
- Transition notifications: `pending -> queued` in Redis Hash and publish to Redis Stream
- Recover stuck `queued` notifications from crashed pods
- True horizontal scaling via Redis Lua scripts for atomic claim operations

## How It Works

### Scheduler Loop (every 5s)

```
1. Lua script (atomic, server-side):
   - ZRANGEBYSCORE schedule:pending -inf <now> LIMIT 0 500
   - For each ID:
     - HGET notification:{id} status        <- verify still 'pending'
     - HSET notification:{id} status queued  <- claim (update status)
     - ZREM schedule:pending {id}            <- remove from schedule
   - Returns list of claimed IDs
2. Go pipeline (index updates for claimed IDs):
   - ZREM idx:status:pending {id}           <- remove from pending index
   - ZADD idx:status:queued <score> {id}    <- add to queued index
   - XADD persist:queue                     <- async PostgreSQL persistence
3. Redis Pipeline XADD (batch publish to priority streams)
```

### Recovery Loop (every 30s)

Three recovery mechanisms run in parallel to catch every failure mode:

**1. Stuck Queued Recovery (> 2min)**
```
ZRANGEBYSCORE idx:status:queued -inf <now - 2min>
→ Lua script resets status to 'pending'
→ Go code re-publishes to priority stream immediately (no waiting for next scheduler cycle)
```
Catches: pod crashed after claim but before stream publish.

**2. Stuck Processing Recovery (> 2min)**
```
ZRANGEBYSCORE idx:status:processing -inf <now - 2min>
→ Lua script resets status to 'queued'
→ Go code re-publishes to priority stream immediately
```
Catches: consumer pod crashed mid-delivery, message ACK'd but status never updated.

**3. Orphaned Pending Recovery (> 30s, instant only)**
```
ZRANGEBYSCORE idx:status:pending -inf <now - 30s>
→ Lua script checks notification is NOT in schedule:pending (skip scheduled ones)
→ Resets status to 'queued' + publishes to stream
```
Catches: API wrote notification to Redis but optimistic stream publish failed.

### Retry Recovery Loop (every 10s)

```
ZRANGEBYSCORE idx:retry -inf <now>
→ Transitions failed → queued, removes from idx:retry
→ Publishes to priority stream for re-delivery
```
Catches: notifications that failed delivery and are waiting for their exponential backoff delay to expire. The consumer writes `next_retry_at` to `idx:retry` sorted set (score = UnixNano), and the scheduler picks them up when the delay has passed.

## Scaling — Race-to-Claim (No Ring Hash)

Unlike ring hash (Kafka-style partition assignment) or leader-election (`SETNX`), this scheduler uses a **race-to-claim** pattern. All pods compete for the same sorted set — whoever grabs an item first via atomic Lua script wins:

```
schedule:pending: [n1, n2, n3, n4, n5, n6, n7, n8, n9]

Pod A: ZRANGEBYSCORE + Lua ZREM → n1, n2, n3 (atomic, gone from set)
Pod B: ZRANGEBYSCORE + Lua ZREM → n4, n5, n6 (atomic, gone from set)
Pod C: ZRANGEBYSCORE + Lua ZREM → n7, n8, n9 (atomic, gone from set)
```

**Why no ring hash?** Ring hash requires coordination (Zookeeper/etcd), rebalancing when pods join/leave, and partition ownership tracking. With race-to-claim, Redis single-threaded Lua execution guarantees atomicity — no coordination needed. Pod dies? Others keep working. New pod joins? Starts claiming immediately.

```lua
-- Lua script runs INSIDE Redis, atomically (no other command can interleave)
local status = redis.call('HGET', 'notification:' .. id, 'status')
if status == 'pending' then
    redis.call('HSET', 'notification:' .. id, 'status', 'queued')
    redis.call('ZREM', 'schedule:pending', id)
    return 1   -- claimed
end
return 0       -- another pod already took it
```

| Aspect | Ring Hash | SETNX (leader) | Race-to-Claim (ours) |
|--------|-----------|-----------------|----------------------|
| Parallelism | All pods, assigned partitions | 1 pod works, others idle | All pods, no assignment |
| Pod joins/leaves | Rebalance required | Lock re-election | Nothing changes |
| Coordination | Zookeeper/etcd | Redis lock | None |
| Throughput scaling | Linear | Vertical only | Linear |
| Complexity | High | Low | Low |

**Throughput:** 3 pods x 500/batch = 1500 notifications per cycle. With drain loop, 100K notifications are processed in seconds.

## Pod Crash Safety

| Scenario | State | Recovery | Max delay |
|----------|-------|----------|-----------|
| Crash before Lua script completes | Items remain in `schedule:pending` sorted set | Other pods claim immediately | ~5s |
| Crash after claim, before Redis Stream publish | Stuck as `queued` in Hash + sorted set | Recovery loop resets to `pending` + re-publishes to stream | ~2min |
| Crash after Redis Stream publish | All good, consumer processes | N/A | 0 |
| Consumer delivery failure (retryable) | Status set to `failed`, added to `idx:retry` | Retry recovery loop re-enqueues after backoff delay | 2s–60s |
| Consumer delivery failure (permanent) | Moved to DLQ | No retry — permanent failure | Immediate |

## Batch Publishing (Redis Pipeline)

Instead of individual `XADD` commands (500 round trips), uses Redis Pipeline to send all commands in a single round trip:

```go
pipe := redis.Pipeline()
for _, n := range notifications {
    pipe.XAdd(ctx, ...)  // buffered, not sent yet
}
pipe.Exec(ctx)           // single round trip for all 500
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULER_POLL_INTERVAL` | `5s` | How often to check for ready notifications |
| `SCHEDULER_BATCH_SIZE` | `500` | Notifications to claim per cycle |

See [.env.example](.env.example) for all configuration options.

## Run

```bash
go run .
```

## Build

```bash
docker build -t notification-scheduler -f Dockerfile ..
```
