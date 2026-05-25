# notification-dbwriter

Async persistence service for the Event-Driven Notification System. Drains the Redis `persist:queue` stream to PostgreSQL for cold storage, reporting, and compliance.

## Responsibilities

- Consume events from the `persist:queue` Redis Stream via consumer group
- Batch INSERT notifications and status updates to PostgreSQL
- Replay status transitions (pending -> queued -> delivered/failed) in order
- Persist DLQ entries for audit and analysis
- **Cleanup job**: evict Redis entries older than 1 hour (Hash + all sorted set indexes) to keep RAM bounded
- Auto-migration via Redis leader election (only one pod migrates)

## How It Works

```
persist:queue (Redis Stream)
       |
       v
  Consumer Group (XREADGROUP)
       |
       v
  Batch Collector
  (500 items or 100ms, whichever comes first)
       |
       v
  Coalesce & Deduplicate
  (merge multiple updates for the same notification into one)
       |
       v
  Bulk INSERT / UPDATE via PgBouncer -> PostgreSQL
       |
       v
  XACK (acknowledge processed messages)
```

### Event Types

| Event | Source | PostgreSQL Action |
|-------|--------|-------------------|
| `notification.created` | notification-api | INSERT into `notifications` table |
| `notification.status_changed` | notification-api, consumer, scheduler | UPDATE status + timestamp |
| `notification.dlq` | notification-consumer | INSERT into `dead_letter_queue` table |
| `notification.cancelled` | notification-api | UPDATE status to `cancelled` |

### Batch Coalescing

Events are collected into a batch buffer and flushed under either condition:

- **Size trigger**: 500 events accumulated
- **Time trigger**: 100ms elapsed since first event in the batch

Within each batch, multiple status updates for the same notification ID are coalesced into a single UPDATE reflecting the latest state. This reduces PostgreSQL write amplification under high throughput.

```
Without coalescing:  pending -> queued -> delivered  =  3 UPDATEs
With coalescing:     pending -> delivered             =  1 UPDATE (if all arrive in same batch)
```

## Redis Cleanup — Hot/Cold Eviction

The dbwriter runs a cleanup goroutine every 1 minute that evicts Redis entries older than 1 hour:

```
Every 1 minute:
  ZRANGEBYSCORE idx:created_at -inf <now - 1h> LIMIT 500
  For each expired notification ID:
    DEL notification:{id}              ← remove Hash
    ZREM idx:status:{status} {id}      ← remove from status index
    ZREM idx:channel:{channel} {id}    ← remove from channel index
    ZREM idx:created_at {id}           ← remove from time index
    SREM idx:batch:{batchId} {id}      ← remove from batch set (if any)
    DEL dlq:{id}                       ← remove DLQ entry (if any)
```

This keeps Redis RAM bounded — only the last 1 hour of data stays in Redis. Older data is served by PostgreSQL via the API's tiered read fallback.

```
Without cleanup:  10M notifications × 2KB = 20GB RAM (unbounded growth)
With cleanup:     ~50K notifications × 2KB = ~100MB RAM (bounded to 1h window)
```

## PostgreSQL Connection — PgBouncer

The primary writer to PostgreSQL. The notification-api also connects to PostgreSQL (read-only) for cold data fallback when querying data older than 1 hour. Consumer and scheduler operate exclusively against Redis.

- Connection goes through PgBouncer in transaction pooling mode
- Multiple dbwriter replicas share the PgBouncer pool
- 10 dbwriter pods x 10 connections = 100 connections -> PgBouncer multiplexes to ~30 real DB connections

## Auto-Migration

On startup, dbwriter pods compete for a Redis lock (`migration-leader-lock`). The winner runs database migrations via `golang-migrate`. Other pods wait until the lock is released. No separate init container needed.

This responsibility was moved from notification-api since dbwriter is now the only service with a PostgreSQL connection.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DBWRITER_BATCH_SIZE` | `500` | Max events per batch flush |
| `DBWRITER_FLUSH_INTERVAL` | `100ms` | Max time before batch flush |
| `DBWRITER_CONSUMER_GROUP` | `dbwriter-group` | Redis consumer group name |
| `DBWRITER_CONSUMER_NAME` | `<hostname>` | Unique consumer name within the group |
| `DATABASE_URL` | - | PostgreSQL connection string (via PgBouncer) |
| `REDIS_URL` | - | Redis connection string |

## Scaling — Consumer Groups (Race-to-Claim)

Multiple dbwriter replicas share work via Redis consumer groups (`XREADGROUP`). Each replica gets a unique consumer name and processes a non-overlapping slice of the `persist:queue` stream. Adding replicas increases drain throughput linearly.

No ring hash or partition assignment needed — Redis guarantees each stream message is delivered to exactly one consumer in the group:

```
persist:queue stream: [evt1, evt2, evt3, evt4]

Pod A (XREADGROUP): gets evt1, evt3 → batch INSERT to PostgreSQL
Pod B (XREADGROUP): gets evt2, evt4 → batch INSERT to PostgreSQL

Pod B dies? → evt2, evt4 unacknowledged → XPENDING+XCLAIM → Pod A takes over
```

If a dbwriter pod crashes mid-batch, unacknowledged messages remain in the pending entries list (`XPENDING`) and are claimed by other replicas after the idle threshold.

## Metrics (Prometheus)

| Metric | Type | Description |
|--------|------|-------------|
| `dbwriter_events_persisted_total` | Counter | Events written to PostgreSQL by type |
| `dbwriter_batch_size` | Histogram | Number of events per flush |
| `dbwriter_flush_duration_seconds` | Histogram | Time to flush a batch to PostgreSQL |
| `dbwriter_lag_seconds` | Gauge | Time difference between newest pending event and last flushed event |
| `dbwriter_coalesced_total` | Counter | Events merged by batch coalescing |

## Run

```bash
go run .
```

## Build

```bash
docker build -t notification-dbwriter -f Dockerfile ..
```

## Environment Variables

See [.env.example](.env.example) for all configuration options.
