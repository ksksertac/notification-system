package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	cbKeyPrefix = "cb:"
)

// RedisCircuitBreaker stores state in Redis so all pods share the same breaker.
// Uses a Redis Hash per channel: `cb:{channel}` with fields: failures, state, last_failure_at, opened_at
type RedisCircuitBreaker struct {
	client       *redis.Client
	key          string
	threshold    int
	openDuration time.Duration
	halfOpenMax  int
}

func NewRedisCircuitBreaker(client *redis.Client, channel string, cfg CircuitBreakerConfig) *RedisCircuitBreaker {
	return &RedisCircuitBreaker{
		client:       client,
		key:          cbKeyPrefix + channel,
		threshold:    cfg.FailureThreshold,
		openDuration: cfg.OpenDuration,
		halfOpenMax:  cfg.HalfOpenMax,
	}
}

var cbAllowScript = redis.NewScript(`
local key = KEYS[1]
local openDurationMs = tonumber(ARGV[1])
local nowMs = tonumber(ARGV[2])
local halfOpenMax = tonumber(ARGV[3])

local state = redis.call('HGET', key, 'state')
if not state or state == 'closed' then
    return 1
end

if state == 'open' then
    local openedAt = tonumber(redis.call('HGET', key, 'opened_at') or '0')
    if (nowMs - openedAt) >= openDurationMs then
        redis.call('HSET', key, 'state', 'half-open')
        redis.call('HSET', key, 'half_open_count', '1')
        return 1
    end
    return 0
end

if state == 'half-open' then
    local count = tonumber(redis.call('HGET', key, 'half_open_count') or '0')
    if count < halfOpenMax then
        redis.call('HINCRBY', key, 'half_open_count', 1)
        return 1
    end
    return 0
end

return 0
`)

func (cb *RedisCircuitBreaker) Allow() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	nowMs := time.Now().UnixMilli()
	openDurationMs := cb.openDuration.Milliseconds()

	result, err := cbAllowScript.Run(ctx, cb.client, []string{cb.key},
		openDurationMs, nowMs, cb.halfOpenMax,
	).Int64()
	if err != nil {
		// On error, allow the request (fail open)
		return true
	}
	return result == 1
}

var cbRecordSuccessScript = redis.NewScript(`
local key = KEYS[1]
local state = redis.call('HGET', key, 'state')
if state == 'half-open' then
    redis.call('HSET', key, 'state', 'closed')
    redis.call('HSET', key, 'failures', '0')
    redis.call('HDEL', key, 'half_open_count')
    return 1
end
if state == 'closed' or not state then
    redis.call('HSET', key, 'failures', '0')
    return 1
end
return 0
`)

func (cb *RedisCircuitBreaker) RecordSuccess() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = cbRecordSuccessScript.Run(ctx, cb.client, []string{cb.key}).Err()
}

var cbRecordFailureScript = redis.NewScript(`
local key = KEYS[1]
local threshold = tonumber(ARGV[1])
local nowMs = tonumber(ARGV[2])

local state = redis.call('HGET', key, 'state')
if not state then
    state = 'closed'
    redis.call('HSET', key, 'state', 'closed')
end

local failures = redis.call('HINCRBY', key, 'failures', 1)
redis.call('HSET', key, 'last_failure_at', tostring(nowMs))

if state == 'closed' then
    if failures >= threshold then
        redis.call('HSET', key, 'state', 'open')
        redis.call('HSET', key, 'opened_at', tostring(nowMs))
    end
elseif state == 'half-open' then
    redis.call('HSET', key, 'state', 'open')
    redis.call('HSET', key, 'opened_at', tostring(nowMs))
    redis.call('HDEL', key, 'half_open_count')
end

return failures
`)

func (cb *RedisCircuitBreaker) RecordFailure() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	nowMs := time.Now().UnixMilli()
	_ = cbRecordFailureScript.Run(ctx, cb.client, []string{cb.key},
		cb.threshold, nowMs,
	).Err()
}

var cbStateScript = redis.NewScript(`
local key = KEYS[1]
local state = redis.call('HGET', key, 'state')
if not state then
    return 'closed'
end
return state
`)

func (cb *RedisCircuitBreaker) State() CircuitState {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	result, err := cbStateScript.Run(ctx, cb.client, []string{cb.key}).Text()
	if err != nil {
		return StateClosed
	}
	switch result {
	case "open":
		return StateOpen
	case "half-open":
		return StateHalfOpen
	default:
		return StateClosed
	}
}

func (cb *RedisCircuitBreaker) String() string {
	return fmt.Sprintf("RedisCircuitBreaker{key=%s, threshold=%d, openDuration=%s}",
		cb.key, cb.threshold, cb.openDuration)
}

// NewRedisCircuitBreakerRegistry creates a CircuitBreakerRegistry that uses
// Redis-backed circuit breakers. If redisClient is nil, the registry falls back
// to in-memory breakers.
func NewRedisCircuitBreakerRegistry(redisClient *redis.Client, cfg CircuitBreakerConfig) *CircuitBreakerRegistry {
	return &CircuitBreakerRegistry{
		breakers:    make(map[string]CircuitBreaker),
		cfg:         cfg,
		redisClient: redisClient,
	}
}
