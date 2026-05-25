package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RateLimiter interface {
	Allow(ctx context.Context, channel string) (bool, error)
}

type redisRateLimiter struct {
	client       *redis.Client
	limitPerSec  int
	windowSize   time.Duration
}

func NewRedisRateLimiter(client *redis.Client, limitPerSecond int) RateLimiter {
	return &redisRateLimiter{
		client:      client,
		limitPerSec: limitPerSecond,
		windowSize:  time.Second,
	}
}

var slidingWindowScript = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
    redis.call('PEXPIRE', key, window)
    return 1
end

return 0
`)

func (r *redisRateLimiter) Allow(ctx context.Context, channel string) (bool, error) {
	key := fmt.Sprintf("ratelimit:%s", channel)
	now := time.Now().UnixMilli()

	result, err := slidingWindowScript.Run(ctx, r.client, []string{key},
		r.limitPerSec,
		r.windowSize.Milliseconds(),
		now,
	).Int64()

	if err != nil {
		return false, fmt.Errorf("rate limiter script: %w", err)
	}

	return result == 1, nil
}
