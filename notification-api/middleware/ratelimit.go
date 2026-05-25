package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

var apiRateLimitScript = redis.NewScript(`
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

func RateLimit(redisClient *redis.Client, requestsPerSecond int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
			defer cancel()

			key := "ratelimit:api:global"
			now := time.Now().UnixMilli()

			result, err := apiRateLimitScript.Run(ctx, redisClient, []string{key},
				requestsPerSecond,
				1000,
				now,
			).Int64()

			if err != nil {
				next.ServeHTTP(w, r)
				return
			}

			if result == 0 {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				json.NewEncoder(w).Encode(map[string]string{
					"error": fmt.Sprintf("rate limit exceeded (%d req/s), try again later", requestsPerSecond),
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
