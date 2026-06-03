package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"golang.org/x/time/rate"
)

var apiRateLimitScript = goredis.NewScript(`
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

func RateLimit(redisClient goredis.UniversalClient, globalLimit, perUserLimit int) func(http.Handler) http.Handler {
	globalLimiter := rate.NewLimiter(rate.Limit(globalLimit), globalLimit)
	userLimiters := &sync.Map{}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
			defer cancel()

			now := time.Now().UnixMilli()

			if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
				userKey := fmt.Sprintf("ratelimit:api:user:%s", apiKey)
				result, err := apiRateLimitScript.Run(ctx, redisClient, []string{userKey},
					perUserLimit, 1000, now).Int64()
				if err != nil {
					slog.Warn("rate limiter redis error for user key, using in-memory fallback", "error", err)
					limiterVal, _ := userLimiters.LoadOrStore(apiKey, rate.NewLimiter(rate.Limit(perUserLimit), perUserLimit))
					limiter := limiterVal.(*rate.Limiter)
					if !limiter.Allow() {
						rateLimitResponse(w, r, perUserLimit)
						return
					}
				} else if result == 0 {
					rateLimitResponse(w, r, perUserLimit)
					return
				}
			}

			globalKey := "ratelimit:api:global"
			result, err := apiRateLimitScript.Run(ctx, redisClient, []string{globalKey},
				globalLimit, 1000, now).Int64()

			if err != nil {
				slog.Warn("rate limiter redis error, using in-memory fallback", "error", err)
				if !globalLimiter.Allow() {
					rateLimitResponse(w, r, globalLimit)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			if result == 0 {
				rateLimitResponse(w, r, globalLimit)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func rateLimitResponse(w http.ResponseWriter, r *http.Request, limit int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", "1")
	w.WriteHeader(http.StatusTooManyRequests)
	json.NewEncoder(w).Encode(domain.APIResponse{
		Success: false,
		Error: &domain.APIError{
			Code:    "RATE_LIMIT_EXCEEDED",
			Message: fmt.Sprintf("rate limit exceeded (%d req/s), try again later", limit),
		},
		CorrelationID: GetCorrelationID(r.Context()),
	})
}
