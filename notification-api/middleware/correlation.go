package middleware

import (
	"context"
	"net/http"
	"regexp"

	"github.com/google/uuid"
)

type contextKey string

const CorrelationIDKey contextKey = "correlation_id"

const maxCorrelationIDLen = 64

// correlationIDPattern allows alphanumeric characters and hyphens only.
var correlationIDPattern = regexp.MustCompile(`^[a-zA-Z0-9\-]+$`)

func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if id == "" || len(id) > maxCorrelationIDLen || !correlationIDPattern.MatchString(id) {
			id = uuid.New().String()
		}

		ctx := context.WithValue(r.Context(), CorrelationIDKey, id)
		w.Header().Set("X-Correlation-ID", id)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func GetCorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(CorrelationIDKey).(string); ok {
		return id
	}
	return ""
}
