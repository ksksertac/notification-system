package middleware

import (
	"context"
	"net/http"
	"regexp"

	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/tracing"
)

const maxCorrelationIDLen = 64

var correlationIDPattern = regexp.MustCompile(`^[a-zA-Z0-9\-]+$`)

func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if id == "" || len(id) > maxCorrelationIDLen || !correlationIDPattern.MatchString(id) {
			id = uuid.New().String()
		}

		ctx := tracing.WithCorrelationID(r.Context(), id)
		w.Header().Set("X-Correlation-ID", id)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func GetCorrelationID(ctx context.Context) string {
	return tracing.GetCorrelationID(ctx)
}
