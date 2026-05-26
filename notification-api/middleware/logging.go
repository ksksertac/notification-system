package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

type HTTPRecorder interface {
	RecordHTTP(method, path, status string, durationSeconds float64)
}

func Logging(logger *slog.Logger, recorder HTTPRecorder) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(rw, r)

			duration := time.Since(start)

			// Use chi's route pattern to avoid high-cardinality path params (UUIDs etc.)
			routePattern := r.URL.Path
			if rctx := chi.RouteContext(r.Context()); rctx != nil {
				if pattern := rctx.RoutePattern(); pattern != "" {
					routePattern = pattern
				}
			}

			logger.Info("request completed",
				"method", r.Method,
				"path", routePattern,
				"status", rw.statusCode,
				"duration_ms", duration.Milliseconds(),
				"correlation_id", GetCorrelationID(r.Context()),
				"remote_addr", r.RemoteAddr,
			)

			if recorder != nil {
				recorder.RecordHTTP(r.Method, routePattern, fmt.Sprintf("%d", rw.statusCode), duration.Seconds())
			}
		})
	}
}
