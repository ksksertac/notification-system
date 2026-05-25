package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"
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

			logger.Info("request completed",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.statusCode,
				"duration_ms", duration.Milliseconds(),
				"correlation_id", GetCorrelationID(r.Context()),
				"remote_addr", r.RemoteAddr,
			)

			if recorder != nil {
				recorder.RecordHTTP(r.Method, r.URL.Path, fmt.Sprintf("%d", rw.statusCode), duration.Seconds())
			}
		})
	}
}
