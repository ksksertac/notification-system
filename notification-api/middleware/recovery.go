package middleware

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/sertacyildirim/notification-system/shared/domain"
)

func Recovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					logger.Error("panic recovered",
						"error", err,
						"stack", string(debug.Stack()),
						"correlation_id", GetCorrelationID(r.Context()),
					)

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)

					json.NewEncoder(w).Encode(domain.APIResponse{
						Success: false,
						Error: &domain.APIError{
							Code:    "INTERNAL_ERROR",
							Message: "an unexpected error occurred",
						},
						CorrelationID: GetCorrelationID(r.Context()),
					})
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}
