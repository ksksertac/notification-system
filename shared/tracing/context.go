package tracing

import "context"

type contextKey string

const CorrelationIDKey contextKey = "correlation_id"

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, CorrelationIDKey, id)
}

func GetCorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(CorrelationIDKey).(string); ok {
		return id
	}
	return ""
}
