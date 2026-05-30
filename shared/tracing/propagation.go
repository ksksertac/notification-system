package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type MapCarrier map[string]string

func (c MapCarrier) Get(key string) string { return c[key] }
func (c MapCarrier) Set(key, value string) { c[key] = value }
func (c MapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

func InjectTraceContext(ctx context.Context) MapCarrier {
	carrier := make(MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

func ExtractTraceContext(ctx context.Context, carrier MapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func StartLinkedSpan(ctx context.Context, name string, remoteCtx context.Context) (context.Context, trace.Span) {
	remoteSpanCtx := trace.SpanContextFromContext(remoteCtx)
	opts := []trace.SpanStartOption{
		trace.WithLinks(trace.Link{SpanContext: remoteSpanCtx}),
	}
	if remoteSpanCtx.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, remoteSpanCtx)
	}
	return otel.Tracer("").Start(ctx, name, opts...)
}

func SpanContextFromCarrier(carrier MapCarrier) context.Context {
	prop := propagation.TraceContext{}
	return prop.Extract(context.Background(), carrier)
}
