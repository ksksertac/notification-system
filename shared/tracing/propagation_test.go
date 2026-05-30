package tracing

import (
	"context"
	"sort"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func setupTestTracer() func() {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return func() { tp.Shutdown(context.Background()) }
}

func TestMapCarrier_GetSetKeys(t *testing.T) {
	c := make(MapCarrier)
	c.Set("traceparent", "00-abc-def-01")
	c.Set("tracestate", "vendor=value")

	if got := c.Get("traceparent"); got != "00-abc-def-01" {
		t.Errorf("Get(traceparent) = %q, want %q", got, "00-abc-def-01")
	}
	if got := c.Get("tracestate"); got != "vendor=value" {
		t.Errorf("Get(tracestate) = %q, want %q", got, "vendor=value")
	}
	if got := c.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want empty", got)
	}

	keys := c.Keys()
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "traceparent" || keys[1] != "tracestate" {
		t.Errorf("Keys() = %v, want [traceparent tracestate]", keys)
	}
}

func TestInjectExtractTraceContext_RoundTrip(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	ctx, span := otel.Tracer("test").Start(context.Background(), "test-span")
	defer span.End()

	originalSpanCtx := trace.SpanContextFromContext(ctx)
	if !originalSpanCtx.IsValid() {
		t.Fatal("expected valid span context")
	}

	carrier := InjectTraceContext(ctx)

	if carrier.Get("traceparent") == "" {
		t.Fatal("expected non-empty traceparent after inject")
	}

	extractedCtx := ExtractTraceContext(context.Background(), carrier)
	extractedSpanCtx := trace.SpanContextFromContext(extractedCtx)

	if originalSpanCtx.TraceID() != extractedSpanCtx.TraceID() {
		t.Errorf("TraceID mismatch: original=%s extracted=%s", originalSpanCtx.TraceID(), extractedSpanCtx.TraceID())
	}
}

func TestExtractTraceContext_EmptyCarrier(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	carrier := make(MapCarrier)
	ctx := ExtractTraceContext(context.Background(), carrier)
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		t.Error("expected invalid span context from empty carrier")
	}
}

func TestStartLinkedSpan(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	remoteCtx, remoteSpan := otel.Tracer("remote").Start(context.Background(), "remote-op")
	defer remoteSpan.End()

	ctx, linkedSpan := StartLinkedSpan(context.Background(), "linked-op", remoteCtx)
	defer linkedSpan.End()

	localSpanCtx := trace.SpanContextFromContext(ctx)
	if !localSpanCtx.IsValid() {
		t.Fatal("expected valid linked span context")
	}

	remoteSpanCtx := trace.SpanContextFromContext(remoteCtx)
	if localSpanCtx.TraceID() != remoteSpanCtx.TraceID() {
		t.Errorf("linked span should share trace ID: local=%s remote=%s", localSpanCtx.TraceID(), remoteSpanCtx.TraceID())
	}
}

func TestSpanContextFromCarrier(t *testing.T) {
	cleanup := setupTestTracer()
	defer cleanup()

	ctx, span := otel.Tracer("test").Start(context.Background(), "test-span")
	defer span.End()
	originalTraceID := trace.SpanContextFromContext(ctx).TraceID()

	carrier := InjectTraceContext(ctx)

	extracted := SpanContextFromCarrier(carrier)
	extractedSpanCtx := trace.SpanContextFromContext(extracted)
	if extractedSpanCtx.TraceID() != originalTraceID {
		t.Errorf("TraceID mismatch: original=%s extracted=%s", originalTraceID, extractedSpanCtx.TraceID())
	}
}
