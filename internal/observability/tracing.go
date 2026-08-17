/*
 * Distributed tracing for WouriFS gRPC calls.
 *
 * Follows the W3C Trace Context standard (traceparent header) and OpenTelemetry
 * semantic conventions. Trace context is propagated through gRPC metadata
 * across the Gateway → Namenode → Datanode chain. Each component creates spans
 * with RPC attributes.
 *
 * Two modes:
 *   - Local only (NewTracer): spans are kept in memory for tests/metrics.
 *   - OTLP export (NewTracerWithOtel): every span is additionally exported to
 *     the configured OTLP/gRPC collector (default localhost:4317) through the
 *     OpenTelemetry SDK. The W3C IDs used locally are the ones the SDK exports,
 *     so local bookkeeping and the collector always agree.
 *
 * Architecture:
 *   - A root span is created by the Gateway for each incoming HTTP request.
 *   - The span context is injected into outgoing gRPC metadata via the
 *     traceparent/tracestate headers.
 *   - Each gRPC server extracts the trace context from incoming metadata
 *     and creates a child span for the RPC handler.
 */
package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// traceparentVersion is the W3C Trace Context version (00).
const traceparentVersion = "00"

// Span represents a single operation within a trace.
type Span struct {
	TraceID    string // 32 hex chars
	SpanID     string // 16 hex chars
	ParentID   string // empty for root spans
	Name       string
	StartTime  time.Time
	Attributes map[string]string
	ended      bool
	endTime    time.Time
	otelSpan   trace.Span // mirror span in the OTel SDK (nil when OTLP is off)
}

// Tracer creates new spans and manages trace context propagation.
type Tracer struct {
	mu     sync.Mutex
	spans  []Span
	rate   float64      // 0.0 to 1.0, fraction of traces to sample
	active int64        // number of in-flight spans
	otel   trace.Tracer // optional OTel tracer; nil = local bookkeeping only
}

// NewTracer creates a tracer. sampleRate of 1.0 samples every trace;
// 0.0 disables tracing entirely.
func NewTracer(sampleRate float64) *Tracer {
	if sampleRate < 0 {
		sampleRate = 0
	}
	if sampleRate > 1 {
		sampleRate = 1
	}
	return &Tracer{
		spans: make([]Span, 0, 1024),
		rate:  sampleRate,
	}
}

// NewTracerWithOtel creates a tracer that additionally exports every span
// through the given OTel TracerProvider (OTLP/gRPC). A nil provider behaves
// like NewTracer.
func NewTracerWithOtel(sampleRate float64, provider trace.TracerProvider) *Tracer {
	t := NewTracer(sampleRate)
	if provider != nil {
		t.otel = provider.Tracer("wourifs", trace.WithInstrumentationVersion("1.0.0"))
	}
	return t
}

// shouldSample returns true if the trace should be captured.
func (t *Tracer) shouldSample() bool {
	if t.rate >= 1.0 {
		return true
	}
	if t.rate <= 0 {
		return false
	}
	b := make([]byte, 4)
	rand.Read(b)
	// Simple probabilistic sampling using the first byte
	return float64(b[0])/256.0 < t.rate
}

// StartSpan creates a new span. If parentCtx contains a trace context,
// this span becomes a child of that span. Otherwise a new trace is started.
func (t *Tracer) StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	if !t.shouldSample() {
		return ctx, nil
	}

	// Extract parent from incoming gRPC metadata if present (W3C traceparent).
	traceID, spanID, parentID := "", "", ""
	var parentSC trace.SpanContext
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if tp := md.Get("traceparent"); len(tp) > 0 {
			if tid, pid, okp := parseTraceParent(tp[0]); okp {
				traceID = tid
				parentID = pid
				parentSC = remoteSpanContext(tid, pid)
			}
		}
	}
	// Fall back to a local parent span context carried in ctx.
	if !parentSC.IsValid() {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			parentSC = sc
			parentID = sc.SpanID().String()
			traceID = sc.TraceID().String()
		}
	}

	span := &Span{
		TraceID:    traceID,
		SpanID:     spanID,
		ParentID:   parentID,
		Name:       name,
		StartTime:  time.Now(),
		Attributes: make(map[string]string),
	}

	if t.otel != nil {
		spanCtx := ctx
		if parentSC.IsValid() {
			if parentSC.IsRemote() {
				spanCtx = trace.ContextWithRemoteSpanContext(ctx, parentSC)
			} else {
				spanCtx = trace.ContextWithSpanContext(ctx, parentSC)
			}
		}
		spanCtx, span.otelSpan = t.otel.Start(spanCtx, name, trace.WithSpanKind(trace.SpanKindServer))
		sc := span.otelSpan.SpanContext()
		span.TraceID = sc.TraceID().String()
		span.SpanID = sc.SpanID().String()
		if span.ParentID == "" && parentSC.IsValid() {
			span.ParentID = parentSC.SpanID().String()
		}
		ctx = spanCtx
	} else {
		if span.TraceID == "" {
			span.TraceID, span.SpanID = newIDs()
		}
	}

	t.mu.Lock()
	t.active++
	t.mu.Unlock()

	// Inject trace context into outgoing metadata
	ctx = metadata.AppendToOutgoingContext(ctx,
		"traceparent", buildTraceParent(span.TraceID, span.SpanID),
	)

	return ctx, span
}

// EndSpan marks a span as complete and records its duration.
func (t *Tracer) EndSpan(span *Span) {
	if span == nil || span.ended {
		return
	}
	span.ended = true
	span.endTime = time.Now()

	if span.otelSpan != nil {
		span.otelSpan.End()
	}

	t.mu.Lock()
	t.spans = append(t.spans, *span)
	t.active--
	t.mu.Unlock()
}

// SetAttribute adds a key-value attribute to the span.
func (s *Span) SetAttribute(key, value string) {
	if s == nil {
		return
	}
	s.Attributes[key] = value
	if s.otelSpan != nil {
		s.otelSpan.SetAttributes(attribute.String(key, value))
	}
}

// Duration returns the elapsed time of the span.
func (s *Span) Duration() time.Duration {
	if s == nil {
		return 0
	}
	if s.ended {
		return s.endTime.Sub(s.StartTime)
	}
	return time.Since(s.StartTime)
}

// Spans returns a snapshot of completed spans.
func (t *Tracer) Spans() []Span {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Span, len(t.spans))
	copy(out, t.spans)
	return out
}

// ActiveSpans returns the count of in-flight spans.
func (t *Tracer) ActiveSpans() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.active
}

// Metrics returns tracing metrics for Prometheus.
func (t *Tracer) Metrics() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return map[string]int64{
		"wourifs_traces_total":  int64(len(t.spans)),
		"wourifs_traces_active": t.active,
	}
}

// UnaryServerInterceptor returns a gRPC interceptor that extracts trace
// context from incoming metadata and creates a span for the RPC call.
func (t *Tracer) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, span := t.StartSpan(ctx, info.FullMethod)
		resp, err := handler(ctx, req)
		if span != nil {
			span.SetAttribute("rpc.method", info.FullMethod)
			if err != nil {
				span.SetAttribute("rpc.error", "true")
			}
		}
		t.EndSpan(span)
		return resp, err
	}
}

// StreamServerInterceptor returns a gRPC interceptor for streaming RPCs.
func (t *Tracer) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, span := t.StartSpan(ss.Context(), info.FullMethod)
		if span != nil {
			span.SetAttribute("rpc.method", info.FullMethod)
			span.SetAttribute("rpc.stream", "true")
		}
		wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
		err := handler(srv, wrapped)
		t.EndSpan(span)
		return err
	}
}

// wrappedStream overrides the context to carry the span.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// --- W3C Trace Context helpers ---

func newIDs() (traceID, spanID string) {
	tid := make([]byte, 16)
	sid := make([]byte, 8)
	rand.Read(tid)
	rand.Read(sid)
	return hex.EncodeToString(tid), hex.EncodeToString(sid)
}

// buildTraceParent returns a W3C traceparent header value.
//
//	version(2)-trace-id(32)-parent-id(16)-trace-flags(2)
func buildTraceParent(traceID, spanID string) string {
	flags := "01" // sampled
	return fmt.Sprintf("%s-%s-%s-%s", traceparentVersion, traceID, spanID, flags)
}

// parseTraceParent extracts traceID and parentID from a traceparent header.
func parseTraceParent(tp string) (traceID, parentID string, ok bool) {
	var version, flags string
	n, err := fmt.Sscanf(tp, "%2s-%32s-%16s-%2s", &version, &traceID, &parentID, &flags)
	if err != nil || n != 4 {
		return "", "", false
	}
	return traceID, parentID, true
}

// remoteSpanContext builds an OTel remote SpanContext from a W3C traceparent
// (version 00, sampled flag set).
func remoteSpanContext(traceIDHex, spanIDHex string) trace.SpanContext {
	tid, err1 := trace.TraceIDFromHex(traceIDHex)
	sid, err2 := trace.SpanIDFromHex(spanIDHex)
	if err1 != nil || err2 != nil {
		return trace.SpanContext{}
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}
