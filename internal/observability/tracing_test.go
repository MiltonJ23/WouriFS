package observability

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestTracer_StartAndEndSpan(t *testing.T) {
	tr := NewTracer(1.0)
	_, span := tr.StartSpan(context.Background(), "TestOp")
	if span == nil {
		t.Fatal("span must not be nil with sample rate 1.0")
	}
	if span.TraceID == "" || span.SpanID == "" {
		t.Error("trace ID and span ID must not be empty")
	}
	if span.Name != "TestOp" {
		t.Errorf("expected span name TestOp, got %s", span.Name)
	}
	if span.ParentID != "" {
		t.Errorf("root span must have empty parent ID, got %s", span.ParentID)
	}

	tr.EndSpan(span)
	if !span.ended {
		t.Error("span must be ended after EndSpan")
	}
	if span.Duration() <= 0 {
		t.Error("span duration must be positive")
	}
	if tr.ActiveSpans() != 0 {
		t.Errorf("expected 0 active spans, got %d", tr.ActiveSpans())
	}
}

func TestTracer_ChildSpan(t *testing.T) {
	tr := NewTracer(1.0)

	// Parent span
	parentCtx, parent := tr.StartSpan(context.Background(), "ParentOp")
	if parent == nil {
		t.Fatal("parent span must not be nil")
	}

	// Extract trace context from parent's outgoing metadata
	md, ok := metadata.FromOutgoingContext(parentCtx)
	if !ok {
		t.Fatal("expected traceparent in outgoing metadata")
	}
	tp := md.Get("traceparent")
	if len(tp) == 0 {
		t.Fatal("expected traceparent header")
	}

	// Child span: extract parent from incoming metadata
	childCtx := metadata.NewIncomingContext(context.Background(), md)
	_, child := tr.StartSpan(childCtx, "ChildOp")
	if child == nil {
		t.Fatal("child span must not be nil")
	}
	if child.TraceID != parent.TraceID {
		t.Errorf("child trace ID must match parent: got %s want %s", child.TraceID, parent.TraceID)
	}
	if child.ParentID != parent.SpanID {
		t.Errorf("child parent ID must match parent span ID: got %s want %s", child.ParentID, parent.SpanID)
	}

	tr.EndSpan(child)
	tr.EndSpan(parent)
}

func TestTracer_ZeroRate(t *testing.T) {
	tr := NewTracer(0.0)
	_, span := tr.StartSpan(context.Background(), "ShouldNotExist")
	if span != nil {
		t.Error("span must be nil with sample rate 0")
	}
}

func TestTracer_SpansCollection(t *testing.T) {
	tr := NewTracer(1.0)
	for i := 0; i < 5; i++ {
		_, span := tr.StartSpan(context.Background(), "Op")
		tr.EndSpan(span)
	}

	spans := tr.Spans()
	if len(spans) != 5 {
		t.Errorf("expected 5 completed spans, got %d", len(spans))
	}
}

func TestTracer_SetAttribute(t *testing.T) {
	tr := NewTracer(1.0)
	_, span := tr.StartSpan(context.Background(), "RPC")
	span.SetAttribute("rpc.method", "/namenode.NameNodeService/CreateFile")
	span.SetAttribute("user.id", "user-abc")
	tr.EndSpan(span)

	spans := tr.Spans()
	if len(spans) != 1 {
		t.Fatal("expected 1 span")
	}
	if spans[0].Attributes["rpc.method"] != "/namenode.NameNodeService/CreateFile" {
		t.Errorf("expected rpc.method attribute")
	}
	if spans[0].Attributes["user.id"] != "user-abc" {
		t.Errorf("expected user.id attribute")
	}
}

func TestTracer_DoubleEndIsSafe(t *testing.T) {
	tr := NewTracer(1.0)
	_, span := tr.StartSpan(context.Background(), "Op")
	tr.EndSpan(span)
	tr.EndSpan(span) // must not panic
	if tr.ActiveSpans() != 0 {
		t.Error("active spans should be 0")
	}
}

func TestTracer_Metrics(t *testing.T) {
	tr := NewTracer(1.0)
	for i := 0; i < 3; i++ {
		_, span := tr.StartSpan(context.Background(), "Op")
		tr.EndSpan(span)
	}

	m := tr.Metrics()
	if m["wourifs_traces_total"] != 3 {
		t.Errorf("expected 3 total traces, got %d", m["wourifs_traces_total"])
	}
}

func TestTracer_UnaryInterceptor(t *testing.T) {
	tr := NewTracer(1.0)
	interceptor := tr.UnaryServerInterceptor()

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		time.Sleep(5 * time.Millisecond)
		return "ok", nil
	}

	resp, err := interceptor(context.Background(), "test-req", &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/Op",
	}, handler)

	if err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if resp != "ok" {
		t.Errorf("expected 'ok', got %v", resp)
	}

	spans := tr.Spans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span from interceptor, got %d", len(spans))
	}
	if spans[0].Attributes["rpc.method"] != "/test.Service/Op" {
		t.Errorf("expected rpc.method attribute set by interceptor")
	}
}

func TestTracer_StreamInterceptor(t *testing.T) {
	tr := NewTracer(1.0)
	interceptor := tr.StreamServerInterceptor()

	handler := func(srv interface{}, ss grpc.ServerStream) error {
		return nil
	}

	baseStream := &mockServerStream{ctx: context.Background()}
	err := interceptor(nil, baseStream, &grpc.StreamServerInfo{
		FullMethod: "/test.Service/StreamOp",
	}, handler)

	if err != nil {
		t.Fatalf("stream interceptor: %v", err)
	}

	spans := tr.Spans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span from stream interceptor, got %d", len(spans))
	}
	if spans[0].Attributes["rpc.stream"] != "true" {
		t.Error("expected rpc.stream attribute on stream span")
	}
}

func TestParseTraceParent(t *testing.T) {
	traceID, parentID, ok := parseTraceParent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	if !ok {
		t.Fatal("parse failed for valid traceparent")
	}
	if traceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace ID mismatch: got %s", traceID)
	}
	if parentID != "b7ad6b7169203331" {
		t.Errorf("parent ID mismatch: got %s", parentID)
	}
}

func TestParseTraceParent_Invalid(t *testing.T) {
	_, _, ok := parseTraceParent("garbage")
	if ok {
		t.Error("parse should fail for garbage input")
	}
}

func TestBuildTraceParent(t *testing.T) {
	tp := buildTraceParent("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
	expected := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	if tp != expected {
		t.Errorf("expected %q, got %q", expected, tp)
	}
}

// mockServerStream implements grpc.ServerStream for testing.
type mockServerStream struct {
	ctx context.Context
}

func (m *mockServerStream) SetHeader(md metadata.MD) error  { return nil }
func (m *mockServerStream) SendHeader(md metadata.MD) error { return nil }
func (m *mockServerStream) SetTrailer(md metadata.MD)       {}
func (m *mockServerStream) Context() context.Context         { return m.ctx }
func (m *mockServerStream) SendMsg(msg interface{}) error    { return nil }
func (m *mockServerStream) RecvMsg(msg interface{}) error    { return nil }
