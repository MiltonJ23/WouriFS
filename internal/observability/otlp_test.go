package observability

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelLog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/metadata"
)

func TestTracer_OTelExport(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(time.Millisecond)),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())

	tr := NewTracerWithOtel(1.0, tp)
	_, span := tr.StartSpan(context.Background(), "op/exported")
	if span == nil {
		t.Fatal("span must not be nil with sample rate 1.0")
	}
	tr.EndSpan(span)

	waitExported := func() tracetest.SpanStubs {
		dl := time.After(5 * time.Second)
		for {
			spans := exp.GetSpans()
			if len(spans) > 0 {
				return spans
			}
			select {
			case <-dl:
				return nil
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	spans := waitExported()
	if len(spans) != 1 {
		t.Fatalf("expected 1 exported span, got %d", len(spans))
	}
	if spans[0].Name != "op/exported" {
		t.Errorf("expected span name op/exported, got %s", spans[0].Name)
	}
	if got := spans[0].SpanContext.TraceID().String(); got != span.TraceID {
		t.Errorf("local TraceID %s must match exported %s", span.TraceID, got)
	}
}

func TestTracer_OTelChildParentLinkage(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(time.Millisecond)),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())

	tr := NewTracerWithOtel(1.0, tp)
	parentCtx, parent := tr.StartSpan(context.Background(), "ParentOp")
	if parent == nil {
		t.Fatal("parent span must not be nil")
	}
	_, child := tr.StartSpan(parentCtx, "ChildOp")
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

func TestTracer_OTelRemoteParentFromMetadata(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(time.Millisecond)),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())

	tr := NewTracerWithOtel(1.0, tp)

	// Simulate an upstream hop: a W3C traceparent in incoming metadata.
	md := metadata.Pairs("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, span := tr.StartSpan(ctx, "ServerOp")
	if span == nil {
		t.Fatal("span must not be nil")
	}
	if span.TraceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("expected upstream trace ID, got %s", span.TraceID)
	}
	if span.ParentID != "b7ad6b7169203331" {
		t.Errorf("expected upstream parent ID, got %s", span.ParentID)
	}
	tr.EndSpan(span)
}

func TestMetrics_OTelMirror(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	m := NewMetrics()
	m.AttachOtel(mp.Meter("test"))
	m.Record("CreateFile", "ok", 10*time.Millisecond, 1024)
	m.Record("CreateFile", "error", 5*time.Millisecond, 0)
	m.Record("ReadChunk", "ok", 25*time.Millisecond, 65536)
	m.SetGauge("wourifs_datanodes_available", 3)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	var opTotalOK, opTotalErr int64
	var histCount uint64
	var bytesTotal int64
	var gaugeVal int64
	foundGauge := false

	for _, sm := range rm.ScopeMetrics {
		for _, met := range sm.Metrics {
			switch met.Name {
			case "wourifs_op_total":
				sum, ok := met.Data.(metricdata.Sum[int64])
				if !ok {
					t.Fatalf("unexpected data type %T", met.Data)
				}
				for _, dp := range sum.DataPoints {
					status, _ := dp.Attributes.Value(attribute.Key("status"))
					op, _ := dp.Attributes.Value(attribute.Key("op"))
					_ = op
					if status.AsString() == "ok" {
						opTotalOK += dp.Value
					} else {
						opTotalErr += dp.Value
					}
				}
			case "wourifs_op_duration_seconds":
				hist, ok := met.Data.(metricdata.Histogram[float64])
				if !ok {
					t.Fatalf("unexpected data type %T", met.Data)
				}
				for _, dp := range hist.DataPoints {
					histCount += dp.Count
				}
			case "wourifs_op_bytes_total":
				sum, ok := met.Data.(metricdata.Sum[int64])
				if !ok {
					t.Fatalf("unexpected data type %T", met.Data)
				}
				for _, dp := range sum.DataPoints {
					bytesTotal += dp.Value
				}
			case "wourifs_datanodes_available":
				g, ok := met.Data.(metricdata.Gauge[int64])
				if !ok {
					t.Fatalf("unexpected gauge type %T", met.Data)
				}
				if len(g.DataPoints) != 1 {
					t.Fatalf("expected 1 gauge datapoint, got %d", len(g.DataPoints))
				}
				gaugeVal = g.DataPoints[0].Value
				foundGauge = true
			}
		}
	}

	if opTotalOK != 2 {
		t.Errorf("expected 2 ok ops exported, got %d", opTotalOK)
	}
	if opTotalErr != 1 {
		t.Errorf("expected 1 error op exported, got %d", opTotalErr)
	}
	if histCount != 3 {
		t.Errorf("expected histogram count 3, got %d", histCount)
	}
	if bytesTotal != 1024+65536 {
		t.Errorf("expected bytes total %d, got %d", 1024+65536, bytesTotal)
	}
	if !foundGauge || gaugeVal != 3 {
		t.Errorf("expected gauge wourifs_datanodes_available=3, got %d (found=%v)", gaugeVal, foundGauge)
	}
}

// recordingProcessor stores OTel log records for tests.
type recordingProcessor struct {
	mu      sync.Mutex
	records []otelLog.Record
}

func (r *recordingProcessor) OnEmit(_ context.Context, rec *otelLog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, *rec)
	return nil
}

func (r *recordingProcessor) Enabled(context.Context, otelLog.EnabledParameters) bool { return true }
func (r *recordingProcessor) Shutdown(context.Context) error                          { return nil }
func (r *recordingProcessor) ForceFlush(context.Context) error                        { return nil }

func TestLogger_OTelExport(t *testing.T) {
	rec := &recordingProcessor{}
	lp := otelLog.NewLoggerProvider(otelLog.WithProcessor(rec))
	l := NewLoggerWithOtel(slog.LevelInfo, lp)
	l.Info("hello_otlp", "id", "node-1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lp.ForceFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.records) != 1 {
		t.Fatalf("expected 1 exported log record, got %d", len(rec.records))
	}
	if got := rec.records[0].Body().AsString(); got != "hello_otlp" {
		t.Errorf("expected body hello_otlp, got %s", got)
	}
}

func TestLogger_NilProviderFallsBackToStdout(t *testing.T) {
	l := NewLoggerWithOtel(slog.LevelInfo, nil)
	if l == nil || l.slog == nil {
		t.Fatal("logger must not be nil with nil provider")
	}
}
