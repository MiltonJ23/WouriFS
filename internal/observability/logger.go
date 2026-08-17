package observability

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	otelLog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Logger wraps slog with WouriFS-specific structured logging conventions.
// All gRPC handlers log through this; metrics are emitted alongside logs.
type Logger struct {
	slog    *slog.Logger
	metrics *Metrics
	level   slog.Level
}

// NewLogger creates a production logger writing JSON to stdout.
func NewLogger(level slog.Level) *Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return &Logger{
		slog:    slog.New(handler),
		metrics: NewMetrics(),
		level:   level,
	}
}

// NewLoggerWithOtel creates a production logger that writes JSON to stdout
// AND exports every record through the OTel LoggerProvider (OTLP/gRPC).
// A nil provider falls back to NewLogger.
func NewLoggerWithOtel(level slog.Level, provider *otelLog.LoggerProvider) *Logger {
	if provider == nil {
		return NewLogger(level)
	}
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	otelHandler := otelslog.NewHandler("wourifs",
		otelslog.WithLoggerProvider(provider),
		otelslog.WithSource(true),
	)
	return &Logger{
		slog:    slog.New(multiHandler{handlers: []slog.Handler{jsonHandler, otelHandler}}),
		metrics: NewMetrics(),
		level:   level,
	}
}

// multiHandler fans out slog records to several handlers (stdout + OTLP).
type multiHandler struct {
	handlers []slog.Handler
}

func (m multiHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (m multiHandler) Handle(ctx context.Context, r slog.Record) error {
	var firstErr error
	for _, h := range m.handlers {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (m multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cloned := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		cloned[i] = h.WithAttrs(attrs)
	}
	return multiHandler{handlers: cloned}
}

func (m multiHandler) WithGroup(name string) slog.Handler {
	cloned := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		cloned[i] = h.WithGroup(name)
	}
	return multiHandler{handlers: cloned}
}

// NewTestLogger returns a no-op logger for tests.
func NewTestLogger() *Logger {
	return &Logger{
		slog:    slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})),
		metrics: NewMetrics(),
	}
}

// Metrics returns the Prometheus-compatible metrics registry.
func (l *Logger) Metrics() *Metrics {
	return l.metrics
}

// OpStart begins timing an operation. Pair with OpEnd.
func (l *Logger) OpStart(op, path string) (time.Time, context.Context) {
	start := time.Now()
	l.slog.Info("op_start",
		"op", op,
		"path", path,
	)
	return start, context.Background()
}

// OpEnd records the operation result and duration.
func (l *Logger) OpEnd(start time.Time, op, path string, err error, bytes int64) {
	dur := time.Since(start)
	status := "ok"
	if err != nil {
		status = "error"
		l.slog.Warn("op_error",
			"op", op,
			"path", path,
			"error", err,
			"duration_ms", dur.Milliseconds(),
		)
	} else {
		l.slog.Info("op_end",
			"op", op,
			"path", path,
			"duration_ms", dur.Milliseconds(),
			"bytes", bytes,
		)
	}
	l.metrics.Record(op, status, dur, bytes)
}

// Info logs at info level.
func (l *Logger) Info(msg string, args ...any) {
	l.slog.Info(msg, args...)
}

// Warn logs at warn level.
func (l *Logger) Warn(msg string, args ...any) {
	l.slog.Warn(msg, args...)
}

// Error logs at error level.
func (l *Logger) Error(msg string, args ...any) {
	l.slog.Error(msg, args...)
}

// Debug logs at debug level.
func (l *Logger) Debug(msg string, args ...any) {
	l.slog.Debug(msg, args...)
}

// GRPCInterceptor returns a unary interceptor that logs and times every gRPC call.
func (l *Logger) GRPCInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start, _ := l.OpStart("grpc:"+info.FullMethod, "")
		resp, err := handler(ctx, req)
		l.OpEnd(start, "grpc:"+info.FullMethod, "", err, 0)
		return resp, err
	}
}

// LogGRPCError converts a gRPC error to a structured log entry.
func (l *Logger) LogGRPCError(method string, err error) {
	if err == nil {
		return
	}
	st, ok := status.FromError(err)
	if !ok {
		l.Warn("grpc_error", "method", method, "error", err.Error())
		return
	}
	if st.Code() == codes.Unauthenticated || st.Code() == codes.PermissionDenied {
		l.Warn("grpc_auth_rejected", "method", method, "code", st.Code().String(), "msg", st.Message())
		return
	}
	l.Error("grpc_error", "method", method, "code", st.Code().String(), "msg", st.Message())
}
