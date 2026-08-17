/*
 * OpenTelemetry OTLP/gRPC pipeline.
 *
 * Creates the three OTel SDK providers (traces, metrics, logs) wired to a
 * single OTLP/gRPC collector endpoint (default localhost:4317 per the OTLP
 * specification; 4318 is the OTLP/HTTP port). Export failures are
 * fail-open: they never block WouriFS operations.
 *
 * The rest of the observability package keeps its local bookkeeping
 * (Prometheus text exposition, in-memory spans) and *mirrors* the same data
 * into these providers, so a collector ingests everything in standard OTLP.
 */
package observability

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// OTelPipeline bundles the three providers so callers can shut them down
// together (each Shutdown flushes buffered telemetry).
type OTelPipeline struct {
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
	LoggerProvider *log.LoggerProvider
}

// Shutdown flushes and stops all providers. Safe to call multiple times.
func (p *OTelPipeline) Shutdown(ctx context.Context) error {
	var first error
	if p.TracerProvider != nil {
		if err := p.TracerProvider.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	if p.MeterProvider != nil {
		if err := p.MeterProvider.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	if p.LoggerProvider != nil {
		if err := p.LoggerProvider.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// SetupOTLP builds the full OTLP/gRPC pipeline. endpoint is the collector
// address (host:port), serviceName and instanceID identify this process in
// the exported Resource.
func SetupOTLP(ctx context.Context, endpoint, serviceName, instanceID string) (*OTelPipeline, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("otlp endpoint is empty")
	}
	if serviceName == "" {
		serviceName = "wourifs"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
			semconv.ServiceInstanceID(instanceID),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp resource: %w", err)
	}

	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		tracerProvider.Shutdown(ctx)
		return nil, fmt.Errorf("otlp metric exporter: %w", err)
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)

	logExp, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(endpoint),
		otlploggrpc.WithInsecure(),
		otlploggrpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		tracerProvider.Shutdown(ctx)
		meterProvider.Shutdown(ctx)
		return nil, fmt.Errorf("otlp log exporter: %w", err)
	}
	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExp)),
		log.WithResource(res),
	)

	return &OTelPipeline{
		TracerProvider: tracerProvider,
		MeterProvider:  meterProvider,
		LoggerProvider: loggerProvider,
	}, nil
}
