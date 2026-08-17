package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/MiltonJ23/WouriFS/internal/observability"
)

// shutdownOTLPTimeout bounds how long we wait for buffered telemetry to
// flush on exit.
const shutdownOTLPTimeout = 3 * time.Second

// setupObservability builds the OTLP/gRPC pipeline for one process role.
// It is fail-open: when disabled or misconfigured, WouriFS still starts and
// only exports nothing. The returned shutdown func flushes buffered
// telemetry and must be called on exit.
func setupObservability(cfg Config, role string) (*observability.OTelPipeline, func()) {
	if !cfg.Observability.Enabled && !cfg.Tracing.Enabled {
		return nil, func() {}
	}
	instanceID := cfg.Node.ID
	if instanceID == "" {
		instanceID = role
	} else {
		instanceID = role + "/" + instanceID
	}
	pipeline, err := observability.SetupOTLP(context.Background(),
		cfg.ResolveOTLPEndpoint(), cfg.ResolveServiceName(), instanceID)
	if err != nil {
		log.Printf("[%s] OTLP export disabled: %v", role, err)
		return nil, func() {}
	}
	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownOTLPTimeout)
		defer cancel()
		if err := pipeline.Shutdown(ctx); err != nil {
			log.Printf("[%s] otlp shutdown: %v", role, err)
		}
	}
	return pipeline, shutdown
}

// startMetricsServer serves the Prometheus text exposition on the configured
// metrics port (default 9102). It keeps running until the process exits;
// failures are logged, not fatal (fail-open observability).
func startMetricsServer(port int, handler http.Handler, role string) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	srv := &http.Server{Addr: addr, Handler: handler}
	go func() {
		log.Printf("[%s] metrics endpoint on %s/metrics (Prometheus text)", role, addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[%s] metrics server stopped: %v", role, err)
		}
	}()
}
