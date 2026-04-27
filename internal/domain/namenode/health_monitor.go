package namenode

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Clock abstracts time retrieval to allow deterministic testing without real sleep delays.
type Clock interface {
	Now() time.Time
}

// realClock is the production implementation of Clock backed by the system clock.
type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

// DefaultClock uses the real system clock and is the default for production use.
var DefaultClock Clock = realClock{}

// HealthRegistry is the minimal interface the monitor requires from a DataNode registry.
type HealthRegistry interface {
	GetAllAvailable() []*DataNodeStatus
	MarkUnavailable(id string) error
}

// HealthMonitor periodically sweeps registered DataNodes and marks unavailable any node
// that has not sent a heartbeat within the configured timeout, enforcing FR-N-005.
type HealthMonitor struct {
	registry  HealthRegistry
	clock     Clock
	interval  time.Duration
	timeout   time.Duration
	onError   func(error) // optional callback for unexpected sweep errors
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	isRunning bool
}

// NewHealthMonitor creates a HealthMonitor. It returns an error if interval or timeout
// are non-positive, which would cause a panic in time.NewTicker at runtime.
func NewHealthMonitor(registry HealthRegistry, clock Clock, interval, timeout time.Duration) (*HealthMonitor, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("health monitor: interval must be positive, got %v", interval)
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("health monitor: timeout must be positive, got %v", timeout)
	}
	if clock == nil {
		clock = DefaultClock
	}
	return &HealthMonitor{
		registry: registry,
		clock:    clock,
		interval: interval,
		timeout:  timeout,
	}, nil
}

// WithErrorHandler registers an optional callback that is invoked when sweep encounters
// an unexpected error from MarkUnavailable (ErrNodeNotFound is always silently ignored).
func (h *HealthMonitor) WithErrorHandler(fn func(error)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onError = fn
}

// Start launches the background monitoring goroutine under the supplied context.
// It is a no-op when the monitor is already running.
func (h *HealthMonitor) Start(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.isRunning {
		return
	}

	monitorCtx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.isRunning = true
	h.wg.Add(1)

	go h.run(monitorCtx)
}

// Stop signals the monitor to cease operation and blocks until the background goroutine
// has fully exited, guaranteeing no goroutine leaks.
func (h *HealthMonitor) Stop() {
	h.mu.Lock()
	if !h.isRunning {
		h.mu.Unlock()
		return
	}
	h.cancel()
	h.isRunning = false
	h.mu.Unlock()

	h.wg.Wait()
}

// run is the main ticker loop. A deferred cleanup always resets isRunning so that
// Start can be called again after the parent context is cancelled externally.
func (h *HealthMonitor) run(ctx context.Context) {
	// Reset isRunning when the goroutine exits regardless of how termination occurs
	// (external context cancellation or an explicit Stop call). This ensures that
	// Start can be called again after the parent context is cancelled.
	defer func() {
		h.mu.Lock()
		h.isRunning = false
		h.mu.Unlock()
	}()
	defer h.wg.Done()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.sweep() //nolint:errcheck // errors are forwarded via the onError callback
		}
	}
}

// sweep evaluates all currently available nodes and marks those that have exceeded
// the heartbeat timeout. ErrNodeNotFound is silently ignored as it indicates that a
// node was concurrently removed; all other errors are passed to the onError handler
// (if set) and also collected and returned to the caller.
func (h *HealthMonitor) sweep() error {
	now := h.clock.Now()
	availableNodes := h.registry.GetAllAvailable()

	var errs []error
	for _, node := range availableNodes {
		if now.Sub(node.LastHeartbeat) >= h.timeout {
			if err := h.registry.MarkUnavailable(node.ID); err != nil && !errors.Is(err, ErrNodeNotFound) {
				errs = append(errs, err)
			}
		}
	}

	combined := errors.Join(errs...)
	if combined != nil && h.onError != nil {
		h.onError(combined)
	}
	return combined
}
