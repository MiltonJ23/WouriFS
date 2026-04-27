package namenode

import (
	"context"
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

var DefaultClock Clock = realClock{}

type HealthRegistry interface {
	GetAllAvailable() []*DataNodeStatus
	MarkUnavailable(id string) error
}

type HealthMonitor struct {
	registry  HealthRegistry
	clock     Clock
	interval  time.Duration
	timeout   time.Duration
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	isRunning bool
}

func NewHealthMonitor(registry HealthRegistry, clock Clock, interval, timeout time.Duration) *HealthMonitor {
	if clock == nil {
		clock = DefaultClock
	}
	return &HealthMonitor{
		registry: registry,
		clock:    clock,
		interval: interval,
		timeout:  timeout,
	}
}

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

func (h *HealthMonitor) run(ctx context.Context) {
	defer h.wg.Done()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.sweep()
		}
	}
}

func (h *HealthMonitor) sweep() {
	now := h.clock.Now()
	availableNodes := h.registry.GetAllAvailable()

	for _, node := range availableNodes {
		if now.Sub(node.LastHeartbeat) >= h.timeout {
			_ = h.registry.MarkUnavailable(node.ID)
		}
	}
}
