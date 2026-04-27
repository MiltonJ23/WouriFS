package namenode

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// MockClock enables manual time travel for fully deterministic tests without real delays.
type MockClock struct {
	currentTime time.Time
}

func (m *MockClock) Now() time.Time {
	return m.currentTime
}

// testRegistry is a minimal in-memory HealthRegistry that sets LastHeartbeat directly,
// decoupling the health-monitor tests from the real system clock entirely.
type testRegistry struct {
	mu    sync.Mutex
	nodes []*DataNodeStatus
}

func (r *testRegistry) GetAllAvailable() []*DataNodeStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []*DataNodeStatus
	for _, n := range r.nodes {
		if n.IsAvailable {
			copy := *n
			result = append(result, &copy)
		}
	}
	return result
}

func (r *testRegistry) MarkUnavailable(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.nodes {
		if n.ID == id {
			n.IsAvailable = false
			return nil
		}
	}
	return ErrNodeNotFound
}

func TestHealthMonitor_BDD(t *testing.T) {
	t.Run("Given an active DataNode in the registry", func(t *testing.T) {
		// Arrange: all timestamps are fully controlled via testRegistry and MockClock,
		// so there is no drift from the real system clock.
		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		mockClock := &MockClock{currentTime: baseTime}
		registry := &testRegistry{
			nodes: []*DataNodeStatus{
				{
					ID:            "node-xyz",
					Address:       "192.168.1.50:9000",
					LastHeartbeat: baseTime,
					IsAvailable:   true,
				},
			},
		}

		// Sweep every 5s; mark unavailable after 15s (3 missed heartbeats) — FR-N-005.
		monitor, err := NewHealthMonitor(registry, mockClock, 5*time.Second, 15*time.Second)
		if err != nil {
			t.Fatalf("Failed to create health monitor: %v", err)
		}

		t.Run("When 10 seconds pass (below threshold)", func(t *testing.T) {
			// Act: advance clock by 10s (2 missed heartbeats — within tolerance).
			mockClock.currentTime = baseTime.Add(10 * time.Second)
			_ = monitor.sweep()

			// Assert: node must still be available.
			available := registry.GetAllAvailable()
			if len(available) != 1 {
				t.Errorf("Expected 1 available node, got %d. Node should NOT be marked unavailable yet.", len(available))
			}
		})

		t.Run("When 15 seconds pass (threshold exactly reached — FR-N-005)", func(t *testing.T) {
			// Act: advance clock to exactly the timeout boundary.
			// Because LastHeartbeat is set via testRegistry, no extra margin is needed.
			mockClock.currentTime = baseTime.Add(15 * time.Second)
			_ = monitor.sweep()

			// Assert: node must now be unavailable.
			available := registry.GetAllAvailable()
			if len(available) != 0 {
				t.Errorf("Expected 0 available nodes, got %d. Node MUST be marked UNAVAILABLE.", len(available))
			}
		})
	})

	t.Run("Given invalid monitor configuration", func(t *testing.T) {
		reg := &testRegistry{}

		t.Run("When interval is zero", func(t *testing.T) {
			_, err := NewHealthMonitor(reg, nil, 0, 15*time.Second)
			if err == nil {
				t.Error("Expected an error for a zero interval, got nil")
			}
		})

		t.Run("When timeout is zero", func(t *testing.T) {
			_, err := NewHealthMonitor(reg, nil, 5*time.Second, 0)
			if err == nil {
				t.Error("Expected an error for a zero timeout, got nil")
			}
		})

		t.Run("When interval is negative", func(t *testing.T) {
			_, err := NewHealthMonitor(reg, nil, -1*time.Second, 15*time.Second)
			if err == nil {
				t.Error("Expected an error for a negative interval, got nil")
			}
		})
	})

	t.Run("Given the monitor sweep encounters an unexpected registry error", func(t *testing.T) {
		// Arrange: a registry whose MarkUnavailable always returns an unexpected error.
		backendErr := errors.New("storage backend unavailable")
		errReg := &errMarkRegistry{err: backendErr}
		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		mockClock := &MockClock{currentTime: baseTime.Add(20 * time.Second)}

		errReg.nodes = []*DataNodeStatus{
			{ID: "node-fail", LastHeartbeat: baseTime, IsAvailable: true},
		}

		monitor, err := NewHealthMonitor(errReg, mockClock, 5*time.Second, 15*time.Second)
		if err != nil {
			t.Fatalf("Failed to create health monitor: %v", err)
		}

		var captured error
		monitor.WithErrorHandler(func(e error) { captured = e })

		t.Run("When sweep runs", func(t *testing.T) {
			_ = monitor.sweep()

			// Assert: the error handler must have received the unexpected error.
			if captured == nil {
				t.Fatal("Expected error handler to be called, but it was not")
			}
			if !errors.Is(captured, backendErr) {
				t.Errorf("Expected %v, got %v", backendErr, captured)
			}
		})
	})

	t.Run("Given concurrent monitor lifecycle (Start and graceful Stop)", func(t *testing.T) {
		// Verify that there are no goroutine leaks or deadlocks during Start/Stop.
		registry := &testRegistry{}
		monitor, err := NewHealthMonitor(registry, DefaultClock, 1*time.Millisecond, 15*time.Second)
		if err != nil {
			t.Fatalf("Failed to create health monitor: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		monitor.Start(ctx)

		// Stop blocks until the background goroutine has fully exited;
		// no sleep is needed to "let the goroutine initialise".
		monitor.Stop()

		// Reaching this line without blocking indefinitely proves that the
		// WaitGroup and cancel function work correctly.
	})

	t.Run("Given the monitor goroutine exits via parent context cancellation", func(t *testing.T) {
		// Verify that isRunning is reset after an external context cancellation,
		// allowing Start to be called again on the same monitor instance.
		registry := &testRegistry{}
		monitor, err := NewHealthMonitor(registry, DefaultClock, 1*time.Millisecond, 15*time.Second)
		if err != nil {
			t.Fatalf("Failed to create health monitor: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		monitor.Start(ctx)

		// Cancel the parent context; the goroutine will exit on its own.
		cancel()
		// Stop gracefully waits for the goroutine to finish regardless of whether
		// it was already exiting due to context cancellation.
		monitor.Stop()

		// Start must succeed without deadlock after the goroutine has fully exited.
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		monitor.Start(ctx2)
		monitor.Stop()
	})
}

// errMarkRegistry is a HealthRegistry stub whose MarkUnavailable always returns
// the configured error, used to test the sweep error-handler path.
type errMarkRegistry struct {
	mu    sync.Mutex
	nodes []*DataNodeStatus
	err   error
}

func (r *errMarkRegistry) GetAllAvailable() []*DataNodeStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []*DataNodeStatus
	for _, n := range r.nodes {
		if n.IsAvailable {
			copy := *n
			result = append(result, &copy)
		}
	}
	return result
}

func (r *errMarkRegistry) MarkUnavailable(_ string) error {
	return r.err
}
