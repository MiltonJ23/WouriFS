package namenode

import (
	"sync"
	"testing"
)

func newNodeFixture() *DataNodeStatus {
	return &DataNodeStatus{
		ID:                "dn-1",
		Address:           "100.x.x.1:9001",
		TotalStorageBytes: 1000,
		FreeStorageBytes:  1000,
	}
}

func TestInMemoryDataNodeRegistry_BDD(t *testing.T) {
	t.Run("Given a new DataNode", func(t *testing.T) {
		t.Run("When registering the node", func(t *testing.T) {
			registry := NewInMemoryDataNodeRegistry()
			err := registry.Register(newNodeFixture())
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			nodes := registry.GetAllAvailable()
			if len(nodes) != 1 {
				t.Fatalf("Expected 1 available node, got %d", len(nodes))
			}
			if !nodes[0].IsAvailable {
				t.Errorf("Expected node to be available initially")
			}
		})

		t.Run("When updating heartbeat", func(t *testing.T) {
			registry := NewInMemoryDataNodeRegistry()
			if err := registry.Register(newNodeFixture()); err != nil {
				t.Fatalf("Setup: Register failed: %v", err)
			}

			err := registry.UpdateHeartbeat("dn-1", 500, 2)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			nodes := registry.GetAllAvailable()
			if len(nodes) == 0 {
				t.Fatalf("Expected at least 1 available node after heartbeat update")
			}
			node := nodes[0]

			if node.FreeStorageBytes != 500 || node.ActiveConnection != 2 {
				t.Errorf("Heartbeat update failed. FreeStorage: %d, Conns: %d", node.FreeStorageBytes, node.ActiveConnection)
			}
		})

		t.Run("When marking node as unavailable", func(t *testing.T) {
			registry := NewInMemoryDataNodeRegistry()
			if err := registry.Register(newNodeFixture()); err != nil {
				t.Fatalf("Setup: Register failed: %v", err)
			}

			err := registry.MarkUnavailable("dn-1")
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if len(registry.GetAllAvailable()) != 0 {
				t.Errorf("Expected 0 available nodes after MarkUnavailable")
			}

			if len(registry.GetAll()) != 1 {
				t.Errorf("Expected 1 node in global registry")
			}
		})

		t.Run("When an unknown node sends a heartbeat", func(t *testing.T) {
			registry := NewInMemoryDataNodeRegistry()
			err := registry.UpdateHeartbeat("unknown-dn", 100, 0)
			if err != ErrNodeNotFound {
				t.Errorf("Expected ErrNodeNotFound, got %v", err)
			}
		})
	})

	t.Run("Given concurrent access", func(t *testing.T) {
		const concurrentWorkers = 100
		var wg sync.WaitGroup
		errCh := make(chan error, concurrentWorkers*2)

		concurrentRegistry := NewInMemoryDataNodeRegistry()
		if err := concurrentRegistry.Register(&DataNodeStatus{ID: "dn-concurrent", Address: "test"}); err != nil {
			t.Fatalf("Setup: Register failed: %v", err)
		}

		for i := 0; i < concurrentWorkers; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				if err := concurrentRegistry.UpdateHeartbeat("dn-concurrent", 100, 1); err != nil {
					errCh <- err
				}
			}()
			go func() {
				defer wg.Done()
				concurrentRegistry.GetAllAvailable()
			}()
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			t.Errorf("Unexpected error during concurrent access: %v", err)
		}
		// THEN: All goroutines complete without errors. Run with `go test -race` to detect data races.
	})
}
