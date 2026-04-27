package namenode

import (
	"sync"
	"testing"
)

func TestInMemoryDataNodeRegistry_BDD(t *testing.T) {
	registry := NewInMemoryDataNodeRegistry()

	t.Run("Given a new DataNode", func(t *testing.T) {
		validNode := &DataNodeStatus{
			ID:                "dn-1",
			Address:           "100.x.x.1:9001",
			TotalStorageBytes: 1000,
			FreeStorageBytes:  1000,
		}

		t.Run("When registering the node", func(t *testing.T) {
			err := registry.Register(validNode)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// THEN: Il doit être disponible
			nodes := registry.GetAllAvailable()
			if len(nodes) != 1 {
				t.Fatalf("Expected 1 available node, got %d", len(nodes))
			}
			if !nodes[0].IsAvailable {
				t.Errorf("Expected node to be available initially")
			}
		})

		t.Run("When updating heartbeat", func(t *testing.T) {
			err := registry.UpdateHeartbeat("dn-1", 500, 2)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// CORRECTIF : On vérifie la taille avant d'accéder à l'index [0] pour éviter la panic
			nodes := registry.GetAllAvailable()
			if len(nodes) == 0 {
				t.Fatalf("Critical Error: Node list is empty. Check if Register() in memory_registry.go actually saves the data.")
			}
			node := nodes[0]

			if node.FreeStorageBytes != 500 || node.ActiveConnection != 2 {
				t.Errorf("Heartbeat update failed. FreeStorage: %d, Conns: %d", node.FreeStorageBytes, node.ActiveConnection)
			}
		})

		t.Run("When marking node as unavailable", func(t *testing.T) {
			err := registry.MarkUnavailable("dn-1")
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// THEN: Il ne doit plus apparaître dans les noeuds disponibles (FR-N-006)
			if len(registry.GetAllAvailable()) != 0 {
				t.Errorf("Expected 0 available nodes after MarkUnavailable")
			}

			// Mais il doit toujours exister dans le registre global
			if len(registry.GetAll()) != 1 {
				t.Errorf("Expected 1 node in global registry")
			}
		})

		t.Run("When an unknown node sends a heartbeat", func(t *testing.T) {
			err := registry.UpdateHeartbeat("unknown-dn", 100, 0)
			if err != ErrNodeNotFound {
				t.Errorf("Expected ErrNodeNotFound, got %v", err)
			}
		})
	})

	t.Run("Given concurrent access (Thread-Safety Proof)", func(t *testing.T) {
		var wg sync.WaitGroup
		concurrentRegistry := NewInMemoryDataNodeRegistry()
		concurrentRegistry.Register(&DataNodeStatus{ID: "dn-concurrent", Address: "test"})

		// Lancement de 100 goroutines qui lisent et écrivent en même temps
		for i := 0; i < 100; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				_ = concurrentRegistry.UpdateHeartbeat("dn-concurrent", 100, 1)
			}()
			go func() {
				defer wg.Done()
				_ = concurrentRegistry.GetAllAvailable()
			}()
		}

		wg.Wait()
		// THEN: Si le test arrive ici sans planter (panic), la thread-safety est prouvée.
	})
}
