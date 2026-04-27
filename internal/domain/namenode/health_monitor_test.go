package namenode

import (
	"context"
	"testing"
	"time"
)

// MockClock permet d'avancer le temps manuellement (Time Travel) pour des tests 100% déterministes.
type MockClock struct {
	currentTime time.Time
}

func (m *MockClock) Now() time.Time {
	return m.currentTime
}

func TestHealthMonitor_BDD(t *testing.T) {
	t.Run("Given an active DataNode in the registry", func(t *testing.T) {
		// Arrange
		registry := NewInMemoryDataNodeRegistry()

		// IMPORTANT : Comme le registre utilise le VRAI time.Now() lors de l'enregistrement,
		// notre horloge factice doit démarrer exactement au même instant pour être synchrone.
		baseTime := time.Now()
		mockClock := &MockClock{currentTime: baseTime}

		// Configuration : Sweep toutes les 5s, Timeout après 15s (3 heartbeats manqués)
		monitor := NewHealthMonitor(registry, mockClock, 5*time.Second, 15*time.Second)

		err := registry.Register(&DataNodeStatus{
			ID:      "node-xyz",
			Address: "192.168.1.50:9000",
		})
		if err != nil {
			t.Fatalf("Failed to register node: %v", err)
		}

		t.Run("When 10 seconds pass (less than threshold)", func(t *testing.T) {
			// Act : On avance le temps de 10 secondes (2 heartbeats manqués, on tolère)
			mockClock.currentTime = baseTime.Add(10 * time.Second)
			monitor.sweep() // On appelle sweep() directement pour éviter l'asynchronisme instable dans les tests

			// Assert
			available := registry.GetAllAvailable()
			if len(available) != 1 {
				t.Errorf("Expected 1 available node, got %d. Node should NOT be marked unavailable yet.", len(available))
			}
		})

		t.Run("When 16 seconds pass (threshold exceeded - FR-N-005)", func(t *testing.T) {
			// Act : On avance le temps de 16 secondes pour absorber la latence d'initialisation en nanosecondes
			// et garantir le dépassement strict du seuil des 3 heartbeats manqués.
			mockClock.currentTime = baseTime.Add(16 * time.Second)
			monitor.sweep()

			// Assert
			available := registry.GetAllAvailable()
			if len(available) != 0 {
				t.Errorf("Expected 0 available nodes, got %d. Node MUST be marked UNAVAILABLE.", len(available))
			}
		})
	})

	t.Run("Given concurrent monitor lifecycle (Start & Graceful Stop)", func(t *testing.T) {
		// Vérification qu'il n'y a pas de goroutine leaks ou de deadlocks
		registry := NewInMemoryDataNodeRegistry()
		monitor := NewHealthMonitor(registry, DefaultClock, 1*time.Millisecond, 15*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		monitor.Start(ctx)

		// On le laisse tourner une fraction de seconde pour que la goroutine s'initialise et boucle
		time.Sleep(10 * time.Millisecond)

		// Stop doit terminer la goroutine proprement
		monitor.Stop()

		// Si le test atteint cette ligne sans rester bloqué indéfiniment (timeout du test),
		// c'est que le WaitGroup et le Contexte de fermeture fonctionnent parfaitement.
	})
}
