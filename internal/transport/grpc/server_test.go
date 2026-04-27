package grpc

import (
	"crypto/tls"
	"testing"
	"time"
)

func TestSecureServer_BDD(t *testing.T) {
	dummyTLSConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}

	t.Run("Given empty address or nil TLS configuration", func(t *testing.T) {
		t.Run("When initializing SecureServer without address", func(t *testing.T) {
			_, err := NewSecureServer("", dummyTLSConfig)
			if err == nil {
				t.Errorf("Expected error for empty address, got nil")
			}
		})

		t.Run("When initializing SecureServer without TLS", func(t *testing.T) {
			_, err := NewSecureServer("127.0.0.1:9000", nil)
			if err == nil {
				t.Errorf("Expected error for nil TLS config, got nil")
			}
		})
	})

	t.Run("Given valid server parameters", func(t *testing.T) {
		// On utilise le port 0 pour que l'OS choisisse un port libre automatiquement
		server, err := NewSecureServer("127.0.0.1:0", dummyTLSConfig)
		if err != nil {
			t.Fatalf("Failed to create secure server: %v", err)
		}

		t.Run("When starting and stopping the server", func(t *testing.T) {
			// On utilise un canal pour capturer les erreurs de la goroutine du serveur
			errChan := make(chan error, 1)

			// Le serveur est bloquant, on doit le lancer dans une goroutine
			go func() {
				errChan <- server.Start()
			}()

			// THEN: Le serveur doit s'instancier correctement et exposer le serveur gRPC sous-jacent
			if server.GetRawServer() == nil {
				t.Errorf("Expected raw gRPC server to be accessible, got nil")
			}

			// On attend un court instant pour s'assurer que Start() ne panique pas immédiatement
			time.Sleep(100 * time.Millisecond)

			// Action: Arrêt gracieux du serveur
			server.Stop()

			// Vérification qu'aucune erreur fatale ne s'est produite (hors l'arrêt normal)
			select {
			case err := <-errChan:
				if err != nil && err.Error() != "closed" && err.Error() != "use of closed network connection" {
					t.Errorf("Server stopped with unexpected error: %v", err)
				}
			case <-time.After(1 * time.Second):
				t.Errorf("Server did not stop within the expected time")
			}
		})
	})
}
