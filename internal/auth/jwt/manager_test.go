package jwt

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/MiltonJ23/WouriFS/internal/domain"
)

func generateTestRSAKeys(t *testing.T) (*rsa.PrivateKey, *rsa.PublicKey) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate test RSA keys: %v", err)
	}
	return privKey, &privKey.PublicKey
}

func TestRSATokenManager_BDD(t *testing.T) {
	privKey, pubKey := generateTestRSAKeys(t)
	manager := NewRSATokenManager(privKey, pubKey)
	ctx := context.Background()

	testUser := &domain.User{
		ID:        "user-uuid-1234",
		Username:  "auditor_chief",
		Namespace: "/wourifs/auditors",
	}

	t.Run("Given a valid user", func(t *testing.T) {
		t.Run("When generating and verifying a valid token", func(t *testing.T) {
			// Étape 1 : Génération (Isolée et atomique)
			validToken, err := manager.GenerateToken(ctx, testUser, time.Hour)
			if err != nil {
				t.Fatalf("Expected no error during generation, got %v", err)
			}
			if validToken == "" {
				t.Fatalf("Generated token is empty")
			}

			// Étape 2 : Vérification immédiate
			payload, err := manager.VerifyToken(ctx, validToken)
			if err != nil {
				t.Fatalf("Expected token to be valid, got error: %v", err)
			}

			// Assertions sur les données hydratées
			if payload.UserID != testUser.ID {
				t.Errorf("Expected UserID %s, got %s", testUser.ID, payload.UserID)
			}
			if payload.Namespace != testUser.Namespace {
				t.Errorf("Expected Namespace %s, got %s", testUser.Namespace, payload.Namespace)
			}
			if payload.Username != testUser.Username {
				t.Errorf("Expected Username %s, got %s", testUser.Username, payload.Username)
			}
		})
	})

	t.Run("Given an expired token", func(t *testing.T) {
		expiredToken, _ := manager.GenerateToken(ctx, testUser, -1*time.Hour)

		_, err := manager.VerifyToken(ctx, expiredToken)
		if err == nil {
			t.Errorf("Expected error for expired token, got nil")
		}
	})

	t.Run("Given a canceled context", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := manager.GenerateToken(canceledCtx, testUser, time.Hour)
		if err == nil {
			t.Errorf("Expected error due to canceled context, got nil")
		}
	})
}
