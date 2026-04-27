package interceptor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MiltonJ23/WouriFS/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// =============================================================================
// MOCKS
// =============================================================================

// mockTokenManager simule la validation cryptographique pour isoler le test.
type mockTokenManager struct {
	shouldFail bool
	payload    *domain.TokenPayload
}

func (m *mockTokenManager) GenerateToken(ctx context.Context, user *domain.User, duration time.Duration) (string, error) {
	return "", nil // Non utilisé dans ce test
}

func (m *mockTokenManager) VerifyToken(ctx context.Context, token string) (*domain.TokenPayload, error) {
	if m.shouldFail {
		return nil, errors.New("mocked signature validation failed")
	}
	return m.payload, nil
}

// mockServerStream simule un flux gRPC entrant.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

// =============================================================================
// BDD TESTS
// =============================================================================

func TestAuthInterceptor_BDD(t *testing.T) {
	validPayload := &domain.TokenPayload{
		UserID:    "user-123",
		Username:  "auditor",
		Namespace: "/wourifs/auditors",
	}

	successTM := &mockTokenManager{shouldFail: false, payload: validPayload}
	failingTM := &mockTokenManager{shouldFail: true, payload: nil}

	interceptor := NewAuthInterceptor(successTM)

	// dummyUnaryHandler sert à vérifier si l'intercepteur laisse passer la requête
	dummyUnaryHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		// On vérifie que le payload a bien été injecté par l'intercepteur
		_, err := PayloadFromContext(ctx)
		if err != nil {
			return nil, err
		}
		return "success", nil
	}

	t.Run("Given a request without metadata", func(t *testing.T) {
		t.Run("When intercepted by Unary", func(t *testing.T) {
			_, err := interceptor.Unary()(context.Background(), nil, nil, dummyUnaryHandler)

			// THEN: Elle doit être rejetée avec Unauthenticated
			if err == nil || status.Code(err) != codes.Unauthenticated {
				t.Errorf("Expected Unauthenticated error, got %v", err)
			}
		})
	})

	t.Run("Given a request with metadata but no authorization token", func(t *testing.T) {
		t.Run("When intercepted by Unary", func(t *testing.T) {
			md := metadata.Pairs("some-other-header", "value")
			ctx := metadata.NewIncomingContext(context.Background(), md)

			_, err := interceptor.Unary()(ctx, nil, nil, dummyUnaryHandler)

			// THEN: Elle doit être rejetée
			if err == nil || status.Code(err) != codes.Unauthenticated {
				t.Errorf("Expected Unauthenticated error due to missing token, got %v", err)
			}
		})
	})

	t.Run("Given a request with a malformed authorization token (no Bearer)", func(t *testing.T) {
		t.Run("When intercepted by Unary", func(t *testing.T) {
			md := metadata.Pairs("authorization", "JustTheTokenNoBearer")
			ctx := metadata.NewIncomingContext(context.Background(), md)

			_, err := interceptor.Unary()(ctx, nil, nil, dummyUnaryHandler)

			// THEN: Elle doit être rejetée pour format invalide
			if err == nil || status.Code(err) != codes.Unauthenticated {
				t.Errorf("Expected Unauthenticated error due to bad format, got %v", err)
			}
		})
	})

	t.Run("Given a request with an invalid/forged JWT token", func(t *testing.T) {
		t.Run("When intercepted by Unary", func(t *testing.T) {
			md := metadata.Pairs("authorization", "Bearer forged.token.here")
			ctx := metadata.NewIncomingContext(context.Background(), md)

			strictInterceptor := NewAuthInterceptor(failingTM)
			_, err := strictInterceptor.Unary()(ctx, nil, nil, dummyUnaryHandler)

			// THEN: Elle doit être rejetée car VerifyToken échoue
			if err == nil || status.Code(err) != codes.Unauthenticated {
				t.Errorf("Expected Unauthenticated error from VerifyToken failure, got %v", err)
			}
		})
	})

	t.Run("Given a request with a valid JWT token", func(t *testing.T) {
		md := metadata.Pairs("authorization", "Bearer valid.token.here")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		t.Run("When intercepted by Unary", func(t *testing.T) {
			res, err := interceptor.Unary()(ctx, nil, nil, dummyUnaryHandler)

			// THEN: La requête passe et le handler métier est exécuté avec succès
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if res != "success" {
				t.Errorf("Expected handler to return 'success', got %v", res)
			}
		})

		t.Run("When intercepted by Stream", func(t *testing.T) {
			dummyStream := &mockServerStream{ctx: ctx}

			dummyStreamHandler := func(srv interface{}, stream grpc.ServerStream) error {
				// Vérifie que le payload est bien dans le stream.Context()
				_, extractErr := PayloadFromContext(stream.Context())
				return extractErr
			}

			err := interceptor.Stream()(nil, dummyStream, nil, dummyStreamHandler)

			// THEN: Le flux doit être autorisé et le payload injecté
			if err != nil {
				t.Fatalf("Expected stream to pass successfully, got error: %v", err)
			}
		})
	})

	// Test spécifique pour PayloadFromContext (Isolation FR-N-007)
	t.Run("Given a context inspection", func(t *testing.T) {
		t.Run("When extracting payload from an unauthenticated context", func(t *testing.T) {
			_, err := PayloadFromContext(context.Background())
			// THEN: Une erreur Interne doit être levée (protection contre l'usurpation de namespace)
			if err == nil || status.Code(err) != codes.Internal {
				t.Errorf("Expected Internal error for missing payload, got %v", err)
			}
		})
	})
}
