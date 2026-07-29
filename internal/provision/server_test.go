package provision

import (
	"context"
	"testing"

	provisionpb "github.com/MiltonJ23/WouriFS/api/gen/v1/provision"
	"github.com/MiltonJ23/WouriFS/internal/domain"
	interceptor "github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestShareServer_GetShare(t *testing.T) {
	db := NewDB()
	// Simulate a loaded institution
	db.institutions["emf-001"] = &Institution{
		ID:      "emf-001",
		Name:    "Test EMF",
		Share:   "010203",
		ShareIdx: 1,
	}

	srv := NewShareServer(db)

	t.Run("GetShare with valid auth", func(t *testing.T) {
		payload := &domain.TokenPayload{
			UserID:    "admin",
			Namespace: "emf-001",
		}
		ctx := interceptor.SetPayloadInContext(context.Background(), payload)

		resp, err := srv.GetShare(ctx, &provisionpb.GetShareRequest{
			InstitutionId: "emf-001",
			NodeId:       "node-xyz",
		})
		if err != nil {
			t.Fatalf("get share: %v", err)
		}
		if resp.ShareIndex != 1 {
			t.Errorf("expected share index 1, got %d", resp.ShareIndex)
		}
		if len(resp.Share) == 0 {
			t.Error("share must not be empty")
		}
	})

	t.Run("GetShare institution mismatch", func(t *testing.T) {
		payload := &domain.TokenPayload{
			UserID:    "admin",
			Namespace: "emf-002",
		}
		ctx := interceptor.SetPayloadInContext(context.Background(), payload)

		_, err := srv.GetShare(ctx, &provisionpb.GetShareRequest{
			InstitutionId: "emf-001",
			NodeId:       "node-xyz",
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied for institution mismatch, got %v", err)
		}
	})

	t.Run("GetShare without auth fails", func(t *testing.T) {
		_, err := srv.GetShare(context.Background(), &provisionpb.GetShareRequest{
			InstitutionId: "emf-001", NodeId: "node-xyz",
		})
		if err == nil {
			t.Error("expected error without auth")
		}
	})
}

func TestShareServer_RevokeInstitution(t *testing.T) {
	db := NewDB()
	db.institutions["emf-001"] = &Institution{
		ID: "emf-001", Name: "Test EMF", Share: "0102",
		ShareIdx: 1,
	}
	srv := NewShareServer(db)

	t.Run("Revoke with COBAC namespace succeeds", func(t *testing.T) {
		payload := &domain.TokenPayload{
			UserID:    "cobac-auditor",
			Namespace: "cobac",
		}
		ctx := interceptor.SetPayloadInContext(context.Background(), payload)

		_, err := srv.RevokeInstitution(ctx, &provisionpb.RevokeInstitutionRequest{
			InstitutionId: "emf-001",
			Reason:       "license suspended",
		})
		if err != nil {
			t.Fatalf("revoke: %v", err)
		}
		// After revoke, GetShare should fail
		_, _, err = db.GetShare("emf-001", "node-xyz")
		if err == nil {
			t.Error("expected error for revoked institution")
		}
	})

	t.Run("Revoke without COBAC namespace fails", func(t *testing.T) {
		db.institutions["emf-002"] = &Institution{
			ID: "emf-002", Name: "Another EMF", Share: "0304",
			ShareIdx: 1,
		}
		payload := &domain.TokenPayload{
			UserID:    "admin",
			Namespace: "emf-001",
		}
		ctx := interceptor.SetPayloadInContext(context.Background(), payload)

		_, err := srv.RevokeInstitution(ctx, &provisionpb.RevokeInstitutionRequest{
			InstitutionId: "emf-002",
			Reason:       "unauthorized attempt",
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})

	t.Run("Revoke non-existent institution", func(t *testing.T) {
		payload := &domain.TokenPayload{
			UserID: "cobac-auditor", Namespace: "cobac",
		}
		ctx := interceptor.SetPayloadInContext(context.Background(), payload)

		_, err := srv.RevokeInstitution(ctx, &provisionpb.RevokeInstitutionRequest{
			InstitutionId: "ghost-emf",
			Reason:       "testing",
		})
		if status.Code(err) != codes.NotFound {
			t.Errorf("expected NotFound for unknown institution, got %v", err)
		}
	})

	t.Run("Revoke without auth fails", func(t *testing.T) {
		_, err := srv.RevokeInstitution(context.Background(), &provisionpb.RevokeInstitutionRequest{
			InstitutionId: "emf-001", Reason: "test",
		})
		if err == nil {
			t.Error("expected error without auth")
		}
	})
}

func TestShareServer_AuditLog(t *testing.T) {
	db := NewDB()
	db.institutions["emf-001"] = &Institution{
		ID: "emf-001", Name: "Test EMF", Share: "0102",
		ShareIdx: 1,
	}
	// Generate some audit entries
	db.GetShare("emf-001", "node-1")
	db.GetShare("emf-001", "node-2")

	srv := NewShareServer(db)

	t.Run("AuditLog with matching institution namespace", func(t *testing.T) {
		payload := &domain.TokenPayload{
			UserID: "admin", Namespace: "emf-001",
		}
		ctx := interceptor.SetPayloadInContext(context.Background(), payload)

		resp, err := srv.AuditLog(ctx, &provisionpb.AuditLogRequest{
			InstitutionId: "emf-001",
			SinceUnix:    0,
		})
		if err != nil {
			t.Fatalf("audit: %v", err)
		}
		if len(resp.Entries) < 2 {
			t.Errorf("expected at least 2 audit entries, got %d", len(resp.Entries))
		}
	})

	t.Run("AuditLog with COBAC namespace", func(t *testing.T) {
		payload := &domain.TokenPayload{
			UserID: "cobac-auditor", Namespace: "cobac",
		}
		ctx := interceptor.SetPayloadInContext(context.Background(), payload)

		resp, err := srv.AuditLog(ctx, &provisionpb.AuditLogRequest{
			InstitutionId: "emf-001",
			SinceUnix:    0,
		})
		if err != nil {
			t.Fatalf("cobac audit: %v", err)
		}
		if len(resp.Entries) < 2 {
			t.Errorf("expected at least 2 audit entries for COBAC, got %d", len(resp.Entries))
		}
	})

	t.Run("AuditLog with unauthorized namespace", func(t *testing.T) {
		payload := &domain.TokenPayload{
			UserID: "hacker", Namespace: "emf-002",
		}
		ctx := interceptor.SetPayloadInContext(context.Background(), payload)

		_, err := srv.AuditLog(ctx, &provisionpb.AuditLogRequest{
			InstitutionId: "emf-001",
			SinceUnix:    0,
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied for unauthorized namespace, got %v", err)
		}
	})

	t.Run("AuditLog without auth fails", func(t *testing.T) {
		_, err := srv.AuditLog(context.Background(), &provisionpb.AuditLogRequest{
			InstitutionId: "emf-001", SinceUnix: 0,
		})
		if err == nil {
			t.Error("expected error without auth")
		}
	})
}
