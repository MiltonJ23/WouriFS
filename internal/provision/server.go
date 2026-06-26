package provision

import (
	"context"

	provisionpb "github.com/MiltonJ23/WouriFS/api/gen/v1/provision"
	"github.com/MiltonJ23/WouriFS/internal/domain"
	interceptor "github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ShareServer implements the COBAC ShareService gRPC interface.
type ShareServer struct {
	provisionpb.UnimplementedShareServiceServer
	db *DB
}

// NewShareServer creates a gRPC handler for COBAC share delivery.
func NewShareServer(db *DB) *ShareServer {
	return &ShareServer{db: db}
}

// GetShare delivers the institution's Shamir share after JWT+mTLS authentication.
func (s *ShareServer) GetShare(ctx context.Context, req *provisionpb.GetShareRequest) (*provisionpb.GetShareResponse, error) {
	payload, err := interceptor.PayloadFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// Verify the JWT institution matches the request
	if payload.Namespace != req.InstitutionId {
		return nil, status.Error(codes.PermissionDenied, "institution mismatch in JWT")
	}

	share, shareIdx, err := s.db.GetShare(req.InstitutionId, req.NodeId)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	return &provisionpb.GetShareResponse{
		Share:     share,
		ShareIndex: shareIdx,
	}, nil
}

// RevokeInstitution permanently blocks an institution.
// Only callable by COBAC auditors with appropriate JWT claims.
func (s *ShareServer) RevokeInstitution(ctx context.Context, req *provisionpb.RevokeInstitutionRequest) (*provisionpb.RevokeInstitutionResponse, error) {
	payload, err := interceptor.PayloadFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// Only COBAC root namespace can revoke
	if payload.Namespace != "cobac" {
		return nil, status.Error(codes.PermissionDenied, "only COBAC can revoke institutions")
	}

	if err := s.db.Revoke(req.InstitutionId, req.Reason); err != nil {
		return nil, status.Errorf(codes.NotFound, "revoke: %v", err)
	}

	return &provisionpb.RevokeInstitutionResponse{}, nil
}

// AuditLog returns the provisioning audit trail.
func (s *ShareServer) AuditLog(ctx context.Context, req *provisionpb.AuditLogRequest) (*provisionpb.AuditLogResponse, error) {
	payload, err := interceptor.PayloadFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if payload.Namespace != "cobac" && payload.Namespace != req.InstitutionId {
		return nil, status.Error(codes.PermissionDenied, "audit access denied")
	}

	entries := s.db.AuditTrail(req.InstitutionId, req.SinceUnix)
	pbEntries := make([]*provisionpb.ProvisionEntry, len(entries))
	for i, e := range entries {
		pbEntries[i] = &provisionpb.ProvisionEntry{
			InstitutionId: e.InstitutionID,
			NodeId:        e.NodeID,
			TimestampUnix: e.TimestampUnix,
			Action:        e.Action,
		}
	}
	return &provisionpb.AuditLogResponse{Entries: pbEntries}, nil
}

// Compile-time check for domain import usage
var _ = domain.TokenPayload{}
