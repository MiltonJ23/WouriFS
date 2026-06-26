package namenode

import (
	"context"
	"testing"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	"github.com/MiltonJ23/WouriFS/internal/domain"
	domainnn "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	interceptor "github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestNameNodeServer_FileOps tests file operations via direct handler with auth context.
func TestNameNodeServer_FileOps(t *testing.T) {
	store := NewMetadataStore(3)
	reg := domainnn.NewInMemoryDataNodeRegistry()

	nn := NewNameNodeServer(store, reg, nil)

	payload := &domain.TokenPayload{
		UserID:    "user-1",
		Username:  "auditor",
		Namespace: "/wourifs/test",
	}

	ctx := interceptor.SetPayloadInContext(context.Background(), payload)

	t.Run("CreateFile with valid auth", func(t *testing.T) {
		resp, err := nn.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/test/f.csv"})
		if err != nil {
			t.Fatalf("create file: %v", err)
		}
		if resp.FileId == "" {
			t.Error("file ID must not be empty")
		}
	})

	t.Run("CreateFile namespace violation", func(t *testing.T) {
		_, err := nn.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/other/f.csv"})
		if err == nil || status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})

	t.Run("CreateFile duplicate", func(t *testing.T) {
		_, err := nn.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/test/f.csv"})
		if err == nil || status.Code(err) != codes.AlreadyExists {
			t.Errorf("expected AlreadyExists, got %v", err)
		}
	})

	t.Run("LookupFile existing", func(t *testing.T) {
		resp, err := nn.LookupFile(ctx, &pb.LookupFileRequest{Path: "/wourifs/test/f.csv"})
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if resp.FileId == "" {
			t.Error("file ID must not be empty")
		}
	})

	t.Run("LookupFile non-existent", func(t *testing.T) {
		_, err := nn.LookupFile(ctx, &pb.LookupFileRequest{Path: "/wourifs/test/ghost.csv"})
		if err == nil || status.Code(err) != codes.NotFound {
			t.Errorf("expected NotFound, got %v", err)
		}
	})

	t.Run("DeleteFile valid", func(t *testing.T) {
		nn.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/test/del.csv"})
		_, err := nn.DeleteFile(ctx, &pb.DeleteFileRequest{Path: "/wourifs/test/del.csv"})
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
	})

	t.Run("DeleteFile non-existent", func(t *testing.T) {
		_, err := nn.DeleteFile(ctx, &pb.DeleteFileRequest{Path: "/wourifs/test/ghost2.csv"})
		if err == nil || status.Code(err) != codes.NotFound {
			t.Errorf("expected NotFound, got %v", err)
		}
	})

	t.Run("ListDirectory", func(t *testing.T) {
		nn.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/test/a.csv"})
		nn.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/test/b.csv"})

		resp, err := nn.ListDirectory(ctx, &pb.ListDirectoryRequest{Path: "/wourifs/test"})
		if err != nil {
			t.Fatalf("list dir: %v", err)
		}
		if len(resp.Entries) < 1 {
			t.Error("expected at least 1 entry")
		}
	})

	t.Run("ListDirectory namespace violation", func(t *testing.T) {
		_, err := nn.ListDirectory(ctx, &pb.ListDirectoryRequest{Path: "/wourifs/evil"})
		if err == nil || status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})

	t.Run("AllocateChunk with available nodes", func(t *testing.T) {
		// Register 3 datanodes
		reg.Register(&domainnn.DataNodeStatus{
			ID: "dn-a", Address: "100.64.0.1:9001", IsAvailable: true,
		})
		reg.Register(&domainnn.DataNodeStatus{
			ID: "dn-b", Address: "100.64.0.2:9001", IsAvailable: true,
		})
		reg.Register(&domainnn.DataNodeStatus{
			ID: "dn-c", Address: "100.64.0.3:9001", IsAvailable: true,
		})

		// Create file and allocate chunk
		fresp, _ := nn.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/test/big.csv"})
		resp, err := nn.AllocateChunk(ctx, &pb.AllocateChunkRequest{
			FileId:            fresp.FileId,
			ChunkIndex:        0,
			ReplicationFactor: 3,
		})
		if err != nil {
			t.Fatalf("allocate: %v", err)
		}
		if len(resp.DatanodeAddresses) != 3 {
			t.Errorf("expected 3 datanode addresses, got %d", len(resp.DatanodeAddresses))
		}
	})
}
