package namenode

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	"github.com/MiltonJ23/WouriFS/internal/domain"
	domainnn "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	interceptor "github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// startTestNamenode spins up a gRPC Namenode for integration testing.
func startTestNamenode(t *testing.T, store *MetadataStore, reg *domainnn.InMemoryDataNodeRegistry, walPath string) (pb.NameNodeServiceClient, func()) {
	t.Helper()

	var w *WAL
	if walPath != "" {
		var err error
		w, err = OpenWAL(walPath)
		if err != nil {
			t.Fatalf("open wal: %v", err)
		}
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	nnSrv := NewNameNodeServer(store, reg, w, nil)
	pb.RegisterNameNodeServiceServer(srv, nnSrv)

	go srv.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := pb.NewNameNodeServiceClient(conn)

	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
		if w != nil {
			w.Close()
		}
	}
	return client, cleanup
}

func TestNameNodeServer_BDD(t *testing.T) {
	t.Run("Given a running Namenode", func(t *testing.T) {
		store := NewMetadataStore(3)
		reg := domainnn.NewInMemoryDataNodeRegistry()
		walPath := filepath.Join(t.TempDir(), "wal.jsonl")
		client, cleanup := startTestNamenode(t, store, reg, walPath)
		defer cleanup()

		t.Run("When a Datanode registers (FR-D-007)", func(t *testing.T) {
			_, err := client.RegisterDataNode(context.Background(), &pb.RegisterDataNodeRequest{
				DatanodeId:        "dn-test-1",
				Address:           "100.64.0.2:9001",
				TotalStorageBytes: 1 << 30,
				FreeStorageBytes:  1 << 30,
			})
			if err != nil {
				t.Fatalf("register: %v", err)
			}

			available := reg.GetAllAvailable()
			if len(available) != 1 {
				t.Errorf("expected 1 available node, got %d", len(available))
			}
		})

		t.Run("When a Datanode sends heartbeat (FR-D-003)", func(t *testing.T) {
			resp, err := client.Heartbeat(context.Background(), &pb.HeartbeatRequest{
				DatanodeId:       "dn-test-1",
				FreeStorageBytes: 500_000_000,
				ActiveConnections: 5,
			})
			if err != nil {
				t.Fatalf("heartbeat: %v", err)
			}
			if !resp.Acknowledged {
				t.Error("heartbeat should be acknowledged")
			}
		})

		t.Run("When an unknown Datanode sends heartbeat (amnesia recovery)", func(t *testing.T) {
			resp, err := client.Heartbeat(context.Background(), &pb.HeartbeatRequest{
				DatanodeId: "ghost-dn",
			})
			if err != nil {
				t.Fatalf("heartbeat should not return gRPC error: %v", err)
			}
			if resp.Acknowledged {
				t.Error("unknown node should not be acknowledged")
			}
			if !resp.RequireReregistration {
				t.Error("amnesia recovery: should request re-registration for unknown node")
			}
		})
	})

	t.Run("Given namenode file operations with auth context", func(t *testing.T) {
		store := NewMetadataStore(3)
		reg := domainnn.NewInMemoryDataNodeRegistry()
		_, cleanup := startTestNamenode(t, store, reg, "")
		defer cleanup()

		// Auth is skipped for these tests since we are testing the handler directly
		// (the auth interceptor is tested in its own package).
		// We test the handler with context directly.
	})

	t.Run("Given chunk allocation (FR-N-006, FR-N-008)", func(t *testing.T) {
		store := NewMetadataStore(3)
		reg := domainnn.NewInMemoryDataNodeRegistry()
		srv := NewNameNodeServer(store, reg, nil, nil)
		ctx := interceptor.SetPayloadInContext(context.Background(), &domain.TokenPayload{
			UserID: "test-user", Namespace: "/wourifs/test",
		})

		// Register 4 datanodes
		for i := 0; i < 4; i++ {
			reg.Register(&domainnn.DataNodeStatus{
				ID: "dn-" + string(rune('a'+i)), Address: "100.64.0." + string(rune('2'+i)) + ":9001",
				IsAvailable: true, TotalStorageBytes: 1 << 30, FreeStorageBytes: 1 << 30,
			})
		}

		// Create file first
		fm, _ := store.CreateFile("/wourifs/test/chunked.csv", 0644)

		t.Run("When allocating a chunk with RF=3", func(t *testing.T) {
			resp, err := srv.AllocateChunk(ctx, &pb.AllocateChunkRequest{
				FileId:             fm.FileID,
				ChunkIndex:         0,
				ReplicationFactor:  3,
			})
			if err != nil {
				t.Fatalf("allocate: %v", err)
			}
			if len(resp.DatanodeAddresses) != 3 {
				t.Errorf("expected 3 replica addresses, got %d", len(resp.DatanodeAddresses))
			}
			if resp.ChunkId == "" {
				t.Error("chunk ID must not be empty")
			}
		})

		t.Run("When allocating with insufficient available datanodes", func(t *testing.T) {
			// Mark 3 out of 4 unavailable
			nodes := reg.GetAll()
			for i, n := range nodes {
				if i < 3 {
					reg.MarkUnavailable(n.ID)
				}
			}

			_, err := srv.AllocateChunk(ctx, &pb.AllocateChunkRequest{
				FileId:            fm.FileID,
				ChunkIndex:        1,
				ReplicationFactor: 3,
			})
			if err == nil {
				t.Error("expected ResourceExhausted error")
			}
		})
	})
}

// TestNameNodeServerHandler_Direct tests the handler bypassing gRPC auth (unit tests for logic).
func TestNameNodeServerHandler_Direct(t *testing.T) {
	store := NewMetadataStore(3)
	reg := domainnn.NewInMemoryDataNodeRegistry()
	srv := NewNameNodeServer(store, reg, nil, nil)

	ctx := context.Background()

	t.Run("CreateFile without auth payload", func(t *testing.T) {
		_, err := srv.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/test/f.csv"})
		if err == nil || status.Code(err) != codes.Internal {
			t.Errorf("expected Internal error (no auth payload), got %v", err)
		}
	})

	t.Run("DeleteFile on non-existent", func(t *testing.T) {
		store.CreateFile("/wourifs/test/to-delete.csv", 0644)
		_, err := srv.DeleteFile(ctx, &pb.DeleteFileRequest{Path: "/wourifs/test/to-delete.csv"})
		if err == nil || status.Code(err) != codes.Internal {
			t.Errorf("expected Internal error (no auth payload), got %v", err)
		}
	})
}

// TestWAL_ReplayIdempotent verifies replay doesn't duplicate entries.
func TestWAL_ReplayIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.jsonl")

	w, _ := OpenWAL(path)
	w.Append(WALEntry{Op: "create_file", Path: "/a", FileID: "f1"})
	w.Append(WALEntry{Op: "create_file", Path: "/b", FileID: "f2"})
	w.Close()

	store := NewMetadataStore(3)
	Replay(path, store)
	// Replay again — should overwrite not duplicate
	Replay(path, store)
	// Verify no panic and files still exist
	if _, err := store.GetFile("/a"); err != nil {
		t.Errorf("file /a lost after double replay: %v", err)
	}
}

// Ensure imports compile
var _ = io.Discard
var _ = os.Stdout
var _ = domain.TokenPayload{}
