package namenode

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	domainnn "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	"github.com/MiltonJ23/WouriFS/internal/domain"
	interceptor "github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*
 * Targeted tests for paths currently below 80 % line coverage.
 */

func TestNameNodeServer_CoverageGaps(t *testing.T) {
	srv := newTestServer()
	ctx := testContext()

	t.Run("LookupFile returns chunk locations", func(t *testing.T) {
		fm, _ := srv.store.CreateFile("/wourifs/test/has_chunks.csv", 0644)
		srv.store.AddChunk(fm.FileID, "c-1", []string{"100.64.0.1:9001", "100.64.0.2:9001"})
		srv.store.AddChunk(fm.FileID, "c-2", []string{})

		resp, err := srv.LookupFile(ctx, &pb.LookupFileRequest{Path: "/wourifs/test/has_chunks.csv"})
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if len(resp.Chunks) != 2 {
			t.Errorf("expected 2 chunks, got %d", len(resp.Chunks))
		}
		if resp.Chunks[0].DatanodeAddress != "100.64.0.1:9001" {
			t.Errorf("expected first replica address, got %q", resp.Chunks[0].DatanodeAddress)
		}
	})

	t.Run("LookupFile default mode", func(t *testing.T) {
		srv.store.PutFile(&FileMeta{FileID: "f-def", Path: "/wourifs/test/default.csv"})
		_, err := srv.LookupFile(ctx, &pb.LookupFileRequest{Path: "/wourifs/test/default.csv"})
		if err != nil {
			t.Fatalf("lookup default mode: %v", err)
		}
	})

	t.Run("DeleteFile on directory returns Internal", func(t *testing.T) {
		srv.store.MakeDir("/wourifs/test/adir")
		_, err := srv.DeleteFile(ctx, &pb.DeleteFileRequest{Path: "/wourifs/test/adir"})
		if status.Code(err) != codes.Internal {
			t.Errorf("expected Internal, got %v", err)
		}
	})

	t.Run("DeleteFile namespace violation", func(t *testing.T) {
		_, err := srv.DeleteFile(ctx, &pb.DeleteFileRequest{Path: "/wourifs/evil/secret.csv"})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})

	t.Run("AllocateChunk zero RF uses default", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			srv.registry.Register(&domainnn.DataNodeStatus{
				ID: "dn-" + string(rune('a'+i)), Address: "100.64.0." + string(rune('1'+i)) + ":9001",
				IsAvailable: true,
			})
		}
		fm, _ := srv.store.CreateFile("/wourifs/test/default_rf.csv", 0644)
		resp, err := srv.AllocateChunk(ctx, &pb.AllocateChunkRequest{
			FileId: fm.FileID, ChunkIndex: 0, ReplicationFactor: 0,
		})
		if err != nil {
			t.Fatalf("allocate: %v", err)
		}
		if len(resp.DatanodeAddresses) != 3 {
			t.Errorf("expected 3 replicas from default RF, got %d", len(resp.DatanodeAddresses))
		}
	})

	t.Run("AllocateChunk RF=2", func(t *testing.T) {
		fm, _ := srv.store.CreateFile("/wourifs/test/rf2.csv", 0644)
		resp, err := srv.AllocateChunk(ctx, &pb.AllocateChunkRequest{
			FileId: fm.FileID, ChunkIndex: 0, ReplicationFactor: 2,
		})
		if err != nil {
			t.Fatalf("allocate rf2: %v", err)
		}
		if len(resp.DatanodeAddresses) != 2 {
			t.Errorf("expected 2 replicas, got %d", len(resp.DatanodeAddresses))
		}
	})

	t.Run("AllocateChunk non-existent file", func(t *testing.T) {
		_, err := srv.AllocateChunk(ctx, &pb.AllocateChunkRequest{
			FileId: "ghost-id", ChunkIndex: 0, ReplicationFactor: 1,
		})
		if status.Code(err) != codes.NotFound {
			t.Errorf("expected NotFound, got %v", err)
		}
	})

	t.Run("RegisterDataNode validates and overwrites duplicates", func(t *testing.T) {
		// Register is idempotent — duplicate overwrites silently
		_, err := srv.RegisterDataNode(context.Background(), &pb.RegisterDataNodeRequest{
			DatanodeId: "dn-a", Address: "100.64.0.1:9001",
			TotalStorageBytes: 2 << 30, FreeStorageBytes: 2 << 30,
		})
		if err != nil {
			t.Errorf("re-register should succeed (idempotent), got %v", err)
		}
		// Register with empty address should return Internal
		_, err = srv.RegisterDataNode(context.Background(), &pb.RegisterDataNodeRequest{
			DatanodeId: "bad-node",
		})
		if status.Code(err) != codes.Internal {
			t.Errorf("expected Internal for invalid node (empty address), got %v", err)
		}
	})

	t.Run("RenameFile directory", func(t *testing.T) {
		srv.store.MakeDir("/wourifs/test/dir_src")
		_, err := srv.RenameFile(ctx, &pb.RenameFileRequest{
			OldPath: "/wourifs/test/dir_src", NewPath: "/wourifs/test/dir_dst",
		})
		if err != nil {
			t.Fatalf("rename dir: %v", err)
		}
		if !srv.store.IsDir("/wourifs/test/dir_dst") {
			t.Error("renamed directory should exist")
		}
	})

	t.Run("CreateFile default mode", func(t *testing.T) {
		resp, err := srv.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/test/defmode.txt"})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		fm, _ := srv.store.GetFile("/wourifs/test/defmode.txt")
		if fm.Mode != 0644 {
			t.Errorf("expected 0644, got %d", fm.Mode)
		}
		_ = resp
	})
}

// RaftFSM unit tests — Apply, Snapshot, Restore.
func TestRaftFSM_ApplySnapshotRestore(t *testing.T) {
	store := NewMetadataStore(3)
	fsm := NewRaftFSM(store)

	t.Run("Apply create_file", func(t *testing.T) {
		entry := WALEntry{Op: "create_file", Path: "/raft/a.txt", FileID: "f-raft-1"}
		b, _ := json.Marshal(entry)
		log := &raft.Log{Data: b, Index: 1, Term: 1, Type: raft.LogCommand}
		fsm.Apply(log)
		if _, err := store.GetFile("/raft/a.txt"); err != nil {
			t.Fatalf("file should exist after Apply: %v", err)
		}
	})

	t.Run("Apply mkdir, rename, truncate sequence", func(t *testing.T) {
		var apply = func(op string, path string, extra func(e *WALEntry)) {
			e := WALEntry{Op: op, Path: path}
			if extra != nil {
				extra(&e)
			}
			b, _ := json.Marshal(e)
			log := &raft.Log{Data: b, Index: 1, Term: 1, Type: raft.LogCommand}
			fsm.Apply(log)
		}

		apply("mkdir", "/raft/dir", nil)
		if !store.IsDir("/raft/dir") {
			t.Error("mkdir via raft FSM failed")
		}
		apply("rename", "/raft/dir", func(e *WALEntry) {
			e.OldPath = "/raft/dir"
			e.NewPath = "/raft/moved"
		})
		if store.IsDir("/raft/dir") {
			t.Error("/raft/dir should be gone after rename")
		}
		if !store.IsDir("/raft/moved") {
			t.Error("/raft/moved should exist after rename")
		}
		apply("rmdir", "/raft/moved", nil)
		if store.IsDir("/raft/moved") {
			t.Error("/raft/moved should be gone after rmdir")
		}
	})

	t.Run("Apply unknown op is no-op", func(t *testing.T) {
		b, _ := json.Marshal(WALEntry{Op: "bogus_op", Path: "/ghost"})
		log := &raft.Log{Data: b, Index: 1, Term: 1, Type: raft.LogCommand}
		fsm.Apply(log) // must not panic
	})

	t.Run("Apply corrupt data does not crash", func(t *testing.T) {
		log := &raft.Log{Data: []byte("not-json"), Index: 1, Term: 1, Type: raft.LogCommand}
		fsm.Apply(log) // must not panic
	})

	t.Run("Snapshot and Persist", func(t *testing.T) {
		snap, err := fsm.Snapshot()
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		sink := &testSink{}
		if err := snap.Persist(sink); err != nil {
			t.Fatalf("persist: %v", err)
		}
		if len(sink.written) == 0 {
			t.Error("snapshot sink should have received data")
		}
		snap.Release()
	})

	t.Run("Snapshot Persist with write error cancels", func(t *testing.T) {
		snap, _ := fsm.Snapshot()
		sink := &testSink{failWrite: true}
		err := snap.Persist(sink)
		if err == nil {
			t.Error("expected error on write failure")
		}
		if !sink.cancelled {
			t.Error("sink should be cancelled on write failure")
		}
		snap.Release()
	})

	t.Run("Restore from snapshot", func(t *testing.T) {
		// Build a snapshot from a clean store
		cleanStore := NewMetadataStore(3)
		cleanStore.MakeDir("/restore/dir")
		cleanStore.CreateFile("/restore/file.txt", 0644)

		snap, _ := (&RaftFSM{store: cleanStore}).Snapshot()
		sink := &testSink{}
		snap.Persist(sink)

		// Restore into fsm's store
		rc := &bytesReadCloser{data: sink.written}
		if err := fsm.Restore(rc); err != nil {
			t.Fatalf("restore: %v", err)
		}
		if !store.IsDir("/restore/dir") {
			t.Error("restored store should have /restore/dir")
		}
		if _, err := store.GetFile("/restore/file.txt"); err != nil {
			t.Error("restored store should have /restore/file.txt")
		}
	})

	t.Run("Restore corrupt data returns error", func(t *testing.T) {
		err := fsm.Restore(&bytesReadCloser{data: []byte("not-json")})
		if err == nil {
			t.Error("expected error restoring corrupt data")
		}
	})

	t.Run("Store returns metadata store", func(t *testing.T) {
		if fsm.Store() != store {
			t.Error("Store() should return the underlying MetadataStore")
		}
	})
}

// testSink implements raft.SnapshotSink for testing.
type testSink struct {
	written   []byte
	failWrite bool
	cancelled bool
}

func (s *testSink) ID() string                    { return "test-sink" }
func (s *testSink) Cancel() error                  { s.cancelled = true; return nil }
func (s *testSink) Write(b []byte) (int, error) {
	if s.failWrite {
		return 0, io.ErrShortWrite
	}
	s.written = append(s.written, b...)
	return len(b), nil
}
func (s *testSink) Close() error { return nil }

// bytesReadCloser implements io.ReadCloser over a byte slice.
type bytesReadCloser struct {
	data   []byte
	offset int
}

func (r *bytesReadCloser) Read(b []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(b, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *bytesReadCloser) Close() error { return nil }

// Compile-time guard: ensure imports are used
var _ = interceptor.SetPayloadInContext
var _ = domain.TokenPayload{}
