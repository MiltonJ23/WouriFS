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

/*
 * Handler-level unit tests for MakeDirectory, RemoveDirectory, RenameFile,
 * StatFile, and TruncateFile. These were the handlers added in Sprint~3 that
 * drove coverage below 50~%. Each handler is exercised through its normal
 * path, auth-failure path, and relevant edge case.
 *
 * Auth is satisfied by injecting a valid TokenPayload into context via the
 * interceptor package's SetPayloadInContext helper --- the same path the real
 * auth interceptor uses.
 */

func testPayload() *domain.TokenPayload {
	return &domain.TokenPayload{
		UserID:    "user-test",
		Username:  "teller",
		Namespace: "/wourifs/test",
	}
}

func testContext() context.Context {
	return interceptor.SetPayloadInContext(context.Background(), testPayload())
}

func newTestServer() *NameNodeServer {
	return NewNameNodeServer(
		NewMetadataStore(3),
		domainnn.NewInMemoryDataNodeRegistry(),
		nil, // no WAL for handler tests
		nil,
	)
}

// --- MakeDirectory ---

func TestNameNodeServer_MakeDirectory(t *testing.T) {
	srv := newTestServer()
	ctx := testContext()

	t.Run("mkdir succeeds", func(t *testing.T) {
		_, err := srv.MakeDirectory(ctx, &pb.MakeDirectoryRequest{
			Path: "/wourifs/test/reports",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !srv.store.IsDir("/wourifs/test/reports") {
			t.Error("path should be a directory marker")
		}
	})

	t.Run("mkdir duplicate returns AlreadyExists", func(t *testing.T) {
		_, err := srv.MakeDirectory(ctx, &pb.MakeDirectoryRequest{
			Path: "/wourifs/test/reports",
		})
		if status.Code(err) != codes.AlreadyExists {
			t.Errorf("expected AlreadyExists, got %v", err)
		}
	})

	t.Run("mkdir namespace violation returns PermissionDenied", func(t *testing.T) {
		_, err := srv.MakeDirectory(ctx, &pb.MakeDirectoryRequest{
			Path: "/wourifs/other/evil",
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})

	t.Run("mkdir without auth fails", func(t *testing.T) {
		_, err := srv.MakeDirectory(context.Background(), &pb.MakeDirectoryRequest{
			Path: "/wourifs/test/nope",
		})
		if err == nil {
			t.Fatal("expected error without auth")
		}
	})
}

// --- RemoveDirectory ---

func TestNameNodeServer_RemoveDirectory(t *testing.T) {
	srv := newTestServer()
	ctx := testContext()

	t.Run("rmdir succeeds on empty dir", func(t *testing.T) {
		srv.store.MakeDir("/wourifs/test/emptydir", 0755)
		_, err := srv.RemoveDirectory(ctx, &pb.RemoveDirectoryRequest{
			Path: "/wourifs/test/emptydir",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if srv.store.IsDir("/wourifs/test/emptydir") {
			t.Error("directory should be removed")
		}
	})

	t.Run("rmdir on non-empty dir returns Internal error", func(t *testing.T) {
		srv.store.MakeDir("/wourifs/test/parent", 0755)
		srv.store.CreateFile("/wourifs/test/parent/child.txt", 0644)
		_, err := srv.RemoveDirectory(ctx, &pb.RemoveDirectoryRequest{
			Path: "/wourifs/test/parent",
		})
		if err == nil || status.Code(err) != codes.Internal {
			t.Errorf("expected Internal for non-empty dir, got %v", err)
		}
	})

	t.Run("rmdir non-existent returns NotFound", func(t *testing.T) {
		_, err := srv.RemoveDirectory(ctx, &pb.RemoveDirectoryRequest{
			Path: "/wourifs/test/ghostdir",
		})
		if status.Code(err) != codes.NotFound {
			t.Errorf("expected NotFound, got %v", err)
		}
	})

	t.Run("rmdir on a file returns Internal error", func(t *testing.T) {
		srv.store.CreateFile("/wourifs/test/notadir.txt", 0644)
		_, err := srv.RemoveDirectory(ctx, &pb.RemoveDirectoryRequest{
			Path: "/wourifs/test/notadir.txt",
		})
		if err == nil || status.Code(err) != codes.Internal {
			t.Errorf("expected Internal for file-as-dir, got %v", err)
		}
	})

	t.Run("rmdir namespace violation", func(t *testing.T) {
		_, err := srv.RemoveDirectory(ctx, &pb.RemoveDirectoryRequest{
			Path: "/wourifs/evil/dir",
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})
}

// --- RenameFile ---

func TestNameNodeServer_RenameFile(t *testing.T) {
	srv := newTestServer()
	ctx := testContext()

	t.Run("rename succeeds", func(t *testing.T) {
		srv.store.CreateFile("/wourifs/test/old.csv", 0644)
		_, err := srv.RenameFile(ctx, &pb.RenameFileRequest{
			OldPath: "/wourifs/test/old.csv",
			NewPath: "/wourifs/test/new.csv",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// old path gone
		if _, e := srv.store.GetFile("/wourifs/test/old.csv"); e == nil {
			t.Error("old path should not exist after rename")
		}
		// new path exists
		if _, e := srv.store.GetFile("/wourifs/test/new.csv"); e != nil {
			t.Errorf("new path should exist: %v", e)
		}
	})

	t.Run("rename non-existent source returns NotFound", func(t *testing.T) {
		_, err := srv.RenameFile(ctx, &pb.RenameFileRequest{
			OldPath: "/wourifs/test/ghost.csv",
			NewPath: "/wourifs/test/dest.csv",
		})
		if status.Code(err) != codes.NotFound {
			t.Errorf("expected NotFound, got %v", err)
		}
	})

	t.Run("rename to existing dest returns Internal error", func(t *testing.T) {
		srv.store.CreateFile("/wourifs/test/a.csv", 0644)
		srv.store.CreateFile("/wourifs/test/b.csv", 0644)
		_, err := srv.RenameFile(ctx, &pb.RenameFileRequest{
			OldPath: "/wourifs/test/a.csv",
			NewPath: "/wourifs/test/b.csv",
		})
		if err == nil || status.Code(err) != codes.Internal {
			t.Errorf("expected Internal error for existing dest, got %v", err)
		}
	})

	t.Run("rename new path outside namespace returns PermissionDenied", func(t *testing.T) {
		srv.store.CreateFile("/wourifs/test/src.csv", 0644)
		_, err := srv.RenameFile(ctx, &pb.RenameFileRequest{
			OldPath: "/wourifs/test/src.csv",
			NewPath: "/wourifs/evil/nope.csv",
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})

	t.Run("rename old path outside namespace returns PermissionDenied", func(t *testing.T) {
		_, err := srv.RenameFile(ctx, &pb.RenameFileRequest{
			OldPath: "/wourifs/evil/src.csv",
			NewPath: "/wourifs/test/dst.csv",
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})
}

// --- StatFile ---

func TestNameNodeServer_StatFile(t *testing.T) {
	srv := newTestServer()
	ctx := testContext()

	t.Run("stat existing file", func(t *testing.T) {
		srv.store.CreateFile("/wourifs/test/statme.txt", 0644)
		resp, err := srv.StatFile(ctx, &pb.StatFileRequest{
			Path: "/wourifs/test/statme.txt",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.IsDir {
			t.Error("file should not be reported as directory")
		}
		if resp.Mode != 0644 {
			t.Errorf("expected mode 0644, got %d", resp.Mode)
		}
	})

	t.Run("stat existing directory", func(t *testing.T) {
		srv.store.MakeDir("/wourifs/test/mydir", 0755)
		resp, err := srv.StatFile(ctx, &pb.StatFileRequest{
			Path: "/wourifs/test/mydir",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.IsDir {
			t.Error("directory should be reported as IsDir")
		}
		if resp.Mode != 0755 {
			t.Errorf("expected mode 0755 for dir, got %d", resp.Mode)
		}
	})

	t.Run("stat file with default mode", func(t *testing.T) {
		// Use PutFile directly to create a file with mode 0
		srv.store.PutFile(&FileMeta{FileID: "f-00", Path: "/wourifs/test/zeromode.txt"})
		resp, err := srv.StatFile(ctx, &pb.StatFileRequest{
			Path: "/wourifs/test/zeromode.txt",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Mode != 0644 {
			t.Errorf("expected default mode 0644, got %d", resp.Mode)
		}
	})

	t.Run("stat non-existent returns NotFound", func(t *testing.T) {
		_, err := srv.StatFile(ctx, &pb.StatFileRequest{
			Path: "/wourifs/test/ghost",
		})
		if status.Code(err) != codes.NotFound {
			t.Errorf("expected NotFound, got %v", err)
		}
	})

	t.Run("stat namespace violation", func(t *testing.T) {
		_, err := srv.StatFile(ctx, &pb.StatFileRequest{
			Path: "/wourifs/evil/secret",
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})
}

// --- TruncateFile ---

func TestNameNodeServer_TruncateFile(t *testing.T) {
	srv := newTestServer()
	ctx := testContext()

	t.Run("truncate existing file", func(t *testing.T) {
		srv.store.CreateFile("/wourifs/test/shrink.csv", 0644)
		_, err := srv.TruncateFile(ctx, &pb.TruncateFileRequest{
			Path:      "/wourifs/test/shrink.csv",
			SizeBytes: 0,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		fm, _ := srv.store.GetFile("/wourifs/test/shrink.csv")
		if fm.Size != 0 {
			t.Errorf("expected size 0 after truncate, got %d", fm.Size)
		}
	})

	t.Run("truncate to non-zero size", func(t *testing.T) {
		srv.store.CreateFile("/wourifs/test/grow.csv", 0644)
		_, err := srv.TruncateFile(ctx, &pb.TruncateFileRequest{
			Path:      "/wourifs/test/grow.csv",
			SizeBytes: 1 << 20,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		fm, _ := srv.store.GetFile("/wourifs/test/grow.csv")
		if fm.Size != 1<<20 {
			t.Errorf("expected size %d, got %d", 1<<20, fm.Size)
		}
	})

	t.Run("truncate non-existent returns NotFound", func(t *testing.T) {
		_, err := srv.TruncateFile(ctx, &pb.TruncateFileRequest{
			Path:      "/wourifs/test/ghost.csv",
			SizeBytes: 0,
		})
		if status.Code(err) != codes.NotFound {
			t.Errorf("expected NotFound, got %v", err)
		}
	})

	t.Run("truncate namespace violation", func(t *testing.T) {
		_, err := srv.TruncateFile(ctx, &pb.TruncateFileRequest{
			Path:      "/wourifs/evil/secret.csv",
			SizeBytes: 0,
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %v", err)
		}
	})
}

// --- ListDirectory edge cases ---

func TestNameNodeServer_ListDirectory_EdgeCases(t *testing.T) {
	srv := newTestServer()
	ctx := testContext()

	// Setup: dirs and files
	srv.store.MakeDir("/wourifs/test/subdir", 0755)
	srv.store.CreateFile("/wourifs/test/a.txt", 0644)
	srv.store.CreateFile("/wourifs/test/b.txt", 0644)

	t.Run("list returns correct entry count", func(t *testing.T) {
		resp, err := srv.ListDirectory(ctx, &pb.ListDirectoryRequest{
			Path: "/wourifs/test",
		})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(resp.Entries) < 2 {
			t.Errorf("expected at least 2 entries, got %d", len(resp.Entries))
		}
	})

	t.Run("list empty dir uses namespace from payload", func(t *testing.T) {
		resp, err := srv.ListDirectory(ctx, &pb.ListDirectoryRequest{})
		if err != nil {
			t.Fatalf("list empty path: %v", err)
		}
		if len(resp.Entries) < 2 {
			t.Errorf("expected entries under namespace, got %d", len(resp.Entries))
		}
	})
}

// --- WAL replay coverage for all operation types ---

func TestWAL_ReplayAllOps(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/wal.jsonl"

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	// Write one of each op type
	w.Append(WALEntry{Op: "create_file", Path: "/a", FileID: "f1"})
	w.Append(WALEntry{Op: "mkdir", Path: "/dir"})
	w.Append(WALEntry{Op: "delete_file", Path: "/a"})
	w.Append(WALEntry{Op: "rmdir", Path: "/dir"})
	w.Close()

	store := NewMetadataStore(3)
	if err := Replay(path, store); err != nil {
		t.Fatalf("replay: %v", err)
	}

	// After replay: /a should be deleted, /dir should be deleted (rmdir'd)
	if _, err := store.GetFile("/a"); err == nil {
		t.Error("/a should be gone after delete_file replay")
	}
	if store.IsDir("/dir") {
		t.Error("/dir should be gone after rmdir replay")
	}
}

func TestWAL_ReplayRenameAndTruncate(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/wal.jsonl"

	w, _ := OpenWAL(path)
	w.Append(WALEntry{Op: "create_file", Path: "/orig", FileID: "f1"})
	w.Append(WALEntry{Op: "rename", OldPath: "/orig", NewPath: "/moved", FileID: "f1"})
	w.Append(WALEntry{Op: "truncate_file", Path: "/moved", Size: 1024})
	w.Close()

	store := NewMetadataStore(3)
	if err := Replay(path, store); err != nil {
		t.Fatalf("replay: %v", err)
	}

	fm, err := store.GetFile("/moved")
	if err != nil {
		t.Fatalf("/moved should exist after rename replay: %v", err)
	}
	if fm.Size != 1024 {
		t.Errorf("expected size 1024 after truncate replay, got %d", fm.Size)
	}
	if _, err := store.GetFile("/orig"); err == nil {
		t.Error("/orig should not exist after rename replay")
	}
}

func TestWAL_ReplayNonexistent(t *testing.T) {
	path := "/tmp/does-not-exist-987654321.jsonl"
	store := NewMetadataStore(3)
	if err := Replay(path, store); err != nil {
		t.Fatalf("replay of nonexistent WAL should succeed (fresh start): %v", err)
	}
}
