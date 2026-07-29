package namenode

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MiltonJ23/WouriFS/internal/domain"
)

func newTestStore() *MetadataStore {
	return NewMetadataStore(3)
}

func TestMetadataStore_BDD(t *testing.T) {
	t.Run("Given a fresh metadata store", func(t *testing.T) {
		store := newTestStore()

		t.Run("When creating a new file", func(t *testing.T) {
			fm, err := store.CreateFile("/wourifs/test/ledger.csv", 0644)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if fm.FileID == "" {
				t.Error("file ID must not be empty")
			}
			if fm.Path != "/wourifs/test/ledger.csv" {
				t.Errorf("expected path /wourifs/test/ledger.csv, got %s", fm.Path)
			}
		})

		t.Run("When creating a duplicate file path", func(t *testing.T) {
			store.CreateFile("/wourifs/test/dup.csv", 0644)
			_, err := store.CreateFile("/wourifs/test/dup.csv", 0644)
			if err != ErrFileExists {
				t.Errorf("expected ErrFileExists, got %v", err)
			}
		})

		t.Run("When looking up a non-existent file", func(t *testing.T) {
			_, err := store.GetFile("/wourifs/test/ghost.csv")
			if err != ErrFileNotFound {
				t.Errorf("expected ErrFileNotFound, got %v", err)
			}
		})
	})

	t.Run("Given a file in the store", func(t *testing.T) {
		store := newTestStore()
		store.CreateFile("/wourifs/test/data.csv", 0644)

		t.Run("When looking it up by path", func(t *testing.T) {
			fm, err := store.GetFile("/wourifs/test/data.csv")
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if fm.Path != "/wourifs/test/data.csv" {
				t.Errorf("unexpected path: %s", fm.Path)
			}
		})

		t.Run("When deleting the file", func(t *testing.T) {
			if err := store.DeleteFile("/wourifs/test/data.csv"); err != nil {
				t.Fatalf("delete: %v", err)
			}
			_, err := store.GetFile("/wourifs/test/data.csv")
			if err != ErrFileNotFound {
				t.Errorf("expected ErrFileNotFound after delete, got %v", err)
			}
		})

		t.Run("When deleting a non-existent file", func(t *testing.T) {
			err := store.DeleteFile("/wourifs/test/ghost2.csv")
			if err != ErrFileNotFound {
				t.Errorf("expected ErrFileNotFound, got %v", err)
			}
		})
	})

	t.Run("Given chunk allocation on a file", func(t *testing.T) {
		store := newTestStore()
		fm, _ := store.CreateFile("/wourifs/test/big.csv", 0644)

		t.Run("When adding a chunk with replicas", func(t *testing.T) {
			replicas := []string{"dn1:9001", "dn2:9001", "dn3:9001"}
			err := store.AddChunk(fm.FileID, "chunk-abc", replicas)
			if err != nil {
				t.Fatalf("add chunk: %v", err)
			}

			got, _ := store.GetFile("/wourifs/test/big.csv")
			if len(got.Chunks) != 1 {
				t.Fatalf("expected 1 chunk, got %d", len(got.Chunks))
			}
			if got.Chunks[0].ChunkID != "chunk-abc" {
				t.Errorf("expected chunk-abc, got %s", got.Chunks[0].ChunkID)
			}
			if len(got.Chunks[0].Replicas) != 3 {
				t.Errorf("expected 3 replicas, got %d", len(got.Chunks[0].Replicas))
			}
		})

		t.Run("When adding chunk to non-existent file", func(t *testing.T) {
			err := store.AddChunk("bad-file-id", "chunk-xyz", []string{"dn1:9001"})
			if err != ErrFileNotFound {
				t.Errorf("expected ErrFileNotFound, got %v", err)
			}
		})
	})

	t.Run("Given namespace isolation (FR-N-007)", func(t *testing.T) {
		store := newTestStore()

		payload := &domain.TokenPayload{
			UserID:    "user-1",
			Username:  "auditor_a",
			Namespace: "/wourifs/audit/a",
		}

		t.Run("When accessing own namespace path", func(t *testing.T) {
			err := store.CheckNamespace(payload, "/wourifs/audit/a/ledger.csv")
			if err != nil {
				t.Errorf("expected access granted for own namespace, got %v", err)
			}
		})

		t.Run("When accessing another namespace path", func(t *testing.T) {
			err := store.CheckNamespace(payload, "/wourifs/audit/b/ledger.csv")
			if err != ErrNamespaceDenied {
				t.Errorf("expected ErrNamespaceDenied for cross-namespace access, got %v", err)
			}
		})

		t.Run("When payload is nil", func(t *testing.T) {
			err := store.CheckNamespace(nil, "/wourifs/audit/a/ledger.csv")
			if err != ErrNamespaceDenied {
				t.Errorf("expected ErrNamespaceDenied for nil payload, got %v", err)
			}
		})
	})

	t.Run("Given directory listing", func(t *testing.T) {
		store := newTestStore()
		store.CreateFile("/wourifs/test/a/one.csv", 0644)
		store.CreateFile("/wourifs/test/a/two.csv", 0644)
		store.CreateFile("/wourifs/test/b/three.csv", 0644)

		t.Run("When listing a directory with files", func(t *testing.T) {
			entries := store.ListDirectory("/wourifs/test")
			if len(entries) != 2 {
				t.Errorf("expected 2 entries (a, b), got %d: %+v", len(entries), entries)
			}
		})

		t.Run("When listing an empty directory", func(t *testing.T) {
			entries := store.ListDirectory("/wourifs/nonexistent")
			if len(entries) != 0 {
				t.Errorf("expected 0 entries, got %d", len(entries))
			}
		})
	})

	t.Run("Given snapshot and restore", func(t *testing.T) {
		store := newTestStore()
		store.CreateFile("/wourifs/test/snap.csv", 0644)

		snap := store.Snapshot()

		newStore := NewMetadataStore(3)
		newStore.Restore(snap)

		fm, err := newStore.GetFile("/wourifs/test/snap.csv")
		if err != nil {
			t.Fatalf("restore: file not found: %v", err)
		}
		if fm.Path != "/wourifs/test/snap.csv" {
			t.Errorf("restored path mismatch: %s", fm.Path)
		}
	})

	t.Run("Given replication factor config (FR-N-008)", func(t *testing.T) {
		t.Run("When using default RF", func(t *testing.T) {
			store := NewMetadataStore(0) // should default to 3
			if store.ReplicationFactor() != 3 {
				t.Errorf("expected default rf=3, got %d", store.ReplicationFactor())
			}
		})

		t.Run("When setting custom RF", func(t *testing.T) {
			store := NewMetadataStore(5)
			if store.ReplicationFactor() != 5 {
				t.Errorf("expected rf=5, got %d", store.ReplicationFactor())
			}
		})
	})
}

func TestWAL_BDD(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.jsonl")

	t.Run("Given a fresh WAL", func(t *testing.T) {
		w, err := OpenWAL(walPath)
		if err != nil {
			t.Fatalf("open wal: %v", err)
		}

		t.Run("When appending create_file entry", func(t *testing.T) {
			err := w.Append(WALEntry{Op: "create_file", Path: "/test/f.csv", FileID: "f1"})
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			w.Close()

			// Verify replay
			store := NewMetadataStore(3)
			if err := Replay(walPath, store); err != nil {
				t.Fatalf("replay: %v", err)
			}
			fm, err := store.GetFile("/test/f.csv")
			if err != nil {
				t.Fatalf("file not restored: %v", err)
			}
			if fm.FileID != "f1" {
				t.Errorf("expected fileID f1, got %s", fm.FileID)
			}
		})
	})

	t.Run("Given WAL replay on non-existent file", func(t *testing.T) {
		store := NewMetadataStore(3)
		err := Replay(filepath.Join(tmpDir, "nonexistent.jsonl"), store)
		if err != nil {
			t.Errorf("expected no error for missing WAL (fresh start), got %v", err)
		}
	})

	t.Run("Given WAL close", func(t *testing.T) {
		walPath2 := filepath.Join(tmpDir, "wal2.jsonl")
		w, _ := OpenWAL(walPath2)
		w.Append(WALEntry{Op: "create_file", Path: "/x"})
		if err := w.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
}

// TestWAL_Persistence ensures entries survive process restart simulation.
func TestWAL_Persistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.jsonl")

	w, _ := OpenWAL(path)
	w.Append(WALEntry{Op: "create_file", Path: "/a"})
	w.Append(WALEntry{Op: "create_file", Path: "/b"})
	w.Close()

	store := NewMetadataStore(3)
	if err := Replay(path, store); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := store.GetFile("/a"); err != nil {
		t.Error("file /a not restored")
	}
	if _, err := store.GetFile("/b"); err != nil {
		t.Error("file /b not restored")
	}
}

// Verify WAL file is non-empty after writes.
func TestWAL_NonEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.jsonl")
	w, _ := OpenWAL(path)
	w.Append(WALEntry{Op: "create_file", Path: "/test"})
	w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}
	if len(data) == 0 {
		t.Error("WAL file is empty after append")
	}
}
