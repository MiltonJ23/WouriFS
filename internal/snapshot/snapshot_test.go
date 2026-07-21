package snapshot

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testStore struct {
	data map[string]interface{}
}

func (s *testStore) Snapshot() map[string]interface{} {
	return s.data
}

func TestManager_CreateAndPrune(t *testing.T) {
	dir := t.TempDir()
	store := &testStore{data: map[string]interface{}{"key": "value"}}

	mgr := NewManager(store, dir, 100*time.Millisecond, 500*time.Millisecond)
	if err := mgr.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer mgr.Stop()

	// Wait for at least one tick
	time.Sleep(300 * time.Millisecond)

	entries, _ := os.ReadDir(dir)
	if len(entries) == 0 {
		t.Error("expected at least one snapshot file")
	}

	// Prune should remove old snapshots
	time.Sleep(time.Second)
	entries2, _ := os.ReadDir(dir)
	for _, e := range entries2 {
		t.Logf("snapshot file: %s", e.Name())
	}
}

func TestManager_Take(t *testing.T) {
	dir := t.TempDir()
	store := &testStore{data: map[string]interface{}{"version": 1}}

	mgr := NewManager(store, dir, time.Hour, 30*24*time.Hour)
	path, err := mgr.Take()
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("snapshot file not created")
	}
	if mgr.LastSnapshot().IsZero() {
		t.Error("last snapshot time not set")
	}
}

func TestManager_InvalidDir(t *testing.T) {
	store := &testStore{data: map[string]interface{}{}}
	mgr := NewManager(store, "/proc/does-not-exist-xyz", time.Hour, time.Hour)
	err := mgr.Start()
	if err == nil {
		t.Error("expected error for invalid directory")
	}
}

func TestManager_PruneEmpty(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "not-a-snapshot.txt"), []byte("x"), 0644)
	store := &testStore{data: map[string]interface{}{}}
	mgr := NewManager(store, dir, 50*time.Millisecond, 10*time.Millisecond)
	mgr.prune() // must not panic
}
