package offline

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type testReplayer struct {
	mu      sync.Mutex
	files   map[string]bool
	dirs    map[string]bool
	written map[string][]byte
}

func newTestReplayer() *testReplayer {
	return &testReplayer{
		files:   make(map[string]bool),
		dirs:    make(map[string]bool),
		written: make(map[string][]byte),
	}
}

func (r *testReplayer) ReplayCreateFile(path string, mode uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.files[path] {
		return fmt.Errorf("already exists")
	}
	r.files[path] = true
	return nil
}

func (r *testReplayer) ReplayWriteChunk(path string, data []byte, offset int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.written[path] = append(r.written[path], data...)
	return nil
}

func (r *testReplayer) ReplayDeleteFile(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.files, path)
	return nil
}

func (r *testReplayer) ReplayMkdir(path string, mode uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dirs[path] = true
	return nil
}

func (r *testReplayer) ReplayRmdir(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.dirs, path)
	return nil
}

func TestBuffer_WriteAndReplay(t *testing.T) {
	dir := t.TempDir()
	bufPath := filepath.Join(dir, "offline.jsonl")

	buf, err := NewBuffer(bufPath)
	if err != nil {
		t.Fatalf("new buffer: %v", err)
	}

	// Queue operations
	ops := []Op{
		{Type: "mkdir", Path: "/wourifs/clients", Mode: 0755},
		{Type: "create_file", Path: "/wourifs/clients/a.csv", Mode: 0644},
		{Type: "write_chunk", Path: "/wourifs/clients/a.csv", Data: base64.StdEncoding.EncodeToString([]byte("hello"))},
	}
	for _, op := range ops {
		if err := buf.Write(op); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	buf.Close()

	if buf.Pending() != 3 {
		t.Errorf("expected 3 pending, got %d", buf.Pending())
	}

	// Replay
	buf2, _ := NewBuffer(bufPath)
	defer buf2.Close()
	r := newTestReplayer()
	if err := buf2.Replay(r); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if !r.dirs["/wourifs/clients"] {
		t.Error("mkdir not replayed")
	}
	if !r.files["/wourifs/clients/a.csv"] {
		t.Error("create_file not replayed")
	}
	if string(r.written["/wourifs/clients/a.csv"]) != "hello" {
		t.Errorf("write not replayed: got %q", r.written["/wourifs/clients/a.csv"])
	}
	if buf2.Pending() != 0 {
		t.Errorf("expected 0 pending after replay, got %d", buf2.Pending())
	}
}

func TestBuffer_PartialReplay(t *testing.T) {
	dir := t.TempDir()
	bufPath := filepath.Join(dir, "offline.jsonl")

	buf, _ := NewBuffer(bufPath)
	buf.Write(Op{Type: "create_file", Path: "/a", Mode: 0644})
	buf.Write(Op{Type: "create_file", Path: "/a", Mode: 0644}) // duplicate → fails
	buf.Write(Op{Type: "create_file", Path: "/b", Mode: 0644})
	buf.Close()

	buf2, _ := NewBuffer(bufPath)
	defer buf2.Close()
	r := newTestReplayer()
	if err := buf2.Replay(r); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// /a succeeded, /a duplicate failed, /b kept
	if buf2.Pending() != 1 {
		t.Errorf("expected 1 remaining, got %d", buf2.Pending())
	}
}

func TestBuffer_EmptyReplay(t *testing.T) {
	bufPath := filepath.Join(t.TempDir(), "empty.jsonl")
	os.WriteFile(bufPath, []byte{}, 0644)
	buf, _ := NewBuffer(bufPath)
	defer buf.Close()
	if err := buf.Replay(newTestReplayer()); err != nil {
		t.Fatalf("empty replay: %v", err)
	}
}
