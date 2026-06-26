package namenode

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// WALEntry represents one logged metadata mutation (FR-N-002).
type WALEntry struct {
	Op       string   `json:"op"` // "create_file", "delete_file", "add_chunk", "mkdir", "rmdir", "rename", "truncate_file"
	Path     string   `json:"path"`
	OldPath  string   `json:"old_path,omitempty"` // for rename
	NewPath  string   `json:"new_path,omitempty"` // for rename
	FileID   string   `json:"file_id,omitempty"`
	ChunkID  string   `json:"chunk_id,omitempty"`
	Replicas []string `json:"replicas,omitempty"`
	Size     int64    `json:"size,omitempty"`
}

// WAL is a simple append-only JSON-lines Write-Ahead Log (FR-N-002, FR-N-003).
// JSON chosen over binary for debuggability; metadata volume is orders of magnitude
// smaller than chunk data, so I/O overhead is negligible.
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// OpenWAL creates or opens the WAL file for append.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

// Append writes one entry to the WAL and syncs to disk.
func (w *WAL) Append(e WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	enc := json.NewEncoder(w.file)
	if err := enc.Encode(e); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Replay reads all WAL entries from path and replays them into the store (FR-N-003).
func Replay(path string, store *MetadataStore) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh start, no WAL
		}
		return err
	}
	defer f.Close()

	w := &WAL{file: f}
	entries, err := w.readAll()
	if err != nil {
		return err
	}

	for _, e := range entries {
		switch e.Op {
		case "create_file":
			store.PutFile(&FileMeta{FileID: e.FileID, Path: e.Path, Mode: 0644})
		case "delete_file":
			store.DeleteFile(e.Path)
		case "add_chunk":
			store.AddChunk(e.FileID, e.ChunkID, e.Replicas)
		case "mkdir":
			store.MakeDir(e.Path)
		case "rmdir":
			store.RemoveDir(e.Path)
		case "rename":
			store.Rename(e.OldPath, e.NewPath)
		case "truncate_file":
			if fm, err := store.GetFile(e.Path); err == nil {
				fm.Size = e.Size
				fm.Mtime = time.Now()
			}
		}
	}
	return nil
}

// readAll parses all JSON entries from the WAL file.
func (w *WAL) readAll() ([]WALEntry, error) {
	var entries []WALEntry
	dec := json.NewDecoder(w.file)
	for dec.More() {
		var e WALEntry
		if err := dec.Decode(&e); err != nil {
			return entries, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}
