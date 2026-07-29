/*
 * Offline write buffer for branch agencies disconnected from the Namenode.
 *
 * When the Namenode is unreachable, local FUSE write operations fail.
 * Instead of rejecting them, the offline buffer records the operation
 * as a WAL-like entry in a local JSON-lines file. When connectivity is
 * restored, the buffer replays queued operations to the Namenode in order.
 *
 * Architecture:
 *   - Buffer lives alongside the datanode data directory.
 *   - Each entry records: operation type, path, data (base64), timestamp.
 *   - Replay is sequential — operations are applied in insertion order.
 *   - On successful replay, the buffer file is truncated.
 *   - On partial replay failure, remaining entries stay for next attempt.
 */
package offline

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Op represents a single buffered filesystem operation.
type Op struct {
	Type      string `json:"type"`     // "create_file", "write_chunk", "delete_file", "mkdir", "rmdir"
	Path      string `json:"path"`
	Data      string `json:"data"`     // base64-encoded content for writes
	Mode      uint32 `json:"mode"`     // for create_file / mkdir
	Timestamp int64  `json:"timestamp"` // unix nano
}

// Replayer is the interface for applying buffered operations.
type Replayer interface {
	ReplayCreateFile(path string, mode uint32) error
	ReplayWriteChunk(path string, data []byte, offset int64) error
	ReplayDeleteFile(path string) error
	ReplayMkdir(path string, mode uint32) error
	ReplayRmdir(path string) error
}

// Buffer is a local write-ahead buffer for offline operations.
type Buffer struct {
	mu   sync.Mutex
	path string
	file *os.File
}

// NewBuffer opens or creates the buffer file.
func NewBuffer(path string) (*Buffer, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, fmt.Errorf("offline buffer: %w", err)
	}
	return &Buffer{path: path, file: f}, nil
}

// Write queues an operation for later replay.
func (b *Buffer) Write(op Op) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return json.NewEncoder(b.file).Encode(op)
}

// Pending returns the number of queued operations.
func (b *Buffer) Pending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.countLines()
}

func (b *Buffer) countLines() int {
	raw, err := os.ReadFile(b.path)
	if err != nil {
		return 0
	}
	return len(splitLines(raw))
}

// Replay applies all queued operations via the Replayer, then truncates
// the buffer on success. If replay fails partway through, remaining
// entries are preserved for the next attempt.
func (b *Buffer) Replay(r Replayer) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	raw, err := os.ReadFile(b.path)
	if err != nil {
		return fmt.Errorf("read buffer: %w", err)
	}
	if len(raw) == 0 {
		return nil
	}

	// JSON-lines format — parse line by line
	var keep []Op
	lines := splitLines(raw)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var op Op
		if err := json.Unmarshal(line, &op); err != nil {
			keep = append(keep, Op{Type: "corrupt", Path: string(line)})
			continue
		}
		if b.applyOne(op, r) != nil {
			keep = append(keep, op)
		}
	}

	return b.rewrite(keep)
}

func splitLines(raw []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, c := range raw {
		if c == '\n' {
			lines = append(lines, raw[start:i])
			start = i + 1
		}
	}
	if start < len(raw) {
		lines = append(lines, raw[start:])
	}
	return lines
}

func (b *Buffer) applyOne(op Op, r Replayer) error {
	switch op.Type {
	case "create_file":
		return r.ReplayCreateFile(op.Path, op.Mode)
	case "write_chunk":
		data, _ := base64.StdEncoding.DecodeString(op.Data)
		return r.ReplayWriteChunk(op.Path, data, 0)
	case "delete_file":
		return r.ReplayDeleteFile(op.Path)
	case "mkdir":
		return r.ReplayMkdir(op.Path, op.Mode)
	case "rmdir":
		return r.ReplayRmdir(op.Path)
	default:
		return fmt.Errorf("unknown op type: %s", op.Type)
	}
}

func (b *Buffer) rewrite(keep []Op) error {
	b.file.Close()
	if len(keep) == 0 {
		return os.Truncate(b.path, 0)
	}
	f, err := os.Create(b.path)
	if err != nil {
		return err
	}
	b.file = f
	for _, op := range keep {
		json.NewEncoder(f).Encode(op)
	}
	return nil
}

// Close flushes and closes the buffer.
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.file.Close()
}
