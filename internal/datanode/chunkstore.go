package datanode

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ChunkStore persists chunks as flat binary files (FR-D-001).
// Directory layout: <dataDir>/<chunk_id>
type ChunkStore struct {
	mu      sync.RWMutex
	dataDir string
}

// NewChunkStore validates the data directory (creates if missing).
func NewChunkStore(dataDir string) (*ChunkStore, error) {
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, fmt.Errorf("chunk store: %w", err)
	}
	return &ChunkStore{dataDir: dataDir}, nil
}

// chunkPath validates chunkID and returns the absolute path under dataDir.
// Rejects IDs containing directory traversal sequences.
func (c *ChunkStore) chunkPath(chunkID string) (string, error) {
	if chunkID == "" {
		return "", fmt.Errorf("chunkstore: empty chunk ID")
	}
	if strings.Contains(chunkID, "..") || strings.Contains(chunkID, "/") || strings.Contains(chunkID, "\\") {
		return "", fmt.Errorf("chunkstore: invalid chunk ID %q", chunkID)
	}

	raw := filepath.Join(c.dataDir, chunkID)
	cleaned := filepath.Clean(raw)
	prefix := filepath.Clean(c.dataDir) + string(os.PathSeparator)

	if !strings.HasPrefix(cleaned, prefix) && cleaned != filepath.Clean(c.dataDir) {
		return "", fmt.Errorf("chunkstore: chunk ID escapes data directory")
	}

	return raw, nil
}

// Write streams blocks into the chunk file (overwrites if exists).
func (c *ChunkStore) Write(chunkID string, reader io.Reader) (int64, error) {
	path, err := c.chunkPath(chunkID)
	if err != nil {
		return 0, err
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("write chunk %s: %w", chunkID, err)
	}
	defer f.Close()

	n, err := io.Copy(f, reader)
	if err != nil {
		os.Remove(path)
		return 0, fmt.Errorf("write chunk %s: %w", chunkID, err)
	}
	return n, nil
}

// Read opens the chunk file for reading.
func (c *ChunkStore) Read(chunkID string) (io.ReadCloser, int64, error) {
	path, err := c.chunkPath(chunkID)
	if err != nil {
		return nil, 0, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, fmt.Errorf("chunk %s: %w", chunkID, os.ErrNotExist)
		}
		return nil, 0, fmt.Errorf("read chunk %s: %w", chunkID, err)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

// Delete removes a chunk from disk.
func (c *ChunkStore) Delete(chunkID string) error {
	path, err := c.chunkPath(chunkID)
	if err != nil {
		return err
	}
	err = os.Remove(path)
	if os.IsNotExist(err) {
		return fmt.Errorf("chunk %s: %w", chunkID, os.ErrNotExist)
	}
	return err
}

// Exists reports whether a chunk file is present.
func (c *ChunkStore) Exists(chunkID string) bool {
	path, err := c.chunkPath(chunkID)
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return err == nil
}

// ChunkCount returns the number of chunks stored.
func (c *ChunkStore) ChunkCount() int64 {
	entries, err := os.ReadDir(c.dataDir)
	if err != nil {
		return 0
	}
	var count int64
	for _, e := range entries {
		if !e.IsDir() {
			count++
		}
	}
	return count
}

// TotalSize returns the sum of all chunk file sizes (for capacity reporting).
func (c *ChunkStore) TotalSize() int64 {
	entries, err := os.ReadDir(c.dataDir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if !e.IsDir() {
			info, err := e.Info()
			if err == nil {
				total += info.Size()
			}
		}
	}
	return total
}
