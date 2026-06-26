package namenode

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/MiltonJ23/WouriFS/internal/domain"
)

// FileMeta holds the Namenode record for one file.
type FileMeta struct {
	FileID string
	Path   string
	Size   int64
	Chunks []ChunkMeta // ordered, index = chunk_index
}

// ChunkMeta maps a chunk to its replicas.
type ChunkMeta struct {
	ChunkID  string
	Index    int
	Replicas []string // datanode addresses holding this chunk
}

// MetadataStore is the in-memory path→file mapping (FR-N-001).
type MetadataStore struct {
	mu        sync.RWMutex
	files     map[string]*FileMeta // keyed by path
	replFac   int32               // default replication factor (FR-N-008)
}

var (
	ErrFileNotFound    = errors.New("file not found")
	ErrFileExists      = errors.New("file already exists")
	ErrNotADirectory   = errors.New("not a directory")
	ErrPathInvalid     = errors.New("path is invalid")
	ErrNamespaceDenied = errors.New("namespace access denied")
)

func NewMetadataStore(replicationFactor int32) *MetadataStore {
	if replicationFactor < 1 {
		replicationFactor = 3
	}
	return &MetadataStore{
		files:   make(map[string]*FileMeta),
		replFac: replicationFactor,
	}
}

// CheckNamespace verifies the user's payload can access the given path (FR-N-007).
func (m *MetadataStore) CheckNamespace(payload *domain.TokenPayload, path string) error {
	if payload == nil {
		return ErrNamespaceDenied
	}
	if payload.Namespace == "" {
		return ErrNamespaceDenied
	}
	if !hasPrefix(path, payload.Namespace) {
		return ErrNamespaceDenied
	}
	return nil
}

// PutFile inserts a file record directly (used for WAL replay).
func (m *MetadataStore) PutFile(fm *FileMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[fm.Path] = fm
}

// CreateFile records a new empty file (FR-N-004: CreateFile).
func (m *MetadataStore) CreateFile(path string) (*FileMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.files[path]; ok {
		return nil, ErrFileExists
	}

	fm := &FileMeta{
		FileID: newID(),
		Path:   path,
	}
	m.files[path] = fm
	return fm, nil
}

// GetFile retrieves file metadata by path.
func (m *MetadataStore) GetFile(path string) (*FileMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fm, ok := m.files[path]
	if !ok {
		return nil, ErrFileNotFound
	}
	return fm, nil
}

// DeleteFile removes a file entry (does not delete chunks).
func (m *MetadataStore) DeleteFile(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.files[path]; !ok {
		return ErrFileNotFound
	}
	delete(m.files, path)
	return nil
}

// ListDirectory returns DirEntry for all files whose path starts with dir + "/" (FR-N-004: ListDirectory).
func (m *MetadataStore) ListDirectory(dirPath string) []DirEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]bool)
	var entries []DirEntry

	for p, fm := range m.files {
		if !hasPrefix(p, dirPath) || len(p) <= len(dirPath) {
			continue
		}
		// extract the immediate child name
		rest := p[len(dirPath):]
		if rest[0] != '/' {
			continue
		}
		rest = rest[1:]
		idx := 0
		for idx < len(rest) && rest[idx] != '/' {
			idx++
		}
		name := rest[:idx]
		if seen[name] {
			continue
		}
		seen[name] = true
		// Determine if it's a directory: any entry has prefix dirPath+"/"+name+"/"
		isDir := false
		prefix := p[:len(dirPath)+1+len(name)] + "/"
		for q := range m.files {
			if q != p && hasPrefix(q, prefix) {
				isDir = true
				break
			}
		}
		entries = append(entries, DirEntry{
			Name:  name,
			IsDir: isDir,
			Size:  fm.Size,
		})
	}
	return entries
}

// AddChunk appends a chunk to a file's metadata (FR-N-001, FR-N-006).
func (m *MetadataStore) AddChunk(fileID string, chunkID string, replicas []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, fm := range m.files {
		if fm.FileID == fileID {
			fm.Chunks = append(fm.Chunks, ChunkMeta{
				ChunkID:  chunkID,
				Index:    len(fm.Chunks),
				Replicas: replicas,
			})
			return nil
		}
	}
	return ErrFileNotFound
}

// UpdateSize sets the file byte size.
func (m *MetadataStore) UpdateSize(path string, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fm, ok := m.files[path]; ok {
		fm.Size = size
	}
}

// ReplicationFactor returns the store's default replication factor.
func (m *MetadataStore) ReplicationFactor() int32 {
	return m.replFac
}

// Snapshot returns a copy of all metadata for WAL checkpointing.
func (m *MetadataStore) Snapshot() map[string]*FileMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]*FileMeta, len(m.files))
	for k, v := range m.files {
		cpy := *v
		if v.Chunks != nil {
			cpy.Chunks = make([]ChunkMeta, len(v.Chunks))
			copy(cpy.Chunks, v.Chunks)
		}
		out[k] = &cpy
	}
	return out
}

// Restore replaces the in-memory state with a previously saved snapshot.
func (m *MetadataStore) Restore(snapshot map[string]*FileMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files = snapshot
}

// DirEntry is exported for use by the gRPC server layer.
type DirEntry struct {
	Name  string
	IsDir bool
	Size  int64
}

// Internal helper: prefix matching.
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// newID generates a random hex ID, avoiding external UUID dependency.
func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b)
}

var _ = time.Now
