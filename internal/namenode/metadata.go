package namenode

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/MiltonJ23/WouriFS/internal/domain"
)

// FileMeta holds the Namenode record for a file or directory.
type FileMeta struct {
	FileID string
	Path   string
	Size   int64
	Mode   uint32 // POSIX permission bits
	IsDir  bool
	Mtime  time.Time
	Ctime  time.Time
	Chunks []ChunkMeta
}

// ChunkMeta maps a chunk to its replicas.
type ChunkMeta struct {
	ChunkID  string
	Index    int
	Replicas []string
}

// MetadataStore is the in-memory path→file mapping (FR-N-001).
type MetadataStore struct {
	mu      sync.RWMutex
	files   map[string]*FileMeta
	replFac int32
}

var (
	ErrFileNotFound    = errors.New("file not found")
	ErrFileExists      = errors.New("file already exists")
	ErrNotEmpty        = errors.New("directory not empty")
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

// CheckNamespace verifies the user can access the given path (FR-N-007).
// The check requires the path to exactly equal the namespace or be a child
// separated by a path delimiter. A bare prefix match is insufficient:
// namespace "/wourifs/a" must not grant access to "/wourifs/ab".
func (m *MetadataStore) CheckNamespace(payload *domain.TokenPayload, path string) error {
	if payload == nil || payload.Namespace == "" {
		return ErrNamespaceDenied
	}
	if path == payload.Namespace {
		return nil
	}
	if hasPrefix(path, payload.Namespace) && len(path) > len(payload.Namespace) && path[len(payload.Namespace)] == '/' {
		return nil
	}
	return ErrNamespaceDenied
}

// PutFile inserts a file record directly (WAL replay).
func (m *MetadataStore) PutFile(fm *FileMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[fm.Path] = fm
}

// CreateFile records a new empty file.
func (m *MetadataStore) CreateFile(path string, mode uint32) (*FileMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.files[path]; ok {
		return nil, ErrFileExists
	}

	now := time.Now()
	fm := &FileMeta{
		FileID: newID(),
		Path:   path,
		Mode:   mode,
		Mtime:  now,
		Ctime:  now,
	}
	m.files[path] = fm
	return fm, nil
}

// MakeDir creates a directory marker entry with the requested mode.
func (m *MetadataStore) MakeDir(path string, mode uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.files[path]; ok {
		return ErrFileExists
	}

	if mode == 0 {
		mode = 0755
	}

	now := time.Now()
	m.files[path] = &FileMeta{
		Path:  path,
		Mode:  mode,
		IsDir: true,
		Mtime: now,
		Ctime: now,
	}
	return nil
}

// RemoveDir removes a directory marker only if it has no children.
func (m *MetadataStore) RemoveDir(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fm, ok := m.files[path]
	if !ok {
		return ErrFileNotFound
	}
	if !fm.IsDir {
		return ErrNotADirectory
	}

	prefix := path + "/"
	for p := range m.files {
		if p != path && hasPrefix(p, prefix) {
			return ErrNotEmpty
		}
	}

	delete(m.files, path)
	return nil
}

// IsDir reports whether a path is a directory marker.
func (m *MetadataStore) IsDir(path string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fm, ok := m.files[path]
	return ok && fm.IsDir
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

// GetFileByID retrieves file metadata by its FileID.
// Used by AllocateChunk to resolve the owning path for namespace checks.
func (m *MetadataStore) GetFileByID(fileID string) (*FileMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, fm := range m.files {
		if fm.FileID == fileID {
			return fm, nil
		}
	}
	return nil, ErrFileNotFound
}

// DeleteFile removes a file entry.
func (m *MetadataStore) DeleteFile(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fm, ok := m.files[path]
	if !ok {
		return ErrFileNotFound
	}
	if fm.IsDir {
		return ErrNotADirectory
	}
	delete(m.files, path)
	return nil
}

// Rename atomically moves a path from old to new, rewriting child paths
// when the source is a directory with descendants.
func (m *MetadataStore) Rename(oldPath, newPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fm, ok := m.files[oldPath]
	if !ok {
		return ErrFileNotFound
	}
	if _, exists := m.files[newPath]; exists {
		return ErrFileExists
	}

	if fm.IsDir {
		prefix := oldPath + "/"
		for p, child := range m.files {
			if hasPrefix(p, prefix) {
				newChildPath := newPath + "/" + p[len(prefix):]
				child.Path = newChildPath
				delete(m.files, p)
				m.files[newChildPath] = child
			}
		}
	}

	fm.Path = newPath
	fm.Mtime = time.Now()
	delete(m.files, oldPath)
	m.files[newPath] = fm
	return nil
}

// ListDirectory returns entries for immediate children of dirPath.
func (m *MetadataStore) ListDirectory(dirPath string) []DirEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]bool)
	var entries []DirEntry

	for p, fm := range m.files {
		if !hasPrefix(p, dirPath) || len(p) <= len(dirPath) {
			continue
		}
		rest := p[len(dirPath):]
		// Root dirPath == "/" means rest is e.g. "foo" (no leading slash).
		// Non-root dirPath means rest starts with "/" (e.g. "/wourifs/test" → "/foo").
		if dirPath != "/" {
			if rest[0] != '/' {
				continue
			}
			rest = rest[1:]
		}
		idx := 0
		for idx < len(rest) && rest[idx] != '/' {
			idx++
		}
		name := rest[:idx]
		if seen[name] {
			continue
		}
		seen[name] = true

		isDir := fm.IsDir
		// Also detect implicit directories (files with deeper children)
		if !isDir {
			prefix := p[:len(dirPath)+1+len(name)] + "/"
			for q := range m.files {
				if q != p && hasPrefix(q, prefix) {
					isDir = true
					break
				}
			}
		}

		mode := fm.Mode
		if mode == 0 && isDir {
			mode = 0755
		}
		if mode == 0 {
			mode = 0644
		}

		entries = append(entries, DirEntry{
			Name:  name,
			IsDir: isDir,
			Size:  fm.Size,
			Mode:  mode,
			Mtime: fm.Mtime,
		})
	}
	return entries
}

// AddChunk appends a chunk to a file's metadata.
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

// ReplicationFactor returns the store's default replication factor.
func (m *MetadataStore) ReplicationFactor() int32 {
	return m.replFac
}

// TruncateFile atomically updates the size of a file. Must be called under
// write lock to avoid racing with concurrent GetFile callers.
func (m *MetadataStore) TruncateFile(path string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fm, ok := m.files[path]
	if !ok {
		return ErrFileNotFound
	}
	fm.Size = size
	fm.Mtime = time.Now()
	return nil
}

// Snapshot returns a copy of all metadata.
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

// Restore replaces in-memory state with a snapshot.
func (m *MetadataStore) Restore(snapshot map[string]*FileMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files = snapshot
}

// DirEntry is exported for the gRPC server layer.
type DirEntry struct {
	Name  string
	IsDir bool
	Size  int64
	Mode  uint32
	Mtime time.Time
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b)
}
