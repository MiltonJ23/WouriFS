package audit

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Entry represents one immutable audit record in the Merkle chain.
type Entry struct {
	Index     int64     `json:"index"`
	Timestamp time.Time `json:"timestamp"`
	Operation string    `json:"op"`   // "create_file", "delete_file", "write_chunk"
	FilePath  string    `json:"path"`
	Hash      string    `json:"hash"` // SHA-256(content) for writes, empty for metadata ops
	Identity  string    `json:"identity"` // authenticated user from JWT
	PrevHash  string    `json:"prev_hash"` // H(prev_entry || prev.PrevHash)
}

// Chain is an append-only Merkle-chained audit log.
// Writes to a JSON-lines file, same pattern as the Sprint 1 WAL.
type Chain struct {
	mu       sync.Mutex
	file     *os.File
	lastHash string
	index    int64
}

// OpenChain creates or opens the audit log file.
func OpenChain(path string) (*Chain, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}

	// Recover last hash and index by parsing existing entries
	c := &Chain{file: f, lastHash: "genesis", index: -1}
	c.recoverState()
	return c, nil
}

// Append records a new entry and updates the Merkle chain.
func (c *Chain) Append(op, path, contentHash, identity string) (*Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.index++

	entry := &Entry{
		Index:     c.index,
		Timestamp: time.Now().UTC(),
		Operation: op,
		FilePath:  path,
		Hash:      contentHash,
		Identity:  identity,
		PrevHash:  c.lastHash,
	}

	// Merkle link: hash of (serialized entry || prev entry hash)
	data, _ := json.Marshal(entry)
	h := sha256.Sum256(append(data, []byte(c.lastHash)...))
	entry.PrevHash = fmt.Sprintf("%x", h)

	enc := json.NewEncoder(c.file)
	if err := enc.Encode(entry); err != nil {
		return nil, err
	}
	if err := c.file.Sync(); err != nil {
		return nil, err
	}

	c.lastHash = entry.PrevHash
	return entry, nil
}

// Verify replays the entire chain and returns nil if all Merkle links are intact.
func (c *Chain) Verify() error {
	f, err := os.Open(c.file.Name())
	if err != nil {
		return err
	}
	defer f.Close()

	prevHash := "genesis"
	dec := json.NewDecoder(f)

	for dec.More() {
		var e Entry
		if err := dec.Decode(&e); err != nil {
			return fmt.Errorf("audit chain: corrupt entry at index ~%d: %w", 0, err)
		}

		// Recompute expected hash
		data, _ := json.Marshal(&struct {
			Index     int64     `json:"index"`
			Timestamp time.Time `json:"timestamp"`
			Operation string    `json:"op"`
			FilePath  string    `json:"path"`
			Hash      string    `json:"hash"`
			Identity  string    `json:"identity"`
			PrevHash  string    `json:"prev_hash"`
		}{
			Index:     e.Index,
			Timestamp: e.Timestamp,
			Operation: e.Operation,
			FilePath:  e.FilePath,
			Hash:      e.Hash,
			Identity:  e.Identity,
			PrevHash:  prevHash,
		})
		h := sha256.Sum256(append(data, []byte(prevHash)...))
		expected := fmt.Sprintf("%x", h)

		if e.PrevHash != expected {
			return fmt.Errorf("audit chain break at index %d: expected %s, got %s",
				e.Index, expected, e.PrevHash)
		}
		prevHash = e.PrevHash
	}

	return nil
}

// Close flushes and closes the audit log.
func (c *Chain) Close() error {
	return c.file.Close()
}

// recoverState reads existing entries to set lastHash and index for continued appends.
func (c *Chain) recoverState() {
	f, err := os.Open(c.file.Name())
	if err != nil {
		return
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	for dec.More() {
		var e Entry
		if err := dec.Decode(&e); err != nil {
			break // corrupt tail, will start fresh from here
		}
		c.index = e.Index
		c.lastHash = e.PrevHash
	}
}
