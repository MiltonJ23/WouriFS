/*
 * AuditReplicationService — secondary audit log replication (FR-A-010, FR-A-011).
 *
 * Every committed audit entry is streamed to a dedicated secondary replica node.
 * If the replica is unreachable, entries are buffered up to a configurable
 * limit (default: 1000). When the buffer is exhausted, write operations are
 * rejected with AuditReplicaUnavailable to prevent silent loss of audit coverage.
 *
 * Architecture:
 *   - The Namenode Leader pushes entries to the replica via gRPC stream.
 *   - The replica acknowledges each entry after fsyncing to disk.
 *   - A background goroutine maintains the connection and drains the buffer.
 *   - On transient failures, the buffer absorbs entries until the replica
 *     recovers or the limit is reached.
 */
package auditsvc

import (
	"fmt"
	"sync"
	"time"
)

// AuditReplicationEntry is a single entry pushed to the replica.
type AuditReplicationEntry struct {
	EntryID      int64
	Timestamp    time.Time
	UserID       string
	Operation    string
	FilePath     string
	ChunkID      string
	SHA256Before string
	SHA256After  string
	ChainHash    string
}

// ReplicaClient is the interface for sending entries to the replica.
type ReplicaClient interface {
	Send(entry AuditReplicationEntry) error
	Close() error
}

// Replicator manages secondary audit log replication.
type Replicator struct {
	mu        sync.Mutex
	client    ReplicaClient
	buffer    []AuditReplicationEntry
	maxBuffer int
	running   bool
	notFull   *sync.Cond
}

// NewReplicator creates a replicator with the given buffer limit.
func NewReplicator(maxBuffer int) *Replicator {
	r := &Replicator{
		maxBuffer: maxBuffer,
		buffer:    make([]AuditReplicationEntry, 0, maxBuffer),
	}
	r.notFull = sync.NewCond(&r.mu)
	return r
}

// SetClient attaches a replica connection. Called after the connection
// is established (or re-established after a disconnect).
func (r *Replicator) SetClient(c ReplicaClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client = c
	r.running = true
	r.notFull.Broadcast()
}

// Disconnect marks the replica as unreachable. Entries will be buffered.
func (r *Replicator) Disconnect() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.client != nil {
		r.client.Close()
		r.client = nil
	}
	r.running = false
}

// Push adds an entry to the replication buffer. If the client is connected,
// it sends immediately. If the buffer is full, returns an error (fail-safe).
func (r *Replicator) Push(entry AuditReplicationEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client != nil && r.running {
		if err := r.client.Send(entry); err != nil {
			// Send failed — disconnect and buffer
			r.client.Close()
			r.client = nil
			r.running = false
		} else {
			return nil
		}
	}

	if len(r.buffer) >= r.maxBuffer {
		return fmt.Errorf("audit replica buffer exhausted (%d entries)", r.maxBuffer)
	}
	r.buffer = append(r.buffer, entry)
	return nil
}

// Drain attempts to flush buffered entries to the client. Called when
// the replica connection is re-established.
func (r *Replicator) Drain() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil || len(r.buffer) == 0 {
		return nil
	}

	for i, entry := range r.buffer {
		if err := r.client.Send(entry); err != nil {
			// Keep remaining entries
			r.buffer = r.buffer[i:]
			return fmt.Errorf("drain interrupted at entry %d: %w", i, err)
		}
	}
	r.buffer = r.buffer[:0]
	return nil
}

// Lag returns the number of entries pending replication.
func (r *Replicator) Lag() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.buffer)
}

// Shutdown closes the client and clears the buffer.
func (r *Replicator) Shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.client != nil {
		r.client.Close()
		r.client = nil
	}
	r.buffer = nil
	r.running = false
}
