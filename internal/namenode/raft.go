package namenode

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
)

// RaftConfig holds all parameters needed to bootstrap a Raft cluster node.
type RaftConfig struct {
	NodeID      string        // unique raft.ServerID
	BindAddr    string        // raft transport bind address
	DataDir     string        // bolt store + stable store directory
	Bootstrap   bool          // true for first node (cluster bootstrap)
	Peers       []string      // []raft.ServerAddress of peers for joining
	ApplyTimeout time.Duration // timeout for applying log entries
}

// RaftFSM implements raft.FSM — the replicated state machine for metadata.
type RaftFSM struct {
	mu    sync.RWMutex
	store *MetadataStore
}

// NewRaftFSM creates a FSM wrapping the existing MetadataStore.
func NewRaftFSM(store *MetadataStore) *RaftFSM {
	return &RaftFSM{store: store}
}

// Apply replicates a Raft log entry into the metadata store.
func (f *RaftFSM) Apply(logEntry *raft.Log) interface{} {
	var e WALEntry
	if err := json.Unmarshal(logEntry.Data, &e); err != nil {
		log.Printf("raft fsm: unmarshal entry: %v", err)
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch e.Op {
	case "create_file":
		f.store.PutFile(&FileMeta{FileID: e.FileID, Path: e.Path, Mode: 0644})
	case "delete_file":
		f.store.DeleteFile(e.Path)
	case "add_chunk":
		f.store.AddChunk(e.FileID, e.ChunkID, e.Replicas)
	case "mkdir":
		f.store.MakeDir(e.Path)
	case "rmdir":
		f.store.RemoveDir(e.Path)
	case "rename":
		f.store.Rename(e.OldPath, e.NewPath)
	case "truncate_file":
		if fm, err := f.store.GetFile(e.Path); err == nil {
			fm.Size = e.Size
			fm.Mtime = time.Now()
		}
	}
	return nil
}

// Snapshot serializes the entire metadata store for log compaction.
func (f *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	data, err := json.Marshal(f.store.Snapshot())
	if err != nil {
		return nil, err
	}
	return &raftSnapshot{data: data}, nil
}

// Restore rebuilds the metadata store from a snapshot.
func (f *RaftFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	var snap map[string]*FileMeta
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.store.Restore(snap)
	return nil
}

// Store returns the underlying MetadataStore (for read ops from handlers).
func (f *RaftFSM) Store() *MetadataStore {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.store
}

// raftSnapshot implements raft.FSMSnapshot for metadata state.
type raftSnapshot struct {
	data []byte
}

func (s *raftSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *raftSnapshot) Release() {}

// BootstrapRaft creates a fully initialized Raft node for the Namenode.
// Replaces the single-node WAL approach from Sprint 1.
func BootstrapRaft(cfg RaftConfig, store *MetadataStore) (*raft.Raft, *RaftFSM, error) {
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)

	if cfg.ApplyTimeout > 0 {
		rc.HeartbeatTimeout = cfg.ApplyTimeout / 2
		rc.ElectionTimeout = cfg.ApplyTimeout
		rc.LeaderLeaseTimeout = cfg.ApplyTimeout / 2
	}

	// BoltDB for stable log storage
	logStore, err := raftbolt.New(raftbolt.Options{
		Path: cfg.DataDir + "/raft-log.bolt",
	})
	if err != nil {
		return nil, nil, err
	}

	// BoltDB for stable store (current term, vote)
	stableStore, err := raftbolt.New(raftbolt.Options{
		Path: cfg.DataDir + "/raft-stable.bolt",
	})
	if err != nil {
		return nil, nil, err
	}

	// In-memory snapshot store (snapshots replayed on restart)
	snapStore := raft.NewInmemSnapshotStore()

	// TCP transport
	transport, err := raft.NewTCPTransport(cfg.BindAddr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	fsm := NewRaftFSM(store)

	r, err := raft.NewRaft(rc, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, nil, err
	}

	// Bootstrap cluster if this is the first node
	if cfg.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(cfg.NodeID),
				Address: transport.LocalAddr(),
			},
		}
		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, nil, fmt.Errorf("bootstrap: %w", err)
		}
		log.Printf("raft: bootstrapped cluster with node %s", cfg.NodeID)
	}

	return r, fsm, nil
}

// Ensure import usage
var _ = fmt.Sprintf
