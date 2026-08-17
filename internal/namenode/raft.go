package namenode

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	domainnn "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
)

// RaftConfig holds all parameters needed to bootstrap a Raft cluster node.
type RaftConfig struct {
	NodeID       string        // unique raft.ServerID
	BindAddr     string        // raft transport bind address
	DataDir      string        // bolt store + stable store directory
	Bootstrap    bool          // true for first node (cluster bootstrap)
	Peers        []string      // []raft.ServerAddress of peers for joining
	ApplyTimeout time.Duration // timeout for applying log entries
}

// RaftFSM implements raft.FSM — the replicated state machine for metadata
// AND the cluster-wide datanode registry. The registry is nil in tests that
// only exercise metadata ops.
type RaftFSM struct {
	mu       sync.RWMutex
	store    *MetadataStore
	registry *domainnn.InMemoryDataNodeRegistry
}

// NewRaftFSM creates a FSM wrapping the existing MetadataStore and (when
// non-nil) the datanode registry replicated to every cluster member.
func NewRaftFSM(store *MetadataStore, registry *domainnn.InMemoryDataNodeRegistry) *RaftFSM {
	return &RaftFSM{store: store, registry: registry}
}

// Apply replicates a Raft log entry into the metadata store and registry.
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
		mode := e.Mode
		if mode == 0 {
			mode = 0644
		}
		f.store.PutFile(&FileMeta{FileID: e.FileID, Path: e.Path, Mode: mode})
	case "delete_file":
		if err := f.store.DeleteFile(e.Path); err != nil {
			log.Printf("raft fsm: delete %s: %v", e.Path, err)
		}
	case "add_chunk":
		if err := f.store.AddChunk(e.FileID, e.ChunkID, e.Replicas); err != nil {
			log.Printf("raft fsm: addchunk %s: %v", e.ChunkID, err)
		}
	case "mkdir":
		if err := f.store.MakeDir(e.Path, 0755); err != nil {
			log.Printf("raft fsm: mkdir %s: %v", e.Path, err)
		}
	case "rmdir":
		if err := f.store.RemoveDir(e.Path); err != nil {
			log.Printf("raft fsm: rmdir %s: %v", e.Path, err)
		}
	case "rename":
		if err := f.store.Rename(e.OldPath, e.NewPath); err != nil {
			log.Printf("raft fsm: rename %s: %v", e.OldPath, err)
		}
	case "truncate_file":
		if err := f.store.TruncateFile(e.Path, e.Size); err != nil {
			log.Printf("raft fsm: truncate %s: %v", e.Path, err)
		}
	case "register_datanode":
		if err := f.applyRegisterDataNode(e); err != nil {
			log.Printf("raft fsm: register datanode %s: %v", e.DatanodeID, err)
			return err
		}
	case "datanode_heartbeat":
		if err := f.applyHeartbeat(e); err != nil {
			log.Printf("raft fsm: heartbeat datanode %s: %v", e.DatanodeID, err)
			return err
		}
	case "datanode_unavailable":
		if err := f.applyUnavailable(e); err != nil {
			log.Printf("raft fsm: mark unavailable %s: %v", e.DatanodeID, err)
			return err
		}
	}
	return nil
}

func (f *RaftFSM) applyRegisterDataNode(e WALEntry) error {
	if f.registry == nil {
		return nil
	}
	return f.registry.Register(&domainnn.DataNodeStatus{
		ID:                e.DatanodeID,
		Address:           e.DatanodeAddr,
		TotalStorageBytes: e.TotalBytes,
		FreeStorageBytes:  e.FreeBytes,
		LastHeartbeat:     time.Now(),
		IsAvailable:       true,
	})
}

func (f *RaftFSM) applyHeartbeat(e WALEntry) error {
	if f.registry == nil {
		return nil
	}
	return f.registry.UpdateHeartbeat(e.DatanodeID, e.FreeBytes, 0)
}

func (f *RaftFSM) applyUnavailable(e WALEntry) error {
	if f.registry == nil {
		return nil
	}
	return f.registry.MarkUnavailable(e.DatanodeID)
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
// Replaces the single-node WAL approach from Sprint 1. The registry is
// replicated to every member through the FSM so all namenodes share the
// same datanode view.
func BootstrapRaft(cfg RaftConfig, store *MetadataStore, registry *domainnn.InMemoryDataNodeRegistry) (*raft.Raft, *RaftFSM, error) {
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

	fsm := NewRaftFSM(store, registry)

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
