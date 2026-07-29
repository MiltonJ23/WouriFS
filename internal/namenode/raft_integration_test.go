package namenode

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

var raftTestMu sync.Mutex

func TestRaftCluster_LeaderElection(t *testing.T) {
	raftTestMu.Lock()
	defer raftTestMu.Unlock()

	dir := t.TempDir()
	basePort := 12000 + int(time.Now().UnixNano()%10000)
	store := NewMetadataStore(3)
	cfg := RaftConfig{
		NodeID: fmt.Sprintf("n%d", basePort), BindAddr: fmt.Sprintf("127.0.0.1:%d", basePort),
		DataDir: dir, Bootstrap: true, ApplyTimeout: 2 * time.Second,
	}
	os.MkdirAll(cfg.DataDir, 0750)
	r, _, err := BootstrapRaft(cfg, store)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	defer r.Shutdown()

	dl := time.After(5 * time.Second)
	for {
		select {
		case <-dl:
			t.Fatal("no leader elected")
		default:
			if r.State() == raft.Leader {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func TestRaftFSM_ApplyAndVerify(t *testing.T) {
	raftTestMu.Lock()
	defer raftTestMu.Unlock()

	dir := t.TempDir()
	basePort := 13000 + int(time.Now().UnixNano()%10000)
	store := NewMetadataStore(3)
	cfg := RaftConfig{
		NodeID: fmt.Sprintf("n%d", basePort), BindAddr: fmt.Sprintf("127.0.0.1:%d", basePort),
		DataDir: dir, Bootstrap: true, ApplyTimeout: 2 * time.Second,
	}
	os.MkdirAll(cfg.DataDir, 0750)
	r, _, _ := BootstrapRaft(cfg, store)
	defer r.Shutdown()

	dl := time.After(5 * time.Second)
	for r.State() != raft.Leader {
		select {
		case <-dl:
			t.Fatal("leader not elected")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	srv := &NameNodeServer{store: store}
	srv.SetRaft(r)

	ops := []WALEntry{
		{Op: "create_file", Path: "/raft/a.txt", FileID: "f-a", Mode: 0644},
		{Op: "mkdir", Path: "/raft/dir"},
		{Op: "create_file", Path: "/raft/dir/b.txt", FileID: "f-b", Mode: 0644},
	}
	for _, e := range ops {
		if err := srv.raftSubmit(e); err != nil {
			t.Fatalf("raftSubmit %s: %v", e.Op, err)
		}
	}
	srv.raftSubmit(WALEntry{Op: "delete_file", Path: "/raft/a.txt"})
	srv.raftSubmit(WALEntry{Op: "rename", OldPath: "/raft/dir/b.txt", NewPath: "/raft/dir/c.txt"})

	if _, err := store.GetFile("/raft/dir/c.txt"); err != nil {
		t.Errorf("rename failed: %v", err)
	}
}

// TestRaftCluster_MultiNodeFailover starts 3 nodes in a single cluster,
// kills the leader, and verifies a new leader is elected.
func TestRaftCluster_MultiNodeFailover(t *testing.T) {
	raftTestMu.Lock()
	defer raftTestMu.Unlock()

	basePort := 24000 + int(time.Now().UnixNano()%10000)
	const n = 3

	stores := make([]*MetadataStore, n)
	rafts := make([]*raft.Raft, n)
	dirs := make([]string, n)

	// Bootstrap node 0 as single-node cluster
	dirs[0] = t.TempDir()
	stores[0] = NewMetadataStore(3)
	cfg0 := RaftConfig{
		NodeID: "m0", BindAddr: fmt.Sprintf("127.0.0.1:%d", basePort),
		DataDir: dirs[0], Bootstrap: true, ApplyTimeout: 2 * time.Second,
	}
	os.MkdirAll(cfg0.DataDir, 0750)
	r0, _, err := BootstrapRaft(cfg0, stores[0])
	if err != nil {
		t.Fatalf("bootstrap node 0: %v", err)
	}
	rafts[0] = r0

	// Wait for node 0 to become leader
	waitLeader(t, r0)

	// Add nodes 1 and 2 as voters to the cluster
	for i := 1; i < n; i++ {
		dirs[i] = t.TempDir()
		stores[i] = NewMetadataStore(3)
		cfg := RaftConfig{
			NodeID: fmt.Sprintf("m%d", i), BindAddr: fmt.Sprintf("127.0.0.1:%d", basePort+i),
			DataDir: dirs[i], Bootstrap: false, ApplyTimeout: 2 * time.Second,
		}
		os.MkdirAll(cfg.DataDir, 0750)
		r, _, err := BootstrapRaft(cfg, stores[i])
		if err != nil {
			t.Fatalf("bootstrap node %d: %v", i, err)
		}
		rafts[i] = r

		// Add this node as a voter to the cluster
		future := r0.AddVoter(
			raft.ServerID(cfg.NodeID),
			raft.ServerAddress(cfg.BindAddr),
			0, 0,
		)
		if err := future.Error(); err != nil {
			t.Logf("AddVoter node %d: %v (nodes may need explicit join protocol)", i, err)
		}
	}

	defer func() {
		for _, r := range rafts {
			if r != nil {
				r.Shutdown()
			}
		}
	}()

	// Kill leader
	leader := 0
	for i, r := range rafts {
		if r != nil && r.State() == raft.Leader {
			leader = i
			break
		}
	}
	t.Logf("killing leader node %d", leader)
	rafts[leader].Shutdown()
	rafts[leader] = nil

	// Wait for new leader from remaining nodes
	newLeader := waitLeaderIdx(t, rafts, leader)
	t.Logf("new leader: node %d", newLeader)
}

func waitLeader(t *testing.T, r *raft.Raft) {
	t.Helper()
	dl := time.After(5 * time.Second)
	for r.State() != raft.Leader {
		select {
		case <-dl:
			t.Fatal("leader not elected")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func waitLeaderIdx(t *testing.T, rafts []*raft.Raft, exclude int) int {
	t.Helper()
	dl := time.After(15 * time.Second)
	for {
		select {
		case <-dl:
			for i, r := range rafts {
				if r != nil {
					t.Logf("node %d state: %s", i, r.State())
				}
			}
			t.Fatal("no leader elected")
		default:
			for i, r := range rafts {
				if i == exclude {
					continue
				}
				if r != nil && r.State() == raft.Leader {
					return i
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}
