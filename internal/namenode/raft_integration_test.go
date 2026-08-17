package namenode

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	domainnn "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var raftTestMu sync.Mutex

func TestRaftCluster_LeaderElection(t *testing.T) {
	raftTestMu.Lock()
	defer raftTestMu.Unlock()

	dir := t.TempDir()
	basePort := 12000 + int(time.Now().UnixNano()%10000)
	store := NewMetadataStore(3)
	reg := domainnn.NewInMemoryDataNodeRegistry()
	cfg := RaftConfig{
		NodeID: fmt.Sprintf("n%d", basePort), BindAddr: fmt.Sprintf("127.0.0.1:%d", basePort),
		DataDir: dir, Bootstrap: true, ApplyTimeout: 2 * time.Second,
	}
	os.MkdirAll(cfg.DataDir, 0750)
	r, _, err := BootstrapRaft(cfg, store, reg)
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
	r, _, _ := BootstrapRaft(cfg, store, domainnn.NewInMemoryDataNodeRegistry())
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
	r0, _, err := BootstrapRaft(cfg0, stores[0], domainnn.NewInMemoryDataNodeRegistry())
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
		r, _, err := BootstrapRaft(cfg, stores[i], domainnn.NewInMemoryDataNodeRegistry())
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

// TestRaftCluster_RegistryReplication verifies that datanode registrations
// submitted to the leader are applied on EVERY namenode's registry: the fix
// for registrations landing on a single namenode.
func TestRaftCluster_RegistryReplication(t *testing.T) {
	raftTestMu.Lock()
	defer raftTestMu.Unlock()

	basePort := 35000 + int(time.Now().UnixNano()%10000)
	const n = 3

	stores := make([]*MetadataStore, n)
	regs := make([]*domainnn.InMemoryDataNodeRegistry, n)
	rafts := make([]*raft.Raft, n)

	bootstrap := func(i int, port int) {
		dirs := t.TempDir()
		stores[i] = NewMetadataStore(3)
		regs[i] = domainnn.NewInMemoryDataNodeRegistry()
		cfg := RaftConfig{
			NodeID: fmt.Sprintf("r%d", i), BindAddr: fmt.Sprintf("127.0.0.1:%d", port),
			DataDir: dirs, Bootstrap: i == 0, ApplyTimeout: 2 * time.Second,
		}
		os.MkdirAll(cfg.DataDir, 0750)
		r, _, err := BootstrapRaft(cfg, stores[i], regs[i])
		if err != nil {
			t.Fatalf("bootstrap node %d: %v", i, err)
		}
		rafts[i] = r
	}

	bootstrap(0, basePort)
	waitLeader(t, rafts[0])
	for i := 1; i < n; i++ {
		bootstrap(i, basePort+i)
		future := rafts[0].AddVoter(
			raft.ServerID(fmt.Sprintf("r%d", i)),
			raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", basePort+i)),
			0, 0,
		)
		if err := future.Error(); err != nil {
			t.Fatalf("AddVoter node %d: %v", i, err)
		}
	}

	defer func() {
		for _, r := range rafts {
			if r != nil {
				r.Shutdown()
			}
		}
	}()

	srv := &NameNodeServer{store: stores[0], registry: regs[0]}
	srv.SetRaft(rafts[0])

	start := time.Now()
	if err := srv.ReplicateRegister(&domainnn.DataNodeStatus{
		ID: "dn-repl", Address: "100.64.0.42:9100",
		TotalStorageBytes: 1 << 30, FreeStorageBytes: 1 << 30,
	}); err != nil {
		t.Fatalf("ReplicateRegister: %v", err)
	}
	if err := srv.ReplicateUnavailable("dn-repl"); err != nil {
		t.Fatalf("ReplicateUnavailable: %v", err)
	}
	t.Logf("replication took %v", time.Since(start))

	// Every node must end with dn-repl registered but unavailable.
	dl := time.After(10 * time.Second)
	for {
		all := true
		for i := 0; i < n; i++ {
			nodes := regs[i].GetAll()
			if len(nodes) != 1 || nodes[0].ID != "dn-repl" || nodes[0].IsAvailable {
				all = false
			}
		}
		if all {
			return
		}
		select {
		case <-dl:
			for i := 0; i < n; i++ {
				t.Logf("node %d registry: %+v", i, regs[i].GetAll())
			}
			t.Fatal("registry not replicated to all namenodes")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// TestRaftCluster_FollowerRejectsRegistration verifies that a follower
// answers codes.Unavailable so datanode clients rotate to the leader.
func TestRaftCluster_FollowerRejectsRegistration(t *testing.T) {
	raftTestMu.Lock()
	defer raftTestMu.Unlock()

	basePort := 36000 + int(time.Now().UnixNano()%10000)
	const n = 3

	stores := make([]*MetadataStore, n)
	regs := make([]*domainnn.InMemoryDataNodeRegistry, n)
	rafts := make([]*raft.Raft, n)

	bootstrap := func(i int, port int) {
		dirs := t.TempDir()
		stores[i] = NewMetadataStore(3)
		regs[i] = domainnn.NewInMemoryDataNodeRegistry()
		cfg := RaftConfig{
			NodeID: fmt.Sprintf("f%d", i), BindAddr: fmt.Sprintf("127.0.0.1:%d", port),
			DataDir: dirs, Bootstrap: i == 0, ApplyTimeout: 2 * time.Second,
		}
		os.MkdirAll(cfg.DataDir, 0750)
		r, _, err := BootstrapRaft(cfg, stores[i], regs[i])
		if err != nil {
			t.Fatalf("bootstrap node %d: %v", i, err)
		}
		rafts[i] = r
	}

	bootstrap(0, basePort)
	waitLeader(t, rafts[0])
	for i := 1; i < n; i++ {
		bootstrap(i, basePort+i)
		future := rafts[0].AddVoter(
			raft.ServerID(fmt.Sprintf("f%d", i)),
			raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", basePort+i)),
			0, 0,
		)
		if err := future.Error(); err != nil {
			t.Fatalf("AddVoter node %d: %v", i, err)
		}
	}

	defer func() {
		for _, r := range rafts {
			if r != nil {
				r.Shutdown()
			}
		}
	}()

	// Leadership can churn right after AddVoter; retry until we find a node
	// that is a stable follower and observe the expected rejection.
	dl := time.After(10 * time.Second)
	rejected := false
	for !rejected {
		select {
		case <-dl:
			for i := 0; i < n; i++ {
				t.Logf("node %d state: %s", i, rafts[i].State())
			}
			t.Fatal("never observed follower rejection")
		default:
		}
		for i := 0; i < n; i++ {
			if rafts[i].State() != raft.Follower {
				continue
			}
			followerSrv := NewNameNodeServer(stores[i], regs[i], nil, nil)
			followerSrv.SetRaft(rafts[i])
			_, err := followerSrv.RegisterDataNode(context.Background(), &pb.RegisterDataNodeRequest{
				DatanodeId: "dn-follower-rejected", Address: "100.64.0.7:9100",
			})
			if err == nil {
				// Node won an election between the check and the call.
				continue
			}
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("expected Unavailable from follower, got %v", err)
			}
			_, err = followerSrv.Heartbeat(context.Background(), &pb.HeartbeatRequest{DatanodeId: "dn-follower-rejected"})
			if err == nil {
				continue
			}
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("expected Unavailable heartbeat from follower, got %v", err)
			}
			if len(regs[i].GetAll()) != 0 {
				t.Errorf("follower registry should be empty, got %+v", regs[i].GetAll())
			}
			rejected = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
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
