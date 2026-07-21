package namenode

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

/*
 * Raft integration tests — single-node consensus and restart.
 * Multi-node peer joining requires the Raft AddVoter protocol and
 * proper transport wiring (future work: Sprint 4+).
 */

func TestRaftCluster_LeaderElection(t *testing.T) {
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

	// Wait for leader election before submitting
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
		{Op: "mkdir", Path: "/raft/dir", Mode: 0755},
		{Op: "create_file", Path: "/raft/dir/b.txt", FileID: "f-b", Mode: 0644},
	}
	for _, e := range ops {
		if err := srv.raftSubmit(e); err != nil {
			t.Fatalf("raftSubmit %s: %v", e.Op, err)
		}
	}
	if _, err := store.GetFile("/raft/a.txt"); err != nil {
		t.Error("missing /raft/a.txt")
	}
	if !store.IsDir("/raft/dir") {
		t.Error("missing /raft/dir")
	}

	srv.raftSubmit(WALEntry{Op: "delete_file", Path: "/raft/a.txt"})
	if _, err := store.GetFile("/raft/a.txt"); err == nil {
		t.Error("/raft/a.txt not deleted")
	}

	srv.raftSubmit(WALEntry{Op: "rename", OldPath: "/raft/dir/b.txt", NewPath: "/raft/dir/c.txt"})
	if _, err := store.GetFile("/raft/dir/b.txt"); err == nil {
		t.Error("/raft/dir/b.txt not renamed")
	}
	if _, err := store.GetFile("/raft/dir/c.txt"); err != nil {
		t.Error("missing /raft/dir/c.txt after rename")
	}
}
