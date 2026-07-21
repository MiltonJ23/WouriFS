package auditsvc

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type testReplica struct {
	mu      sync.Mutex
	entries []AuditReplicationEntry
	failing bool
}

func (r *testReplica) Send(entry AuditReplicationEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing {
		return errors.New("connection lost")
	}
	r.entries = append(r.entries, entry)
	return nil
}

func (r *testReplica) Close() error { return nil }

func TestReplicator_SendDirect(t *testing.T) {
	replica := &testReplica{}
	rep := NewReplicator(10)
	rep.SetClient(replica)

	entry := AuditReplicationEntry{EntryID: 1, UserID: "user-1", Operation: "create_file"}
	if err := rep.Push(entry); err != nil {
		t.Fatalf("push: %v", err)
	}
	if len(replica.entries) != 1 {
		t.Errorf("expected 1 entry sent, got %d", len(replica.entries))
	}
}

func TestReplicator_BufferOnDisconnect(t *testing.T) {
	replica := &testReplica{failing: true}
	rep := NewReplicator(5)
	rep.SetClient(replica)

	for i := 0; i < 5; i++ {
		if err := rep.Push(AuditReplicationEntry{EntryID: int64(i)}); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if rep.Lag() != 5 {
		t.Errorf("expected lag=5, got %d", rep.Lag())
	}
}

func TestReplicator_BufferOverflow(t *testing.T) {
	replica := &testReplica{failing: true}
	rep := NewReplicator(3)
	rep.SetClient(replica)

	for i := 0; i < 3; i++ {
		rep.Push(AuditReplicationEntry{EntryID: int64(i)})
	}
	err := rep.Push(AuditReplicationEntry{EntryID: 4})
	if err == nil {
		t.Error("expected buffer overflow error")
	}
}

func TestReplicator_Drain(t *testing.T) {
	replica := &testReplica{failing: true}
	rep := NewReplicator(10)
	rep.SetClient(replica)

	for i := 0; i < 5; i++ {
		rep.Push(AuditReplicationEntry{EntryID: int64(i)})
	}

	// Reconnect
	replica.failing = false
	rep.SetClient(&testReplica{entries: replica.entries}) // new client

	rep.SetClient(replica)
	if err := rep.Drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if rep.Lag() != 0 {
		t.Errorf("expected lag=0 after drain, got %d", rep.Lag())
	}
}

func TestReplicator_Shutdown(t *testing.T) {
	replica := &testReplica{}
	rep := NewReplicator(10)
	rep.SetClient(replica)
	rep.Shutdown()
	if rep.Lag() != 0 {
		t.Error("expected empty after shutdown")
	}
}

func TestReplicator_DisconnectAndReconnect(t *testing.T) {
	rep := NewReplicator(10)

	// Connect, push, disconnect
	rep.SetClient(&testReplica{})
	rep.Push(AuditReplicationEntry{EntryID: 1, Timestamp: time.Now()})
	rep.Disconnect()

	// Push while disconnected
	rep.Push(AuditReplicationEntry{EntryID: 2})

	// Reconnect and drain
	replica2 := &testReplica{}
	rep.SetClient(replica2)
	rep.Drain()

	if len(replica2.entries) != 1 {
		t.Errorf("expected 1 drained entry, got %d", len(replica2.entries))
	}
}
