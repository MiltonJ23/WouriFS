package datanode

import (
	"bytes"
	"context"
	"testing"

	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"
)

/*
 * Targeted tests for Status, ChunkCount, and DeleteChunk error paths
 * that were below 80 % line coverage.
 */

func TestDataNodeServer_Status(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewChunkStore(dir)
	srv := NewServer(store, 1<<30)

	resp, err := srv.Status(context.Background(), &datanodepb.StatusRequest{})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if resp.ChunkCount != 0 {
		t.Errorf("expected ChunkCount=0 for empty store, got %d", resp.ChunkCount)
	}
	if resp.UsedBytes != 0 {
		t.Errorf("expected UsedBytes=0, got %d", resp.UsedBytes)
	}
}

func TestChunkStore_ChunkCount(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewChunkStore(dir)

	if n := store.ChunkCount(); n != 0 {
		t.Errorf("expected ChunkCount=0, got %d", n)
	}

	store.Write("a", bytes.NewReader([]byte("hello")))
	store.Write("b", bytes.NewReader([]byte("world")))

	if n := store.ChunkCount(); n != 2 {
		t.Errorf("expected ChunkCount=2, got %d", n)
	}
}

func TestDataNodeServer_DeleteChunk_NotFound(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewChunkStore(dir)
	srv := NewServer(store, 1<<30)

	_, err := srv.DeleteChunk(context.Background(), &datanodepb.DeleteChunkRequest{
		ChunkId: "does-not-exist",
	})
	if err == nil {
		t.Error("expected NotFound error for non-existent chunk")
	}
}
