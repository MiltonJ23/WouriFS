package datanode

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestChunkStore_BDD(t *testing.T) {
	dir := t.TempDir()

	t.Run("Given a fresh chunk store", func(t *testing.T) {
		store, err := NewChunkStore(dir)
		if err != nil {
			t.Fatalf("new store: %v", err)
		}

		t.Run("When writing a chunk", func(t *testing.T) {
			data := bytes.NewReader([]byte("hello distributed filesystem"))
			n, err := store.Write("chunk-001", data)
			if err != nil {
				t.Fatalf("write: %v", err)
			}
			if n != 28 {
				t.Errorf("expected 28 bytes written, got %d", n)
			}
			if !store.Exists("chunk-001") {
				t.Error("chunk should exist after write")
			}
		})

		t.Run("When reading an existing chunk", func(t *testing.T) {
			store.Write("chunk-002", bytes.NewReader([]byte("read-me")))
			rc, size, err := store.Read("chunk-002")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			defer rc.Close()
			if size != 7 {
				t.Errorf("expected 7 bytes, got %d", size)
			}
			buf, _ := io.ReadAll(rc)
			if string(buf) != "read-me" {
				t.Errorf("expected 'read-me', got '%s'", string(buf))
			}
		})

		t.Run("When reading a non-existent chunk", func(t *testing.T) {
			_, _, err := store.Read("chunk-ghost")
			if err == nil {
				t.Error("expected error for non-existent chunk")
			}
		})

		t.Run("When deleting a chunk", func(t *testing.T) {
			store.Write("chunk-003", bytes.NewReader([]byte("temp")))
			if err := store.Delete("chunk-003"); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if store.Exists("chunk-003") {
				t.Error("chunk should not exist after delete")
			}
		})

		t.Run("When deleting a non-existent chunk", func(t *testing.T) {
			err := store.Delete("chunk-ghost2")
			if err == nil {
				t.Error("expected error for non-existent chunk")
			}
		})
	})

	t.Run("Given multiple chunks", func(t *testing.T) {
		dir2 := t.TempDir()
		store, _ := NewChunkStore(dir2)
		store.Write("a", bytes.NewReader(make([]byte, 100)))
		store.Write("b", bytes.NewReader(make([]byte, 200)))

		total := store.TotalSize()
		if total != 300 {
			t.Errorf("expected total=300, got %d", total)
		}
	})

	t.Run("Given auto-created data dir", func(t *testing.T) {
		nested := filepath.Join(dir, "nested", "chunks")
		store, err := NewChunkStore(nested)
		if err != nil {
			t.Fatalf("should auto-create dirs: %v", err)
		}
		if _, err := os.Stat(store.dataDir); os.IsNotExist(err) {
			t.Error("data dir not created")
		}
	})
}

// startTestDatanode spins up a real gRPC Datanode for integration testing.
func startTestDatanode(t *testing.T, dir string) (*grpc.ClientConn, datanodepb.DataNodeServiceClient, func()) {
	t.Helper()

	store, err := NewChunkStore(dir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	datanodepb.RegisterDataNodeServiceServer(srv, NewServer(store))

	go srv.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := datanodepb.NewDataNodeServiceClient(conn)

	cleanup := func() {
		conn.Close()
		srv.GracefulStop()
	}
	return conn, client, cleanup
}

func TestDataNodeServer_BDD(t *testing.T) {
	t.Run("Given a running Datanode gRPC server", func(t *testing.T) {
		dir := t.TempDir()
		_, client, cleanup := startTestDatanode(t, dir)
		defer cleanup()

		t.Run("When writing a chunk via streaming", func(t *testing.T) {
			stream, err := client.WriteChunk(context.Background())
			if err != nil {
				t.Fatalf("write chunk stream: %v", err)
			}

			data := []byte("distributed write test data")
			stream.Send(&datanodepb.WriteChunkRequest{
				ChunkId:    "chunk-stream-1",
				Data:       data,
				BlockIndex: 0,
				IsLast:     true,
			})

			resp, err := stream.CloseAndRecv()
			if err != nil {
				t.Fatalf("close and recv: %v", err)
			}
			if resp.BytesWritten != int64(len(data)) {
				t.Errorf("expected %d bytes, got %d", len(data), resp.BytesWritten)
			}
		})

		t.Run("When reading a chunk via streaming", func(t *testing.T) {
			// Write first
			wstream, _ := client.WriteChunk(context.Background())
			expectedData := []byte("read-stream-test-data-here!")
			wstream.Send(&datanodepb.WriteChunkRequest{
				ChunkId: "chunk-read-1", Data: expectedData, IsLast: true,
			})
			wstream.CloseAndRecv()

			// Read
			rstream, err := client.ReadChunk(context.Background(), &datanodepb.ReadChunkRequest{ChunkId: "chunk-read-1"})
			if err != nil {
				t.Fatalf("read chunk: %v", err)
			}

			var buf bytes.Buffer
			for {
				resp, err := rstream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("recv: %v", err)
				}
				buf.Write(resp.Data)
				if resp.IsLast {
					break
				}
			}

			if !bytes.Equal(buf.Bytes(), expectedData) {
				t.Errorf("data mismatch: got %q, want %q", buf.Bytes(), expectedData)
			}
		})

		t.Run("When reading a non-existent chunk", func(t *testing.T) {
			stream, err := client.ReadChunk(context.Background(), &datanodepb.ReadChunkRequest{ChunkId: "no-such-chunk"})
			if err != nil {
				t.Fatalf("stream creation: %v", err)
			}
			_, err = stream.Recv() // error arrives on first Recv for server-side streaming
			if err == nil {
				t.Error("expected error for non-existent chunk")
			}
		})

		t.Run("When deleting a chunk", func(t *testing.T) {
			// Write then delete
			wstream, _ := client.WriteChunk(context.Background())
			wstream.Send(&datanodepb.WriteChunkRequest{
				ChunkId: "chunk-del-1", Data: []byte("delete-me"), IsLast: true,
			})
			wstream.CloseAndRecv()

			_, err := client.DeleteChunk(context.Background(), &datanodepb.DeleteChunkRequest{ChunkId: "chunk-del-1"})
			if err != nil {
				t.Fatalf("delete: %v", err)
			}

			stream, _ := client.ReadChunk(context.Background(), &datanodepb.ReadChunkRequest{ChunkId: "chunk-del-1"})
			_, err = stream.Recv() // error on Recv for server-streaming
			if err == nil {
				t.Error("expected error after delete")
			}
		})

		t.Run("When replicating a chunk (FR-D-004)", func(t *testing.T) {
			rstream, err := client.ReplicateChunk(context.Background())
			if err != nil {
				t.Fatalf("replicate stream: %v", err)
			}

			repData := []byte("replicated-data-block")
			rstream.Send(&datanodepb.ReplicateChunkRequest{
				ChunkId: "chunk-rep-1", Data: repData, IsLast: true,
			})

			resp, err := rstream.CloseAndRecv()
			if err != nil {
				t.Fatalf("replicate close: %v", err)
			}
			if resp.BytesWritten != int64(len(repData)) {
				t.Errorf("expected %d bytes, got %d", len(repData), resp.BytesWritten)
			}
		})

		t.Run("When replicating an already existing chunk (idempotent)", func(t *testing.T) {
			// Replicate same chunk again
			rstream, _ := client.ReplicateChunk(context.Background())
			rstream.Send(&datanodepb.ReplicateChunkRequest{
				ChunkId: "chunk-rep-1", Data: []byte("replicated-data-block"), IsLast: true,
			})
			resp, err := rstream.CloseAndRecv()
			if err != nil {
				t.Fatalf("idempotent replicate: %v", err)
			}
			// BytesWritten should be 0 since chunk already exists
			if resp.BytesWritten != 0 {
				t.Errorf("expected 0 bytes for idempotent replicate, got %d", resp.BytesWritten)
			}
		})
	})

	t.Run("Given multi-block streaming (FR-D-006)", func(t *testing.T) {
		dir := t.TempDir()
		_, client, cleanup := startTestDatanode(t, dir)
		defer cleanup()

		t.Run("When writing 3MB in 1MB blocks", func(t *testing.T) {
			stream, _ := client.WriteChunk(context.Background())

			total := 0
			for i := 0; i < 3; i++ {
				block := make([]byte, 1<<20) // 1MB
				for j := range block {
					block[j] = byte(i)
				}
				total += len(block)
				stream.Send(&datanodepb.WriteChunkRequest{
					ChunkId:    "chunk-multi",
					Data:       block,
					BlockIndex: int32(i),
					IsLast:     i == 2,
				})
			}

			resp, err := stream.CloseAndRecv()
			if err != nil {
				t.Fatalf("close: %v", err)
			}
			if resp.BytesWritten != int64(total) {
				t.Errorf("expected %d bytes, got %d", total, resp.BytesWritten)
			}
		})

		t.Run("When reading back multi-block chunk", func(t *testing.T) {
			rstream, _ := client.ReadChunk(context.Background(), &datanodepb.ReadChunkRequest{ChunkId: "chunk-multi"})

			var buf bytes.Buffer
			blockCount := 0
			for {
				resp, err := rstream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("recv: %v", err)
				}
				buf.Write(resp.Data)
				blockCount++
				if resp.IsLast {
					break
				}
			}
			if buf.Len() != 3<<20 {
				t.Errorf("expected 3MB, got %d", buf.Len())
			}
			if blockCount != 3 {
				t.Errorf("expected 3 blocks, got %d", blockCount)
			}
		})
	})
}

func TestChunkStore_Concurrent(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewChunkStore(dir)

	const workers = 50
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			chunkID := fmt.Sprintf("concurrent-chunk-%d", id)
			data := bytes.NewReader([]byte(fmt.Sprintf("data-%d", id)))
			if _, err := store.Write(chunkID, data); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent write error: %v", err)
	}
}

// init ensures all deps are compiled.
func init() {
	_ = fmt.Sprintf
}
