package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"
	"github.com/MiltonJ23/WouriFS/internal/datanode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// bench tool measures chunk write/read latency and throughput (fio-like for WouriFS).
// Usage: go run ./cmd/bench -size=64 -blocks=64

func main() {
	chunkSize := flag.Int64("size", 64, "chunk size in MB")
	numBlocks := flag.Int("blocks", 64, "number of 1MB blocks per chunk")
	iterations := flag.Int("n", 5, "number of iterations")
	flag.Parse()

	dir, err := os.MkdirTemp("", "wourifs-bench-*")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store, err := datanode.NewChunkStore(dir)
	if err != nil {
		log.Fatalf("store: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	datanodepb.RegisterDataNodeServiceServer(srv, datanode.NewServer(store, 1<<30))
	go srv.Serve(lis)
	defer srv.GracefulStop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	client := datanodepb.NewDataNodeServiceClient(conn)
	defer conn.Close()

	block := make([]byte, 1<<20) // 1 MB

	fmt.Println("=== WouriFS Chunk I/O Benchmark ===")
	fmt.Printf("chunk size: %d MB | blocks: %d | iterations: %d\n\n", *chunkSize, *numBlocks, *iterations)

	// --- Write benchmark ---
	var writeLatencies []time.Duration
	for i := 0; i < *iterations; i++ {
		chunkID := fmt.Sprintf("bench-chunk-%d", i)
		start := time.Now()

		stream, err := client.WriteChunk(context.Background())
		if err != nil {
			log.Fatalf("write stream: %v", err)
		}

		for j := 0; j < *numBlocks; j++ {
			stream.Send(&datanodepb.WriteChunkRequest{
				ChunkId:    chunkID,
				Data:       block,
				BlockIndex: int32(j),
				IsLast:     j == *numBlocks-1,
			})
		}

		_, err = stream.CloseAndRecv()
		if err != nil {
			log.Fatalf("write close: %v", err)
		}
		elapsed := time.Since(start)
		writeLatencies = append(writeLatencies, elapsed)
	}

	// --- Read benchmark ---
	var readLatencies []time.Duration
	for i := 0; i < *iterations; i++ {
		chunkID := fmt.Sprintf("bench-chunk-%d", i)
		start := time.Now()

		stream, err := client.ReadChunk(context.Background(), &datanodepb.ReadChunkRequest{ChunkId: chunkID})
		if err != nil {
			log.Fatalf("read stream: %v", err)
		}

		total := int64(0)
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("read recv: %v", err)
			}
			total += int64(len(resp.Data))
			if resp.IsLast {
				break
			}
		}

		elapsed := time.Since(start)
		readLatencies = append(readLatencies, elapsed)
		_ = total
	}

	fmt.Printf("%-12s %12s %12s %12s\n", "op", "min", "avg", "max")
	printStats("write", writeLatencies, int64(*numBlocks)<<20)
	printStats("read", readLatencies, int64(*numBlocks)<<20)

	// --- Direct disk I/O (bypass gRPC) ---
	fmt.Println("\n--- Direct Disk I/O (no gRPC overhead) ---")
	var diskWriteLat, diskReadLat []time.Duration

	for i := 0; i < *iterations; i++ {
		chunkID := fmt.Sprintf("disk-bench-%d", i)
		buf := bytes.NewReader(bytes.Repeat(block, *numBlocks))

		start := time.Now()
		_, err := store.Write(chunkID, buf)
		if err != nil {
			log.Fatalf("disk write: %v", err)
		}
		diskWriteLat = append(diskWriteLat, time.Since(start))

		start = time.Now()
		rc, _, err := store.Read(chunkID)
		if err != nil {
			log.Fatalf("disk read: %v", err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
		diskReadLat = append(diskReadLat, time.Since(start))
	}

	printStats("disk-write", diskWriteLat, int64(*numBlocks)<<20)
	printStats("disk-read", diskReadLat, int64(*numBlocks)<<20)
}

func printStats(op string, latencies []time.Duration, bytes int64) {
	min, max, sum := latencies[0], latencies[0], time.Duration(0)
	for _, l := range latencies {
		if l < min {
			min = l
		}
		if l > max {
			max = l
		}
		sum += l
	}
	avg := sum / time.Duration(len(latencies))
	throughputMBps := float64(bytes) / avg.Seconds() / (1 << 20)
	fmt.Printf("%-12s %10v %10v %10v %10.1f MB/s\n", op, min.Round(time.Microsecond), avg.Round(time.Microsecond), max.Round(time.Microsecond), throughputMBps)
}
