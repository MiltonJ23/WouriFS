package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"
	"github.com/MiltonJ23/WouriFS/internal/datanode"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// benchCmd measures chunk write/read latency and throughput (fio-like for
// WouriFS): gRPC stream path vs direct disk I/O.
//
// Example:
//
//	wourifs bench --blocks 64 --n 5
func benchCmd() *cobra.Command {
	var chunkSize int64
	var numBlocks int
	var iterations int

	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Benchmark chunk I/O latency and throughput",
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, err := os.MkdirTemp("", "wourifs-bench-*")
			if err != nil {
				return err
			}
			defer os.RemoveAll(dir)

			store, err := datanode.NewChunkStore(dir)
			if err != nil {
				return err
			}

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return err
			}

			srv := grpc.NewServer()
			datanodepb.RegisterDataNodeServiceServer(srv, datanode.NewServer(store, 1<<30))
			go srv.Serve(lis)
			defer srv.GracefulStop()

			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("dial: %w", err)
			}
			client := datanodepb.NewDataNodeServiceClient(conn)
			defer conn.Close()

			block := make([]byte, 1<<20) // 1 MB

			fmt.Println("=== WouriFS Chunk I/O Benchmark ===")
			fmt.Printf("chunk size: %d MB | blocks: %d | iterations: %d\n\n", chunkSize, numBlocks, iterations)

			// --- Write benchmark ---
			var writeLatencies []time.Duration
			for i := 0; i < iterations; i++ {
				chunkID := fmt.Sprintf("bench-chunk-%d", i)
				start := time.Now()

				stream, err := client.WriteChunk(context.Background())
				if err != nil {
					return fmt.Errorf("write stream: %w", err)
				}

				for j := 0; j < numBlocks; j++ {
					stream.Send(&datanodepb.WriteChunkRequest{
						ChunkId:    chunkID,
						Data:       block,
						BlockIndex: int32(j),
						IsLast:     j == numBlocks-1,
					})
				}

				if _, err = stream.CloseAndRecv(); err != nil {
					return fmt.Errorf("write close: %w", err)
				}
				writeLatencies = append(writeLatencies, time.Since(start))
			}

			// --- Read benchmark ---
			var readLatencies []time.Duration
			for i := 0; i < iterations; i++ {
				chunkID := fmt.Sprintf("bench-chunk-%d", i)
				start := time.Now()

				stream, err := client.ReadChunk(context.Background(), &datanodepb.ReadChunkRequest{ChunkId: chunkID})
				if err != nil {
					return fmt.Errorf("read stream: %w", err)
				}

				total := int64(0)
				for {
					resp, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return fmt.Errorf("read recv: %w", err)
					}
					total += int64(len(resp.Data))
					if resp.IsLast {
						break
					}
				}
				readLatencies = append(readLatencies, time.Since(start))
				_ = total
			}

			fmt.Printf("%-12s %12s %12s %12s\n", "op", "min", "avg", "max")
			printBenchStats("write", writeLatencies, int64(numBlocks)<<20)
			printBenchStats("read", readLatencies, int64(numBlocks)<<20)

			// --- Direct disk I/O (bypass gRPC) ---
			fmt.Println("\n--- Direct Disk I/O (no gRPC overhead) ---")
			var diskWriteLat, diskReadLat []time.Duration

			for i := 0; i < iterations; i++ {
				chunkID := fmt.Sprintf("disk-bench-%d", i)
				buf := bytes.NewReader(bytes.Repeat(block, numBlocks))

				start := time.Now()
				if _, err := store.Write(chunkID, buf); err != nil {
					return fmt.Errorf("disk write: %w", err)
				}
				diskWriteLat = append(diskWriteLat, time.Since(start))

				start = time.Now()
				rc, _, err := store.Read(chunkID)
				if err != nil {
					return fmt.Errorf("disk read: %w", err)
				}
				io.Copy(io.Discard, rc)
				rc.Close()
				diskReadLat = append(diskReadLat, time.Since(start))
			}

			printBenchStats("disk-write", diskWriteLat, int64(numBlocks)<<20)
			printBenchStats("disk-read", diskReadLat, int64(numBlocks)<<20)
			return nil
		},
	}

	cmd.Flags().Int64Var(&chunkSize, "size", 64, "chunk size in MB")
	cmd.Flags().IntVar(&numBlocks, "blocks", 64, "number of 1MB blocks per chunk")
	cmd.Flags().IntVarP(&iterations, "iterations", "n", 5, "number of iterations")
	return cmd
}

func printBenchStats(op string, latencies []time.Duration, bytes int64) {
	if len(latencies) == 0 {
		log.Printf("no samples for %s", op)
		return
	}
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
