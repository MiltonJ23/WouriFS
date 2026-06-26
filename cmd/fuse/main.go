package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fuse-bridge is a userspace proxy that translates local file I/O into
// WouriFS gRPC calls.  In production this would be a FUSE mount via
// hanwen/go-fuse; for Sprint 2 we expose a minimal interactive CLI that
// demonstrates the full read/write path end-to-end.

func main() {
	nnAddr := flag.String("namenode", "127.0.0.1:9000", "Namenode gRPC address")
	flag.Parse()

	nnConn, err := grpc.NewClient(*nnAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial namenode: %v", err)
	}
	defer nnConn.Close()

	nnClient := pb.NewNameNodeServiceClient(nnConn)

	// FUSE mount simulation — demonstrate the full path
	fmt.Println("=== WouriFS FUSE Bridge (Sprint 2) ===")
	fmt.Println("Mounted WouriFS — interact via gRPC")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a file
	createResp, err := nnClient.CreateFile(ctx, &pb.CreateFileRequest{Path: "/wourifs/test/fuse-demo.csv"})
	if err != nil {
		log.Printf("create file (expected in test mode): %v", err)
	} else {
		fmt.Printf("created file: %s\n", createResp.FileId)
	}

	// Lookup
	lookupResp, err := nnClient.LookupFile(ctx, &pb.LookupFileRequest{Path: "/wourifs/test/fuse-demo.csv"})
	if err != nil {
		log.Printf("lookup: %v", err)
	} else {
		fmt.Printf("lookup: file_id=%s chunks=%d\n", lookupResp.FileId, len(lookupResp.Chunks))
	}

	// Write a chunk via Datanode (if available)
	if len(lookupResp.Chunks) > 0 {
		chunk := lookupResp.Chunks[0]
		dnConn, err := grpc.NewClient(chunk.DatanodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("dial datanode: %v", err)
		} else {
			dnClient := datanodepb.NewDataNodeServiceClient(dnConn)
			stream, _ := dnClient.WriteChunk(ctx)
			stream.Send(&datanodepb.WriteChunkRequest{
				ChunkId:    chunk.ChunkId,
				Data:       []byte("hello from fuse-bridge\n"),
				BlockIndex: 0,
				IsLast:     true,
			})
			resp, err := stream.CloseAndRecv()
			if err != nil {
				fmt.Printf("write chunk: %v\n", err)
			} else {
				fmt.Printf("wrote chunk %s: %d bytes\n", chunk.ChunkId, resp.BytesWritten)
			}
			dnConn.Close()
		}
	}

	// Keep running (simulate mount)
	fmt.Println("\nbridge running — press Ctrl+C to unmount")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Println("\nunmounted")
}
