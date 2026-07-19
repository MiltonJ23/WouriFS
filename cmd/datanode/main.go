package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"
	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	"github.com/MiltonJ23/WouriFS/internal/datanode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9001", "gRPC listen address")
	dataDir := flag.String("data", "./data/datanode", "chunk storage directory")
	nodeID := flag.String("id", "", "unique datanode ID (generated if empty)")
	nnAddr := flag.String("namenode", "127.0.0.1:9000", "namenode gRPC address")
	hbFreq := flag.Duration("hb-freq", 5*time.Second, "heartbeat interval")
	tlsCertPath := flag.String("tls-cert", "", "path to TLS certificate PEM")
	tlsKeyPath := flag.String("tls-key", "", "path to TLS private key PEM")
	caCertPath := flag.String("tls-ca", "", "path to CA certificate PEM")
	totalCap := flag.Int64("capacity", 1<<30, "total storage capacity in bytes")
	flag.Parse()

	id := *nodeID
	if id == "" {
		id = fmt.Sprintf("dn-%x", time.Now().UnixNano())
	}

	if err := os.MkdirAll(*dataDir, 0750); err != nil {
		log.Fatalf("data dir: %v", err)
	}

	// Chunk store
	store, err := datanode.NewChunkStore(*dataDir)
	if err != nil {
		log.Fatalf("chunk store: %v", err)
	}

	// gRPC server with optional TLS
	var grpcSrv *grpc.Server
	if *tlsCertPath != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCertPath, *tlsKeyPath)
		if err != nil {
			log.Fatalf("tls cert: %v", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		}
		if *caCertPath != "" {
			caBytes, err := os.ReadFile(*caCertPath)
			if err != nil {
				log.Fatalf("ca cert: %v", err)
			}
			caPool := x509.NewCertPool()
			caPool.AppendCertsFromPEM(caBytes)
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
			tlsCfg.ClientCAs = caPool
		}
		grpcSrv = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	} else {
		grpcSrv = grpc.NewServer()
	}

	dnSrv := datanode.NewServer(store)
	datanodepb.RegisterDataNodeServiceServer(grpcSrv, dnSrv)

	// Listen
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("datanode %s listening on %s", id, *addr)

	// Background: register + heartbeat loop
	ctx, cancel := contextWithShutdown(grpcSrv)
	defer cancel()

	go heartbeatLoop(ctx, id, *addr, *nnAddr, *hbFreq, *totalCap)

	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// heartbeatLoop registers with the Namenode once, then sends periodic heartbeats (FR-D-003, FR-D-007).
func heartbeatLoop(ctx context.Context, nodeID, addr, nnAddr string, freq time.Duration, totalCap int64) {
	// Register
	if err := registerWithNamenode(nnAddr, nodeID, addr, totalCap); err != nil {
		log.Printf("register failed (will retry on heartbeat): %v", err)
	}

	ticker := time.NewTicker(freq)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sendHeartbeat(nnAddr, nodeID); err != nil {
				log.Printf("heartbeat failed: %v", err)
				// If amnesia recovery suggests re-registration, re-register
				_ = registerWithNamenode(nnAddr, nodeID, addr, totalCap)
			}
		}
	}
}

func registerWithNamenode(nnAddr, nodeID, addr string, totalCap int64) error {
	conn, err := grpc.NewClient(nnAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial namenode: %w", err)
	}
	defer conn.Close()

	client := pb.NewNameNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.RegisterDataNode(ctx, &pb.RegisterDataNodeRequest{
		DatanodeId:        nodeID,
		Address:           addr,
		TotalStorageBytes: totalCap,
		FreeStorageBytes:  totalCap,
	})
	return err
}

func sendHeartbeat(nnAddr, nodeID string) error {
	conn, err := grpc.NewClient(nnAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial namenode: %w", err)
	}
	defer conn.Close()

	client := pb.NewNameNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
		DatanodeId: nodeID,
	})
	if err != nil {
		return err
	}
	if resp.RequireReregistration {
		log.Printf("namenode requested re-registration (amnesia recovery)")
		return fmt.Errorf("re-registration required")
	}
	return nil
}

func contextWithShutdown(srv *grpc.Server) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		srv.GracefulStop()
		cancel()
	}()
	return ctx, cancel
}
