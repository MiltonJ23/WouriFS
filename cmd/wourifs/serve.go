package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"
	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	"github.com/MiltonJ23/WouriFS/internal/datanode"
	domain "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	nn "github.com/MiltonJ23/WouriFS/internal/namenode"
	"github.com/MiltonJ23/WouriFS/internal/observability"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start a WouriFS node process",
		Long: `Starts a long-running WouriFS component.

Available subcommands: namenode, datanode, gateway.
Each reads wourifs.yaml and binds to its configured port.

Example:
  wourifs serve namenode   # metadata server + raft consensus
  wourifs serve datanode   # chunk storage node
  wourifs serve gateway    # admin dashboard (WIP)`,
	}

	cmd.AddCommand(serveNamenodeCmd())
	cmd.AddCommand(serveDatanodeCmd())
	cmd.AddCommand(serveGatewayCmd())

	return cmd
}

// --- namenode ---

func serveNamenodeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "namenode",
		Short: "Start a Namenode metadata server",
		RunE:  runNamenode,
	}
}

func runNamenode(cmd *cobra.Command, args []string) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	if !cfg.HasRole("namenode") {
		return fmt.Errorf("node %q missing role 'namenode'", cfg.Node.ID)
	}

	nnDir := cfg.Store.DataDir + "/namenode"
	if err := os.MkdirAll(nnDir, 0750); err != nil {
		return err
	}

	// WAL
	walPath := nnDir + "/wal.jsonl"
	w, err := nn.OpenWAL(walPath)
	if err != nil {
		return fmt.Errorf("wal: %w", err)
	}
	defer w.Close()

	store := nn.NewMetadataStore(cfg.Cluster.ReplicationFactor)
	if err := nn.Replay(walPath, store); err != nil {
		return fmt.Errorf("replay: %w", err)
	}

	reg := domain.NewInMemoryDataNodeRegistry()
	hm, _ := domain.NewHealthMonitor(reg, domain.DefaultClock, 5*time.Second, 15*time.Second)

	grpcSrv := grpc.NewServer()
	nnLogger := observability.NewLogger(slog.LevelInfo)
	nnSrv := nn.NewNameNodeServer(store, reg, w, nnLogger)
	pb.RegisterNameNodeServiceServer(grpcSrv, nnSrv)

	ctx, cancel := shutdownCtx(grpcSrv)
	defer cancel()
	hm.Start(ctx)
	defer hm.Stop()

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Network.NamenodePort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	log.Printf("[namenode] %s listening on %s (rf=%d)", cfg.Node.ID, addr, cfg.Cluster.ReplicationFactor)

	return grpcSrv.Serve(lis)
}

// --- datanode ---

func serveDatanodeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "datanode",
		Short: "Start a Datanode chunk storage server",
		RunE:  runDatanode,
	}
}

func runDatanode(cmd *cobra.Command, args []string) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	if !cfg.HasRole("datanode") {
		return fmt.Errorf("node %q missing role 'datanode'", cfg.Node.ID)
	}

	dnDir := cfg.Store.DataDir + "/datanode"
	store, err := datanode.NewChunkStore(dnDir)
	if err != nil {
		return err
	}

	capacity := int64(cfg.Store.MaxSizeGB) * (1 << 30)
	if capacity == 0 {
		capacity = 1 << 30 // default 1 GiB
	}

	grpcSrv := grpc.NewServer()
	dnSrv := datanode.NewServer(store, capacity)
	datanodepb.RegisterDataNodeServiceServer(grpcSrv, dnSrv)

	// Resolve Namenode address: use first peer if configured, else localhost
	nnAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Network.NamenodePort)
	if len(cfg.Network.NamenodePeers) > 0 {
		nnAddr = cfg.Network.NamenodePeers[0]
	}

	// Background heartbeat loop
	ctx, cancel := shutdownCtx(grpcSrv)
	defer cancel()
	go datanodeHeartbeatLoop(ctx, cfg.Node.ID, dnDir, nnAddr)

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Network.DatanodePort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	log.Printf("[datanode] %s listening on %s (namenode=%s, dir=%s)",
		cfg.Node.ID, addr, nnAddr, dnDir)

	return grpcSrv.Serve(lis)
}

// --- gateway (placeholder) ---

func serveGatewayCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "gateway",
		Short: "Start the Gateway admin dashboard server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadConfig(configPath)
			if err != nil {
				return err
			}
			if !cfg.HasRole("gateway") {
				return fmt.Errorf("node %q missing role 'gateway'", cfg.Node.ID)
			}
			fmt.Printf("[gateway] %s would start on :%d (not yet implemented)\n",
				cfg.Node.ID, cfg.Network.GatewayPort)
			select {} // placeholder
		},
	}
}

// --- helpers ---

func shutdownCtx(srv *grpc.Server) (context.Context, context.CancelFunc) {
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

func datanodeHeartbeatLoop(ctx context.Context, nodeID, dnAddr, nnAddr string) {
	// Register with Namenode
	register := func() {
		conn, err := grpc.NewClient(nnAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		defer conn.Close()
		client := pb.NewNameNodeServiceClient(conn)
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rcancel()
		client.RegisterDataNode(rctx, &pb.RegisterDataNodeRequest{
			DatanodeId:        nodeID,
			Address:           dnAddr,
			TotalStorageBytes: 1 << 30,
			FreeStorageBytes:  1 << 30,
		})
	}

	register()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn, err := grpc.NewClient(nnAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			client := pb.NewNameNodeServiceClient(conn)
			rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
			resp, err := client.Heartbeat(rctx, &pb.HeartbeatRequest{
				DatanodeId: nodeID,
			})
			rcancel()
			conn.Close()
			if err != nil || (resp != nil && resp.RequireReregistration) {
				register()
			}
		}
	}
}

// keep imports alive
var _ = strconv.Itoa
