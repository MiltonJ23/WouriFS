package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	datanodepb "github.com/MiltonJ23/WouriFS/api/gen/v1/datanode"
	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	"github.com/MiltonJ23/WouriFS/internal/datanode"
	domain "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	nn "github.com/MiltonJ23/WouriFS/internal/namenode"
	"github.com/MiltonJ23/WouriFS/internal/observability"
	interceptor "github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"github.com/hashicorp/raft"
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
	var devNoAuth, raftMode bool
	var raftPeer string

	cmd := &cobra.Command{
		Use:   "namenode",
		Short: "Start a Namenode metadata server",
		RunE: func(cmd *cobra.Command, args []string) error {
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

			// OTLP pipeline: logs + metrics + traces exported to the collector.
			pipeline, obsShutdown := setupObservability(cfg, "namenode")
			defer obsShutdown()

			nnLogger := observability.NewLogger(slog.LevelInfo)
			if pipeline != nil {
				nnLogger = observability.NewLoggerWithOtel(slog.LevelInfo, pipeline.LoggerProvider)
				nnLogger.Metrics().AttachOtel(pipeline.MeterProvider.Meter("wourifs"))
			}

			sampleRate := cfg.Tracing.SampleRate
			if sampleRate <= 0 && cfg.Tracing.Enabled {
				sampleRate = 1.0
			}
			tracer := observability.NewTracer(sampleRate)
			if pipeline != nil {
				tracer = observability.NewTracerWithOtel(sampleRate, pipeline.TracerProvider)
			}

			unaryInts := []grpc.UnaryServerInterceptor{tracer.UnaryServerInterceptor()}
			if devNoAuth {
				unaryInts = append(unaryInts, interceptor.DevNoAuthInterceptor())
				log.Printf("[namenode] WARNING: running without auth (--dev-no-auth)")
			}
			grpcOpts := []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(unaryInts...),
				grpc.StreamInterceptor(tracer.StreamServerInterceptor()),
			}
			grpcSrv := grpc.NewServer(grpcOpts...)
			nnSrv := nn.NewNameNodeServer(store, reg, w, nnLogger)
			pb.RegisterNameNodeServiceServer(grpcSrv, nnSrv)

			var raftInstance *raft.Raft
			if raftMode {
				raftCfg := nn.RaftConfig{
					NodeID:       string(raft.ServerID(cfg.Node.ID)),
					BindAddr:     raftPeer,
					DataDir:      nnDir + "/raft",
					Bootstrap:    len(cfg.Network.NamenodePeers) <= 1,
					Peers:        cfg.Network.NamenodePeers,
					ApplyTimeout: 5 * time.Second,
				}
				if err := os.MkdirAll(raftCfg.DataDir, 0750); err != nil {
					return fmt.Errorf("raft data dir: %w", err)
				}
				r, _, err := nn.BootstrapRaft(raftCfg, store, reg)
				if err != nil {
					return fmt.Errorf("raft bootstrap: %w", err)
				}
				raftInstance = r
				nnSrv.SetRaft(r)
				log.Printf("[namenode] raft mode enabled, node=%s peers=%v", cfg.Node.ID, cfg.Network.NamenodePeers)
			}

			// Health monitor. In Raft mode only the leader sweeps, and its
			// MarkUnavailable is replicated through the log, so the decision
			// is cluster-wide instead of local.
			var healthReg domain.HealthRegistry = reg
			if raftInstance != nil {
				healthReg = &raftHealthRegistry{reg: reg, srv: nnSrv}
			}
			hm, _ := domain.NewHealthMonitor(healthReg, domain.DefaultClock, 5*time.Second, 15*time.Second)
			defer hm.Stop()

			var ctx context.Context
			var cancel context.CancelFunc
			if raftInstance != nil {
				ctx, cancel = shutdownCtxWithRaft(grpcSrv, raftInstance)
			} else {
				ctx, cancel = shutdownCtx(grpcSrv)
			}
			defer cancel()

			if raftInstance != nil {
				go runLeaderHealthMonitor(ctx, raftInstance, hm)
			} else {
				hm.Start(ctx)
			}

			if cfg.Metrics.Enabled {
				startMetricsServer(cfg.Metrics.Port, nnLogger.Metrics(), "namenode")
			}

			addr := fmt.Sprintf("0.0.0.0:%d", cfg.Network.NamenodePort)
			lis, err := net.Listen("tcp", addr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", addr, err)
			}
			mode := "standalone"
			if raftMode {
				mode = "raft"
			}
			log.Printf("[namenode] %s listening on %s (rf=%d, mode=%s)", cfg.Node.ID, addr, cfg.Cluster.ReplicationFactor, mode)

			return grpcSrv.Serve(lis)
		},
	}
	cmd.Flags().BoolVar(&devNoAuth, "dev-no-auth", false, "disable JWT authentication (DEVELOPMENT ONLY)")
	cmd.Flags().BoolVar(&raftMode, "raft", false, "enable Raft consensus (requires 3 nodes)")
	cmd.Flags().StringVar(&raftPeer, "raft-bind", "0.0.0.0:9001", "Raft transport bind address")
	return cmd
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

	// OTLP pipeline: logs + metrics + traces exported to the collector.
	pipeline, obsShutdown := setupObservability(cfg, "datanode")
	defer obsShutdown()

	dnLogger := observability.NewLogger(slog.LevelInfo)
	if pipeline != nil {
		dnLogger = observability.NewLoggerWithOtel(slog.LevelInfo, pipeline.LoggerProvider)
		dnLogger.Metrics().AttachOtel(pipeline.MeterProvider.Meter("wourifs"))
	}

	tracer := observability.NewTracer(1.0)
	if pipeline != nil {
		tracer = observability.NewTracerWithOtel(1.0, pipeline.TracerProvider)
	}

	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(tracer.UnaryServerInterceptor()),
		grpc.StreamInterceptor(tracer.StreamServerInterceptor()),
	)
	dnSrv := datanode.NewServer(store, capacity)
	datanodepb.RegisterDataNodeServiceServer(grpcSrv, dnSrv)

	// Resolve namenode addresses: prefer the explicit list (leader discovery),
	// else the first raft peer, else localhost.
	nnAddrs := cfg.Network.NamenodeAddrs
	if len(nnAddrs) == 0 {
		if len(cfg.Network.NamenodePeers) > 0 {
			nnAddrs = []string{cfg.Network.NamenodePeers[0]}
		} else {
			nnAddrs = []string{fmt.Sprintf("127.0.0.1:%d", cfg.Network.NamenodePort)}
		}
	}

	// Advertised datanode address: hostname + configured gRPC port.
	dnAddr := fmt.Sprintf("%s:%d", hostname(), cfg.Network.DatanodePort)

	// Background heartbeat loop
	ctx, cancel := shutdownCtx(grpcSrv)
	defer cancel()
	go datanodeHeartbeatLoop(ctx, cfg.Node.ID, dnAddr, nnAddrs, dnLogger)

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Network.DatanodePort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	log.Printf("[datanode] %s listening on %s (namenodes=%v, dir=%s)",
		cfg.Node.ID, addr, nnAddrs, dnDir)

	return grpcSrv.Serve(lis)
}

// serveGatewayCmd defined in gateway.go

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

func shutdownCtxWithRaft(srv *grpc.Server, r *raft.Raft) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		if r != nil {
			if err := r.Shutdown().Error(); err != nil {
				log.Printf("raft shutdown: %v", err)
			}
		}
		srv.GracefulStop()
		cancel()
	}()
	return ctx, cancel
}

// raftHealthRegistry routes health-monitor mutations through the Raft log so
// unavailability is decided and replicated cluster-wide (leader only).
type raftHealthRegistry struct {
	reg *domain.InMemoryDataNodeRegistry
	srv *nn.NameNodeServer
}

func (r *raftHealthRegistry) GetAllAvailable() []*domain.DataNodeStatus {
	return r.reg.GetAllAvailable()
}

func (r *raftHealthRegistry) MarkUnavailable(id string) error {
	return r.srv.ReplicateUnavailable(id)
}

// runLeaderHealthMonitor starts the health monitor only while this node is
// the Raft leader. Followers must not sweep: their local decisions would
// diverge from the replicated registry.
func runLeaderHealthMonitor(ctx context.Context, r *raft.Raft, hm *domain.HealthMonitor) {
	for {
		if r.State() == raft.Leader {
			hm.Start(ctx)
		} else {
			hm.Stop()
		}
		select {
		case <-ctx.Done():
			return
		case <-r.LeaderCh():
		}
	}
}

// datanodeHeartbeatLoop registers the datanode with the Raft leader and then
// sends heartbeats. It rotates over the configured namenode addresses on any
// failure (connection error, Unavailable from a follower, or
// RequireReregistration), so registrations always converge on the leader and
// survive leader changes.
func datanodeHeartbeatLoop(ctx context.Context, nodeID, dnAddr string, nnAddrs []string, l *observability.Logger) {
	if len(nnAddrs) == 0 {
		return
	}
	idx := 0

	registerAt := func(addr string) error {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		client := pb.NewNameNodeServiceClient(conn)
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rcancel()
		_, err = client.RegisterDataNode(rctx, &pb.RegisterDataNodeRequest{
			DatanodeId:        nodeID,
			Address:           dnAddr,
			TotalStorageBytes: 1 << 30,
			FreeStorageBytes:  1 << 30,
		})
		return err
	}

	register := func() bool {
		start := time.Now()
		for i := 0; i < len(nnAddrs); i++ {
			addr := nnAddrs[(idx+i)%len(nnAddrs)]
			err := registerAt(addr)
			l.OpEnd(start, "datanode_register", addr, err, 0)
			if err == nil {
				idx = (idx + i) % len(nnAddrs)
				l.Info("datanode_registered_with", "id", nodeID, "namenode", addr)
				return true
			}
		}
		l.Warn("datanode_register_failed_all", "id", nodeID, "namenodes", nnAddrs)
		return false
	}

	heartbeat := func() bool {
		start := time.Now()
		conn, err := grpc.NewClient(nnAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			l.OpEnd(start, "datanode_heartbeat", nnAddrs[idx], err, 0)
			idx = (idx + 1) % len(nnAddrs)
			return false
		}
		client := pb.NewNameNodeServiceClient(conn)
		rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := client.Heartbeat(rctx, &pb.HeartbeatRequest{
			DatanodeId: nodeID,
		})
		rcancel()
		conn.Close()
		l.OpEnd(start, "datanode_heartbeat", nnAddrs[idx], err, 0)
		if err != nil || (resp != nil && resp.RequireReregistration) {
			idx = (idx + 1) % len(nnAddrs)
			return false
		}
		return true
	}

	register()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !heartbeat() {
				register()
			}
		}
	}
}
