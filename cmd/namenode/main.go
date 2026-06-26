package main

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/MiltonJ23/WouriFS/api/gen/v1/namenode"
	domain "github.com/MiltonJ23/WouriFS/internal/domain/namenode"
	jwtmgr "github.com/MiltonJ23/WouriFS/internal/auth/jwt"
	"github.com/MiltonJ23/WouriFS/internal/namenode"
	"github.com/MiltonJ23/WouriFS/internal/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9000", "gRPC listen address")
	dataDir := flag.String("data", "./data/namenode", "data directory for WAL")
	replFactor := flag.Int("rf", 3, "default replication factor")
	privKeyPath := flag.String("jwt-priv", "", "path to RSA private key PEM")
	pubKeyPath := flag.String("jwt-pub", "", "path to RSA public key PEM")
	tlsCertPath := flag.String("tls-cert", "", "path to TLS certificate PEM")
	tlsKeyPath := flag.String("tls-key", "", "path to TLS private key PEM")
	caCertPath := flag.String("tls-ca", "", "path to CA certificate PEM")
	hbInterval := flag.Duration("hb-interval", 5*time.Second, "health monitor sweep interval")
	hbTimeout := flag.Duration("hb-timeout", 15*time.Second, "heartbeat timeout before marking dead")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0750); err != nil {
		log.Fatalf("data dir: %v", err)
	}

	// WAL
	walPath := *dataDir + "/wal.jsonl"
	w, err := namenode.OpenWAL(walPath)
	if err != nil {
		log.Fatalf("wal open: %v", err)
	}
	defer w.Close()

	// Metadata store + WAL replay
	store := namenode.NewMetadataStore(int32(*replFactor))
	if err := namenode.Replay(walPath, store); err != nil {
		log.Fatalf("wal replay: %v", err)
	}
	log.Printf("metadata store ready, rf=%d", *replFactor)

	// DataNode registry
	reg := domain.NewInMemoryDataNodeRegistry()

	// Health monitor
	hm, err := domain.NewHealthMonitor(reg, domain.DefaultClock, *hbInterval, *hbTimeout)
	if err != nil {
		log.Fatalf("health monitor: %v", err)
	}

	// TLS config (skip if no certs provided — useful for testing)
	var tlsCfg *tls.Config
	if *tlsCertPath != "" && *tlsKeyPath != "" && *caCertPath != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCertPath, *tlsKeyPath)
		if err != nil {
			log.Fatalf("tls cert: %v", err)
		}
		caBytes, err := os.ReadFile(*caCertPath)
		if err != nil {
			log.Fatalf("ca cert: %v", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caBytes) {
			log.Fatal("failed to parse CA cert")
		}
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		}
	}

	// JWT token manager (wired into auth interceptor when both keys provided)
	if *privKeyPath != "" && *pubKeyPath != "" {
		privKey := loadRSAPrivateKey(*privKeyPath)
		pubKey := loadRSAPublicKey(*pubKeyPath)
		tm := jwtmgr.NewRSATokenManager(privKey, pubKey)
		_ = tm // auth interceptor integration in next sprint
	}

	// Build gRPC server with optional TLS
	var grpcOpts []grpc.ServerOption
	if tlsCfg != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	grpcSrv := grpc.NewServer(grpcOpts...)
	nnLogger := observability.NewLogger(slog.LevelInfo)
	nnSrv := namenode.NewNameNodeServer(store, reg, w, nnLogger)
	pb.RegisterNameNodeServiceServer(grpcSrv, nnSrv)

	// Start health monitor
	ctx, cancel := contextWithShutdown()
	defer cancel()
	hm.Start(ctx)
	defer hm.Stop()

	// Listen
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("namenode listening on %s (tls=%v)", *addr, tlsCfg != nil)

	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func contextWithShutdown() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		cancel()
	}()
	return ctx, cancel
}

func loadRSAPrivateKey(path string) *rsa.PrivateKey {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read private key %s: %v", path, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		log.Fatal("failed to decode PEM block from private key")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		key2, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			log.Fatalf("parse private key: %v", err)
		}
		var ok bool
		key, ok = key2.(*rsa.PrivateKey)
		if !ok {
			log.Fatal("private key is not RSA")
		}
	}
	return key
}

func loadRSAPublicKey(path string) *rsa.PublicKey {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read public key %s: %v", path, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		log.Fatal("failed to decode PEM block from public key")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		log.Fatalf("parse public key: %v", err)
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		log.Fatal("public key is not RSA")
	}
	return rsaPub
}
