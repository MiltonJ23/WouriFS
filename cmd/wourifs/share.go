package main

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	provisionpb "github.com/MiltonJ23/WouriFS/api/gen/v1/provision"
	jwtmgr "github.com/MiltonJ23/WouriFS/internal/auth/jwt"
	"github.com/MiltonJ23/WouriFS/internal/observability"
	"github.com/MiltonJ23/WouriFS/internal/provision"
	interceptor "github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// shareCmd runs the Shamir share gRPC service (provision/audit) — the former
// standalone share-service binary, now part of the unified CLI.
//
// Example:
//
//	wourifs share --institutions configs/institutions.example.json --dev-insecure
func shareCmd() *cobra.Command {
	var addr, instFile string
	var tlsCertPath, tlsKeyPath, caCertPath string
	var jwtPriv, jwtPub string
	var devInsecure bool

	cmd := &cobra.Command{
		Use:   "share",
		Short: "Start the Shamir share gRPC service",
		RunE: func(cmd *cobra.Command, args []string) error {
			// OTLP observability (fail-open when no collector/config).
			pipeline, obsShutdown := (*observability.OTelPipeline)(nil), func() {}
			if cfg, err := LoadConfig(configPath); err == nil {
				pipeline, obsShutdown = setupObservability(cfg, "share")
			}
			defer obsShutdown()
			shareLogger := observability.NewLogger(slog.LevelInfo)
			if pipeline != nil {
				shareLogger = observability.NewLoggerWithOtel(slog.LevelInfo, pipeline.LoggerProvider)
			}
			shareLogger.Info("share_service_starting", "addr", addr, "institutions", instFile)

			if !devInsecure {
				if tlsCertPath == "" || tlsKeyPath == "" {
					return fmt.Errorf("TLS certificate and key required (use --dev-insecure to disable)")
				}
				if jwtPriv == "" || jwtPub == "" {
					return fmt.Errorf("JWT RSA keys required (use --dev-insecure to disable)")
				}
			}

			db := provision.NewDB()
			if err := db.LoadInstitutions(instFile); err != nil {
				return fmt.Errorf("load institutions: %w", err)
			}
			log.Printf("loaded institutions from %s", instFile)

			// TLS config with mTLS
			var tlsCfg *tls.Config
			if tlsCertPath != "" && tlsKeyPath != "" {
				cert, err := tls.LoadX509KeyPair(tlsCertPath, tlsKeyPath)
				if err != nil {
					return fmt.Errorf("tls cert: %w", err)
				}
				tlsCfg = &tls.Config{
					Certificates: []tls.Certificate{cert},
					MinVersion:   tls.VersionTLS13,
				}
				if caCertPath != "" {
					caBytes, err := os.ReadFile(caCertPath)
					if err != nil {
						return fmt.Errorf("ca cert: %w", err)
					}
					caPool := x509.NewCertPool()
					caPool.AppendCertsFromPEM(caBytes)
					tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
					tlsCfg.ClientCAs = caPool
				}
			}

			// JWT verification
			var tm *jwtmgr.RSATokenManager
			if jwtPriv != "" && jwtPub != "" {
				privKey, err := loadRSAPrivate(jwtPriv)
				if err != nil {
					return err
				}
				pubKey, err := loadRSAPublic(jwtPub)
				if err != nil {
					return err
				}
				tm = jwtmgr.NewRSATokenManager(privKey, pubKey)
			}

			// Build gRPC server with auth interceptor
			var opts []grpc.ServerOption
			if tlsCfg != nil {
				opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
			}
			var unaryInts []grpc.UnaryServerInterceptor
			var streamInts []grpc.StreamServerInterceptor
			if tm != nil {
				authInt := interceptor.NewAuthInterceptor(tm)
				unaryInts = append(unaryInts, authInt.Unary())
				streamInts = append(streamInts, authInt.Stream())
			}
			if len(unaryInts) > 0 {
				opts = append(opts, grpc.ChainUnaryInterceptor(unaryInts...))
			}
			if len(streamInts) > 0 {
				opts = append(opts, grpc.ChainStreamInterceptor(streamInts...))
			}
			grpcSrv := grpc.NewServer(opts...)
			provisionpb.RegisterShareServiceServer(grpcSrv, provision.NewShareServer(db))

			lis, err := net.Listen("tcp", addr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", addr, err)
			}
			log.Printf("share-service listening on %s (tls=%v, jwt=%v)", addr, tlsCfg != nil, tm != nil)

			// Graceful shutdown
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigCh
				log.Println("shutting down...")
				grpcSrv.GracefulStop()
			}()

			if err := grpcSrv.Serve(lis); err != nil {
				return fmt.Errorf("serve: %w", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "0.0.0.0:443", "gRPC listen address")
	cmd.Flags().StringVar(&instFile, "institutions", "./configs/institutions.json", "path to institutions share file")
	cmd.Flags().StringVar(&tlsCertPath, "tls-cert", "", "TLS certificate PEM")
	cmd.Flags().StringVar(&tlsKeyPath, "tls-key", "", "TLS private key PEM")
	cmd.Flags().StringVar(&caCertPath, "tls-ca", "", "CA certificate for mTLS")
	cmd.Flags().StringVar(&jwtPriv, "jwt-priv", "", "RSA private key for JWT verification")
	cmd.Flags().StringVar(&jwtPub, "jwt-pub", "", "RSA public key for JWT verification")
	cmd.Flags().BoolVar(&devInsecure, "dev-insecure", false, "disable TLS and JWT requirements (DEVELOPMENT ONLY)")
	return cmd
}

func loadRSAPrivate(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("bad PEM block")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		key2, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse key: %w", err)
		}
		var ok bool
		key, ok = key2.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("not RSA key")
		}
	}
	return key, nil
}

func loadRSAPublic(path string) (*rsa.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("bad PEM block")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse key: %w", err)
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not RSA key")
	}
	return rsaPub, nil
}
