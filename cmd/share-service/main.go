package main

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	provisionpb "github.com/MiltonJ23/WouriFS/api/gen/v1/provision"
	jwtmgr "github.com/MiltonJ23/WouriFS/internal/auth/jwt"
	"github.com/MiltonJ23/WouriFS/internal/provision"
	interceptor "github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:443", "gRPC listen address")
	instFile := flag.String("institutions", "./configs/institutions.json", "path to institutions share file")
	tlsCertPath := flag.String("tls-cert", "", "TLS certificate PEM")
	tlsKeyPath := flag.String("tls-key", "", "TLS private key PEM")
	caCertPath := flag.String("tls-ca", "", "CA certificate for mTLS")
	jwtPriv := flag.String("jwt-priv", "", "RSA private key for JWT verification")
	jwtPub := flag.String("jwt-pub", "", "RSA public key for JWT verification")
	devInsecure := flag.Bool("dev-insecure", false, "disable TLS and JWT requirements (DEVELOPMENT ONLY)")
	flag.Parse()

	if !*devInsecure {
		if *tlsCertPath == "" || *tlsKeyPath == "" {
			log.Fatal("TLS certificate and key required (use --dev-insecure to disable)")
		}
		if *jwtPriv == "" || *jwtPub == "" {
			log.Fatal("JWT RSA keys required (use --dev-insecure to disable)")
		}
	}

	db := provision.NewDB()
	if err := db.LoadInstitutions(*instFile); err != nil {
		log.Fatalf("load institutions: %v", err)
	}
	log.Printf("loaded institutions from %s", *instFile)

	// TLS config with mTLS
	var tlsCfg *tls.Config
	if *tlsCertPath != "" && *tlsKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCertPath, *tlsKeyPath)
		if err != nil {
			log.Fatalf("tls cert: %v", err)
		}
		tlsCfg = &tls.Config{
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
	}

	// JWT verification
	var tm *jwtmgr.RSATokenManager
	if *jwtPriv != "" && *jwtPub != "" {
		privKey := loadRSAPrivate(*jwtPriv)
		pubKey := loadRSAPublic(*jwtPub)
		tm = jwtmgr.NewRSATokenManager(privKey, pubKey)
	}

	// Build gRPC server with auth interceptor
	var opts []grpc.ServerOption
	if tlsCfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	if tm != nil {
		authInt := interceptor.NewAuthInterceptor(tm)
		opts = append(opts,
			grpc.UnaryInterceptor(authInt.Unary()),
			grpc.StreamInterceptor(authInt.Stream()),
		)
	}
	grpcSrv := grpc.NewServer(opts...)
	provisionpb.RegisterShareServiceServer(grpcSrv, provision.NewShareServer(db))

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("share-service listening on %s (tls=%v, jwt=%v)", *addr, tlsCfg != nil, tm != nil)

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		grpcSrv.GracefulStop()
	}()

	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func loadRSAPrivate(path string) *rsa.PrivateKey {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read key: %v", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		log.Fatal("bad PEM block")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		key2, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			log.Fatalf("parse key: %v", err)
		}
		var ok bool
		key, ok = key2.(*rsa.PrivateKey)
		if !ok {
			log.Fatal("not RSA key")
		}
	}
	return key
}

func loadRSAPublic(path string) *rsa.PublicKey {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read key: %v", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		log.Fatal("bad PEM block")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		log.Fatalf("parse key: %v", err)
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		log.Fatal("not RSA key")
	}
	return rsaPub
}
