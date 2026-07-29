package grpc

import (
	"crypto/tls"
	"errors"
	"net"

	"github.com/MiltonJ23/WouriFS/internal/domain"
	"github.com/MiltonJ23/WouriFS/internal/transport/grpc/interceptor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type SecureServer struct {
	grpcServer *grpc.Server
	address    string
}

func NewSecureServer(address string, tlsConfig *tls.Config, tm domain.TokenManager) (*SecureServer, error) {
	if address == "" {
		return nil, errors.New("server address cannot be empty")
	}

	if tlsConfig == nil {
		return nil, errors.New("security violation: Tls Configuration is required")
	}

	if tm == nil {
		return nil, errors.New("security violation: TokenManager is required")
	}

	// let's convert the tls configuration into grpc credentials
	creds := credentials.NewTLS(tlsConfig)

	authInterceptor := interceptor.NewAuthInterceptor(tm)

	opts := []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.UnaryInterceptor(authInterceptor.Unary()),
		grpc.StreamInterceptor(authInterceptor.Stream()),
	}

	server := grpc.NewServer(opts...)

	return &SecureServer{
		grpcServer: server,
		address:    address,
	}, nil
}

func (s *SecureServer) Start() error {
	listener, listeningOntoPortErr := net.Listen("tcp", s.address)
	if listeningOntoPortErr != nil {
		return listeningOntoPortErr
	}
	return s.grpcServer.Serve(listener)
}

func (s *SecureServer) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

func (s *SecureServer) GetRawServer() *grpc.Server {
	return s.grpcServer
}
