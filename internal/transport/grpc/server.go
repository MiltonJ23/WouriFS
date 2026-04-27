package grpc

import (
	"crypto/tls"
	"errors"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type SecureServer struct {
	grpcServer *grpc.Server
	address    string
}

func NewSecureServer(address string, tlsConfig *tls.Config) (*SecureServer, error) {
	if address == "" {
		return nil, errors.New("server address cannot be empty")
	}

	if tlsConfig == nil {
		return nil, errors.New("security violation: Tls Configuration is required")
	}

	// let's convert the tls configuration into grpc credentials
	creds := credentials.NewTLS(tlsConfig)

	// TODO : later we will add our JWT interceptors here
	opts := []grpc.ServerOption{
		grpc.Creds(creds),
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
