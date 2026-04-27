package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
)

type TLSConfigManager struct {
	CaPool *x509.CertPool
}

func NewTLSConfigManager(caCertBytes []byte) (*TLSConfigManager, error) {
	if len(caCertBytes) == 0 {
		return nil, errors.New("security violation: CA certificate cannot be empty")
	}

	caPool := x509.NewCertPool()
	appendWentWell := caPool.AppendCertsFromPEM(caCertBytes)
	if !appendWentWell {
		return nil, errors.New("security violation: failed to parse or append  CA certificate to pool")
	}
	return &TLSConfigManager{CaPool: caPool}, nil
}

// GenerateServerConfig return a tls 13 configuration with enforced mTLS
func (t *TLSConfigManager) GenerateServerConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates:     []tls.Certificate{cert},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: []tls.CurveID{tls.CurveP256, tls.X25519},
		ClientAuth:       tls.RequireAndVerifyClientCert,
		ClientCAs:        t.CaPool,
	}
}

// GenerateClientConfig returns a tls 13 configuration for a client
func (t *TLSConfigManager) GenerateClientConfig(cert tls.Certificate, serverName string) *tls.Config {
	return &tls.Config{
		Certificates:     []tls.Certificate{cert},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: []tls.CurveID{tls.CurveP256, tls.X25519},
		RootCAs:          t.CaPool,
		ServerName:       serverName,
	}
}
