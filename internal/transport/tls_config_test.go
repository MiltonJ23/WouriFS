package transport

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

// generateTestCerts est un helper utilitaire pour créer une Autorité de Certification (CA)
// et un certificat factice en mémoire pour des tests hermétiques.
func generateTestCerts(t *testing.T) ([]byte, tls.Certificate) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate RSA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"WouriFS Test CA"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("Failed to create CA certificate: %v", err)
	}

	caPEM := new(bytes.Buffer)
	pem.Encode(caPEM, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes})

	keyPEM := new(bytes.Buffer)
	pem.Encode(keyPEM, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	cert, err := tls.X509KeyPair(caPEM.Bytes(), keyPEM.Bytes())
	if err != nil {
		t.Fatalf("Failed to load TLS key pair: %v", err)
	}

	return caPEM.Bytes(), cert
}

func TestTLSConfigManager_BDD(t *testing.T) {
	caBytes, dummyCert := generateTestCerts(t)

	t.Run("Given an empty CA certificate", func(t *testing.T) {
		t.Run("When creating a new TLSConfigManager", func(t *testing.T) {
			_, err := NewTLSConfigManager(nil)
			// THEN: Il doit échouer par sécurité
			if err == nil {
				t.Errorf("Expected an error for empty CA bytes, got nil")
			}
		})
	})

	t.Run("Given invalid CA certificate data", func(t *testing.T) {
		t.Run("When creating a new TLSConfigManager", func(t *testing.T) {
			_, err := NewTLSConfigManager([]byte("INVALID_PEM_DATA"))
			// THEN: Il doit rejeter le certificat corrompu
			if err == nil {
				t.Errorf("Expected an error for invalid CA bytes, got nil")
			}
		})
	})

	t.Run("Given a valid CA certificate", func(t *testing.T) {
		manager, err := NewTLSConfigManager(caBytes)
		if err != nil {
			t.Fatalf("Failed to initialize manager: %v", err)
		}

		t.Run("When generating Server Config", func(t *testing.T) {
			serverCfg := manager.GenerateServerConfig(dummyCert)

			// THEN: Les règles strictes du système financier doivent être appliquées
			if serverCfg.MinVersion != tls.VersionTLS13 {
				t.Errorf("Expected MinVersion to be TLS 1.3")
			}
			if serverCfg.ClientAuth != tls.RequireAndVerifyClientCert {
				t.Errorf("Expected mTLS (RequireAndVerifyClientCert) to be enforced")
			}
			if len(serverCfg.Certificates) == 0 {
				t.Errorf("Expected server certificate to be loaded")
			}
		})

		t.Run("When generating Client Config", func(t *testing.T) {
			clientCfg := manager.GenerateClientConfig(dummyCert, "namenode.wourifs.local")

			// THEN: Le client doit aussi forcer TLS 1.3 et valider le nom du serveur
			if clientCfg.MinVersion != tls.VersionTLS13 {
				t.Errorf("Expected MinVersion to be TLS 1.3")
			}
			if clientCfg.ServerName != "namenode.wourifs.local" {
				t.Errorf("Expected ServerName to be set to prevent MITM attacks")
			}
		})
	})
}
