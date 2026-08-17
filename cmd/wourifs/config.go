/*
 * WouriFS unified configuration (YAML).
 *
 * One file describes the entire cluster deployment. Each node reads the
 * same config and uses its own node.id to pick its section.
 *
 * Deployment model for microfinance institutions (no dedicated servers):
 *   - One or more workstations at the head office take the "namenode" role.
 *   - All workstations (head office + branches) contribute storage as
 *     "datanode" actors.
 *   - A workstation can hold any combination of roles.
 *   - wourifs init generates this file interactively.
 */
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config is the root of the WouriFS deployment descriptor.
type Config struct {
	Cluster       ClusterConfig       `yaml:"cluster"`
	Node          NodeConfig          `yaml:"node"`
	Network       NetworkConfig       `yaml:"network"`
	Store         StoreConfig         `yaml:"store"`
	Auth          AuthConfig          `yaml:"auth"`
	Metrics       MetricsConfig       `yaml:"metrics"`
	Tracing       TracingConfig       `yaml:"tracing"`
	Observability ObservabilityConfig `yaml:"observability"`
}

// ClusterConfig holds cluster-wide parameters.
type ClusterConfig struct {
	Name              string `yaml:"name"`
	ReplicationFactor int32  `yaml:"replication_factor"` // default 3
	WriteQuorum       int32  `yaml:"write_quorum"`       // default 2
	ChunkSizeMB       int32  `yaml:"chunk_size_mb"`      // default 64
}

// NodeConfig identifies this workstation and its roles.
type NodeConfig struct {
	ID    string   `yaml:"id"`    // unique across cluster, e.g. "branch-central-01"
	Roles []string `yaml:"roles"` // any of: namenode, datanode, gateway
}

// NetworkConfig defines how nodes discover each other.
type NetworkConfig struct {
	Mode            string   `yaml:"mode"`             // "tailscale" or "lan"
	NamenodePeers   []string `yaml:"namenode_peers"`   // raft peer addresses (host:port)
	NamenodeAddrs   []string `yaml:"namenode_addrs"`   // gRPC addresses of ALL namenodes (host:port) for leader discovery
	DatanodePort    int      `yaml:"datanode_port"`    // default 9100
	NamenodePort    int      `yaml:"namenode_port"`    // default 9000
	GatewayPort     int      `yaml:"gateway_port"`     // default 8443
	MetricsPort     int      `yaml:"metrics_port"`     // default 9102
	HeadscaleServer string   `yaml:"headscale_server"` // only when mode=tailscale
}

// StoreConfig controls local chunk storage and data directory layout.
type StoreConfig struct {
	DataDir   string `yaml:"data_dir"`    // /var/lib/wourifs or user-chosen
	MaxSizeGB int    `yaml:"max_size_gb"` // disk quota for this node (0 = unlimited)
}

// AuthConfig holds JWT and TLS parameters.
type AuthConfig struct {
	JWTSigningKeyPath string `yaml:"jwt_signing_key_path"`
	TLSCertPath       string `yaml:"tls_cert_path"`
	TLSKeyPath        string `yaml:"tls_key_path"`
	CACertPath        string `yaml:"ca_cert_path"`
	JWTExpiryHours    int    `yaml:"jwt_expiry_hours"` // default 8
}

// MetricsConfig controls Prometheus exposition.
type MetricsConfig struct {
	Enabled bool `yaml:"enabled"` // default true
	Port    int  `yaml:"port"`    // default 9102
}

// TracingConfig controls OpenTelemetry export.
type TracingConfig struct {
	Enabled      bool    `yaml:"enabled"`       // default true
	OTLPEndpoint string  `yaml:"otlp_endpoint"` // collector address
	SampleRate   float64 `yaml:"sample_rate"`   // 0.0 - 1.0
}

// ObservabilityConfig controls OTLP export of logs, metrics and traces.
type ObservabilityConfig struct {
	Enabled      bool   `yaml:"enabled"`       // default true
	OTLPEndpoint string `yaml:"otlp_endpoint"` // OTLP gRPC collector, default localhost:4317
	ServiceName  string `yaml:"service_name"`  // default wourifs
}

// Validate checks configuration invariants and returns the first error found.
func (c Config) Validate() error {
	if c.Cluster.WriteQuorum > c.Cluster.ReplicationFactor {
		return fmt.Errorf("write_quorum (%d) must not exceed replication_factor (%d)",
			c.Cluster.WriteQuorum, c.Cluster.ReplicationFactor)
	}
	for _, r := range c.Node.Roles {
		switch r {
		case "namenode", "datanode", "gateway":
		default:
			return fmt.Errorf("unknown role %q (allowed: namenode, datanode, gateway)", r)
		}
	}
	return nil
}

// DefaultConfig returns a reasonable baseline. Callers override via YAML.
func DefaultConfig() Config {
	return Config{
		Cluster: ClusterConfig{
			Name:              "wourifs",
			ReplicationFactor: 3,
			WriteQuorum:       2,
			ChunkSizeMB:       64,
		},
		Node: NodeConfig{
			ID:    hostname(),
			Roles: []string{"datanode"},
		},
		Network: NetworkConfig{
			Mode:         "lan",
			DatanodePort: 9100,
			NamenodePort: 9000,
			GatewayPort:  8443,
			MetricsPort:  9102,
		},
		Store: StoreConfig{
			DataDir:   filepath.Join(os.TempDir(), "wourifs"),
			MaxSizeGB: 0,
		},
		Auth: AuthConfig{
			JWTExpiryHours: 8,
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9102,
		},
		Tracing: TracingConfig{
			Enabled:    true,
			SampleRate: 1.0,
		},
		Observability: ObservabilityConfig{
			Enabled:      true,
			OTLPEndpoint: "localhost:4317", // OTLP/gRPC standard port
			ServiceName:  "wourifs",
		},
	}
}

// ResolveOTLPEndpoint returns the OTLP/gRPC collector endpoint: the
// observability section wins, then the legacy tracing section, then the
// default (localhost:4317).
func (c Config) ResolveOTLPEndpoint() string {
	if c.Observability.OTLPEndpoint != "" {
		return c.Observability.OTLPEndpoint
	}
	if c.Tracing.OTLPEndpoint != "" {
		return c.Tracing.OTLPEndpoint
	}
	return "localhost:4317"
}

// ResolveServiceName returns the OTel service name for this node.
func (c Config) ResolveServiceName() string {
	if c.Observability.ServiceName != "" {
		return c.Observability.ServiceName
	}
	return "wourifs"
}

// LoadConfig reads a YAML file, overlaying values onto defaults.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, fmt.Errorf("config file not found: %s (run 'wourifs init' first)", path)
		}
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// HasRole reports whether this node holds the given role.
func (c Config) HasRole(role string) bool {
	for _, r := range c.Node.Roles {
		if r == role {
			return true
		}
	}
	return false
}

func hostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "wourifs-node"
	}
	return h
}
