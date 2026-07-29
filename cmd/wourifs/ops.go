package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"
)

func provisionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "provision",
		Short: "Dual-control USB key provisioning",
		Long: `Split or combine the cluster master secret using Shamir's
Secret Sharing (k=2, n=2).

Two independent officers must cooperate to provision a new user or
unseal the encryption keyring. Neither officer alone can reconstruct
the secret.`,
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "split",
		Short: "Generate two Shamir shares from the master secret",
		Long: `Takes a 32-byte master secret and splits it into two shares.
Share 1 is written to a USB key. Share 2 is stored in the institution's
COBAC share database.

Usage:
  wourifs provision split --secret-file /secure/master.key`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: wire into cmd/provision logic
			fmt.Println("[provision] split — NOT YET WIRED via Cobra; use cmd/provision directly")
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "combine",
		Short: "Reconstruct the master secret from two shares",
		Long: `Reads share 1 from USB key or stdin, retrieves share 2 from
the share-service gRPC endpoint, and reconstructs the master secret
in memory (never written to disk).

Usage:
  wourifs provision combine --usb /dev/sdb1 --share-service 100.64.0.1:9003`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("[provision] combine — NOT YET WIRED via Cobra; use cmd/provision directly")
			return nil
		},
	})

	return cmd
}

func mountCmd() *cobra.Command {
	var nnAddr string

	cmd := &cobra.Command{
		Use:   "mount [mountpoint]",
		Short: "Mount WouriFS as a local filesystem via FUSE",
		Long: `Mounts the WouriFS namespace as a POSIX-compatible directory.
Applications can read and write files normally. All data is stored
in the distributed cluster — nothing is written to the local disk.

Requires libfuse3 installed on the host.

Usage:
  wourifs mount /mnt/wourifs`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mountPoint := args[0]
			cfg, err := LoadConfig(configPath)
			if err != nil {
				return err
			}

			addr := fmt.Sprintf("127.0.0.1:%d", cfg.Network.NamenodePort)
			if len(cfg.Network.NamenodePeers) > 0 {
				addr = cfg.Network.NamenodePeers[0]
			}
			if nnAddr != "" {
				addr = nnAddr
			}

			if err := os.MkdirAll(mountPoint, 0755); err != nil {
				return err
			}

			root := &wourifsNode{path: "", isDir: true, nnAddr: addr}
			server, err := fs.Mount(mountPoint, root, &fs.Options{
				MountOptions: fuse.MountOptions{Debug: false, Name: "wourifs", FsName: "wourifs", AllowOther: true},
			})
			if err != nil {
				return fmt.Errorf("mount: %w", err)
			}

			fmt.Printf("WouriFS mounted at %s (namenode %s)\n", mountPoint, addr)

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigCh
				fmt.Println("\nunmounting...")
				server.Unmount()
			}()

			server.Wait()
			fmt.Println("unmounted")
			return nil
		},
	}

	cmd.Flags().StringVarP(&nnAddr, "namenode", "n", "", "override Namenode address (host:port)")
	return cmd
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Print cluster health overview",
		Long: `Queries the Namenode for cluster status: Raft leader, online
Datanodes, replication health, audit log entry count, and active
FUSE client sessions. Designed for human consumption.

Usage:
  wourifs status`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadConfig(configPath)
			if err != nil {
				return err
			}
			fmt.Printf("WouriFS Cluster: %s\n", cfg.Cluster.Name)
			fmt.Printf("  Node:          %s\n", cfg.Node.ID)
			fmt.Printf("  Roles:         %v\n", cfg.Node.Roles)
			fmt.Printf("  Repl factor:   %d (quorum %d)\n",
				cfg.Cluster.ReplicationFactor, cfg.Cluster.WriteQuorum)
			fmt.Printf("  Data dir:      %s\n", cfg.Store.DataDir)
			fmt.Printf("  Metrics:       :%d (enabled=%v)\n",
				cfg.Metrics.Port, cfg.Metrics.Enabled)
			fmt.Printf("  Tracing:       %s (rate=%.2f, enabled=%v)\n",
				cfg.Tracing.OTLPEndpoint, cfg.Tracing.SampleRate, cfg.Tracing.Enabled)
			fmt.Println("\n  (live cluster query not yet implemented — connect to Namenode gRPC)")
			return nil
		},
	}
}

func auditCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Query and verify the append-only audit log",
		Long: `The audit log records every write operation with a SHA-256
Merkle chain. Use 'log' to view entries and 'verify' to check
integrity.`,
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "log",
		Short: "Print paginated audit entries",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("[audit log] NOT YET WIRED — query via gRPC AuditLogService")
			return nil
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "verify",
		Short: "Verify the audit chain integrity",
		Long: `Recomputes SHA-256 hashes for every audit entry and verifies
the Merkle chain. Returns the position of the first mismatch, if any.
A failed verification means historical audit entries have been tampered
with.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("[audit verify] NOT YET WIRED — use internal/audit package directly")
			return nil
		},
	})

	return cmd
}
