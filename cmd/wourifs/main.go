/*
 * wourifs — unified CLI for the WouriFS distributed storage system.
 *
 * Every operation — serve, init, provision, mount, status — is exposed
 * through one binary. The complexity of gRPC, Raft consensus, Shamir
 * key splitting, and FUSE mounting is hidden behind subcommands.
 *
 * Usage:
 *   wourifs init                  # guided first-time setup
 *   wourifs serve namenode        # start a namenode (metadata + raft)
 *   wourifs serve datanode        # start a datanode (chunk storage)
 *   wourifs mount /mnt/wourifs    # mount POSIX filesystem via FUSE
 *   wourifs provision split       # generate Shamir shares
 *   wourifs provision combine     # reconstruct master secret
 *   wourifs status                # cluster health overview
 *   wourifs audit log             # query the append-only audit chain
 *
 * Deployment model (small EMF / no server room):
 *   Each office workstation runs 'wourifs serve datanode'. At the head
 *   office, one or two workstations additionally run 'wourifs serve
 *   namenode'. A single wourifs.yaml file, identical on every machine,
 *   describes the cluster. Nodes discover each other via Tailscale or
 *   the local broadcast domain.
 */
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var configPath string

func main() {
	root := &cobra.Command{
		Use:   "wourifs",
		Short: "WouriFS distributed storage for microfinance institutions",
		Long: `WouriFS centralises branch financial data into a replicated,
tamper-evident cluster. No server room required — every workstation
contributes storage. Operations are accessible through a single binary.`,
		SilenceUsage: true,
	}

	root.PersistentFlags().StringVarP(&configPath, "config", "c",
		"/etc/wourifs/wourifs.yaml", "path to wourifs.yaml")

	root.AddCommand(initCmd())
	root.AddCommand(serveCmd())
	root.AddCommand(provisionCmd())
	root.AddCommand(mountCmd())
	root.AddCommand(statusCmd())
	root.AddCommand(auditCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "wourifs: %v\n", err)
		os.Exit(1)
	}
}
