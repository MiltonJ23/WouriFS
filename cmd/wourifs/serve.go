package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

/*
 * wourifs serve — start a WouriFS node process.
 *
 * Subcommands: namenode, datanode, gateway.
 * Each subcommand reads the shared wourifs.yaml, extracts its relevant
 * section, and starts the appropriate gRPC server. A node can run
 * multiple roles by invoking multiple subcommands (e.g. in separate
 * systemd units or tmux panes).
 *
 * The serve commands do NOT fork to background. Use systemd, supervisor,
 * or your init system of choice to daemonize.
 */

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start a WouriFS node process",
		Long: `Starts a long-running WouriFS component.

Available subcommands: namenode, datanode, gateway.
Each reads wourifs.yaml and binds to its configured port.

Example:
  wourifs serve namenode   # metadata server + raft consensus
  wourifs serve datanode   # chunk storage node
  wourifs serve gateway    # web application + REST API`,
	}

	cmd.AddCommand(serveNamenodeCmd())
	cmd.AddCommand(serveDatanodeCmd())
	cmd.AddCommand(serveGatewayCmd())

	return cmd
}

func serveNamenodeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "namenode",
		Short: "Start a Namenode metadata server",
		Long: `Starts the Namenode gRPC server on the configured port.

Handles: metadata CRUD, chunk allocation, Datanode heartbeat
registry, namespace isolation, WAL persistence, and Raft consensus
(when 3 nodes are configured).

Requires: namenode role in wourifs.yaml, and at least one peer if
running in Raft mode.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadConfig(configPath)
			if err != nil {
				return err
			}
			if !cfg.HasRole("namenode") {
				return fmt.Errorf("this node (%s) does not have the 'namenode' role", cfg.Node.ID)
			}
			// TODO: wire into cmd/namenode logic using cfg
			fmt.Printf("[namenode] %s starting on :%d (data: %s)\n",
				cfg.Node.ID, cfg.Network.NamenodePort, cfg.Store.DataDir)
			if len(cfg.Network.NamenodePeers) > 0 {
				fmt.Printf("[namenode] raft peers: %v\n", cfg.Network.NamenodePeers)
			}
			fmt.Println("[namenode] NOT YET WIRED — see cmd/namenode/main.go")
			select {} // block
		},
	}
}

func serveDatanodeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "datanode",
		Short: "Start a Datanode chunk storage server",
		Long: `Starts the Datanode gRPC server.

Registers with the Namenode on startup and sends heartbeats every
5 seconds. Chunks are stored as flat files under the configured
data directory. This process is meant to run on every workstation
in the branch network.

Requires: datanode role in wourifs.yaml.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadConfig(configPath)
			if err != nil {
				return err
			}
			if !cfg.HasRole("datanode") {
				return fmt.Errorf("this node (%s) does not have the 'datanode' role", cfg.Node.ID)
			}
			// TODO: wire into cmd/datanode logic using cfg
			fmt.Printf("[datanode] %s starting on :%d (data: %s, quota: %d GB)\n",
				cfg.Node.ID, cfg.Network.DatanodePort, cfg.Store.DataDir, cfg.Store.MaxSizeGB)
			fmt.Println("[datanode] NOT YET WIRED — see cmd/datanode/main.go")
			select {} // block
		},
	}
}

func serveGatewayCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "gateway",
		Short: "Start the Gateway HTTP server (web app + REST API)",
		Long: `Starts the Gateway HTTP server over TLS 1.3.

Serves the microfinance operations SPA and REST API. Translates
browser requests into gRPC calls to the Namenode and Datanodes.

Requires: gateway role in wourifs.yaml.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadConfig(configPath)
			if err != nil {
				return err
			}
			if !cfg.HasRole("gateway") {
				return fmt.Errorf("this node (%s) does not have the 'gateway' role", cfg.Node.ID)
			}
			fmt.Printf("[gateway] %s starting on :%d\n",
				cfg.Node.ID, cfg.Network.GatewayPort)
			fmt.Println("[gateway] NOT YET WIRED — gateway package not yet implemented")
			select {} // block
		},
	}
}
