package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

/*
 * wourifs init — guided first-time node setup.
 *
 * Walks the operator through: cluster name, node ID, roles, disk quota,
 * and network mode. Writes wourifs.yaml to disk. Idempotent — re-running
 * on an existing config asks for confirmation before overwriting.
 */

func initCmd() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "init",
		Short: "First-time cluster node setup",
		Long: `Creates a wourifs.yaml configuration interactively.

Each workstation in the microfinance institution runs 'wourifs init' once.
At the head office, choose roles [namenode, datanode]. At a branch,
choose [datanode] only. The same config file can be copied to all nodes
(only the node.id field differs per machine).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !force {
				if _, err := os.Stat(configPath); err == nil {
					fmt.Printf("Config %s already exists. Use --force to overwrite.\n", configPath)
					return nil
				}
			}

			cfg := DefaultConfig()

			// Cluster name
			cfg.Cluster.Name = prompt("Cluster name", cfg.Cluster.Name)

			// Node ID
			cfg.Node.ID = prompt("Node ID (unique per machine)", cfg.Node.ID)

			// Roles
			roles := prompt("Roles (namenode,datanode,gateway — comma separated)",
				strings.Join(cfg.Node.Roles, ","))
			cfg.Node.Roles = splitAndTrim(roles)

			// Disk quota
			quota := prompt("Disk quota in GB (0 = unlimited)", "50")
			if gb, err := strconv.Atoi(quota); err == nil {
				cfg.Store.MaxSizeGB = gb
			}

			// Data directory
			defaultData := filepath.Join("/var/lib/wourifs", cfg.Node.ID)
			cfg.Store.DataDir = prompt("Data directory", defaultData)

			// Network mode
			cfg.Network.Mode = prompt("Network mode (lan or tailscale)", cfg.Network.Mode)
			if cfg.Network.Mode == "tailscale" {
				cfg.Network.HeadscaleServer = prompt("Headscale server address", "headscale.lan:443")
			}

			// Namenode peers (only if node has namenode role)
			if cfg.HasRole("namenode") {
				peers := prompt("Namenode raft peers (host:port comma-separated)",
					fmt.Sprintf("%s:%d", "127.0.0.1", cfg.Network.NamenodePort))
				cfg.Network.NamenodePeers = splitAndTrim(peers)
			}

			// Write config
			data, err := yaml.Marshal(&cfg)
			if err != nil {
				return err
			}

			dir := filepath.Dir(configPath)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("create config dir: %w", err)
			}

			os.MkdirAll(cfg.Store.DataDir, 0755)

			if err := os.WriteFile(configPath, data, 0640); err != nil {
				return fmt.Errorf("write config: %w", err)
			}

			fmt.Printf("\nConfiguration written to %s\n", configPath)
			fmt.Printf("Data directory: %s\n", cfg.Store.DataDir)
			fmt.Printf("\nNext: wourifs serve %s\n", cfg.Node.Roles[0])
			return nil
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "overwrite existing config")
	return cmd
}

func prompt(label, def string) string {
	fmt.Printf("%s [%s]: ", label, def)
	var input string
	fmt.Scanln(&input)
	if input == "" {
		return def
	}
	return input
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
