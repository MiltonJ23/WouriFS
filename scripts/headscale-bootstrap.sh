#!/bin/bash
# Bootstrap WireGuard mesh via Headscale for WouriFS Sprint 2.
# All Namenode and Datanode instances communicate over 100.64.0.0/10
# assigned by Headscale.  This script runs on the coordination node.

set -euo pipefail

HEADSCALE_HOME="${HEADSCALE_HOME:-/etc/headscale}"
NAMENODES="${NAMENODES:-3}"
DATANODES="${DATANODES:-3}"

echo "=== WouriFS WireGuard Mesh Bootstrap ==="

# Start Headscale if not running
if ! pgrep -x headscale >/dev/null; then
    echo "starting headscale..."
    headscale serve &
    sleep 2
fi

# Generate pre-auth keys for nodes
for i in $(seq 1 "$NAMENODES"); do
    KEY=$(headscale preauthkeys create --user wourifs --reusable --expiration 24h 2>/dev/null | tail -1)
    echo "namenode-$i pre-auth key: $KEY"
done

for i in $(seq 1 "$DATANODES"); do
    KEY=$(headscale preauthkeys create --user wourifs --reusable --expiration 24h 2>/dev/null | tail -1)
    echo "datanode-$i pre-auth key: $KEY"
done

echo ""
echo "On each node, register with:"
echo "  tailscale up --login-server=http://<headscale-ip>:8080 --authkey=<key> --hostname=<name>"
echo ""

# Print assigned addresses for /etc/hosts or DNS
headscale nodes list 2>/dev/null || echo "(no nodes registered yet)"

echo "=== Bootstrap complete ==="
