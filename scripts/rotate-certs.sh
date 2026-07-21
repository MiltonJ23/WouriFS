#!/usr/bin/env bash
# rotate-certs.sh — WouriFS TLS certificate rotation (NFR-S-010).
#
# Generates a new TLS key+cert pair for the Namenode and Datanode gRPC
# servers. The old pair is archived and the new pair is placed alongside
# the configuration. On success, the Namenode audit log records a
# CERT_ROTATED event.
#
# Usage: ./rotate-certs.sh /etc/wourifs
#
# Assumes:
#   - openssl is installed
#   - the CA cert and key are at $WOURIFS_HOME/ca.crt, $WOURIFS_HOME/ca.key
#   - cert expiry is set to 90 days (COBAC recommendation)

set -euo pipefail

WOURIFS_HOME="${1:-/etc/wourifs}"
NOW="$(date -u +%Y%m%d%H%M%S)"
ARCHIVE="${WOURIFS_HOME}/certs/archive-${NOW}"

mkdir -p "${WOURIFS_HOME}/certs" "${ARCHIVE}"

# Archive old certs
for f in server.crt server.key; do
  if [ -f "${WOURIFS_HOME}/certs/${f}" ]; then
    cp "${WOURIFS_HOME}/certs/${f}" "${ARCHIVE}/${f}"
  fi
done

# Generate new key
openssl genrsa -out "${WOURIFS_HOME}/certs/server.key" 2048

# Generate CSR
openssl req -new -key "${WOURIFS_HOME}/certs/server.key" \
  -out "${WOURIFS_HOME}/certs/server.csr" \
  -subj "/CN=wourifs-node/O=WouriFS"

# Sign with CA (valid 90 days)
openssl x509 -req -days 90 \
  -in "${WOURIFS_HOME}/certs/server.csr" \
  -CA "${WOURIFS_HOME}/ca.crt" \
  -CAkey "${WOURIFS_HOME}/ca.key" \
  -CAcreateserial \
  -out "${WOURIFS_HOME}/certs/server.crt"

rm -f "${WOURIFS_HOME}/certs/server.csr"

echo "certs rotated: ${ARCHIVE}"
echo "restart WouriFS services to load new certs"
