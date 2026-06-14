#!/usr/bin/env bash
# Build agent-quota-gateway and install/upgrade it on a remote systemd host
# over ssh. The target needs no Go toolchain — only systemd and ssh+sudo.
# Idempotent: re-run to upgrade. The remote env file is never overwritten.
#
#   scripts/deploy.sh <ssh-host>        # e.g. scripts/deploy.sh e6420
#   AQG_DEPLOY_HOST=e6420 scripts/deploy.sh
#   AQG_PORT=9949 scripts/deploy.sh e6420   # port for a fresh env template
set -euo pipefail

HOST="${1:-${AQG_DEPLOY_HOST:-}}"
if [[ -z "${HOST}" ]]; then
	echo "usage: $(basename "$0") <ssh-host>   (or set AQG_DEPLOY_HOST)" >&2
	exit 2
fi

cd "$(dirname "${BASH_SOURCE[0]}")/.."

VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT

echo ">> building agent-quota-gateway ${VERSION} (linux/amd64, static)"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
	-trimpath -ldflags "-s -w -X main.version=${VERSION}" \
	-o "${tmp}/agent-quota-gateway" ./cmd/agent-quota-gateway

echo ">> uploading to ${HOST}"
scp -q \
	"${tmp}/agent-quota-gateway" \
	deploy/agent-quota-gateway.service \
	scripts/remote-install.sh \
	"${HOST}:/tmp/"

echo ">> installing on ${HOST} (sudo)"
ssh "${HOST}" "
	set -e
	chmod +x /tmp/remote-install.sh
	sudo ${AQG_PORT:+AQG_PORT=${AQG_PORT}} /tmp/remote-install.sh \
		/tmp/agent-quota-gateway /tmp/agent-quota-gateway.service
	rm -f /tmp/agent-quota-gateway /tmp/agent-quota-gateway.service /tmp/remote-install.sh
"

echo ">> done. Env file on ${HOST}: /etc/agent-quota-gateway/aqg.env"
echo "   logs:   ssh ${HOST} journalctl -u agent-quota-gateway -f"
echo "   verify: ssh ${HOST} agent-quota-gateway -version"
