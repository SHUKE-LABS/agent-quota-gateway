#!/usr/bin/env bash
# Installs (or upgrades) agent-quota-gateway on the local machine. Runs as
# root on the target — invoked by scripts/deploy.sh over ssh+sudo. Takes
# the uploaded binary and unit paths as args; all install destinations are
# fixed. Never overwrites an existing env file.
set -euo pipefail

BIN=agent-quota-gateway
SRC_BIN="${1:?usage: remote-install.sh <binary> <unit>}"
SRC_UNIT="${2:?usage: remote-install.sh <binary> <unit>}"
ENV_DIR=/etc/agent-quota-gateway
ENV_FILE="${ENV_DIR}/aqg.env"
PORT="${AQG_PORT:-9949}"

# install(1) is atomic (write-temp + rename) so an in-flight request never
# sees a half-written binary; the service is restarted after.
install -m 0755 "${SRC_BIN}" "/usr/local/bin/${BIN}"
install -m 0644 "${SRC_UNIT}" "/etc/systemd/system/${BIN}.service"
install -d -m 0750 "${ENV_DIR}"

if [[ ! -e "${ENV_FILE}" ]]; then
	ts_ip="$(tailscale ip -4 2>/dev/null | head -n1 || true)"
	umask 077
	cat >"${ENV_FILE}" <<ENV
# Fill in your pools, then: sudo systemctl restart ${BIN}
# SHARED_LISTEN_ADDR binds this host's Tailscale IP. Omit it for loopback.
SHARED_LISTEN_ADDR=${ts_ip:-100.64.0.0}:${PORT}
# AQG_POOL_AUTO_BACKEND_A=sk-ant-oat...
# AQG_POOL_AUTO_BACKEND_B=sk-ant-oat...
ENV
	chmod 0600 "${ENV_FILE}"
	echo ">> created ${ENV_FILE} (template) — edit it, then: sudo systemctl restart ${BIN}"
else
	echo ">> kept existing ${ENV_FILE}"
fi

systemctl daemon-reload
systemctl enable "${BIN}.service" >/dev/null 2>&1 || true
systemctl restart "${BIN}.service" || true

sleep 1
systemctl --no-pager --full --lines=0 status "${BIN}.service" || true
echo ">> installed version: $(/usr/local/bin/${BIN} -version 2>&1 || echo '?')"
