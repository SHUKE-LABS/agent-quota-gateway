#!/usr/bin/env bash
# Installs (or upgrades) agent-quota-gateway on the local machine. Runs as
# root on the target — invoked by scripts/deploy.sh over ssh+sudo. Takes
# the uploaded binary and unit paths as args; all install destinations are
# fixed.
#
# Config model (issue #198): the living source of truth is aqg.json in the
# service StateDirectory (/var/lib/agent-quota-gateway/aqg.json), written and
# rewritten by the service itself (0600, DynamicUser-owned). The env file this
# script seeds is only a FIRST-START bootstrap: on the first start with no
# aqg.json, the gateway reads its AQG_POOL_* vars (+ any state file) to
# generate aqg.json, then never consults the environment again. This script
# never overwrites an existing env file, and never touches aqg.json.
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
# FIRST-START bootstrap seed only (issue #198). On the first start with no
# aqg.json, the gateway reads these vars to generate its config file at
# /var/lib/${BIN}/aqg.json, then never reads the environment again — edit the
# pools THERE afterwards (or via the UI). Fill in, then: sudo systemctl restart ${BIN}
# SHARED_LISTEN_ADDR binds this host's Tailscale IP. Omit it for loopback.
SHARED_LISTEN_ADDR=${ts_ip:-100.64.0.0}:${PORT}
# AQG_POOL_AUTO_BACKEND_A=sk-ant-oat...
# AQG_POOL_AUTO_BACKEND_B=sk-ant-oat...
ENV
	chmod 0600 "${ENV_FILE}"
	echo ">> created ${ENV_FILE} (bootstrap seed) — fill in pools, then: sudo systemctl restart ${BIN}"
	echo "   after first start the live config is /var/lib/${BIN}/aqg.json (edit there / via the UI)"
else
	echo ">> kept existing ${ENV_FILE}"
	# A common footgun: pointing the unit at a sourced shell env file.
	# systemd's EnvironmentFile is not a shell — it ignores `export `-prefixed
	# lines (logging their values to the journal) and does not expand $VAR.
	if grep -qE '^[[:space:]]*export ' "${ENV_FILE}" 2>/dev/null; then
		echo ">> WARNING: ${ENV_FILE} contains 'export '-prefixed lines."
		echo "   systemd will NOT parse them (and logs their values to the journal)."
		echo "   Use bare KEY=value lines with resolved values; see deploy/aqg.env.example."
	fi
fi

systemctl daemon-reload
systemctl enable "${BIN}.service" >/dev/null 2>&1 || true
systemctl restart "${BIN}.service" || true

sleep 1
systemctl --no-pager --full --lines=0 status "${BIN}.service" || true
echo ">> installed version: $(/usr/local/bin/${BIN} -version 2>&1 || echo '?')"
