# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Loopback-only reverse proxy for the Anthropic Messages API, sized for local
Claude Code workflows. Single Go binary (Go 1.24, **stdlib only — no
third-party deps**). The gateway owns every upstream credential; clients
address a **pool** by name and the gateway auto-rotates within it. Full
context lives in `README.md` (~1500 lines, authoritative on behavior).

## Commands

```bash
go build -o agent-quota-gateway ./cmd/agent-quota-gateway           # build
go vet ./...                                                        # lint
go test -race ./...                                                 # full suite
go test -race ./internal/auto -run TestResolveAuto_stickyWhileHealthy   # one test
```

CI (`.github/workflows/ci.yml`) runs `go build ./... && go vet ./... &&
go test -race ./...` on PRs and pushes to `main`; the suite is
race-clean by design. A release tag is auto-generated on every push to
main (`scripts/release-tag`, `lib/release.sh`).

Run locally (env path keeps zero credentials on disk):

```bash
AQG_POOL_AUTO_BACKEND_A=sk-ant-oat... AQG_POOL_AUTO_BACKEND_B=sk-ant-oat... \
  ./agent-quota-gateway
# point Claude Code at it — ANTHROPIC_AUTH_TOKEN carries the pool name:
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 ANTHROPIC_AUTH_TOKEN=auto claude
```

Deploy (builds a static `linux/amd64` binary + systemd unit, installs over
ssh; never overwrites the remote env file):

```bash
scripts/deploy.sh <ssh-host>            # unit + env template live in deploy/
```

## Config sources

Precedence: `--config` flag > `AQG_CONFIG` env > `./aqg.json` (must be
0600) > environment vars (`internal/configfile.Resolve`). The JSON file is
an opt-in alternative for operators; `aqg.sample.json` shows the shape.

Env grammar (`internal/backend`):

- `AQG_POOL_<POOL>_BASE_URL=<upstream>` — pool default upstream
- `AQG_POOL_<POOL>_BACKEND_<NICK>=<cred>[|<url>]` — a member; `|<url>` overrides the pool's upstream
- `AQG_POOL_<POOL>_PRIORITY=<nick>,<nick>` — ordered preference (vs. default random start + round-robin failover)
- `AQG_POOL_<POOL>_BALANCE=lead` + `BALANCE_GAP` + `BALANCE_DWELL` — opt-in utilization-balanced routing

Pool/nick names are normalized (lowercased, `_`→`-`): `AQG_POOL_Z_AI_BACKEND_KEY_A`
is pool `z-ai`, member `key-a`, selected by sending `z-ai` as the bearer token.

Listen/behavior vars: `ANTHROPIC_BASE_URL` (upstream, default
api.anthropic.com), `LISTEN_ADDR` (loopback, default 127.0.0.1:8080),
`SHARED_LISTEN_ADDR` (Tailscale address — opt into shared mode; mutually
exclusive with `LISTEN_ADDR`), `AQG_STATE_FILE` / `$STATE_DIRECTORY`
(persistence). `AQG_DEBUG_LOG_REQUESTS=1` turns on the inbound/outbound
request dump (`internal/reqlog`, credentials redacted).

## Architecture

### Request flow

1. Client sends `Authorization: Bearer <pool>` (Claude Code puts
   `ANTHROPIC_AUTH_TOKEN` there). `backend.Middleware` extracts the
   selector (falls back to `X-Api-Key`), normalizes it, and calls
   `auto.Pools.Route`.
2. `Route` returns **403 unknown selector** (fail closed, no upstream
   round-trip), **503 + Retry-After** (whole pool exhausted — wait until
   the soonest member resets; 503 not 429 so Claude Code retries and
   auto-resumes rather than ending the turn, issue #203), or the pool's
   current **sticky backend**, stored on the request context.
3. `proxy.New`'s director reads the resolved backend, picks the auth
   scheme by credential prefix (`sk-ant-oat*`→`Bearer`+`oauth-2025-04-20`
   beta; `sk-ant-api*`→`x-api-key`; else `Bearer` no beta), and forwards
   with response buffering disabled so SSE streams as it arrives. **Full
   method+path passthrough** — the selector/auth boundary gates, not a
   route table; `/` is the catch-all, `/_gateway/*` mounts directly with
   no selector.
4. Response observer calls `quota.Extract` (headers only) and **merges**
   the snapshot under the backend's `QuotaKey()` — but only if it carries
   a quota window. `mergeSnapshot` (`internal/quota/quota.go:218-247`)
   carries absent fields forward, so a response reporting only the windows
   it touched never blanks resets it taught us last time (issue #163).
   `hasQuotaWindow` is the **write** gate — `cmd/agent-quota-gateway/main.go`
   drops an empty snapshot before it ever reaches the store (issue #121's
   admission guard). `HasData()` is the **read** gate, used on `Store.Get`
   for a missing key — that returns a stamped-but-empty snapshot, never an
   admitted one.
5. `pools.ModifyResponse` dispatches per-pool response handling: upstream
   **429** → synthetic **503** for a member switch, a precise pool-dry wait,
   or a same-member transient throttle depending on the classifier; native
   Anthropic **529** overload → synthetic same-member **503** with
   `Retry-After: 60`; **401/403** → park the dead credential and fail over (a
   revoked account never 429s, so without this the pool would stick to it).
   The 529 path does not change pool state.

### Packages

- `cmd/agent-quota-gateway/` — entrypoint, HTTP mux wiring, `/_gateway/*`
  handlers, embedded UI (`ui/index.html` via `//go:embed`), integration
  tests. `main.go` also runs the snapshot-key migration (pre-#115
  `<pool>/<nick>` → `<nick>`).
- `internal/backend/` — pool registry, env/JSON spec decoding, the
  resolver middleware, `QuotaKey()`/`NormalizeName`. `PoolRouter` lives
  here so `backend` does not import `auto` (which depends on it).
- `internal/auto/` — the routing brain. One in-memory `Controller` per
  pool (`auto.go`); `Pools` bundles them and implements `PoolRouter`.
  Sticky-reactive-zero-probe rotation, priority/balance modes, runtime
  mutations that write through to the config file (add/remove/move/disable
  members, priority, create pool — via a copy-on-write registry swap +
  `Controller.reconcileLocked`, issue #198), pool status views, and the
  preemptor (`preempt.go`) that returns a priority pool to a higher member
  once its window resets.
- `internal/proxy/` — thin `httputil.ReverseProxy` wrapper; director
  stamps credential + upstream from the context.
- `internal/quota/` — Anthropic unified rate-limit header extraction +
  latest-snapshot-per-key `Store` (no history).
- `internal/poller/` — background poller for providers that emit no
  rate-limit headers (Z.ai/ZhipuAI, MiniMaxi, Volcengine Ark); polls only
  the active member of each pool, writes into the same store.
- `internal/config/`, `internal/configfile/` — env loading/validation and
  JSON file loading + precedence.
- `internal/persist/` — single debounced atomic state file (0600,
  temp+rename) for **runtime observation only**: sticky pointers, exhausted
  maps, snapshots, balance sequence, local-snapshot nicks. Operator intent
  lives in the config file (issue #198), not here. `internal/configfile/`
  owns the config write path (`Marshal` + debounced `Writer`).
- `internal/logging/` — one JSON line/request to stderr; bodies and
  credential headers never logged (V1 hard constraint — the proxy is the
  credential boundary). `internal/reqlog/` — opt-in debug dump.

### Key invariants (read multiple files before changing these)

- **Selector = pool name, never a credential.** It replaces the real
  token on the outbound request and is never forwarded, logged, or
  echoed — a client that mis-puts a real token in `ANTHROPIC_AUTH_TOKEN`
  does not leak it through a 403.
- **`QuotaKey()` is the nick alone, not `pool/nick`.** One account
  shared across multiple pools has a single exhaustion record read by
  every pool that selects it. Deliberate (PR #115) — it kills
  cross-pool staleness where one pool's "fresh-looking" copy gets picked
  after another exhausted it.
- **nick ↔ credential is a 1:1 bijection**, enforced at load. The same
  nick may appear in multiple pools (the intended way to share a
  subscription); a different credential for the same nick, or the same
  credential under a different nick, is a load error naming both.
- **Sticky until 429 or window-blocking.** A member is ridden until it
  returns 429 or its quota window reads blocking. For Anthropic backends
  a window must carry status `rejected` to block (utilization `1.0` +
  `allowed_warning` is the soft-cap/overage zone and still served, to
  preserve the prompt cache); for poller-tracked backends (no status)
  utilization `1.0` is the signal. The gateway never pre-empts a member
  the upstream still serves, and never probes to measure one.
- **The config file is the single source of operator intent (issue #198).**
  A runtime change (add/remove/update/disable member, priority, create pool,
  move) builds a fresh validated `*backend.Registry` via copy-on-write
  (`Registry.With*` → `BuildFromSpec`, so the nick↔credential bijection is
  enforced on every mutation), swaps it in under `Pools.mu`, reconciles the
  affected controllers (`reconcileLocked` preserves sticky/exhausted/balance/
  local-snapshot observation), and flushes to `aqg.json`. There is no
  state-file overlay. On first deploy with no `aqg.json`, env + the legacy
  state overlay are merged once (state-wins) to bootstrap the file; env is
  never read again. `Registry` is immutable-after-build; the hot read path
  stays lock-free.
- **Trust boundary = loopback** (or a Tailscale ACL in shared mode). No
  auth on `/_gateway/*`; the bind address is validated at config load
  (loopback or a literal Tailscale range only).

### Admin API

`GET /_gateway/health`, `/_gateway/quota?backend=<pool>` (active member's
snapshot), `/_gateway/pool[?pool=<name>]` (per-member health),
`/_gateway/activity` (rolling 60-min per-endpoint volume/error-rate/latency,
in-memory, `internal/activity`), `/_gateway/config` (effective config,
credentials redacted), `/_gateway/ui`,
plus runtime mutations under `POST|DELETE /_gateway/pool/{name}/...`
(priority, member add/remove/move, disable/enable) and
`POST /_gateway/clear` (drop live-429 parks). Non-GET probes on the
read endpoints return 405 with an `Allow` header. See README "Runtime pool
configuration" for the full table.

## Conventions

- Conventional commits with package scope (`feat(auto):`, `fix(proxy):`,
  `ci:`, `docs:`) — consistent across history.
- Comments document the *why* at length — issue numbers, design tradeoffs,
  the exact failure mode a guard prevents. Match that density when
  touching load-bearing invariants; the long comment is usually the spec.
