# agent-quota-gateway

Loopback-only reverse proxy for the Anthropic Messages API, sized for
local Claude Code workflows.

## What it is

A single-binary Go server that listens on `127.0.0.1` and forwards
any `POST` to `https://api.anthropic.com` (Claude Code uses
`/v1/messages` and `/v1/messages/count_tokens`), preserving streaming
and the `anthropic-*` headers Claude Code sends.

The gateway owns one or more named **backends**, each a real upstream
credential. A client never sends a real token — it sends a *selector*
(via `ANTHROPIC_AUTH_TOKEN`, which Claude Code puts on the
`Authorization` header), and the gateway swaps in the selected
backend's credential before forwarding. This lets one local user route
across several authorized Claude subscriptions / OAuth identities from a
single gateway without any client ever seeing a credential.

## Scope

- Anthropic-only. No OpenAI / Google / other providers.
- POST-only. Any path is forwarded to the upstream — the upstream is
  the authority on what it serves, so new or compatible-API endpoints
  pass through instead of hitting a gateway 404. Non-POST methods are
  rejected with `405` before any upstream round-trip. Claude Code uses
  `POST /v1/messages` and `POST /v1/messages/count_tokens`.
- Streaming (SSE) is forwarded without buffering — the first event
  reaches the client as soon as the upstream writes it.
- Error responses from upstream propagate to the client with the
  original status code.
- One log line per request (method, path, status, duration, request
  ID). Request bodies, response bodies, and credential headers are
  never logged.
- Selector-based routing. The inbound `ANTHROPIC_AUTH_TOKEN` is a local
  backend selector, never forwarded upstream. Unknown or missing
  selectors fail closed with `403` — there is no silent fallback to
  another account. The reserved selector `auto` lets the gateway pick a
  pooled backend itself (global-sticky with reactive `429` failover — see
  [The `auto` selector](#the-auto-selector)).
- Quota snapshots are captured passively from upstream rate-limit
  headers and exposed at `GET /_gateway/quota`, keyed per backend. No
  synthetic probe requests — freshness depends on real client traffic.

Out of scope:

- Non-Anthropic providers
- Quota-watermark or concurrency-aware load spreading across backends.
  The `auto` selector switches **only** on a real `429` (to maximize
  prompt-cache retention); it never pre-empts on a utilization threshold
  and never spreads concurrent requests across accounts.
- TLS termination (front it with a reverse proxy or `stunnel` if
  you need it)
- Request/response body modification, caching, retries
- Quota history, time-series, or per-request metering — only the
  latest snapshot per backend is kept
- Rate limiting or request blocking based on quota state
- Authentication on `/_gateway/quota` — loopback is the trust
  boundary
- Docker image or other packaging — `go build` is the deliverable

## Quickstart

```bash
go build -o agent-quota-gateway ./cmd/agent-quota-gateway

# Declare one or more backends; the suffix after AQG_BACKEND_ is the nick.
AQG_BACKEND_CLAUDE_A=sk-ant-oat... \
AQG_BACKEND_CLAUDE_B=sk-ant-oat... \
  ./agent-quota-gateway
```

Each backend credential may be a Claude Code OAuth token (`sk-ant-oat…`,
sent upstream as `Authorization: Bearer`) or a plain API key
(`sk-ant-api…`, sent as `x-api-key`); the gateway picks the matching
scheme per backend. Metering quota on OAuth tokens is the primary use —
those carry the limits worth watching.

The gateway listens on `127.0.0.1:8080` by default. Point Claude Code at
it and select a backend by putting its nick in `ANTHROPIC_AUTH_TOKEN`:

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 \
ANTHROPIC_AUTH_TOKEN=claude-a \
claude
```

The nick replaces what used to be a real token — the consumer side
changes only its *value*, not its wiring. The env-key suffix is
normalized to the selector: `AQG_BACKEND_CLAUDE_A` is addressed as
`claude-a` (lowercase, `_`→`-`).

## Environment variables

| Variable                | Default                       | Notes                                                |
|-------------------------|-------------------------------|------------------------------------------------------|
| `AQG_BACKEND_<NICK>`    | _(at least one required)_     | A backend credential. The suffix is normalized to the selector nick (`AQG_BACKEND_CLAUDE_A` → `claude-a`). An OAuth token (`sk-ant-oat…`) is sent upstream as `Authorization: Bearer` with the `oauth-2025-04-20` beta flag; any other value as `x-api-key`. Startup fails if none are set, a credential is empty, or two keys collide on the same nick. |
| `ANTHROPIC_BASE_URL`    | `https://api.anthropic.com`   | Upstream base URL; scheme and host are required.     |
| `LISTEN_ADDR`           | `127.0.0.1:8080`              | Loopback address only; the build refuses anything else. |

Backends live in the environment, not a file — the gateway never reads a
credential from disk (see [Security model](#security-model)). If you
prefer to keep them in a `.env`, source it into the environment before
launch (`set -a; . ./.env; set +a`) or use systemd `EnvironmentFile=` /
a secret manager; the gateway still only reads its environment.

## Smoke test

With the gateway running on `127.0.0.1:8080` and a backend declared as
`AQG_BACKEND_CLAUDE_A=…`, select it with a bearer token equal to the
nick:

```bash
curl -N -X POST http://127.0.0.1:8080/v1/messages \
  -H 'Content-Type: application/json' \
  -H 'anthropic-version: 2023-06-01' \
  -H 'Authorization: Bearer claude-a' \
  -d '{"model":"claude-haiku-4-5-20251001","max_tokens":16,"messages":[{"role":"user","content":"say hi"}]}'
```

You should see streaming SSE events back. The `-N` flag is required so
`curl` does not buffer the response itself. An unknown or missing
selector returns `403 {"error":"unknown backend selector"}` without any
upstream round-trip.

## The `auto` selector

Instead of naming a backend, a client can send the reserved selector
`auto` and let the gateway choose:

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 \
ANTHROPIC_AUTH_TOKEN=auto \
claude
```

The consumer never needs to know pool membership — it sends `auto` and
the gateway routes to one pooled backend, switching accounts on its
behalf when one runs out. The model is **global-sticky, reactive, and
zero-probe**:

- **Sticky.** Every `auto` request reuses the same backend
  (`currentAuto`) so Anthropic's per-account prompt cache keeps paying
  off. The gateway does not compare or balance across backends.
- **Reactive switch, no watermark.** A backend is ridden until it
  actually returns a `429`. There is no utilization threshold — a backend
  at 95% can still finish a small task, and switching only on a real
  rejection means fewer switches and better cache retention.
- **Zero probe.** The starting backend is chosen at random on startup and
  its quota fills in from the first real response; `GET
  /_gateway/quota?backend=auto` is empty until traffic flows. No backend
  is ever contacted just to measure it. This is also why resets stay
  naturally staggered: because each account's rolling 5-hour window is
  anchored to its own real first use (never a synthetic probe), the
  windows drift apart, so there is almost always one account freeing up
  before the others.

### What the client sees on a switch

On a `429` from the current backend the gateway does **not** forward the
`429`. Anthropic's `429` is a pre-stream rejection, so the gateway handles
it on the response side — no request body is buffered; the *client*
replays its own body:

- **A backend is still available →** the response is rewritten to `503`
  with `Retry-After: 1`. The gateway has already advanced `currentAuto`
  to another backend, so the client's retry resolves to it and succeeds,
  rebuilding the cache once on the new account. `503` is a transient
  "retry" signal, deliberately distinct from a `429` ("you are
  rate-limited") — Claude Code and any non-trivial client retry it.
- **Every backend is exhausted →** there is nothing to switch to, so the
  gateway forwards an honest `429` with `Retry-After` set to the precise
  wait until the soonest backend resets (read from the upstream
  `anthropic-ratelimit-unified-reset` header, not estimated).
  `currentAuto` is pre-pointed at that soonest backend so the client's
  post-wait retry lands on it. A backend's exhausted mark clears
  automatically once its reset time passes.

  The header `reset` is a conservative upper bound: the rolling 5-hour
  window actually frees capacity gradually as old usage ages out, so a
  backend may recover sooner than its advertised reset. The gateway
  treats the reset as authoritative and does not bank on early partial
  recovery.

Each switch is logged server-side as one line — `auto: claude-a ->
claude-b (claude-a hit 429)` — naming nicks only, never credentials or
the rejected selector value.

### Reading auto's quota

`GET /_gateway/quota?backend=auto` returns the active backend's snapshot
plus an `active_backend` field naming the nick it resolved to:

```bash
curl http://127.0.0.1:8080/_gateway/quota?backend=auto
```

```json
{
  "backend": "claude-b",
  "active_backend": "claude-b",
  "unified_status": "allowed",
  "unified_5h_utilization": 0.05,
  "as_of": "2026-06-14T13:42:11.038Z"
}
```

Because `active_backend` changes alongside the snapshot, a sudden
utilization jump (e.g. 99% → 5%) on a switch is self-explained: the
gateway moved to a fresher account. The consumer needs no knowledge of
pool membership — it asks `auto` and the response self-describes.

## Layout

- `cmd/agent-quota-gateway/` — service entrypoint and integration tests
- `internal/auto/` — the `auto` selector's global-sticky controller
- `internal/backend/` — backend registry, selector resolution middleware
- `internal/config/` — env loading and validation
- `internal/proxy/` — reverse-proxy handler and tests
- `internal/quota/` — rate-limit header extraction and snapshot store
- `internal/logging/` — middleware and tests

### Health

A loopback-only liveness probe is exposed at `GET /_gateway/health`.
It returns `200` with body `{"status":"ok"}` and a `Content-Type` of
`application/json`. The response is intentionally minimal — no version,
no uptime, no upstream reachability check — because the trust model
treats any local process as legitimate. Use it from a supervisor or
`curl` to confirm the process is alive; pair it with `GET
/_gateway/quota` if you also want to know whether traffic has flowed.

## Security model

The trust boundary is the loopback interface. Everything that can reach
`127.0.0.1:8080` is considered authorised, so the gateway is safe to
run alongside a single user account without authentication. The
guarantees that follow from that:

- The gateway owns every backend credential. Clients never see one —
  they send a selector (`ANTHROPIC_AUTH_TOKEN` → `Authorization:
  Bearer <nick>`), and the proxy replaces it with the resolved
  backend's real credential on every outbound request. The selector is
  never forwarded upstream and never logged or echoed — a client that
  mistakenly put a real token in `ANTHROPIC_AUTH_TOKEN` does not leak
  it through a rejection.
- Unknown or missing selectors fail closed with `403` before any
  upstream round-trip. There is no silent fallback to a default account.
- Request and response bodies are not logged, persisted, or inspected.
  The logging middleware records only `method`, `path`, `status`,
  `duration`, and a request ID.
- Credentials live only in the process environment and in memory —
  the gateway reads no credential file, so its "no on-disk state"
  posture holds. How the environment is populated (shell, systemd
  `EnvironmentFile=`, a secret manager) is the operator's choice and
  the operator's responsibility to protect.
- Quota snapshots are stored only in process memory. There is no on-
  disk state and no telemetry egress; stopping the gateway erases the
  cache.
- The proxy does not issue probe traffic. Every snapshot is the side
  effect of a real request a client made, so there is no covert
  channel that would let an operator learn anything an authorised
  client could not learn themselves.
- The listen address is loopback-only. `config.validate` rejects
  `0.0.0.0`, public IPs, and unresolvable names so a misconfigured
  deployment fails closed at startup.

## Quota snapshots

The gateway watches the `anthropic-ratelimit-unified-*` and
`anthropic-organization-id` response headers on every forwarded
request and keeps the latest snapshot per backend key in an
in-process cache. Reads go through a small loopback endpoint:

```bash
curl http://127.0.0.1:8080/_gateway/quota?backend=claude-a
```

The unified scheme is what subscription / OAuth (Claude Code) tokens
report: usage against rolling 5-hour and 7-day windows, expressed as a
utilization fraction (`0`..`1`) plus an allow/reject status. This is
the quota the gateway exists to meter. The legacy
`anthropic-ratelimit-requests-*` / `-tokens-*` headers — per-minute
RPM/TPM throttles on API-key traffic, not a depletable budget — are
intentionally **not** captured.

Response shape (all unified fields are optional — they are omitted
when the upstream response did not carry the corresponding header):

```json
{
  "backend": "claude-a",
  "unified_status": "allowed",
  "unified_reset": "2026-06-13T13:30:00Z",
  "unified_representative_claim": "five_hour",
  "unified_5h_status": "allowed",
  "unified_5h_utilization": 0.25,
  "unified_5h_reset": "2026-06-13T13:30:00Z",
  "unified_7d_status": "allowed",
  "unified_7d_utilization": 0.07,
  "unified_7d_reset": "2026-06-14T15:20:00Z",
  "unified_fallback_percentage": 0.5,
  "unified_overage_status": "rejected",
  "unified_overage_disabled_reason": "org_level_disabled",
  "org_id": "org_abc123",
  "as_of": "2026-06-13T13:42:11.038Z"
}
```

`as_of` is the gateway-side time the snapshot was recorded; the
`*_reset` fields are absolute upstream timestamps (the gateway decodes
the Unix-seconds headers into RFC 3339). A utilization of `0` means a
window is untouched (full quota); a missing utilization field means the
last response did not advertise that window at all.

### Backend keying

Snapshots are filed under the nick of the backend the request resolved
to — the same nick the client put in `ANTHROPIC_AUTH_TOKEN`. Read a
backend's snapshot by naming it:

```bash
curl http://127.0.0.1:8080/_gateway/quota?backend=claude-a
```

`GET /_gateway/quota?backend=unknown` always returns 200; if no traffic
for that nick has been seen, the body is just `{"backend": "unknown",
"as_of": "..."}`. Use the presence of a `unified_*` field (e.g.
`unified_5h_utilization`) to decide whether quota data is actually
available. The one reserved value is `?backend=auto`, which resolves to
the current sticky backend and adds an `active_backend` field (see [The
`auto` selector](#the-auto-selector)). (The endpoint takes no credential
selector itself — it is a local read-only view, gated by the loopback
boundary like `/_gateway/health`.)

### Freshness model

Snapshots only update when real traffic flows. The gateway never
issues synthetic probe requests — if no client has hit the backend
recently, the snapshot is stale by exactly that gap. Consumers that
need fresh data should issue (or wait for) a real request.

### Consumer contract

The JSON shape returned by `/_gateway/quota` is the producer-side
contract consumed by [`shukebeta/my-ai-team#588`](https://github.com/shukebeta/my-ai-team/issues/588).
The gateway publishes whatever fields the upstream response carried
and omits the rest; consumers are expected to adapt to the shape
they observe rather than rely on a frozen schema. This means:

- Field presence is signal, not noise. A `unified_7d_utilization` field
  that exists today may be absent tomorrow if Anthropic stops sending
  it. Treat missing fields as "not advertised on the last response"
  rather than "zero" — note that an explicit `0` utilization is full
  quota, which is the opposite of absent.
- The endpoint returns `200` for known and unknown backend keys; the
  caller decides whether the snapshot is meaningful by inspecting the
  body.
- The gateway does not ship compatibility shims. If the consumer needs
  a renamed field, a converted unit, or a derived value, that
  translation lives in the consumer.

## Why a thin proxy

The proxy is the trust boundary — it owns the backend credentials and
resolves a selector to one per request, and its logs are safe to share
with any local tool. Quota observation piggy-backs on the same
boundary: rate-limit headers come down on every response, so we capture
them per backend with zero extra upstream load. See
[Security model](#security-model) for the full list of guarantees.