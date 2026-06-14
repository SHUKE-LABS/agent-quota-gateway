# agent-quota-gateway

Loopback-only reverse proxy for the Anthropic Messages API, sized for
local Claude Code workflows.

## What it is

A single-binary Go server that listens on `127.0.0.1` and forwards any
`POST` to an Anthropic-compatible upstream (Claude Code uses
`/v1/messages` and `/v1/messages/count_tokens`), preserving streaming and
the `anthropic-*` headers Claude Code sends.

The gateway owns one or more named **pools**. A pool is a set of
*interchangeable* backends — same protocol, same quota semantics — each
holding a real upstream credential. A client never sends a real token: it
sends a **pool name** (via `ANTHROPIC_AUTH_TOKEN`, which Claude Code puts
on the `Authorization` header), and the gateway picks a backend from that
pool and swaps in its credential before forwarding. The gateway
auto-rotates within the pool, switching members on a real `429` so one
local user can ride several authorized accounts from a single endpoint
without any client ever seeing a credential.

Everything is a pool. There is no non-pool mode: even a single account is
declared inside a pool. Pools let you keep different *kinds* of account
apart — native Claude subscriptions, non-native Claude-compatible
vendors, and pay-as-you-go API keys each live in their own pool, because
mixing kinds breaks the assumptions auto-rotation relies on (a switch
across vendors loses the prompt cache, and quota semantics differ).

## Scope

- Anthropic protocol only. No OpenAI / Google / other protocols. Pools
  may point at non-Anthropic *hosts* as long as they speak the Anthropic
  Messages API (e.g. a Claude-compatible vendor).
- POST-only. Any path is forwarded to the upstream — the upstream is the
  authority on what it serves, so new or compatible-API endpoints pass
  through instead of hitting a gateway 404. Non-POST methods are rejected
  with `405` before any upstream round-trip.
- Streaming (SSE) is forwarded without buffering — the first event
  reaches the client as soon as the upstream writes it.
- Error responses from upstream propagate to the client with the original
  status code (except a `429`, which auto-rotation handles — see
  [Pools and selectors](#pools-and-selectors)).
- One log line per request (method, path, status, duration, request ID).
  Request bodies, response bodies, and credential headers are never
  logged.
- Pool-based routing. The inbound `ANTHROPIC_AUTH_TOKEN` is a local pool
  name, never forwarded upstream. Unknown or missing selectors fail
  closed with `403` — there is no silent fallback.
- Quota snapshots are captured passively from upstream rate-limit headers
  and exposed at `GET /_gateway/quota`, keyed per pool. No synthetic probe
  requests against the Messages API — header-derived freshness depends on
  real client traffic. The exception is providers that never return
  rate-limit headers (Z.ai / ZhipuAI, MiniMaxi): a background poller reads
  their proprietary quota endpoint for the active member of each pool (see
  [Proprietary quota polling](#proprietary-quota-polling)).

Out of scope:

- Non-Anthropic *protocols*.
- Quota-watermark or concurrency-aware load spreading. A pool switches
  **only** on a real `429` (to maximize prompt-cache retention); it never
  pre-empts on a utilization threshold and never spreads concurrent
  requests across accounts.
- **Cross-pool fallback / manual pool switching** — e.g. "all
  subscription pools are exhausted, borrow the `api` pool for 30 minutes".
  Pools are independent here; choosing between them is the client's job
  (it picks the pool name). A scheduler that moves traffic between pools
  is deliberately not built yet.
- TLS termination (front it with a reverse proxy or `stunnel` if needed).
- Request/response body modification, caching, retries.
- Quota history or per-request metering — only the latest snapshot per
  backend is kept.
- Authentication on `/_gateway/*` — loopback is the trust boundary.
- Docker image or other packaging — `go build` is the deliverable.

## Quickstart

```bash
go build -o agent-quota-gateway ./cmd/agent-quota-gateway

# Declare a pool "auto" with two subscription accounts. The upstream
# defaults to api.anthropic.com, so no BASE_URL line is needed here.
AQG_POOL_AUTO_BACKEND_A=sk-ant-oat... \
AQG_POOL_AUTO_BACKEND_B=sk-ant-oat... \
  ./agent-quota-gateway
```

The gateway listens on `127.0.0.1:8080` by default. Point Claude Code at
it and choose a pool by putting its name in `ANTHROPIC_AUTH_TOKEN`:

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 \
ANTHROPIC_AUTH_TOKEN=auto \
claude
```

The pool name replaces what used to be a real token — the consumer side
changes only its *value*, not its wiring. Pool and member names are
normalized: `AQG_POOL_AUTO_BACKEND_A` declares pool `auto`, member `a`
(lowercase, `_`→`-`), and the client selects it by sending `auto` in any
case.

### Auth schemes

The gateway picks the outbound auth scheme per credential, by prefix:

| Credential prefix | Sent upstream as | For |
|-------------------|------------------|-----|
| `sk-ant-oat…`     | `Authorization: Bearer` + `oauth-2025-04-20` beta | native Claude subscription / OAuth (also compatible vendors reselling real Anthropic OAuth tokens) |
| `sk-ant-api…`     | `x-api-key` | Anthropic pay-as-you-go API key |
| anything else     | `Authorization: Bearer` (no beta) | non-native Claude-compatible vendor key |

Metering quota on subscription (`sk-ant-oat…`) tokens is the primary use —
those carry the depletable 5h/7d limits worth watching. API keys and
non-native vendors generally do not report quota headers (see
[Quota snapshots](#quota-snapshots)).

## Pools by kind

A pool groups accounts that are *interchangeable* — same models, same
quota behaviour — so auto-rotation can fail over between them freely. Keep
different kinds in different pools:

```bash
# Native subscriptions — the main pool.
AQG_POOL_AUTO_BACKEND_A=sk-ant-oat...
AQG_POOL_AUTO_BACKEND_B=sk-ant-oat...

# Anthropic API keys — their own pool (no observable quota; they fail when
# the prepaid balance runs out).
AQG_POOL_API_BACKEND_K=sk-ant-api...

# A non-native Claude-compatible vendor — needs its own upstream. A member
# may override the pool default (e.g. a regional mirror) with a |url tail.
AQG_POOL_Z_AI_BASE_URL=https://open.example/anthropic
AQG_POOL_Z_AI_BACKEND_X=vendor-key-x
AQG_POOL_Z_AI_BACKEND_Y=vendor-key-y|https://mirror.example/anthropic
```

Clients then select `auto`, `api`, or `z-ai`. Each pool rotates
independently; the gateway does not move traffic between pools on its own.

## Environment variables

| Variable | Default | Notes |
|----------|---------|-------|
| `AQG_POOL_<POOL>_BACKEND_<NICK>` | _(at least one required)_ | A pool member's credential, optionally `=<cred>\|<base-url>` to override the pool default upstream for that member. `<POOL>` and `<NICK>` are normalized (`AQG_POOL_Z_AI_BACKEND_KEY_A` → pool `z-ai`, member `key-a`). |
| `AQG_POOL_<POOL>_BASE_URL` | `ANTHROPIC_BASE_URL` | The pool's default upstream; scheme and host are required. Omit it for pools that hit `api.anthropic.com`. |
| `ANTHROPIC_BASE_URL` | `https://api.anthropic.com` | Default upstream inherited by any pool without its own `BASE_URL`; scheme and host are required. |
| `LISTEN_ADDR` | `127.0.0.1:8080` | Loopback address only; the build refuses anything else. |

Startup fails closed on: no pools at all, an empty credential, a `BASE_URL`
on a pool with no members, a malformed upstream URL, an unrecognized
`AQG_POOL_*` shape, or two keys colliding on the same pool/member. A `|`
in a credential is rejected because the tail must parse as a URL — tokens
do not contain `|`.

Pools live in the environment, not a file — the gateway never reads a
credential from disk (see [Security model](#security-model)). If you
prefer a `.env`, source it before launch (`set -a; . ./.env; set +a`) or
use systemd `EnvironmentFile=` / a secret manager.

## Smoke test

With the gateway running and a pool declared as
`AQG_POOL_AUTO_BACKEND_A=…`, select it with a bearer token equal to the
pool name:

```bash
curl -N -X POST http://127.0.0.1:8080/v1/messages \
  -H 'Content-Type: application/json' \
  -H 'anthropic-version: 2023-06-01' \
  -H 'Authorization: Bearer auto' \
  -d '{"model":"claude-haiku-4-5-20251001","max_tokens":16,"messages":[{"role":"user","content":"say hi"}]}'
```

You should see streaming SSE events back. The `-N` flag is required so
`curl` does not buffer the response itself. An unknown or missing selector
returns `403 {"error":"unknown backend selector"}` without any upstream
round-trip.

## Pools and selectors

A client sends a pool name and the gateway auto-rotates within it. The
consumer never needs to know pool membership — it sends `auto` (or any
pool name) and the gateway routes to one member, switching accounts on its
behalf when one runs out. The model is **sticky, reactive, and
zero-probe**, per pool:

- **Sticky.** Every request to a pool reuses the same member so Anthropic's
  per-account prompt cache keeps paying off. The gateway does not compare
  or balance across members.
- **Reactive switch, no watermark.** A member is ridden until it actually
  returns a `429`. There is no utilization threshold — a member at 95% can
  still finish a small task, and switching only on a real rejection means
  fewer switches and better cache retention.
- **Zero probe.** The starting member is chosen at random on startup and
  its quota fills in from the first real response. No member is ever
  contacted just to measure it. This is also why resets stay naturally
  staggered: each account's rolling 5-hour window is anchored to its own
  real first use, so the windows drift apart and there is almost always
  one member freeing up before the others.

### What the client sees on a switch

On a `429` from the current member the gateway does **not** forward the
`429`. Anthropic's `429` is a pre-stream rejection, so the gateway handles
it on the response side — no request body is buffered; the *client* replays
its own body:

- **A member is still available →** the response is rewritten to `503`
  with `Retry-After: 1`. The gateway has already advanced the sticky
  pointer to another member, so the client's retry resolves to it and
  succeeds, rebuilding the cache once on the new account. `503` is a
  transient "retry" signal, deliberately distinct from a `429` — Claude
  Code and any non-trivial client retry it.
- **Every member is exhausted →** there is nothing to switch to, so the
  gateway forwards an honest `429` with `Retry-After` set to the precise
  wait until the soonest member resets (read from the upstream
  `anthropic-ratelimit-unified-reset` header when present, otherwise a
  conservative 5-hour window). The sticky pointer is pre-pointed at that
  soonest member so the client's post-wait retry lands on it. An exhausted
  mark clears automatically once its reset time passes.

Each switch is logged server-side as one line — `auto[auto]: a -> b (a hit
429)`, prefixed with the pool name — naming members only, never
credentials or the rejected selector value.

### Reading a pool's quota

`GET /_gateway/quota?backend=<pool>` returns the active member's snapshot
plus an `active_backend` field naming the member it resolved to:

```bash
curl http://127.0.0.1:8080/_gateway/quota?backend=auto
```

```json
{
  "backend": "auto/b",
  "active_backend": "b",
  "unified_status": "allowed",
  "unified_5h_utilization": 0.05,
  "as_of": "2026-06-14T13:42:11.038Z"
}
```

`backend` is the pool-qualified quota key (`<pool>/<member>`);
`active_backend` is the member nick. Because `active_backend` changes
alongside the snapshot, a sudden utilization jump (e.g. 99% → 5%) on a
switch is self-explained: the gateway moved to a fresher account. An
unknown pool returns `200` with an empty snapshot. Pools whose members do
not report `anthropic-ratelimit-unified-*` (API keys, most non-native
vendors) return empty snapshots — failover still works off the real `429`.
Z.ai / ZhipuAI and MiniMaxi backends are the exception: a background poller
fills their snapshots from each provider's own quota endpoint (see
[Proprietary quota polling](#proprietary-quota-polling)).

## Layout

- `cmd/agent-quota-gateway/` — service entrypoint and integration tests
- `internal/auto/` — per-pool sticky controllers and the `Pools` router
- `internal/backend/` — pool registry, selector resolution middleware
- `internal/config/` — env loading and validation
- `internal/proxy/` — reverse-proxy handler and tests
- `internal/quota/` — rate-limit header extraction and snapshot store
- `internal/poller/` — background poller for proprietary quota APIs
- `internal/logging/` — middleware and tests

### Health

A loopback-only liveness probe is exposed at `GET /_gateway/health`. It
returns `200` with body `{"status":"ok"}` and a `Content-Type` of
`application/json`. The response is intentionally minimal — no version, no
uptime, no upstream reachability check — because the trust model treats
any local process as legitimate.

## Security model

The trust boundary is the loopback interface. Everything that can reach
`127.0.0.1:8080` is considered authorised, so the gateway is safe to run
alongside a single user account without authentication. The guarantees
that follow:

- The gateway owns every credential. Clients never see one — they send a
  pool name (`ANTHROPIC_AUTH_TOKEN` → `Authorization: Bearer <pool>`), and
  the proxy replaces it with the resolved member's real credential on
  every outbound request. The selector is never forwarded upstream and
  never logged or echoed — a client that mistakenly put a real token in
  `ANTHROPIC_AUTH_TOKEN` does not leak it through a rejection.
- A credential and its upstream travel together on the request context, so
  one pool's credential can never be sent to another pool's host.
- Unknown or missing selectors fail closed with `403` before any upstream
  round-trip. There is no silent fallback.
- Request and response bodies are not logged, persisted, or inspected. The
  logging middleware records only `method`, `path`, `status`, `duration`,
  and a request ID.
- Credentials live only in the process environment and in memory — the
  gateway reads no credential file, so its "no on-disk state" posture
  holds. How the environment is populated is the operator's choice.
- Quota snapshots are stored only in process memory. There is no on-disk
  state and no telemetry egress; stopping the gateway erases the cache.
- The proxy does not issue probe traffic against the Messages API: every
  header-derived snapshot is the side effect of a real client request. The
  only gateway-originated requests are the background poller's reads of
  Z.ai / ZhipuAI and MiniMaxi quota endpoints, sent with the active
  member's own credential to that member's own provider — never to
  Anthropic, and never carrying request/response bodies.
- The listen address is loopback-only. `config.validate` rejects
  `0.0.0.0`, public IPs, and unresolvable names so a misconfigured
  deployment fails closed at startup.

## Quota snapshots

The gateway watches the `anthropic-ratelimit-unified-*` and
`anthropic-organization-id` response headers on every forwarded request
and keeps the latest snapshot per backend key (`<pool>/<member>`) in an
in-process cache. Reads go through a small loopback endpoint, keyed by
pool:

```bash
curl http://127.0.0.1:8080/_gateway/quota?backend=auto
```

The unified scheme is what subscription / OAuth (Claude Code) tokens
report: usage against rolling 5-hour and 7-day windows, expressed as a
utilization fraction (`0`..`1`) plus an allow/reject status. This is the
quota the gateway exists to meter. The legacy
`anthropic-ratelimit-requests-*` / `-tokens-*` headers — per-minute
RPM/TPM throttles, not a depletable budget — are intentionally **not**
captured.

Response shape (all unified fields are optional — omitted when the
upstream response did not carry the corresponding header):

```json
{
  "backend": "auto/a",
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

`as_of` is the gateway-side time the snapshot was recorded; the `*_reset`
fields are absolute upstream timestamps (decoded from Unix-seconds headers
into RFC 3339). A utilization of `0` means a window is untouched (full
quota); a missing utilization field means the last response did not
advertise that window.

### Pool keying

Snapshots are filed under `<pool>/<member>`, and the read endpoint takes a
**pool** name: it returns the pool's active member with an
`active_backend` field naming the member (see
[Reading a pool's quota](#reading-a-pools-quota)). Per-member historical
snapshots are kept internally but not yet exposed individually.

`GET /_gateway/quota?backend=<pool>` always returns `200`; if no traffic
has flowed (or the pool is unknown), the body carries no `unified_*`
fields. Use the presence of a `unified_*` field to decide whether quota
data is actually available. The endpoint takes no credential — it is a
local read-only view, gated by the loopback boundary like
`/_gateway/health`.

### Freshness model

For Anthropic and other header-reporting backends, snapshots only update
when real traffic flows. The gateway issues no synthetic probe requests
against the Messages API — if no client has hit the pool recently, the
snapshot is stale by exactly that gap.

Z.ai / ZhipuAI and MiniMaxi backends are kept fresh independently of
traffic by the background poller (see
[Proprietary quota polling](#proprietary-quota-polling)).

### Consumer contract

The JSON shape returned by `/_gateway/quota` is the producer-side contract
consumed by [`shukebeta/my-ai-team#588`](https://github.com/shukebeta/my-ai-team/issues/588).
The gateway publishes whatever fields the upstream response carried and
omits the rest; consumers adapt to the shape they observe rather than rely
on a frozen schema:

- Field presence is signal, not noise. Treat missing fields as "not
  advertised on the last response" rather than "zero" — an explicit `0`
  utilization is full quota, the opposite of absent.
- The endpoint returns `200` for known and unknown pools; the caller
  decides whether the snapshot is meaningful by inspecting the body.
- The gateway ships no compatibility shims. Renames, unit conversions, or
  derived values live in the consumer.

### Proprietary quota polling

Z.ai / ZhipuAI and MiniMaxi never return `anthropic-ratelimit-unified-*`
headers, so their store entries would stay permanently empty under the
passive header model. Each exposes a proprietary quota endpoint instead, so
a background poller refreshes them on a fixed cadence and writes the result
into the same per-member store the header path uses. The
`/_gateway/quota?backend=<pool>` response shape is identical — a consumer
cannot tell a polled snapshot from a header-derived one.

How it behaves:

- **Active member only.** Every 2 minutes the poller asks each pool for its
  current sticky member and polls only that backend. A pool that has failed
  over to an untracked member (e.g. Anthropic) is not polled until it fails
  back, so polling naturally tracks where traffic is going.
- **Detection by base URL.** A backend is polled when its base URL contains
  `api.z.ai`, `open.bigmodel.cn`, or `minimaxi.com`. Anything else
  (Anthropic, ByteDance Ark, other vendors) is left to the header path.
- **Per-provider auth and mapping.** Z.ai / Zhipu authenticate with the raw
  credential on `Authorization` and report *used* percentages; MiniMaxi
  authenticates with `Authorization: Bearer` and reports *remaining*
  percentages, which the poller inverts to utilization. Both map onto the
  unified 5h / 7d utilization and reset fields.
- **Failure is silent and non-destructive.** A network error, non-`200`, or
  unparseable body is logged and skipped; the last good snapshot survives.
- **Startup.** The poller runs one pass immediately at startup, so a tracked
  pool's snapshot is populated well within the first 2-minute interval —
  without any client request. It shares the process shutdown signal and
  stops when the gateway does.

The poller's reads are the only gateway-originated upstream traffic; see
[Security model](#security-model).

## Why a thin proxy

The proxy is the trust boundary — it owns the credentials and resolves a
pool name to a member per request, and its logs are safe to share with any
local tool. Quota observation piggy-backs on the same boundary: rate-limit
headers come down on every response, so we capture them per backend with
zero extra upstream load.
