# agent-quota-gateway

Loopback-only reverse proxy for the Anthropic Messages API, sized for
local Claude Code workflows.

## What it is

A single-binary Go server that listens on `127.0.0.1` and forwards any
`POST` to an Anthropic-compatible upstream (Claude Code uses
`/v1/messages` and `/v1/messages/count_tokens`), preserving streaming and
the `anthropic-*` headers Claude Code sends. For multiple machines that
share one set of pool credentials, an opt-in
[shared mode](#shared-mode-over-tailscale) binds a Tailscale address so
they ride one authoritative instance.

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
- Authentication on `/_gateway/*` — loopback is the trust boundary (in
  [shared mode](#shared-mode-over-tailscale) the Tailscale ACL is, and the
  `/_gateway/quota` view becomes readable by every permitted tailnet
  member).
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

# A mixed pool that prefers one member over another. PRIORITY makes the
# pool start on (and fail over toward) the highest-priority healthy member
# instead of a random one — drain the preferred backend first, fall to the
# next when it 429s.
AQG_POOL_CHN_BACKEND_ZAI=zai-key
AQG_POOL_CHN_BACKEND_M3=m3-key
AQG_POOL_CHN_PRIORITY=zai,m3
```

Clients then select `auto`, `api`, `z-ai`, or `chn`. Each pool rotates
independently; the gateway does not move traffic between pools on its own.

### Priority within a pool

By default a pool's members are interchangeable: the controller starts on a
random one and, on a `429`, fails over round-robin (spreading load and
preserving each account's prompt cache — see
[Pools and selectors](#pools-and-selectors)). That is ideal for a pool of
equal-strength subscriptions.

When a pool mixes a *preferred* backend with a weaker fallback, declare an
order with `AQG_POOL_<POOL>_PRIORITY=<nick>,<nick>,...` (highest first):

- The pool **starts on** its highest-priority member instead of a random one.
- On a `429` it **fails over to** the highest-priority *healthy* member, so
  failover always climbs back toward the preferred backend.
- Members omitted from the list rank after the listed ones, in sorted order.
- The variable is **opt-in**: a pool without it keeps the random-start,
  round-robin behaviour unchanged. Listing a nonexistent nick (or a pool
  with no members) is a startup error.

The order is by member nick only — no vendor or model names appear in the
gateway's routing logic, so adding a new vendor's subscription is a config
change, never a code change.

A priority pool also **preempts back**: when a higher-priority member's
quota window resets while a lower-priority member is active, the gateway
switches the pool back to the recovered member so a freshly-reset preferred
backend is drained promptly instead of riding the fallback until it `429`s.
The switch happens within one timer cycle of the reset. It uses the precise
`unified_5h_reset` when known (Anthropic via headers, other vendors via the
quota poller), falls back to the member's parked reset otherwise, and only
idles on a 5-minute poll when neither is available. A member that resets but
is immediately rate-limited again is not switched to repeatedly — reactive
`429` failover keeps precedence. Pools **without** a `PRIORITY` declaration
never preempt, so their prompt cache is never interrupted.

## Environment variables

| Variable | Default | Notes |
|----------|---------|-------|
| `AQG_POOL_<POOL>_BACKEND_<NICK>` | _(at least one required)_ | A pool member's credential, optionally `=<cred>\|<base-url>` to override the pool default upstream for that member. `<POOL>` and `<NICK>` are normalized (`AQG_POOL_Z_AI_BACKEND_KEY_A` → pool `z-ai`, member `key-a`). |
| `AQG_POOL_<POOL>_BASE_URL` | `ANTHROPIC_BASE_URL` | The pool's default upstream; scheme and host are required. Omit it for pools that hit `api.anthropic.com`. |
| `AQG_POOL_<POOL>_PRIORITY` | _(optional)_ | Comma-separated member nicks, highest priority first (e.g. `zai,m3`). When set, the pool starts on and fails over toward the highest-priority healthy member instead of random/round-robin. Unlisted members rank last (sorted). Carries no credential. See [Priority within a pool](#priority-within-a-pool). |
| `ANTHROPIC_BASE_URL` | `https://api.anthropic.com` | Default upstream inherited by any pool without its own `BASE_URL`; scheme and host are required. |
| `LISTEN_ADDR` | `127.0.0.1:8080` | Loopback address only (`127.0.0.1`, `::1`, `localhost`); the build refuses anything else. Mutually exclusive with `SHARED_LISTEN_ADDR`. |
| `SHARED_LISTEN_ADDR` | _(unset)_ | Opt into [shared mode](#shared-mode-over-tailscale): bind a single **Tailscale** address (IPv4 `100.64.0.0/10` or IPv6 `fd7a:115c:a1e0::/48`) instead of loopback, so other tailnet machines share one authoritative gateway. Must be an IP literal; loopback, `0.0.0.0`/`::`, RFC1918, public addresses, and names are rejected at startup. Mutually exclusive with `LISTEN_ADDR`. |

Startup fails closed on: no pools at all, an empty credential, a `BASE_URL`
on a pool with no members, a malformed upstream URL, an unrecognized
`AQG_POOL_*` shape, two keys colliding on the same pool/member, a
`PRIORITY` that is empty, repeats a nick, names a nick that is not a member
of the pool, or targets a pool with no members, both `LISTEN_ADDR` and
`SHARED_LISTEN_ADDR` set at once, or a `SHARED_LISTEN_ADDR` outside the
Tailscale ranges. A `|` in a credential is rejected because the tail must
parse as a URL — tokens do not contain `|`.

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
- **Zero probe.** The starting member is chosen at random on startup (or by
  declared priority — see below) and its quota fills in from the first real
  response. No member is ever contacted just to measure it. This is also why
  resets stay naturally staggered: each account's rolling 5-hour window is
  anchored to its own real first use, so the windows drift apart and there
  is almost always one member freeing up before the others.

A pool may opt out of the random start and round-robin failover by
declaring a preference order with `AQG_POOL_<POOL>_PRIORITY` — see
[Priority within a pool](#priority-within-a-pool). This changes only
*which* healthy member is picked; the sticky, reactive, zero-probe model is
otherwise unchanged.

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

In the default mode the trust boundary is the loopback interface.
Everything that can reach `127.0.0.1:8080` is considered authorised, so the
gateway is safe to run alongside a single user account without
authentication. ([Shared mode](#shared-mode-over-tailscale) moves that
boundary to a Tailscale ACL — see that section for the changed model.) The
guarantees that follow:

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
- The listen address is loopback-only by default. `config.validate`
  rejects `0.0.0.0`, public IPs, and unresolvable names so a misconfigured
  deployment fails closed at startup. The one sanctioned way off loopback
  is [shared mode](#shared-mode-over-tailscale), which accepts only
  Tailscale addresses and nothing else.

## Shared mode over Tailscale

By default the gateway is single-machine: it binds loopback and only local
clients reach it. If several machines **intentionally share the same pool
credentials** and want one authoritative view — one sticky pointer, one
failover decision, one quota snapshot across all of them — run a single
gateway instance and let the others reach it over a [Tailscale](https://tailscale.com)
overlay.

Set `SHARED_LISTEN_ADDR` to this device's Tailscale IP (leave `LISTEN_ADDR`
unset — the two are mutually exclusive):

```bash
SHARED_LISTEN_ADDR=100.101.102.103:8080 \
AQG_POOL_AUTO_BACKEND_A=sk-ant-oat... \
AQG_POOL_AUTO_BACKEND_B=sk-ant-oat... \
  ./agent-quota-gateway
```

Other tailnet machines then point Claude Code at that address (the
Tailscale IP or its MagicDNS name):

```bash
ANTHROPIC_BASE_URL=http://100.101.102.103:8080 \
ANTHROPIC_AUTH_TOKEN=auto \
claude
```

One socket serves both the tailnet and the gateway host itself (a
Tailscale IP is a local interface), so there is no separate loopback
listener — a local client on the gateway box uses the same Tailscale
address.

### What "shared" means

This is not a new coordination protocol. The sticky pointer, exhausted
marks, and quota snapshots have always lived **per process**; shared mode
simply makes that one process reachable from other machines. So by
definition:

- every client drives the **same** sticky member, so the prompt cache on
  the active account keeps paying off across all of them;
- a `429` one machine triggers fails the pool over for **everyone** at
  once — no machine has to independently hit the wall to learn a backend
  is drained;
- `GET /_gateway/quota` returns the one shared view, not a per-machine
  guess.

There is **no per-client fairness or quota partitioning**. The shared 5h
window is first-come: one busy machine can drain it and the others simply
observe the drained state (which is the point — they see the truth). Switch
logs name the member (`auto[auto]: a -> b`) but not which machine drove the
switch.

> Running several **separate** gateway instances against the same
> credentials is **not** an authoritative coordination model. Each instance
> keeps its own sticky pointer, exhausted marks, and quota snapshots, so
> they diverge until each independently draws a `429`. Reactive failover
> still converges each instance to a correct state on its own, but there is
> no shared view. Use one instance in shared mode if you want that.

### The Tailscale ACL is required, not optional

The gateway adds **no authentication of its own** — the identity layer is
the Tailscale overlay. But Tailscale's default ACL is *allow-all*: without
an explicit ACL, any tailnet member can reach the gateway port and drive
your pools (and read `/_gateway/quota`). An ACL restricting the port to
specific tags is a **required** part of running shared mode. Tag the
gateway host and the clients, and allow only the client tag to the port:

```jsonc
{
  "tagOwners": {
    "tag:aqg-gateway": ["autogroup:admin"],
    "tag:aqg-client":  ["autogroup:admin"],
  },
  "acls": [
    // Only aqg clients may reach the gateway port; nothing else on the
    // tailnet can. Everything not matched here is denied by this ACL.
    {
      "action": "accept",
      "src":    ["tag:aqg-client"],
      "dst":    ["tag:aqg-gateway:8080"],
    },
  ],
}
```

Apply the gateway tag to the host running the binary
(`tailscale up --advertise-tags=tag:aqg-gateway`) and the client tag to the
consuming machines.

### Blast radius

The gateway **holds credentials and never hands them out** — a client that
reaches the socket gets *use* of a pool (it can drive the gateway to call
Anthropic), never the credential itself. That bounds the worst case:

- a **subscription** (`sk-ant-oat…`) pool caps at a drained 5h window,
  which recovers on reset;
- an `sk-ant-api…` (pay-as-you-go) pool caps at **dollar spend**, which
  does not recover.

The gateway does not distinguish pool credential types, which is exactly
why the address boundary is uniform — the Tailscale overlay, not "trust the
LAN for subscription pools." Bare-LAN (RFC1918) and public listen addresses
are rejected for this reason: there is no "the LAN is trusted" middle
ground.

## Deploying as a systemd service

For an always-on shared-mode instance, run it under systemd on a host that
stays up. The target needs **no Go toolchain** — the binary is a static
`linux/amd64` build shipped over ssh.

From a checkout on a machine that *does* have Go:

```bash
scripts/deploy.sh <ssh-host>        # e.g. scripts/deploy.sh e6420
```

This builds a version-stamped static binary, copies it (plus the unit and a
remote installer) to the host, and under `sudo`:

- installs `/usr/local/bin/agent-quota-gateway` (atomic replace),
- installs `/etc/systemd/system/agent-quota-gateway.service`,
- creates `/etc/agent-quota-gateway/aqg.env` (`0600 root:root`) from a
  template **only if it does not already exist** — your secrets are never
  overwritten on upgrade,
- `daemon-reload`, enables, and restarts the service.

On a fresh install the env file is a template, so the service will not come
up until you fill it in:

```bash
sudo nano /etc/agent-quota-gateway/aqg.env   # set SHARED_LISTEN_ADDR + pools
sudo systemctl restart agent-quota-gateway
```

See [`deploy/aqg.env.example`](deploy/aqg.env.example) for the full
template. `SHARED_LISTEN_ADDR` should be the host's Tailscale IP
(`tailscale ip -4`); omit it to run loopback-only instead.

> This file is a systemd `EnvironmentFile`, **not** a shell script. Use
> bare `KEY=value` lines — **no `export` prefix** (systemd ignores
> `export …` lines *and* logs their values to the journal as "invalid
> assignment", leaking secrets in plaintext). Give the service its own
> file with only its variables; do not point the unit at a general
> secrets dump.

**Upgrading** is the same command — `scripts/deploy.sh <host>` again. It
rebuilds, re-ships, and restarts; the env file is left untouched. Confirm
what is running:

```bash
ssh <host> agent-quota-gateway -version
ssh <host> journalctl -u agent-quota-gateway -f
```

The unit runs under `DynamicUser=yes` with a strict hardening profile
(`ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, no new privileges,
IP sockets only). The env file is read by the systemd manager and the
values are injected into the process, so the ephemeral service account
never reads the credential file directly. `Restart=always` covers the boot
race where the Tailscale interface IP is not assigned yet — the bind
retries until `tailscaled` brings it up.

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
