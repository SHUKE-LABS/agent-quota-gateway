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
  with `405` (carrying the standard `Allow: POST` response header) before any
  upstream round-trip.
- Streaming (SSE) is forwarded without buffering — the first event
  reaches the client as soon as the upstream writes it.
- Error responses from upstream propagate to the client with the original
  status code, except those auto-rotation handles: a `429` (quota), and a
  `401`/`403` (the backend's credential was rejected — revoked, expired, or
  the account pulled), which fail the pool over to a healthy member rather
  than stick to a dead account. See [Pools and selectors](#pools-and-selectors).
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
  rate-limit headers (Z.ai / ZhipuAI, MiniMaxi, Volcengine Ark): a
  background poller reads their proprietary quota endpoint for the active
  member of each pool (see
  [Proprietary quota polling](#proprietary-quota-polling)).

Out of scope:

- Non-Anthropic *protocols*.
- Quota-watermark or concurrency-aware load spreading. A pool fails off a
  member on a real `429`, or once the quota store reports its window
  **blocking** — a `rejected` status for an Anthropic backend, or (for a
  poller-tracked backend, which reports no status) utilization `1.0`; see
  [Proprietary quota polling](#proprietary-quota-polling). It never pre-empts
  a member the upstream still serves (an Anthropic window at `1.0` with
  `allowed_warning` keeps serving, to maximize prompt-cache retention) and
  never spreads concurrent requests across accounts.
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

A **member name (nick) is the global identity for a physical account.** The
gateway keys quota state by `QuotaKey()` (see `internal/backend`), and
`QuotaKey()` is the nick alone — not `pool/nick`. This is deliberate: a
high-volume subscription added to several pools has its exhaustion recorded
once under that nick and read by every pool that selects it, so the same
account can be shared across pools without the cross-pool staleness where one
pool's "fresh-looking" copy gets picked after the other has already exhausted
it.

Two corollaries keep that sharing honest:

- **The same nick may appear in multiple pools** to share a single
  subscription. It is the intended way to add the same account to a second
  routing context.
- **The nick↔credential mapping is one-to-one.** A credential is bound to
  exactly one nick, and every declaration of a nick must use the identical
  credential. A different credential for the same nick (or the same
  credential under a different nick) is a load error that names both
  occurrences. Without this bijection, two quota keys would still alias one
  physical account.

The runtime add-member API (see [Runtime pool configuration](#runtime-pool-configuration))
is unaffected — it resolves a known subscription's credential and base URL
across pools and keeps its own same-nick-slot overwrite/conflict rules.

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
`429` failover keeps precedence. Pools without a static `PRIORITY` declaration
never preempt unless priority is set at runtime via `POST /_gateway/pool/{name}/priority`,
so their prompt cache is never interrupted.

### Balanced routing within a pool

By default the gateway is intentionally sticky: it rides one member until
that member returns `429` or its quota store reports a fully consumed window.
This maximises prompt-cache locality. The downside is that a pool of
*interchangeable* subscription accounts can repeatedly over-drain one member
across rolling 5-hour windows, burning its 7-day allowance much faster than
the others.

**Lead-based balanced routing** is an opt-in per-pool mode that adds a
proactive switch when the active member's quota consumption is materially
*ahead of schedule* relative to a healthier alternative. The metric is:

```
elapsed_fraction = 1 − (time_until_reset / window_length)   # clamped to [0, 1]
lead = utilization − elapsed_fraction
```

A positive lead means the member is consuming faster than time is passing.
The gateway computes `max(lead_5h, lead_7d)` over any windows whose
utilization and reset are known, and switches when the active member's lead
exceeds the best non-exhausted candidate's lead by at least the configured
gap. A dwell timer prevents churn immediately after a switch.

Enable it with `AQG_POOL_<POOL>_BALANCE=lead`:

```
# A pool of interchangeable subscription accounts, balanced by lead.
AQG_POOL_SUB_BACKEND_A=sk-ant-...
AQG_POOL_SUB_BACKEND_B=sk-ant-...
AQG_POOL_SUB_BACKEND_C=sk-ant-...
AQG_POOL_SUB_BALANCE=lead

# Optional tuning (shown with their defaults):
# AQG_POOL_SUB_BALANCE_GAP=0.15    # switch when active lead − best lead ≥ 0.15
# AQG_POOL_SUB_BALANCE_DWELL=5m    # minimum time between switches
```

**How it interacts with the default sticky design:**

- Between switches the pool is fully sticky: cache locality is preserved.
- The switch fires on the request path (no background goroutine); the gap
  and dwell keep it rare.
- The lead check never synthesises probes — it reads only snapshots learned
  from real traffic or the existing poller.
- Exhausted members (live-429 parked or store-exhausted) are never chosen
  as the balance target.
- When no snapshot data is available for a member its lead is treated as 0
  (neutral); the pool stays sticky until real traffic trains the store.
- **Equal-lead tiebreaker:** when multiple candidates share the same best
  lead (the common case when none have snapshot data yet, all reading as 0),
  the gateway prefers the member that was least recently active — tracked by
  a per-member selection-sequence counter that increments each time a member
  becomes the sticky backend. This prevents the lexically-first nick from
  winning every equal-lead comparison and accumulating disproportionate
  5-hour cycles. The selection-sequence state is persisted in the state file
  and survives restarts.

**Cache-locality tradeoff:** a balance switch breaks prompt-cache continuity
for the in-flight session, just like any other mid-session switch. Unlike a
`429` switch (which is forced), a balance switch is *elective* — the session
cache is sacrificed to avoid a worse outcome (7-day window tragedy). The gap
(default 0.15) and dwell (default 5m) tune how eagerly the gateway makes
that trade.

**Mutual exclusion with `PRIORITY`:** a pool cannot declare both
`BALANCE=lead` and `PRIORITY` — the two modes have conflicting goals.
Declaring both is a startup error.

## Environment variables

| Variable | Default | Notes |
|----------|---------|-------|
| `AQG_POOL_<POOL>_BACKEND_<NICK>` | _(at least one required)_ | A pool member's credential, optionally `=<cred>\|<base-url>` to override the pool default upstream for that member. `<POOL>` and `<NICK>` are normalized (`AQG_POOL_Z_AI_BACKEND_KEY_A` → pool `z-ai`, member `key-a`). |
| `AQG_POOL_<POOL>_BASE_URL` | `ANTHROPIC_BASE_URL` | The pool's default upstream; scheme and host are required. Omit it for pools that hit `api.anthropic.com`. |
| `AQG_POOL_<POOL>_PRIORITY` | _(optional)_ | Comma-separated member nicks, highest priority first (e.g. `zai,m3`). When set, the pool starts on and fails over toward the highest-priority healthy member instead of random/round-robin. Unlisted members rank last (sorted). Carries no credential. See [Priority within a pool](#priority-within-a-pool). Mutually exclusive with `BALANCE`. |
| `AQG_POOL_<POOL>_BALANCE` | _(optional)_ | Set to `lead` to enable lead-based balanced routing. The gateway switches the active member when its lead (utilization minus elapsed window fraction) exceeds the best candidate's lead by at least `BALANCE_GAP`, subject to `BALANCE_DWELL`. Mutually exclusive with `PRIORITY`. See [Balanced routing within a pool](#balanced-routing-within-a-pool). |
| `AQG_POOL_<POOL>_BALANCE_GAP` | `0.15` | Minimum lead difference that triggers a balance switch. Only valid when `BALANCE=lead` is set. |
| `AQG_POOL_<POOL>_BALANCE_DWELL` | `5m` | Minimum time between balance switches. Accepts Go duration strings (e.g. `5m`, `2m30s`). Only valid when `BALANCE=lead` is set. |
| `ANTHROPIC_BASE_URL` | `https://api.anthropic.com` | Default upstream inherited by any pool without its own `BASE_URL`; scheme and host are required. |
| `LISTEN_ADDR` | `127.0.0.1:8080` | Loopback address only (`127.0.0.1`, `::1`, `localhost`); the build refuses anything else. Mutually exclusive with `SHARED_LISTEN_ADDR`. |
| `SHARED_LISTEN_ADDR` | _(unset)_ | Opt into [shared mode](#shared-mode-over-tailscale): bind a single **Tailscale** address (IPv4 `100.64.0.0/10` or IPv6 `fd7a:115c:a1e0::/48`) instead of loopback, so other tailnet machines share one authoritative gateway. Must be an IP literal; loopback, `0.0.0.0`/`::`, RFC1918, public addresses, and names are rejected at startup. Mutually exclusive with `LISTEN_ADDR`. |
| `VOLC_ACCESSKEY` | _(unset)_ | Volcengine IAM Access Key ID. Required when any pool backend has a base URL containing `volces.com` — the background poller needs these account-level credentials to call `GetCodingPlanUsage`. Unrelated to the inference key stored in `AQG_POOL_*_BACKEND_*`. |
| `VOLC_SECRETKEY` | _(unset)_ | Volcengine IAM Secret Access Key. Required alongside `VOLC_ACCESSKEY` for Volcengine Ark quota polling. If either var is absent at poll time, the poll is skipped and the prior snapshot is preserved. |
| `AQG_STATE_FILE` | see notes | Path for the persistent state file. When unset the gateway falls back to `$STATE_DIRECTORY/state.json` (set automatically by systemd when `StateDirectory=agent-quota-gateway` is in the unit — the default install already sets this). An empty resolved path disables persistence: all state is in-memory only and lost on restart. The file stores sticky pointers, exhausted maps, quota snapshots, runtime-added members (including their credentials), and removed-member tombstones (so a removal stays in effect across restart). Writes are atomic (temp-file + rename) at mode 0600 and coalesced via a 200 ms debounce. A missing or unparseable file at startup is silently ignored and a fresh state begins. |

Startup fails closed on: no pools at all, an empty credential, a `BASE_URL`
on a pool with no members, a malformed upstream URL, an unrecognized
`AQG_POOL_*` shape, two keys colliding on the same pool/member, a
`PRIORITY` that is empty, repeats a nick, names a nick that is not a member
of the pool, or targets a pool with no members, a `BALANCE` value other than
`lead`, `BALANCE_GAP` or `BALANCE_DWELL` set without `BALANCE`, `BALANCE`
and `PRIORITY` both declared on the same pool, both `LISTEN_ADDR` and
`SHARED_LISTEN_ADDR` set at once, or a `SHARED_LISTEN_ADDR` outside the
Tailscale ranges. A `|` in a credential is rejected because the tail must
parse as a URL — tokens do not contain `|`.

Pools live in the environment by default — the gateway reads no credential
from disk in that mode (see [Security model](#security-model)). If you
prefer a `.env`, source it before launch (`set -a; . ./.env; set +a`) or
use systemd `EnvironmentFile=` / a secret manager.

## Config file

As an alternative to environment variables, you can declare pools and gateway
settings in a JSON config file. This is opt-in: the gateway uses env vars by
default, and only reads a file when explicitly directed via `--config`,
`AQG_CONFIG`, or by placing `aqg.json` in the working directory.

**Precedence (highest to lowest):**

1. `--config <path>` flag
2. `AQG_CONFIG=<path>` environment variable
3. `./aqg.json` in the current working directory
4. Environment variables (the default)

When a config file is used, the gateway follows an **all-or-nothing** rule:
all pools and gateway settings must come from the file; env vars for pool
configuration are ignored. (The env vars listed in the table below are only
read when no config file is found.) A malformed file, an unknown JSON key,
or a file with looser-than-0600 permissions causes startup to fail closed —
no silent fallback to env.

**File format:**

```json
{
  "base_url": "https://api.anthropic.com",
  "listen_addr": "127.0.0.1:8080",
  "shared_listen_addr": "",
  "state_file": "",
  "pools": {
    "<POOL>": {
      "base_url": "<pool-default-upstream>",
      "members": {
        "<NICK>": {
          "credential": "<real-credential>",
          "base_url": "<optional-per-member-override>"
        }
      },
      "priority": ["nick-a", "nick-b"],
      "balance": "lead",
      "balance_gap": 0.15,
      "balance_dwell": "5m"
    }
  }
}
```

**Env ↔ File mapping table:**

| Env var | JSON path | Notes |
|---------|-----------|-------|
| `AQG_POOL_<P>_BACKEND_<N>` | `pools.<P>.members.<N>.credential` | Required. |
| `AQG_POOL_<P>_BACKEND_<N>\|<URL>` | `pools.<P>.members.<N>.base_url` | Optional per-member override. |
| `AQG_POOL_<P>_BASE_URL` | `pools.<P>.base_url` | Pool-level default. |
| `AQG_POOL_<P>_PRIORITY` | `pools.<P>.priority` | Array of nicks, highest first. |
| `AQG_POOL_<P>_BALANCE` | `pools.<P>.balance` | Set to `"lead"` for balanced routing. |
| `AQG_POOL_<P>_BALANCE_GAP` | `pools.<P>.balance_gap` | Omit for the default (0.15). An explicit non-positive value is rejected. |
| `AQG_POOL_<P>_BALANCE_DWELL` | `pools.<P>.balance_dwell` | Omit for the default (`5m`). An explicit non-positive value is rejected. |
| `ANTHROPIC_BASE_URL` | `base_url` | Gateway default upstream. |
| `LISTEN_ADDR` | `listen_addr` | Loopback-only bind address. |
| `SHARED_LISTEN_ADDR` | `shared_listen_addr` | Tailscale bind address for shared mode. |
| `AQG_STATE_FILE` | `state_file` | Path to persistent state file. |

**Sample file:**

A `aqg.sample.json` file is provided in the repository. Copy it to `aqg.json`,
fill in real credentials, and set permissions:

```bash
cp aqg.sample.json aqg.json
chmod 600 aqg.json   # required: gateway rejects looser permissions
# Edit aqg.json with your real credentials
./agent-quota-gateway   # or: --config aqg.json
```

The sample file contains placeholder credentials (`sk-ant-oat-PLACEHOLDER-*`);
replace them with your real credentials. The repository gitignores `aqg.json`
so your real file is never committed by default.

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
- **Reactive switch, no watermark below full.** A member is ridden until it
  returns a `429` or its quota store window reads **blocking**. What counts as
  blocking depends on the snapshot: for an Anthropic backend, whose headers
  carry a per-window status, only a `rejected` status blocks — a window at
  utilization `1.0` with status `allowed_warning` is in the soft-cap / overage
  zone and **still served**, so the gateway keeps using it rather than wrongly
  parking it (and, with every member at `1.0`-but-allowed, reporting the whole
  pool exhausted). For a poller-tracked backend (Z.ai / MiniMaxi / Ark), whose
  dashboard API reports only a utilization fraction and no status, the
  `1.0` cap is the signal — without it such a member, which emits no clean
  pre-stream `429`, would never fail off.
- **Dead-credential switch.** A member that returns `401`/`403` (its
  credential was revoked, expired, or the account pulled) is parked for the
  conservative default window and the pool fails over — a dead account never
  emits a `429`, so without this the pool would stick to it and return the
  auth error to every client. The park is cleared by `POST /_gateway/clear`
  once the account is restored, or retried automatically when the window
  elapses.
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
  "backend": "b",
  "active_backend": "b",
  "unified_status": "allowed",
  "unified_5h_utilization": 0.05,
  "as_of": "2026-06-14T13:42:11.038Z"
}
```

`backend` is the quota store key — the member nick — under which the
shared `quota.Store` files the most recent snapshot for this member;
`active_backend` is the member nick. Because `active_backend` changes
alongside the snapshot, a sudden utilization jump (e.g. 99% → 5%) on a
switch is self-explained: the gateway moved to a fresher account. An
unknown pool returns `200` with an empty snapshot. Pools whose members do
not report `anthropic-ratelimit-unified-*` (API keys, most non-native
vendors) return empty snapshots — failover still works off the real `429`.
Z.ai / ZhipuAI, MiniMaxi, and Volcengine Ark backends are the exception: a
background poller fills their snapshots from each provider's own quota
endpoint (see [Proprietary quota polling](#proprietary-quota-polling)).

The endpoint is `GET`-only; any other method returns `405` with an
`Allow: GET` response header.

### Inspecting pool health

`GET /_gateway/pool` returns the full member roster for every configured
pool. With `?pool=<name>` it narrows to a single pool; without the
parameter it returns all pools in sorted order.

```bash
curl http://127.0.0.1:8080/_gateway/pool?pool=auto
```

```json
{
  "pool": "auto",
  "active": "b",
  "members": [
    { "nick": "a", "status": "exhausted", "exhausted_until": "2026-06-15T18:00:00Z", "snapshot": { ... } },
    { "nick": "b", "status": "active",    "exhausted_until": null,                   "snapshot": { ... } },
    { "nick": "c", "status": "idle",      "exhausted_until": null,                   "snapshot": null }
  ]
}
```

**`status`** values:

| Value | Meaning |
|-------|---------|
| `active` | Currently selected by the sticky pointer |
| `exhausted` | Parked — either a live-429 park or store-driven exhaustion; `exhausted_until` is the reset time |
| `idle` | Healthy and not currently active |

`exhausted_until` is an RFC 3339 timestamp when `status == "exhausted"`,
`null` otherwise. `snapshot` is the same `quota.Snapshot` object
`/_gateway/quota` returns, or `null` when no snapshot has been recorded
for that member yet.

**Caveat for Anthropic/Claude members:** the gateway never probes — quota
state is learned only from real proxied responses. An idle or never-active
member will have `snapshot: null` or a stale value. This is intentional:
probing would start a new session and consume quota.

`?pool=<unknown>` returns HTTP 404. The endpoint is `GET`-only; non-GET
returns `405` with `Allow: GET`.

### Runtime pool configuration

Priority order, per-member enable/disable, and pool membership can be changed
at runtime, without a restart, through six endpoints. This is for operating a
pool mid-incident — taking a draining account out of rotation, reordering
preference, or adding a fresh account — when editing config and restarting is
the wrong tool.

| Method & path | Effect |
|---------------|--------|
| `GET /_gateway/config` | Effective configuration for every pool, **credentials redacted** |
| `POST /_gateway/pool` | Create a plain pool at runtime; body `{"name": "...", "base_url": "...", "mode": "plain"}` (`name` and `base_url` required, `mode` optional and defaults to `plain`). Returns `201` with `{"pool": "<name>"}`. The pool starts empty; a name that collides with an env-defined or existing runtime pool returns `409`. Persisted and re-instantiated on restart. |
| `POST /_gateway/pool/{name}/priority` | Set a runtime priority override; body is a JSON array of nicks, highest first. Enables preempt-back for the pool. |
| `POST /_gateway/pool/{name}/member/{nick}/disable` | Take a member (static or runtime-added) out of selection and failover |
| `POST /_gateway/pool/{name}/member/{nick}/enable` | Return a disabled member (static or runtime-added) to rotation |
| `POST /_gateway/pool/{name}/member/{nick}` | Add a runtime member; body `{"credential": "...", "base_url": "..."}` (`credential` required, `base_url` optional). Persisted with its credential. |
| `POST /_gateway/pool/{name}/member/{nick}/move` | Move a subscription to another pool; body `{"to": "<pool>", "placement": [...], "force": false}`. |
| `DELETE /_gateway/pool/{name}/member/{nick}` | Remove a member (static or runtime-added) from selection |

```bash
curl http://127.0.0.1:8080/_gateway/config
curl -X POST http://127.0.0.1:8080/_gateway/pool \
  -d '{"name": "spare", "base_url": "https://api.anthropic.com"}'
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/priority -d '["b","a"]'
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/member/a/disable
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/member/a/enable
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/member/d \
  -d '{"credential": "sk-ant-...", "base_url": "https://api.anthropic.com"}'
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/member/d/move \
  -d '{"to": "spare"}'
curl -X DELETE http://127.0.0.1:8080/_gateway/pool/auto/member/d
```

`GET /_gateway/config` returns one object per pool — balance settings, the
effective priority order, and per-member `nick` / `base_url` / `disabled` /
`status`. **No credential ever appears** in the response, a log, or an error.

```json
[
  {
    "pool": "auto",
    "priority": ["b", "a", "c"],
    "members": [
      { "nick": "a", "base_url": "https://api.anthropic.com", "disabled": true,  "status": "disabled" },
      { "nick": "b", "base_url": "https://api.anthropic.com", "disabled": false, "status": "active" }
    ]
  }
]
```

**Overlay on an immutable base.** The static configuration (env vars or the
config file) is never mutated — it stays immutable and lock-free on the hot
path. Runtime changes are an *overlay* layered on top of it: a priority
override replaces the declared order, and a disabled flag removes a member
from selection (like an exhausted member, but operator-set and never
auto-cleared). The overlay is persisted to the state file alongside routing
state and re-applied on top of the static base at startup; a persisted
reference to a member or pool that no longer exists in the base is dropped
with a logged warning, not a startup failure. Runtime-added members are
persisted with their credentials (the state file is protected at mode 0600).

A priority reorder does **not** force the pool off a healthy active member
(prompt-cache preservation is unchanged): the new order takes effect on the
next failover and on reset-driven preempt-back. Validation: an unknown nick
returns `400`, an unknown pool `404`, and a priority override on a
balanced-mode pool returns `409` (priority and balance are mutually
exclusive). All error bodies are credential-free.

**Adding and removing members.** `POST /_gateway/pool/{name}/member/{nick}`
adds a runtime member. The JSON body is `{"credential": "...", "base_url":
"..."}`: `credential` is required, and `base_url` is optional (it falls back to
the pool's static members, so it is required only when the pool has no static
member to inherit from). On success the member is persisted to the state file
*with its credential* (file mode `0600`) and re-applied at startup. Status
codes: `200` on success; `400` on a missing nick, invalid JSON body, missing
credential, invalid `base_url`, or a missing `base_url` for a pool with no
static members; `404` on an unknown pool; `409` when the nick already exists as
a static or runtime-added member. `DELETE /_gateway/pool/{name}/member/{nick}`
removes a member — static or runtime-added — from selection and returns `200`;
`404` on an unknown pool and `400` on a missing nick or a nick not present in
the pool. If the removed member was the active one, the pool force-switches to
the next healthy member. All error bodies are credential-free.

Removal is **permanent and survives restart**: the tombstone is persisted to
the state file, so a removed static member stays removed across a restart
instead of resurfacing. A removed member — static or runtime-added — is omitted
entirely from both `/_gateway/config` and `/_gateway/pool`, not merely flagged
disabled, and is never selected for routing. A removed *runtime-added* member
can be re-added with `POST .../member/{nick}` (which clears its tombstone); a
removed *static* member stays out until its tombstone is cleared from the state
file, since re-adding a still-declared static nick returns `409`.

**Moving a subscription between pools.** `POST
/_gateway/pool/{name}/member/{nick}/move` relocates a subscription from `{name}`
to the pool named in the body. It is the same overlay machinery: a persistent
remove from the source plus an add to the target carrying the source member's
credential and resolved `base_url`, so the move survives restart. The JSON body
is `{"to": "<pool>", "placement": [...], "force": false}`:

- `to` (required) is the target pool. Moving to the same pool returns `400`.
- `placement` is an explicit priority order (highest first, comma/array) that
  **must include** the moved nick. It is **required** when the target is a
  priority pool and has no existing slot for the nick — there is no implicit
  top/bottom/sorted insertion. It is not accepted for a plain/balanced target
  (`400`) and is unnecessary when overwriting an existing same-nick slot (the
  slot is preserved).
- `force` confirms an overwrite when the target already has a *runtime-added*
  member with the same nick but a different credential or `base_url`.

Conflict handling: a same-nick target whose credential **and** resolved
`base_url` match is silently overwritten in place (the slot is preserved); a
differing runtime-added target returns `409` until `force: true` is sent; a
same-nick **static** target can only match (it is immutable here) — a differing
one returns `409` that `force` cannot override. The move does **not** force the
target off a healthy active member; the new order applies on the next selection
event. Status codes: `200` on success; `400` on a missing/empty `to`, a
same-pool move, a missing source member, or an invalid/absent placement; `404`
on an unknown source or target pool; `409` on an unresolved same-nick conflict.
All error bodies are credential-free.

> **Shared mode:** these are **write** endpoints. In shared mode (see below)
> any tailnet member that can reach the port can reorder priority, disable,
> remove, or **move** members, or **add** a member — injecting a credential and a
> new upstream of their choosing. This is a sharper exposure than the read-only
> quota view. The Tailscale ACL restricting this port is the only gate; the
> gateway adds no auth of its own.

A single-file management page is served at `GET /_gateway/ui`. Open it in a
browser to view every pool, its priority order, the active member, and each
member's live status (`active` / `exhausted` / `disabled` / `idle`), and to
reorder priority or toggle enable/disable. It contains no auth and no build
step — it inherits the gateway's trust boundary. In shared mode this exposes
write controls to any tailnet member that can reach the port; a Tailscale
ACL restricting the port is the only gate.

```bash
curl http://127.0.0.1:8080/_gateway/ui
```

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
any local process as legitimate. Like `/_gateway/quota`, it is `GET`-only;
any other method returns `405` with an `Allow: GET` response header.

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
- Credentials live only in the process environment (or an opt-in,
  operator-protected JSON config file at `0600` permissions) and in
  memory — the gateway keeps zero credentials on disk by default, and the
  file path is an explicit opt-in alternative to a `0600` `aqg.env`. How
  the environment (or file) is populated is the operator's choice.
- Quota snapshots, sticky pointers, and exhausted maps can optionally be
  persisted to a local state file (see `AQG_STATE_FILE` below) so state
  survives a restart. The file contains only quota utilization data and
  timing — no credentials — and is `0600` so only the service account can
  read it. There is no telemetry egress.
- The proxy does not issue probe traffic against the Messages API: every
  header-derived snapshot is the side effect of a real client request. The
  only gateway-originated requests are the background poller's reads of
  Z.ai / ZhipuAI, MiniMaxi, and Volcengine Ark quota endpoints, sent with
  the active member's own credential (or IAM key pair for Volcengine) to
  that member's own provider — never to Anthropic, and never carrying
  request/response bodies.
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

`org_id` is the Anthropic organization that owns the account behind the
snapshot, copied verbatim from the `anthropic-organization-id` response
header on the request that drove it. It follows the same presence
semantics as the unified fields — present only when the upstream returned
the header on the most recent response, omitted otherwise — so a consumer
can surface which organization a pool member is using, which matters when a
pool mixes accounts from different orgs.

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

Z.ai / ZhipuAI, MiniMaxi, and Volcengine Ark backends are kept fresh
independently of traffic by the background poller (see
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

Z.ai / ZhipuAI, MiniMaxi, and Volcengine Ark never return
`anthropic-ratelimit-unified-*` headers, so their store entries would stay
permanently empty under the passive header model. Each exposes a proprietary
quota endpoint instead, so a background poller refreshes them on a fixed
cadence and writes the result into the same per-member store the header path
uses. The `/_gateway/quota?backend=<pool>` response shape is identical — a
consumer cannot tell a polled snapshot from a header-derived one.

How it behaves:

- **Active member only.** Every 2 minutes the poller asks each pool for its
  current sticky member and polls only that backend. A pool that has failed
  over to an untracked member (e.g. Anthropic) is not polled until it fails
  back, so polling naturally tracks where traffic is going.
- **Detection by base URL.** A backend is polled when its base URL contains
  `api.z.ai`, `open.bigmodel.cn`, `minimaxi.com`, or `volces.com`. Anything
  else (Anthropic, other vendors) is left to the header path.
- **Per-provider auth and mapping.** Z.ai / Zhipu authenticate with the raw
  credential on `Authorization` and report *used* percentages; MiniMaxi
  authenticates with `Authorization: Bearer` and reports *remaining*
  percentages, which the poller inverts to utilization. Volcengine Ark
  authenticates with HMAC-SHA256 IAM signing (`VOLC_ACCESSKEY` /
  `VOLC_SECRETKEY`) via POST to `GetCodingPlanUsage` and reports *used*
  percentages; its `session` window maps to 5h and `weekly` to 7d (reset
  timestamps are epoch seconds, not milliseconds). All three map onto the
  unified 5h / 7d utilization and reset fields.
- **Failure is silent and non-destructive.** A network error, non-`200`, or
  unparseable body is logged and skipped; the last good snapshot survives.
  For Volcengine, absent `VOLC_ACCESSKEY` or `VOLC_SECRETKEY` is treated the
  same as a network error — the poll is skipped and the prior snapshot is
  preserved.
- **Startup.** The poller runs one pass immediately at startup, so a tracked
  pool's snapshot is populated well within the first 2-minute interval —
  without any client request. It shares the process shutdown signal and
  stops when the gateway does.

The poller's reads are the only gateway-originated upstream traffic; see
[Security model](#security-model).

### Recovery probing for parked members

A poller-tracked backend that hits a transient-overload 429 (z.ai /
MiniMaxi / Ark servers are frequently overloaded, not actually quota-exhausted)
gets parked for the conservative 5h fallback because the 429 carries no
reset header. The poller's active-only tracking means a parked member's
store entry is stale — it does not reflect the upstream's recovery. Without
operator action (`POST /_gateway/clear`), the member would stay parked for
the full 5h.

The recovery probe closes that gap. When every member of a pool is parked
(all-exhausted on the proxy path), the gateway re-probes each parked
poller-tracked member through the same proprietary quota endpoint the
poller uses (cheap, non-billable, returns utilization + precise reset). A
member whose probe no longer satisfies the freshness/exhaustion predicate
is unparked; the request is then routed to it (response rewritten to
503 — the normal switch shape) instead of being forwarded as the
upstream 429. Anthropic is intentionally not probed — its 429s already
carry precise resets and organic traffic refreshes the store.

Probes are rate-limited (≤ 1 per parked member per 30s) and coalesce
across concurrent all-exhausted requests so a stalled upstream does not
block the proxy path or trigger probe storms. The flush-on-unpark goes
through the persister, so a restart cannot resurrect a stale park the
recovery probe has cleared.

## Why a thin proxy

The proxy is the trust boundary — it owns the credentials and resolves a
pool name to a member per request, and its logs are safe to share with any
local tool. Quota observation piggy-backs on the same boundary: rate-limit
headers come down on every response, so we capture them per backend with
zero extra upstream load.
