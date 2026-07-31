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
- Full method + path passthrough. Any method on any path is forwarded to
  the upstream — the upstream is the authority on what it serves, so new
  or compatible-API endpoints (e.g. `GET /v1/models`, batch polling, the
  Files API) pass through instead of hitting a gateway 404 or 405. What
  gates a request is the selector/auth boundary, not its method: an
  unknown or missing selector still fails closed with `403` for every
  method.
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
  backend is kept. Snapshots are merged field-by-field, so a window absent
  from one response/poll no longer clears a reset already learned.
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

The gateway normalizes the leading `/v1` on the request path, so the
client base URL works either way: set it to the gateway root
(`http://127.0.0.1:8080`, what Claude Code uses) **or** with a `/v1`
suffix (`http://127.0.0.1:8080/v1`, what OpenCode / Codex and SDKs that
hardcode `baseURL/v1` require). Both reach the upstream's `/v1` surface
correctly. (This applies to every pool whose upstream is mounted at the
host root, including Anthropic-compat vendors; a pool whose base URL
carries its own path prefix is left untouched.)

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

A park the quota store cannot represent follows the same rule (issue #254). A
`401`/`403` credential rejection, or a `429` with no usable reset, is a fact
about the credential itself — no quota-window field can carry it, so no
sibling pool could ever re-derive it from the shared store the way a
window-backed park is derived. The gateway copies that park directly into
every pool holding the nick the moment one pool observes it, so a revoked or
expired account stops being selected everywhere at once rather than being
discovered separately, pool by pool, on each one's own next `401`. Clearing it
— via `POST /_gateway/clear` or the per-nick clear — is symmetric: releasing
it from any one pool releases it everywhere it was propagated (see
[Clearing live-429 parks](#clearing-live-429-parks)).

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

`window_length` for the long window is **provider-aware**: it is ~30 days
for Z.AI / Zhipu (whose long slot carries the monthly `TIME_LIMIT` quota)
and 7 days for everyone else, resolved from the same provider mapping that
labels the column (see the provider-aware window note below). Using the
fixed 7-day length for a monthly reset weeks out would push
`time_until_reset / window_length` above 1, clamp `elapsed_fraction` to 0,
and collapse the long lead to raw utilization (issue #140).

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
| `AQG_STATE_FILE` | see notes | Path for the persistent state file. When unset the gateway falls back to `$STATE_DIRECTORY/state.json` (set automatically by systemd when `StateDirectory=agent-quota-gateway` is in the unit — the default install already sets this). An empty resolved path disables persistence: all runtime state is in-memory only and lost on restart. The file stores **runtime observation only** — sticky pointers, exhausted maps, quota snapshots, balance selection-sequence, and per-pool local-snapshot nicks. **Operator intent (pools, members, credentials, priority, balance, disabled) lives in the config file, not here** (issue #198). Writes are atomic (temp-file + rename) at mode 0600 and coalesced via a 200 ms debounce. A missing or unparseable file at startup is silently ignored and a fresh state begins. A pre-#198 state file may also contain legacy `config` / `added_pools` keys. First-deploy bootstrap reads the full overlay once; an existing-file start reconciles and removes only a legacy `priority_override` as described in [Config file](#config-file). When `aqg.json` declares an empty `state_file`, that migration may discover the old file through `AQG_STATE_FILE` or `$STATE_DIRECTORY` without enabling persistence or saving the discovered path. |
| `AQG_DEBUG_LOG_REQUESTS` | _(unset)_ | Set to `1` to dump every inbound request and outbound upstream request to stderr for debugging; any other value (or unset) leaves it off. Credentials are always redacted — the `Authorization` and `x-api-key` headers are never logged — but the inbound request body is dumped and may contain user message content, so enable only in dev/debug runs. |

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

In env-only mode (no config file resolved) pools live in the environment and
the gateway reads no credential from disk (see [Security model](#security-model)).
If you prefer a `.env`, source it before launch (`set -a; . ./.env; set +a`) or
use systemd `EnvironmentFile=` / a secret manager. Once a config file is in play
(the deployed default — see the next section), `aqg.json` is the source of truth
and the environment is only a first-start bootstrap seed.

## Config file

`aqg.json` is the **single source of truth for operator intent** (issue #198):
pools, members (credential + `base_url`), priority, balance, and the `disabled`
flag. Every runtime mutation made through the UI/API — add/remove/update a
member, disable/enable, set priority, create a pool, move a member — is
**written through to `aqg.json`** (debounced atomic write at 0600). There is no
separate persisted overlay: the state file holds only runtime *observation*
(see the `AQG_STATE_FILE` note above).

**Config source resolution (highest to lowest):**

1. `--config <path>` flag
2. `AQG_CONFIG=<path>` environment variable
3. `./aqg.json` in the current working directory
4. Environment variables (env-only mode — see below)

**How env and the config file relate (the bootstrap-once contract):**

- If a config path resolves (1–3) **and the file exists**, the gateway reads it
  and **ignores `AQG_POOL_*` env entirely**. UI/API mutations round-trip back
  to that file.
- If a config path resolves **but the file does not exist yet** (first deploy),
  the gateway **bootstraps it once**: it merges the `AQG_POOL_*` env with any
  operator mutations recorded in the state file (`state`-wins precedence),
  writes `aqg.json` at 0600, and then reads only `aqg.json` on every subsequent
  start — **the environment is never consulted again**. This is how an existing
  env-based deploy upgrades: keep the `EnvironmentFile` in place for the first
  start, then edit pools in `aqg.json` (or via the UI).
- If **no config path resolves at all** (no flag, no `AQG_CONFIG`, no
  `./aqg.json`), the gateway runs in **env-only mode**: pools come from the
  environment, nothing is written to disk (zero credentials on disk — the local
  dev default), and UI mutations are in-memory only.

**Legacy state-file operator intent (issue #241):**

When the deploy pins `AQG_CONFIG` to a fixed path under the systemd
`StateDirectory`, `aqg.json` is never overwritten by a redeploy — so a
pre-existing `aqg.json` survives every upgrade. A legacy operator who adjusted
a pool's priority through the pre-#198 runtime API may still have that adjustment
recorded as `config.<pool>.priority_override` in the state file.

On every config-file start the gateway reconciles that legacy priority into
`aqg.json` before serving, then removes the consumed `priority_override` key
from the state file. The deletion is the migration lock: after it succeeds,
`aqg.json` is the sole source of priority intent and later UI changes are not
reverted on restart. A crash after the config write but before key deletion is
safe — the next start sees the same exact order, leaves `aqg.json` untouched,
and retries the deletion. A state-file deletion failure is logged but does not
prevent startup; cleanup is retried on the next start.

If `aqg.json` declares no `state_file`, this migration alone probes
`AQG_STATE_FILE` and then `$STATE_DIRECTORY/state.json` for a legacy overlay.
The discovered location is not written into `aqg.json` and does not re-enable
runtime persistence. A non-empty configured `state_file` is always used as-is;
the gateway never probes past it to a possibly stale file.

Legacy priority nicks are normalized, filtered to current pool members, and
deduplicated before migration. A priority with no surviving member fails
startup and names the state/config paths that need repair. A missing pool is
logged and skipped. A pool that now uses balance mode keeps that newer mode and
has the superseded legacy priority key consumed. Other legacy overlay keys are
not replayed on this existing-file path. Hand-adding a new
`priority_override` after it has been consumed does not create a supported live
overlay; priority changes belong in `aqg.json` or the UI/API.

A malformed file, an unknown JSON key, or a file with looser-than-0600
permissions causes startup to fail closed — no silent fallback to env.

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
          "base_url": "<optional-per-member-override>",
          "disabled": false
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
| _(runtime disable via UI/API)_ | `pools.<P>.members.<N>.disabled` | `true` takes the member out of selection until re-enabled. Persisted to config (issue #198). |
| `AQG_POOL_<P>_BASE_URL` | `pools.<P>.base_url` | Pool-level default. |
| `AQG_POOL_<P>_PRIORITY` | `pools.<P>.priority` | Array of nicks, highest first. |
| `AQG_POOL_<P>_BALANCE` | `pools.<P>.balance` | Set to `"lead"` for balanced routing. |
| `AQG_POOL_<P>_BALANCE_GAP` | `pools.<P>.balance_gap` | Omit for the default (0.15). A fraction in `(0, 1)`; a value `<= 0` or `>= 1.0` is rejected (a gap `>= 1.0` is unreachable — don't pass a percent like `15`). |
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
  auth error to every client. If the same nick is a member of other pools,
  the park is copied into every one of them immediately (issue #254) — the
  credential is equally dead there, and no sibling pool would otherwise learn
  that fact until its own next `401`. The park is cleared, everywhere it was
  copied to, by `POST /_gateway/clear` once the account is restored, or
  retried automatically when the window elapses.
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

### What the client sees on a 429

On a `429` from the current member the gateway does **not** forward the
`429`. Anthropic's `429` is a pre-stream rejection, so the gateway handles
it on the response side — no request body is buffered; the *client* replays
its own body. The four `503` flavours the gateway can emit, each with its
own body and `Retry-After`, are summarised below — bodies are deliberately
distinct so a sustained stream cannot be misread as a stuck failover
(issue #245).

| Flavour | Body | `Retry-After` | Lands on |
|---|---|---|---|
| **Switch** — a member is still available, the sticky pointer has already advanced | `{"error":"backend switching; retry"}` | `1` (fixed) | another member |
| **Pool dry** — every member is exhausted; the sticky pointer is pre-pointed at the soonest-resetting member | `{"error":"all backends rate-limited"}` | precise wait until the soonest member resets (or a conservative 5-hour window) | the soonest-resetting member |
| **Z.ai/Zhipu throttle absorbed** (issue #153) — proxy `429` is the `1302` concurrency throttle, never quota exhaustion | `{"error":"backend throttled; same member"}` | `3` (fixed; longer than the switch hint so a single-member z.ai pool's retry lets the concurrency window free up) | the same member |
| **Anthropic per-minute rate-limit back-off** (issue #191) — transient RPM/ITPM/OTPM throttle, clears in seconds | `{"error":"backend throttled; same member"}` | upstream `retry-after` clamped to `[1, 3]` s, defaulting to `3` | the same member |

The **switch** flavour: the gateway has already advanced the sticky pointer
to another member, so the client's retry resolves to it and succeeds,
rebuilding the cache once on the new account. `503` is a transient "retry"
signal, deliberately distinct from a `429` — Claude Code and any
non-trivial client retry it.

The **pool dry** flavour: there is nothing to switch to, so the gateway
returns the precise wait until the soonest member resets (read from the
upstream `anthropic-ratelimit-unified-reset` header when present, otherwise
a conservative 5-hour window). It is a `503`, not a `429`, on purpose: a
`429` surfaces to Claude Code as a hard rate-limit error that ends the
turn, whereas a `503` is a transient signal it retries — so the agent
auto-resumes once the advertised window elapses instead of the client
giving up while the gateway already knows exactly when a member recovers.
An exhausted mark clears automatically once its reset time passes — or
earlier, the moment the polled quota store reports the member fresh and
non-blocking (see [Store-driven reconciliation](#recovery-probing-for-parked-members)).
This matters for backends whose `429` reset overshoots the real quota
window (Z.ai's `unified-reset` runs hours past its dashboard 5-hour reset):
the live park no longer holds the pool in `429` until that stale reset. The
reconciliation is backend-agnostic and self-correcting — if a forwarded
request still genuinely `429`s, its blocking headers refresh the store and
the member re-parks.

**Store-derived park from a polled snapshot at the cap** (issue #251). The
quota store tracks each member's `as_of` timestamp; when the poller's
most recent measurement lands a member at utilization 1.0 (or `rejected`
status), the gateway asserts the member into an exhausted mark once on
the next routing decision. If the snapshot carries a usable future
reset, the park ends at that reset. If it carries no reset (the
upstream `429` omitted it, or the poller's prior 5h field is the only
data), the park runs for `defaultExhaustionWindow` (5 h) from
`snap.AsOf` — a deliberate over-park rather than a probe. The
as-of-anchored bound is deterministic across reads (it does not re-arm
on each query), and the assert-once prevents a flap on the poll cycle
where the parked member's snapshot ages out minutes later. The
operator escape hatch is `POST /_gateway/clear` (and the per-nick
clear); clearing a store-derived park while a fresh at-cap snapshot is
on file does not move the member — the next routing decision
re-asserts the park from the store. See the comment at
`auto.go:windowBlocks` for the freshness threshold and the
poll-interval coupling the rule depends on.

The **z.ai throttle absorbed** flavour: a z.ai proxy `429` is always the
`1302` "Rate limit reached for requests" concurrency throttle (emitted when
the GLM Coding Plan concurrency cap — often as low as 1 — is hit), never
quota exhaustion — z.ai exhaustion is tracked out-of-band by the poller
(5h / monthly windows), never signalled by a proxy `429`. The gateway
absorbs it, leaves the member in rotation, and never lets the upstream
`1302` message reach the client. Claude Code retries the `503`
transparently instead of stopping on the passed-through `429`. The body
text is what prevents the operator misdiagnosis in issue #245: a sustained
stream of these is not a failover loop, because no failover is being
attempted.

The **Anthropic rate-limit back-off** flavour: a transient per-minute
throttle (`rate_limit_error` for the RPM/ITPM/OTPM throughput limit,
distinct from the 5h/7d quota window) clears in seconds and the *same*
member serves again, so the gateway backs off without switching or
parking. It is identified by the rate-limit signature — an upstream
`retry-after` and/or the legacy `anthropic-ratelimit-requests-*` /
`-tokens-*` per-minute headers — which separates it from a genuine
quota-window `429` (`unified-5h-status` and/or `unified-7d-status` is `rejected`, which parks) and from a
policy `429` (no rate-limit headers, e.g. an "unsupported third-party
client" rejection, which forwards the upstream body on a `503` with the
fixed 1 s hint). The per-minute headers are read only to classify the
response; they are never stored (they are a throughput rate, not the
subscription budget).

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
  "poller": { "last_success": "2026-07-13T11:58:42Z", "consecutive_failures": 0, "stale": false },
  "members": [
    { "nick": "a", "status": "exhausted", "exhausted_until": "2026-06-15T18:00:00Z", "snapshot": { ... }, "disabled": false, "parked": true  },
    { "nick": "b", "status": "active",    "exhausted_until": null,                   "snapshot": { ... }, "disabled": false, "parked": false },
    { "nick": "c", "status": "idle",      "exhausted_until": null,                   "snapshot": null,    "disabled": false, "parked": false },
    { "nick": "d", "status": "disabled",  "exhausted_until": null,                   "snapshot": null,    "disabled": true,  "parked": false }
  ]
}
```

**`status`** values:

| Value | Meaning |
|-------|---------|
| `active` | Currently selected by the sticky pointer **and** available — `exhausted` outranks `active`, so a sticky member that is also parked reports `exhausted`, not `active` |
| `exhausted` | Parked — either a live-429 park or store-driven exhaustion; `exhausted_until` is the reset time |
| `idle` | Healthy and not currently active |
| `disabled` | Taken out of selection and failover by the runtime disable toggle — `disabled` outranks every other state, so a disabled member always reports `disabled` regardless of its quota |

`exhausted_until` is an RFC 3339 timestamp when `status == "exhausted"`,
`null` otherwise. `snapshot` is the same `quota.Snapshot` object
`/_gateway/quota` returns, or `null` when no snapshot has been recorded
for that member yet.

**`parked`** is `true` only when a **live-429 park** is currently holding the
member out of rotation (present, reset not yet elapsed, and not reconciled away
by a fresh healthy store snapshot). It is the precise gate for the per-nick
"clear park" escape hatch — distinct from `status: "exhausted"`, which also
covers store-driven exhaustion that clearing the live park cannot move. The UI
shows the "Clear park" button only on a member with `parked: true`.

**`disabled`** is `true` when the member has been taken out of selection and
failover by the runtime disable toggle (`POST
/_gateway/pool/{name}/member/{nick}/disable`, see below) — the same source
the `status: "disabled"` string derives from, so the two can never disagree.
The UI's per-member toggle reads this on every poll to stay in sync with an
out-of-band disable (another tab, the API, another operator).

**Caveat for Anthropic/Claude members:** the gateway never probes — quota
state is learned only from real proxied responses. An idle or never-active
member will have `snapshot: null` or a stale value. This is intentional:
probing would start a new session and consume quota.

**`poller`** is the per-pool liveness observation (issue #247) for pools
the background poller tracks (Z.ai / ZhipuAI, MiniMaxi, Volcengine Ark).
It is **omitted entirely** for untracked pools — Anthropic and any
provider without a registered proprietary quota endpoint carry no `poller`
key, so an Anthropic-only deployment sees no delta. `last_success` is
`null` for a never-polled pool and an RFC 3339 timestamp after the first
successful poll. `last_error` / `last_error_at` populate only after a
failure; `consecutive_failures` resets to zero on success. `stale` is
the derived verdict — `true` when no successful poll has arrived in
`StaleAfterIntervals × poll_interval` (default 3 × 2 minutes), or when
the pool has never been polled. The UI renders a per-pool badge whose
colour and label track this state ("poller ok" / "poller stale" /
"poller failing (Nx)" / "never polled").

`?pool=<unknown>` returns HTTP 404. The endpoint is `GET`-only; non-GET
returns `405` with `Allow: GET`.

### Recent activity

`GET /_gateway/activity` returns a rolling, in-memory view of per-endpoint
request health over the last 60 one-minute buckets. It turns the same signal
the per-request log line carries (`path`/`status`/`duration`) into a temporal
series the point-in-time `health`/`quota`/`pool`/`config` endpoints don't
expose. It records only `path`/`status`/`duration` — never bodies, headers, or
credentials — and is **ephemeral**: in memory only, lost on restart, no
persistence.

```bash
curl http://127.0.0.1:8080/_gateway/activity
```

The response is a JSON object keyed by request path; each value is a
chronological array of up to 60 points:

```json
{
  "/v1/messages": [
    {
      "bucketStart": "2026-07-13T12:00:00Z",
      "volume": 42,
      "errors": 1,
      "errorRate": 0.0238,
      "latency": { "p50Ms": 820.5, "p95Ms": 2140.0, "maxMs": 4102.7 }
    }
  ]
}
```

`errorRate` is the fraction of non-2xx responses in the bucket. Latency
percentiles are nearest-rank over a bounded per-bucket sample; for streaming
`/v1/messages` they are **full end-to-end (SSE) wall time**, not
time-to-first-byte. Memory is bounded regardless of traffic — a fixed bucket
ring, a per-bucket cap on distinct paths (overflow folds into an `(other)`
key), and a fixed latency sample ring. `/_gateway/*` requests (the dashboard's
own polling) are excluded. The endpoint is `GET`-only; non-GET returns `405`
with `Allow: GET`. The management UI renders this as a "Recent activity
(60 min)" panel that refreshes on the normal poll cadence.

### Runtime pool configuration

Priority order, per-member enable/disable, and pool membership can be changed
at runtime, without a restart, through the endpoints below. This is for
operating a pool mid-incident — taking a draining account out of rotation,
reordering preference, or adding a fresh account — when editing config and
restarting is the wrong tool.

| Method & path | Effect |
|---------------|--------|
| `GET /_gateway/config` | Effective configuration for every pool, **credentials redacted** |
| `POST /_gateway/pool` | Create a plain pool at runtime; body `{"name": "...", "mode": "plain"}` (`name` required, `mode` optional and defaults to `plain`). A runtime pool is a pure named container with no pool-level base_url; each member resolves its own `base_url` via `AddMember`'s fallback chain. To atomically create the first member, include optional `nick`, `credential`, `base_url`, and `placement` fields; `nick` switches to combined mode, and validation failure creates neither resource. Returns `201` with `{"pool": "<name>"}`. The pool starts empty; a name that collides with an env-defined or existing runtime pool returns `409`. Persisted and re-instantiated on restart. |
| `DELETE /_gateway/pool/{name}` | Remove a pool. The pool must be **empty** — drain members first via `DELETE .../member/{nick}`; a pool that still has members returns `409` (no cascade, so no persisted credential is silently discarded). Returns `200` `{"status": "ok"}`; an unknown pool returns `404`. Deleting the last pool is allowed (routing then fails closed with `403` unknown selector). Persisted: a deleted pool does not reappear on restart. |
| `POST /_gateway/pool/{name}/rename` | Rename a pool in place; body `{"name": "<new>"}` (required, normalized server-side). Carries the pool's members, disabled flags, declared priority, and balance parameters over to the new key. The controller's runtime observation (sticky pointer, exhausted marks, balance sequence, local-snapshot set) is keyed by member nick, so it follows the rename unchanged. Returns `200` `{"pool": "<new>"}`. Empty / identical-after-normalize new name → `400`; unknown old pool → `404`; new name collides with a different existing pool → `409`. Persisted: the next config-roundtrip restart restores the rename under the new key. **Caveat for env-only mode** (`AQG_CONFIG` unset, no `aqg.json`): the config writer is a no-op, so the rename is runtime-only and reverts to the env-declared name on restart — same constraint `AddPool`/`AddMember` already carry. |
| `POST /_gateway/pool/{name}/priority` | Set a runtime priority override; body is a JSON array of nicks, highest first. Enables preempt-back for the pool. |
| `POST /_gateway/pool/{name}/member/{nick}/disable` | Take a member (static or runtime-added) out of selection and failover |
| `POST /_gateway/pool/{name}/member/{nick}/enable` | Return a disabled member (static or runtime-added) to rotation |
| `POST /_gateway/pool/{name}/member/{nick}` | Add a runtime member; body `{"credential": "...", "base_url": "...", "placement": [...]}`. `credential` and `base_url` are each optional when the nick is already a known subscription in another pool (resolved independently; ambiguous → `400`). `placement` is a JSON array of nicks (highest priority first, must include the added nick) and is **required** when the target is a priority pool with no existing slot for that nick; rejected (`400`) for plain/balanced targets. Persisted with its credential. |
| `POST /_gateway/pool/{name}/member/{nick}/move` | Move a subscription to another pool; body `{"to": "<pool>", "placement": [...], "force": false}`. |
| `DELETE /_gateway/pool/{name}/member/{nick}` | Remove a member (static or runtime-added) from selection |

```bash
curl http://127.0.0.1:8080/_gateway/config
curl -X POST http://127.0.0.1:8080/_gateway/pool \
  -d '{"name": "spare"}'
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/priority -d '["b","a"]'
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/member/a/disable
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/member/a/enable
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/member/d \
  -d '{"credential": "sk-ant-...", "base_url": "https://api.anthropic.com"}'
# add into a priority pool — placement is required when the nick has no existing slot
curl -X POST http://127.0.0.1:8080/_gateway/pool/priority/member/d \
  -d '{"credential": "sk-ant-...", "placement": ["d", "a", "b"]}'
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/member/d/move \
  -d '{"to": "spare"}'
curl -X DELETE http://127.0.0.1:8080/_gateway/pool/auto/member/d
# delete a pool (must be empty — drain its members first)
curl -X DELETE http://127.0.0.1:8080/_gateway/pool/spare
# rename a pool in place — membership and runtime observation follow
curl -X POST http://127.0.0.1:8080/_gateway/pool/auto/rename -d '{"name":"primary"}'
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

**Write-through to the config file.** Runtime changes mutate the config
registry, not a separate overlay (issue #198). Under the hood the gateway
builds a fresh, fully-validated registry (copy-on-write) and swaps it in
atomically, so the hot read path stays lock-free; the change is then flushed to
`aqg.json` (debounced atomic write at 0600). Priority sets the pool's order, and
a disabled flag removes a member from selection (like an exhausted member, but
operator-set and never auto-cleared) — both are ordinary config fields now, so
they survive restart because the gateway re-reads the same `aqg.json`. Every
mutation is re-validated by the same rules as a startup load (including the
nick↔credential bijection), so an invalid change is rejected and the prior
config is kept. In env-only mode (no config file) mutations are in-memory only
and every successful mutation response carries the
`X-AQG-Persistence: env_only` header plus a `persistence: "env_only"` body
field so the API caller sees the durability state directly.

**Watching for a lagging flush.** The debounced write to `aqg.json` can lag
or fail while the in-memory registry has already moved on. When it does,
`GET /_gateway/config` sets an `X-AQG-Unsaved-Config: true` response header,
and `GET /_gateway/health` adds `unsaved_config_changes: true` to its body
(see [Health](#health)). Either signal means on-disk config trails memory, so
a restart could lose runtime mutations — including credentials — that have not
yet reached disk; monitor for it and expect it to clear once the flush lands.

**Knowing you are in env-only mode (issue #246).** When no config file is
configured (no `--config` flag, no `AQG_CONFIG`, no `./aqg.json`), the
gateway runs in env-only mode and no runtime mutation is persisted. Every
`/_gateway/*` response that reports config durability now distinguishes
this case from both a clean save and a failed flush:

- `GET /_gateway/health` returns
  `{"status":"ok","persistence":"env_only"}` (additive body field; status
  stays 200, `status` stays `"ok"`).
- `GET /_gateway/config` adds a response header
  `X-AQG-Persistence: env_only`.
- Every successful runtime mutation (create/delete/rename pool, set
  priority, disable/enable/add/remove/move member) adds the same
  `X-AQG-Persistence: env_only` header and a `persistence: "env_only"`
  body field on its 200/201 response, so an operator hitting the API
  directly sees the change is in-memory only.

In persisted mode those same responses carry
`X-AQG-Persistence: persisted` and no `persistence` body field — the
documented default stays byte-identical for clean saves, and the legacy
`X-AQG-Unsaved-Config: true` header continues to fire only on flush
failure.

A priority reorder does **not** force the pool off a healthy active member
(prompt-cache preservation is unchanged): the new order takes effect on the
next failover and on reset-driven preempt-back. Validation: an unknown nick
returns `400`, an unknown pool `404`, and a priority override on a
balanced-mode pool returns `409` (priority and balance are mutually
exclusive). All error bodies are credential-free.

**Adding and removing members.** `POST /_gateway/pool/{name}/member/{nick}`
adds a runtime member. The JSON body is `{"credential": "...", "base_url": "...",
"placement": [...]}`:

- `credential` — optional when the nick is already a known subscription in
  another pool; the gateway resolves it by scanning all other pools for the same
  nick. Required when the nick is new (not found in any other pool). The
  nick↔credential bijection guarantees the same nick carries the same credential
  everywhere, so cross-pool resolution is unambiguous; supplying a *different*
  credential for a nick that already exists elsewhere is rejected with `400`.
- `base_url` — optional with the same fallback chain as before: omitting it falls
  back to the other-pool resolution (same logic), then to the pool's first static
  member's URL **only when every existing member already agrees on one effective
  upstream**. In a mixed-provider pool (issue #248) the first member's URL is
  alphabetical, not authoritative, so an omitted `base_url` is rejected with `400
  base_url for nick <nick> is ambiguous across this pool's members; specify it
  explicitly` — the new member would otherwise be silently pointed at whichever
  provider happened to sort first. Returns `400` if the base_url is ambiguous
  across other pools; falls back to the pool default when no other pool has a URL
  for this nick (equivalent to omitting it in a static config). Required only when
  the pool has no members and no other pool resolves a URL for this nick.
- `placement` — a JSON array of nicks, highest priority first; **must include**
  the added nick. Required when the target pool is in priority mode — there is no
  implicit insertion position. Rejected with `400` for plain/balanced-mode targets.

On success the member is written through to the config file *with its
credential* (mode `0600`) and re-read at startup. Status codes: `200` on
success; `400` on a missing or empty nick, invalid JSON body, missing credential
(nick not in any other pool), a credential conflicting with the nick's existing
credential (bijection), invalid `base_url`, ambiguous `base_url` across pools,
ambiguous `base_url` across the target pool's existing members (issue #248),
missing `base_url` for a pool with no members and no resolvable URL, missing
`placement` for a priority target with no existing slot, unknown nick in
`placement`, `placement` not containing the added nick, duplicate nick in
`placement`, or `placement` supplied for a non-priority target; `404` on an
unknown pool; `409` when the nick is already a member.
`DELETE /_gateway/pool/{name}/member/{nick}` removes a member from selection and
returns `200`; `404` on an unknown pool and `400` on a missing nick or a nick not
present in the pool. If the removed member was the active one, the pool
force-switches to the next healthy member. All error bodies are credential-free.

Removal is **permanent and survives restart**: the member is deleted from the
config file, so a fresh start reading `aqg.json` never sees it (there is no env
value to resurface it — the root cause of the old revert bug, #197). A removed
member is omitted entirely from both `/_gateway/config` and `/_gateway/pool`,
not merely flagged disabled, and is never selected for routing. Any removed
member can be re-added with `POST .../member/{nick}`, which writes it back into
the config.

Removing the **last** member drains the pool to a valid zero-member pool — this
holds for a vendor pool with its own `base_url` (`z-ai`, `minimax`, …) just as
it does for a default-upstream pool; the drained pool persists and is refilled
with `POST .../member/{nick}` (which requires an explicit `base_url` for an empty
pool, re-establishing the vendor upstream).

**Deleting a pool.** `DELETE /_gateway/pool/{name}` removes a pool entirely,
closing the create/delete lifecycle `POST /_gateway/pool` opens. The pool must
be **empty first**: a pool that still has one or more members returns `409` with
a message naming the drain-first requirement — there is no cascade or `?force`,
so a delete never silently discards a persisted credential (drain the members
with `DELETE .../member/{nick}`, then delete the pool). An unknown pool returns
`404`; success returns `200` `{"status": "ok"}`. Deletion writes through to
`aqg.json`, so a deleted pool does not reappear on restart. Any empty pool is
deletable regardless of whether it originated from env or a runtime create —
post-#198 every pool is a config entry with no persisted origin distinction.
Deleting the **last** pool is permitted and leaves the gateway with zero pools;
routing afterward fails closed with `403` unknown selector, exactly as an
unknown pool always has.

**Moving a subscription between pools.** `POST
/_gateway/pool/{name}/member/{nick}/move` relocates a subscription from `{name}`
to the pool named in the body. It is the same write-through machinery: a remove
from the source plus an add to the target carrying the source member's
credential and resolved `base_url`, all in one atomic config update, so the move
survives restart. The JSON body
is `{"to": "<pool>", "placement": [...], "force": false}`:

- `to` (required) is the target pool. Moving to the same pool returns `400`.
- `placement` is an explicit priority order (highest first, comma/array) that
  **must include** the moved nick. It is **required** when the target is a
  priority pool and has no existing slot for the nick — there is no implicit
  top/bottom/sorted insertion. It is not accepted for a plain/balanced target
  (`400`) and is unnecessary when overwriting an existing same-nick slot (the
  slot is preserved).
- `force` confirms an overwrite when the target already has a member with the
  same nick but a different resolved `base_url`. (The credential cannot differ:
  the nick↔credential bijection guarantees one credential per nick, so a
  same-nick conflict is always a `base_url` difference.)

Conflict handling: a same-nick target whose resolved `base_url` matches is
silently overwritten in place (the slot is preserved); a differing `base_url`
returns `409` until `force: true` is sent. The move does **not** force the
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

**Clearing live-429 parks.** `POST /_gateway/clear` drops reactive `429` parks
so an over-parked member becomes selectable again without waiting out the park
or restarting:

| Query | Effect |
|-------|--------|
| _(none)_ | Clear every pool's live-429 parks |
| `?pool=<name>` | Clear that one pool's live-429 parks |
| `?pool=<name>&nick=<nick>` | Clear only `<nick>`'s live-429 park — the per-nick escape hatch for a single over-parked member, leaving the rest of the pool parked |

```bash
curl -X POST 'http://127.0.0.1:8080/_gateway/clear'                    # all pools
curl -X POST 'http://127.0.0.1:8080/_gateway/clear?pool=auto'          # one pool
curl -X POST 'http://127.0.0.1:8080/_gateway/clear?pool=auto&nick=a'   # one member
```

It clears **only the reactive 429 park** — store-sourced exhaustion (a window
still at cap with a future reset) reflects polled reality and is left untouched.
Clearing a member that is genuinely out of quota is harmless: it simply re-parks
via the next upstream `429`. The per-nick form responds `{"pool","nick",
"cleared":<bool>}` where `cleared` reports whether a live park was actually
present; an unknown pool returns `404 {"error":"pool not found"}`, and a `nick`
with no `pool` returns `400` rather than clearing every pool. This is the
operator override complementary to the automatic recovery in
[Recovery probing for parked members](#recovery-probing-for-parked-members):
use it when the store itself is stale, poll lag holds a park through a recovery
window, or you simply know the member is fine.

**Cross-pool release (issue #254).** A credential-fatal park (`401`/`403`, or
a `429` with no usable reset) is copied to every pool sharing the nick when it
is first observed, so clearing it is symmetric: a clear issued against *any*
one of those pools releases it in all of them, not only the pool addressed —
scoping the release to one pool would leave the operator no way to act on the
fact they just corrected. This does not change *selectability*: a nick still
blocked by a polled quota window in a given pool stays unavailable there after
the clear (only the reactive part was ever in scope). Both the pool-level and per-nick responses gain a `released_in` field naming
the sibling pools a clear also freed a nick in, present only when a
propagated park actually released elsewhere. The per-nick response names the
pools directly; the pool-level response maps each released nick to the pools
it was freed in (a whole-pool clear can release several nicks at once):

```json
{"pool":"auto","nick":"ccz","cleared":true,"released_in":["chn","minimax"]}
{"pool":"auto","cleared":["ccz"],"released_in":{"ccz":["chn","minimax"]}}
```

A single-file management page is served at `GET /_gateway/ui`. Open it in a
browser to view every pool, its priority order, the active member, and each
member's live status (`active` / `exhausted` / `disabled` / `idle`), and to
reorder priority, toggle enable/disable, or **clear a single member's live-429
park**. The per-member "Clear park" button appears only on a member the gateway
currently reports `parked: true`; clicking it confirms (it overrides the
gateway's own judgment), clears that one nick, and re-fetches the view. A
"Delete pool" control appears in a pool's header only once that pool is empty
(no members): it calls `DELETE /_gateway/pool/{name}`, confirms, and refreshes
the view — matching the empty-only API, which returns `409` for a pool that
still has members. The per-pool member list self-reconciles with the server on
the status poll (issue #250), so a member added or removed in another tab or
via the API shows up here within one tick without a manual refresh; any text
already typed into the add-subscription form survives the re-render. The page
contains no auth and no build step — it inherits the gateway's trust boundary. In shared mode this exposes
write controls to any tailnet member that can reach the port; a Tailscale
ACL restricting the port is the only gate.

A rolling-window utilization cell (5h or long) renders `-` once its reset has
already elapsed, mirroring what the adjacent reset cell and status badge
already show. Recovery from `exhausted` to `idle` happens by wall-clock; the
quota store keeps the frozen at-cap snapshot until the next real response
rewrites it, so a stale `100%` would otherwise read as live load. The next
request that carries a fresh utilization header repopulates the cell.

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
returns `200` with a `Content-Type` of `application/json`. The response
carries no version, uptime, or upstream reachability check — because the
trust model treats any local process as legitimate. The body is one of
three shapes (issue #246):

| Mode | Body | Meaning |
| --- | --- | --- |
| persisted, clean | `{"status":"ok"}` | everything saved |
| persisted, lagging | `{"status":"ok","unsaved_config_changes":true}` | the on-disk `aqg.json` lags the in-memory config (a pending or failed debounced flush — issue #198 decision 3) |
| env-only | `{"status":"ok","persistence":"env_only"}` | no config file is configured; every runtime mutation is in-memory only and is lost on restart |

The `unsaved_config_changes` field is the operator's signal that runtime
mutations — including credentials — may not yet be persisted; watch for it
and expect it to clear once the flush succeeds. The `persistence` field is
the operator's signal that the gateway is in env-only mode and that
runtime mutations will be lost on the next restart; the body is the
canonical surface for this state, and `GET /_gateway/config` advertises
the same state via the `X-AQG-Persistence` response header (see
"Watching for a lagging flush" above).

When the background poller (issue #247) detects at least one tracked pool
with no successful poll for `StaleAfterIntervals × poll_interval` (default
3 × 2 minutes), the response gains the additive field
`"poller_health":"stale"`. The field is **omitted** when every tracked
pool is healthy — its absence means "ok". A tracked pool that has never
been polled, or that is currently sticky on an untracked backend (Anthropic
or any other provider the poller does not recognise), reads stale the same
way: no fresh out-of-band exhaustion signal has arrived. Anthropic-only
deployments carry no `poller_health` field at all (no tracked pools).

A strict readiness probe should assert on `status` rather than a byte-for-byte
body match. Like `/_gateway/quota`, it is `GET`-only; any other method
returns `405` with an `Allow: GET` response header.

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
- Credentials live in memory and, when a config file is in use, in the
  `aqg.json` config file at `0600` (issue #198 makes it the single source of
  truth, so it is the credential store — the gateway writes and re-reads it).
  On a systemd deploy it lives in the service `StateDirectory`
  (`/var/lib/agent-quota-gateway/aqg.json`) so the ephemeral `DynamicUser` that
  runs the process can rewrite it on UI mutations; it stays `0600`, readable
  only by that service account. In env-only mode (no config file) the gateway
  keeps **zero credentials on disk**. Config views (`/_gateway/config`) always
  redact credentials regardless.
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
and keeps the latest snapshot per backend key (the member nick) in an
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
  "backend": "a",
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

### Snapshot keying

Snapshots are filed under the member nick alone (not `pool/nick`) — one
account shared across multiple pools has a single exhaustion record read by
every pool that selects it (see `QuotaKey()` in `internal/backend` and the
invariant at the top of this file). The read endpoint takes a **pool** name:
it returns the pool's active member with an `active_backend` field naming
the member (see [Reading a pool's quota](#reading-a-pools-quota)).

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
- **The long window is provider-aware — label *and* length.** The members
  table renders the long-window column with a header the gateway supplies
  per pool. For Anthropic, MiniMaxi, and Volcengine Ark it reads "7d" /
  "7d reset"; for Z.ai / Zhipu it reads "monthly" / "monthly reset",
  because the upstream `TIME_LIMIT` entry is the **monthly** quota, not a
  7-day rolling window (the snapshot's `unified_7d_*` fields are still the
  right data shape — only the human label moves). The lead-routing
  elapsed-fraction divides by the matching **length** from the same
  mapping (~30 days for Z.AI/Zhipu, 7 days otherwise; issue #140), so the
  long-window lead reflects the monthly schedule rather than collapsing to
  raw utilization. Any Z.AI
  response that contains only one of `TOKENS_LIMIT` / `TIME_LIMIT` still
  produces a usable snapshot, so an exhausted 5h window no longer hides
  the monthly reset from the operator.
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

Two complementary mechanisms shorten that wait, split by whether the parked
member's store entry is **fresh** or **stale**:

**Store-driven reconciliation (fresh entry).** When the parked member is
still being polled — the sole selectable member of its pool, or otherwise
still the sticky one the poller refreshes — its store entry stays current.
The gateway reconciles the live `429` park against that entry at read time
(in the single live-park ∪ store union both routing and the pool UI consult):
once a **fresh** snapshot (`as_of` within ~5 minutes) shows no blocking
window, the live park is treated as stale and the member becomes selectable
immediately, without waiting for the `429`'s own (possibly overshooting)
reset. This is the fix for a backend whose `429` reset runs past its real
quota window (issue #145: Z.ai's `unified-reset` ~2h52m past its dashboard
5h reset). The freshness gate is load-bearing — an empty or frozen snapshot
never un-parks a member; it falls back to wall-clock aging (below) or the
recovery probe. The reconcile is non-destructive and self-correcting: if a
forwarded request still genuinely `429`s, its blocking headers refresh the
store and the member re-parks.

**Recovery probe (stale entry).** When every member of a pool is parked
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

**Background recovery of parked non-active members.** A plain pool whose
parked nick is no longer the active sticky backend — because a healthy
sibling (possibly added after the park) has taken over — falls outside
both the all-exhausted probe above and the priority preemptor (which
only visits higher-priority members). For that shape the gateway runs a
bounded-cadence background loop that re-checks every parked non-active
probe-eligible member (issue #242):

- **Scope.** Any pool, any parked member whose base URL matches a
  registered proprietary provider (z.ai / MiniMaxi / Ark). The active
  sticky member is skipped — probing it would duplicate the request-path
  all-exhausted recovery and race its in-flight bookkeeping. Anthropic is
  skipped for the same reason as the request-path probe: its `429`s
  carry precise resets and organic traffic refreshes the store.
- **Cadence.** The loop ticks every 5 minutes, matching the preemptor's
  idle fallback interval. The per-member `30s` probe cooldown still
  bounds concurrent overlaps with request-path probes (the two paths
  share one cooldown / coalescing window).
- **Effect on the active member.** None. The loop only clears the live
  park via the same decision as the request-path probe (`snapRejects`
  shared, freshness predicate shared). It never moves the sticky
  pointer — the healthy active member is left alone, matching the
  "without force-switching away from the current healthy active nick"
  requirement. The recovered member rejoins rotation on the next
  selection event.
- **Safety guards.** Identical to the request-path probe: failed probe
  (network error, non-`200`, no provider match) leaves the park intact;
  a probe whose snapshot still satisfies `snapRejects` leaves the park
  intact; a frozen / stale store entry never produces a decision (the
  store-driven reconciliation path above is what covers that case).
- **No-op when healthy.** A pool with no parked non-active member
  performs zero upstream probes per tick — the "healthy pool pays no
  extra probe traffic" property is load-bearing and tested.
- **Store display.** The recovery probe does not `Merge` the recovered
  snapshot into the store (matching the existing `tryRecoverParked`
  contract); a recovered member's `/_gateway/pool` snapshot cell may
  briefly show the previously-frozen data until organic traffic
  refreshes the store. The recovery decision and the displayed
  snapshot are independent signals — the cell is informational, the
  pool's eligibility is what the routing path uses.

## Why a thin proxy

The proxy is the trust boundary — it owns the credentials and resolves a
pool name to a member per request, and its logs are safe to share with any
local tool. Quota observation piggy-backs on the same boundary: rate-limit
headers come down on every response, so we capture them per backend with
zero extra upstream load.

## License

`agent-quota-gateway` is **source-available** under the
[Business Source License 1.1](LICENSE) (BSL 1.1), copyright SHUKE LABS LTD.

You may read, modify, and run it — including in production — with one
carve-out: you may not offer the gateway (or a substantially similar work) to
third parties as a hosted or managed commercial service that competes with a
paid SHUKE LABS offering. Running your own instance for your own workloads,
at home or at work, is exactly the use it is built for and is permitted.

Each released version converts to the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
four years after its release date. See [`LICENSE`](LICENSE) for the
authoritative terms.
