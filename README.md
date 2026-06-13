# agent-quota-gateway

Loopback-only reverse proxy for the Anthropic Messages API, sized for
local Claude Code workflows.

## What it is

A single-binary Go server that listens on `127.0.0.1` and forwards
any `POST` to `https://api.anthropic.com` (Claude Code uses
`/v1/messages` and `/v1/messages/count_tokens`), preserving streaming
and the `anthropic-*` headers Claude Code sends. The gateway owns the
`ANTHROPIC_API_KEY` so client processes never see the upstream
credential directly.

## V1 scope

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
- Quota snapshots are captured passively from upstream rate-limit
  headers and exposed at `GET /_gateway/quota`. No synthetic probe
  requests — freshness depends on real client traffic.

Out of scope for V1:

- Non-Anthropic providers
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

ANTHROPIC_API_KEY=sk-ant-... \
  ./agent-quota-gateway
```

`ANTHROPIC_API_KEY` accepts either a Claude Code OAuth token
(`sk-ant-oat…`) or a plain API key (`sk-ant-api…`); the gateway picks
the matching auth scheme automatically. Metering quota on OAuth tokens
is the gateway's primary use — those carry the rate limits worth
watching.

The gateway listens on `127.0.0.1:8080` by default. Point Claude Code
at it:

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 \
ANTHROPIC_API_KEY=any-non-empty-value \
claude
```

Claude Code requires the env var to be present even though the
gateway supplies the real key. Any non-empty placeholder is fine.

## Environment variables

| Variable              | Default                       | Notes                                                |
|-----------------------|-------------------------------|------------------------------------------------------|
| `ANTHROPIC_BASE_URL`  | `https://api.anthropic.com`   | Upstream base URL; scheme and host are required.     |
| `ANTHROPIC_API_KEY`   | _(required)_                  | Upstream credential. An OAuth token (`sk-ant-oat…`) is sent as `Authorization: Bearer` with the `oauth-2025-04-20` beta flag; any other key is sent as `x-api-key`. |
| `LISTEN_ADDR`         | `127.0.0.1:8080`              | Loopback address only; the V1 build refuses anything else. |

## Smoke test

With the gateway running on `127.0.0.1:8080`:

```bash
curl -N -X POST http://127.0.0.1:8080/v1/messages \
  -H 'Content-Type: application/json' \
  -H 'anthropic-version: 2023-06-01' \
  -d '{"model":"claude-haiku-4-5-20251001","max_tokens":16,"messages":[{"role":"user","content":"say hi"}]}'
```

You should see streaming SSE events back. The `-N` flag is required so
`curl` does not buffer the response itself.

## Layout

- `cmd/agent-quota-gateway/` — service entrypoint and integration tests
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

- The gateway owns the API key. Clients never see it — they may set
  any placeholder in `ANTHROPIC_API_KEY` and the proxy replaces it
  with the configured value on every outbound request.
- Request and response bodies are not logged, persisted, or inspected.
  The logging middleware records only `method`, `path`, `status`,
  `duration`, and a request ID.
- The client-supplied `x-api-key` is replaced with the configured key
  before the upstream call; all other headers forwarded to the
  upstream are otherwise untouched. No credential-sensitive field is
  written to stderr.
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
curl http://127.0.0.1:8080/_gateway/quota?backend=mybackend
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
  "backend": "mybackend",
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

Clients identify the backend a request is bound for by setting the
`X-Mux-Backend-Nick` header on the inbound request. The proxy
forwards it to Anthropic untouched, and the observer files the
response snapshot under that key. When the header is absent or
empty, the snapshot is filed under `default` and `GET
/_gateway/quota` (no query) returns it.

`GET /_gateway/quota?backend=unknown` always returns 200; if no
traffic for that key has been seen, the body is just `{"backend":
"unknown", "as_of": "..."}`. Use the presence of a `unified_*` field
(e.g. `unified_5h_utilization`) to decide whether quota data is
actually available.

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

The proxy is the trust boundary — it owns the API key, and its
logs are safe to share with any local tool. Quota observation
piggy-backs on the same boundary: rate-limit headers come down on
every response, so we capture them with zero extra upstream load.
See [Security model](#security-model) for the full list of guarantees.