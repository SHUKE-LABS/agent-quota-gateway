# agent-quota-gateway

Loopback-only reverse proxy for the Anthropic Messages API, sized for
local Claude Code workflows.

## What it is

A single-binary Go server that listens on `127.0.0.1` and forwards
`/v1/messages` and `/v1/messages/count_tokens` to
`https://api.anthropic.com`, preserving streaming and the
`anthropic-*` headers Claude Code sends. The gateway owns the
`ANTHROPIC_API_KEY` so client processes never see the upstream
credential directly.

## V1 scope

- Anthropic-only. No OpenAI / Google / other providers.
- The Messages surface only: `POST /v1/messages` and
  `POST /v1/messages/count_tokens`.
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
| `ANTHROPIC_API_KEY`   | _(required)_                  | Forwarded as `x-api-key` on every request.           |
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

- `cmd/agent-quota-gateway/` — service entrypoint
- `internal/config/` — env loading and validation
- `internal/proxy/` — reverse-proxy handler and tests
- `internal/quota/` — rate-limit header extraction and snapshot store
- `internal/logging/` — middleware and tests
- `test/` — reserved for end-to-end coverage

## Quota snapshots

The gateway watches the `anthropic-ratelimit-*` and
`anthropic-organization-id` response headers on every forwarded
request and keeps the latest snapshot per backend key in an
in-process cache. Reads go through a small loopback endpoint:

```bash
curl http://127.0.0.1:8080/_gateway/quota?backend=mybackend
```

Response shape (all rate-limit fields are optional — they are
omitted when the upstream response did not carry the corresponding
header):

```json
{
  "backend": "mybackend",
  "requests_limit": 1000,
  "requests_remaining": 997,
  "requests_reset": "2026-06-13T13:45:00Z",
  "tokens_limit": 80000,
  "tokens_remaining": 79412,
  "tokens_reset": "2026-06-13T13:45:30Z",
  "org_id": "org_abc123",
  "as_of": "2026-06-13T13:42:11.038Z"
}
```

`as_of` is the gateway-side time the snapshot was recorded; the
`*_reset` fields are absolute upstream timestamps.

### Backend keying

Clients identify the backend a request is bound for by setting the
`X-Mux-Backend-Nick` header on the inbound request. The proxy
forwards it to Anthropic untouched, and the observer files the
response snapshot under that key. When the header is absent or
empty, the snapshot is filed under `default` and `GET
/_gateway/quota` (no query) returns it.

`GET /_gateway/quota?backend=unknown` always returns 200; if no
traffic for that key has been seen, the body is just `{"backend":
"unknown", "as_of": "..."}`. Use the presence of `tokens_limit` /
`requests_limit` to decide whether quota data is actually available.

### Freshness model

Snapshots only update when real traffic flows. The gateway never
issues synthetic probe requests — if no client has hit the backend
recently, the snapshot is stale by exactly that gap. Consumers that
need fresh data should issue (or wait for) a real request.

## Why a thin proxy

The proxy is the trust boundary — it owns the API key, and its
logs are safe to share with any local tool. Quota observation
piggy-backs on the same boundary: rate-limit headers come down on
every response, so we capture them with zero extra upstream load.