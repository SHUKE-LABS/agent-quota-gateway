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

Out of scope for V1:

- Quota tracking, rate limiting, cost snapshots
- Non-Anthropic providers
- TLS termination (front it with a reverse proxy or `stunnel` if
  you need it)
- Request/response modification, caching, retries
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
- `internal/logging/` — middleware and tests
- `test/` — reserved for end-to-end coverage

## Why a thin proxy

V1 is deliberately a pass-through. The proxy exists to be the trust
boundary — it owns the API key, and its logs are safe to share with
any local tool. Future versions will add quota capture and rate
windows on top of this same boundary; this README will be updated
when those land.