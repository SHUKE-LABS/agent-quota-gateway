# agent-quota-gateway

Thin local gateway for quota-aware AI clients.

## Purpose

`agent-quota-gateway` is a companion service for local agent workflows that need to:

- forward provider traffic without exposing provider credentials to the client process
- observe real quota / rate-limit headers from normal inference requests
- cache a local quota snapshot for sidecar tools such as statuslines and CLIs
- stay thin enough to run on a developer workstation

## V1 scope

The first provider target is Anthropic / Claude Code.

The first client integration target is `my-ai-team`, where native Claude backends can optionally route through this gateway when a dedicated gateway base URL is configured.

V1 should focus on:

- transparent forwarding for the Anthropic Messages API surface used by Claude Code
- preserving streaming behavior
- extracting and caching rate-limit / quota metadata from normal upstream responses
- exposing a small local read interface for quota consumers
- avoiding synthetic probe requests whose only purpose is quota observation

## Non-goals for V1

- multi-provider abstraction before the Anthropic path is solid
- prompt/body analytics or broad observability features
- centralized enterprise governance
- replacing full API gateways such as Aperture

## Initial layout

The repository is intentionally small at bootstrap time.

- `cmd/agent-quota-gateway/` — service entrypoint
- `internal/` — provider-specific forwarding and quota cache logic
- `test/` — integration and regression coverage

## Current state

This repository has only the initial scaffold.

The implementation contract currently lives in:

- `my-ai-team` issue `shukebeta/my-ai-team#588`

