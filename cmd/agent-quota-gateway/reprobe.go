package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/codexhello"
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

const (
	// reprobeTimeout bounds one operator reprobe query (issue #350). It
	// matches the Codex hello's request timeout: the query is a real model
	// call for Anthropic and Codex, not a sub-second quota endpoint.
	reprobeTimeout = 30 * time.Second

	// reprobeMessagesBody is the minimal Anthropic Messages request a reprobe
	// sends. Unified rate-limit headers arrive only on real responses — there
	// is no Anthropic quota endpoint — so one max_tokens:1 Haiku call is the
	// cheapest way to read fresh quota truth. The model is the README
	// smoke-test model, which every subscription and API key can call.
	reprobeMessagesBody = `{"model":"claude-haiku-4-5-20251001","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}`
)

// Reprobe query failures, mapped by reprobeHandler to the 502 error body.
var (
	errReprobeCredentialRejected = errors.New("credential rejected")
	errReprobeNoQuotaSignal      = errors.New("no quota signal")
	errReprobeUnreachable        = errors.New("upstream unreachable")
)

// newReprobeClient returns the HTTP client operator reprobes use. Redirects
// are not followed, the same policy as the Codex hello: a credential-bearing
// request must never be replayed to a host the operator did not configure.
func newReprobeClient() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

// reprobeQuery builds the per-class upstream quota query behind the operator
// reprobe (issue #350). There is no single quota-fetch path in the gateway,
// so it dispatches on the backend:
//
//   - a poller-registered provider (z.ai/zhipu, MiniMaxi, Ark) uses
//     poller.Probe, the same call the automatic recovery probe makes;
//   - a ChatGPT-Codex member sends the Codex hello request and reads its
//     x-codex-* windows, overlaid on the stored snapshot exactly as the hello
//     does;
//   - anything else (Anthropic, untracked providers) sends one minimal
//     Messages call and reads the unified rate-limit headers.
//
// Headers are extracted regardless of status: a fresh 429 carries the
// current rejected window and its reset, which is a successful reprobe that
// confirms exhaustion. Only 401/403 is classified as a rejected credential.
// A response with no quota window is an error, never an empty snapshot, so
// it cannot overwrite the last known-good one (issue #121). Response bodies
// are drained and discarded, never logged.
func reprobeQuery(store *quota.Store, client *http.Client, now func() time.Time) auto.ReprobeQuery {
	return func(parent context.Context, b backend.Backend) (quota.Snapshot, error) {
		ctx, cancel := context.WithTimeout(parent, reprobeTimeout)
		defer cancel()

		if _, ok := poller.ProviderFor(b.BaseURL); ok {
			snap, err := poller.Probe(ctx, b, client, now)
			var se *poller.StatusError
			if errors.As(err, &se) && rejectsCredential(se.Code) {
				return quota.Snapshot{}, errReprobeCredentialRejected
			}
			if err != nil {
				return quota.Snapshot{}, fmt.Errorf("%w: %v", errReprobeUnreachable, err)
			}
			if !hasQuotaWindow(snap) {
				return quota.Snapshot{}, errReprobeNoQuotaSignal
			}
			return snap, nil
		}

		codex := auto.IsCodexBackend(b)
		var req *http.Request
		var err error
		if codex {
			req, err = codexhello.NewRequest(ctx, b)
		} else {
			req, err = messagesRequest(ctx, b)
		}
		if err != nil {
			return quota.Snapshot{}, fmt.Errorf("%w: %v", errReprobeUnreachable, err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return quota.Snapshot{}, fmt.Errorf("%w: request failed", errReprobeUnreachable)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		if rejectsCredential(resp.StatusCode) {
			return quota.Snapshot{}, errReprobeCredentialRejected
		}

		if codex {
			// Same admission as the hello: only quota-bearing x-codex-*
			// headers, so an empty 2xx cannot advance AsOf over the old
			// snapshot.
			observedAt := now().UTC()
			fresh := quota.ExtractCodex(resp.Header, observedAt)
			if !fresh.HasData() {
				return quota.Snapshot{}, errReprobeNoQuotaSignal
			}
			snap := quota.Snapshot{AsOf: observedAt}
			snap.OverlayCodex(store.Get(b.QuotaKey()), fresh)
			return snap, nil
		}
		snap := quota.Extract(resp)
		if !hasQuotaWindow(snap) {
			return quota.Snapshot{}, errReprobeNoQuotaSignal
		}
		return snap, nil
	}
}

func rejectsCredential(status int) bool {
	return status == http.StatusUnauthorized || status == http.StatusForbidden
}

// messagesRequest builds the reprobe's Messages call for b: POST
// {base}/v1/messages, or {base}/messages when the base already ends in /v1.
// A path-mounted base keeps its prefix, matching what the proxy forwards for
// Claude Code's /v1/messages.
func messagesRequest(ctx context.Context, b backend.Backend) (*http.Request, error) {
	u, err := url.Parse(b.BaseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid upstream URL")
	}
	path := strings.TrimRight(u.EscapedPath(), "/")
	if strings.HasSuffix(path, "/v1") {
		path += "/messages"
	} else {
		path += "/v1/messages"
	}
	u.Path, err = url.PathUnescape(path)
	if err != nil {
		return nil, fmt.Errorf("invalid upstream URL path")
	}
	u.RawPath = path
	u.RawQuery = ""
	u.Fragment = ""
	u.RawFragment = ""
	u.User = nil
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewBufferString(reprobeMessagesBody))
	if err != nil {
		return nil, fmt.Errorf("request construction failed")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-version", "2023-06-01")
	proxy.StampAuth(req.Header, b.Credential)
	return req, nil
}

// reprobeHandler serves POST /_gateway/pool/{name}/member/{nick}/reprobe —
// the operator's "Reset & re-probe" (issue #350). It queries upstream once for
// the member, merges the fresh snapshot, reconciles the member's live park,
// and reports whether the member is healthy now. Registered without a method
// in the pattern so a non-POST gets 405 here instead of falling through to
// the proxy catch-all.
//
// 200 carries the merged snapshot's window fields; healthy:false is a
// successful reprobe that confirmed exhaustion. A failed query is 502 with
// the store and parks untouched: "credential rejected" (401/403), "no quota
// signal", or "upstream unreachable".
func reprobeHandler(pools *auto.Pools, query auto.ReprobeQuery) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		res, status, err := pools.Reprobe(r.Context(), r.PathValue("name"), r.PathValue("nick"), query)
		if err != nil {
			msg := err.Error()
			if errors.Is(err, auto.ErrReprobeQuery) {
				switch {
				case errors.Is(err, errReprobeCredentialRejected):
					msg = errReprobeCredentialRejected.Error()
				case errors.Is(err, errReprobeNoQuotaSignal):
					msg = errReprobeNoQuotaSignal.Error()
				default:
					msg = errReprobeUnreachable.Error()
				}
			}
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
			return
		}
		snap := res.Snapshot
		_ = json.NewEncoder(w).Encode(map[string]any{
			"pool":                   res.Pool,
			"nick":                   res.Nick,
			"healthy":                res.Healthy,
			"unified_status":         snap.UnifiedStatus,
			"unified_5h_utilization": snap.Unified5hUtilization,
			"unified_7d_utilization": snap.Unified7dUtilization,
			"unified_5h_reset":       snap.Unified5hReset,
			"unified_7d_reset":       snap.Unified7dReset,
		})
	}
}
