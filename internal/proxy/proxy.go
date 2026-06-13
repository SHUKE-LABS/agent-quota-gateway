// Package proxy implements the Anthropic Messages reverse proxy.
//
// The proxy is intentionally thin: it forwards every POST through Go's
// standard httputil.ReverseProxy, sets the upstream auth header from
// config, and disables response buffering so server-sent events stream
// as they arrive. Nothing about request or response bodies is
// inspected. Paths are not whitelisted — any path reaches the upstream,
// which is the authority on what it serves. The loopback-only bind
// (enforced at config load) is the security boundary, not a route table.
//
// An optional response observer hook lets the caller inspect each
// upstream *http.Response (headers only, post-roundtrip) without
// touching the body or interfering with streaming. The proxy itself
// stays header-agnostic; quota capture lives in package quota.
package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// allowedMethods is the HTTP method set the proxy forwards. Anthropic's
// API surface is POST-only; a GET reaching the upstream with the API key
// attached is unnecessary exposure, so non-POST requests are rejected
// here with 405 before any upstream round-trip.
var allowedMethods = map[string]bool{
	http.MethodPost: true,
}

// ResponseObserver is invoked once per successful upstream round-trip,
// after the response status and headers are known and before the proxy
// writes the response back to the client. The body must not be read;
// doing so would race with the proxy's own copy loop and break
// streaming. nil is a valid value and disables the hook.
type ResponseObserver func(*http.Response)

// ResponseModifier runs in ModifyResponse after the observer and may
// mutate the response (status, headers, body) before the proxy streams
// it to the client — the supported httputil.ReverseProxy mechanism for
// rewriting a response. It is the auto selector's failover hook: an
// upstream 429 on an auto-routed request becomes a 503 (switchable) or a
// Retry-After 429 (pool dry). A returned error surfaces as a 502, so
// implementations should return nil for the pass-through case. nil
// disables the hook.
type ResponseModifier func(*http.Response) error

// New builds the proxy http.Handler.
//
// baseURL must be a fully qualified upstream URL (e.g.
// "https://api.anthropic.com"). The credential is not configured here:
// it is resolved per request from the backend that the resolver
// middleware stored on the request context, so one proxy serves every
// configured backend. The auth scheme is chosen from that credential's
// class — an OAuth token (prefix sk-ant-oat) is sent as
// "Authorization: Bearer <token>" with the oauth-2025-04-20 beta flag,
// while any other key is sent as the x-api-key header. observer, if
// non-nil, is invoked once per upstream response for header-only
// inspection (see ResponseObserver). modifier, if non-nil, runs after the
// observer and may rewrite the response (see ResponseModifier).
func New(baseURL string, observer ResponseObserver, modifier ResponseModifier) (http.Handler, error) {
	upstream, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	rp := httputil.NewSingleHostReverseProxy(upstream)

	// Replace the default director entirely. NewSingleHostReverseProxy's
	// default director parses the upstream URL on every request and then
	// joins paths in a way that makes the dataflow hard to audit. We do
	// the same job inline — re-derive scheme/host/path from the
	// configured upstream so a malicious inbound Host header cannot
	// redirect traffic, and stamp the credential of the resolved backend
	// so the inbound selector is always replaced before it goes upstream.
	basePath := strings.TrimRight(upstream.Path, "/")
	rp.Director = func(r *http.Request) {
		r.URL.Scheme = upstream.Scheme
		r.URL.Host = upstream.Host
		r.Host = upstream.Host
		r.URL.Path = joinPath(basePath, r.URL.Path)

		b, ok := backend.FromContext(r.Context())
		if !ok {
			// The resolver middleware guarantees a backend before the
			// proxy runs; if it is somehow absent, fail safe by
			// stripping every inbound credential so neither the selector
			// nor a stray client key reaches the upstream.
			r.Header.Del("Authorization")
			r.Header.Del("x-api-key")
			return
		}

		cred := strings.TrimSpace(b.Credential)
		if isOAuthToken(cred) {
			// OAuth tokens authenticate over Bearer, not x-api-key.
			r.Header.Del("x-api-key")
			r.Header.Set("Authorization", "Bearer "+cred)
			ensureBeta(r.Header, oauthBeta)
		} else {
			// API keys use x-api-key; drop the inbound selector that
			// arrived on Authorization so it never goes upstream.
			r.Header.Del("Authorization")
			r.Header.Set("x-api-key", cred)
		}
	}

	// ModifyResponse runs after headers are received but before the body
	// copy starts. The observer inspects headers only (its body-read
	// caveat still holds — reading the streaming body here would race the
	// copy loop). The modifier runs next and is the one hook allowed to
	// rewrite the response, which ModifyResponse-time mutation supports
	// because the proxy streams whatever response object survives this
	// callback. The observer is best-effort (returns nothing); only the
	// modifier can surface an error, which becomes a 502.
	if observer != nil || modifier != nil {
		rp.ModifyResponse = func(resp *http.Response) error {
			if observer != nil {
				observer(resp)
			}
			if modifier != nil {
				return modifier(resp)
			}
			return nil
		}
	}

	// A negative FlushInterval means "flush immediately after each
	// Write". This is the documented way to keep SSE frames from
	// accumulating in the response writer's buffer while a slow
	// upstream finishes the full payload. Without this, a streaming
	// /v1/messages response can be held until the upstream completes
	// the entire stream, which breaks Claude Code's incremental UI.
	rp.FlushInterval = -1

	// Transport tuning: keep the upstream connection pool warm but cap
	// per-request idle time so a hung upstream does not pin goroutines
	// forever. These values match the standard library defaults, made
	// explicit so the SSE behavior is auditable.
	rp.Transport = &http.Transport{
		IdleConnTimeout:    90 * time.Second,
		DisableCompression: true,
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !allowedMethods[r.Method] {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		rp.ServeHTTP(w, r)
	}), nil
}

// joinPath concatenates a base path with a request path, ensuring
// exactly one slash between them and a leading slash on the result.
// For the Anthropic V1 surface, basePath is always empty (the upstream
// lives at the root), but the function preserves correct behavior if
// future versions point at a subpath.
func joinPath(basePath, requestPath string) string {
	base := strings.TrimRight(basePath, "/")
	req := strings.TrimLeft(requestPath, "/")
	switch {
	case base == "" && req == "":
		return "/"
	case base == "":
		return "/" + req
	case req == "":
		return base
	default:
		return base + "/" + req
	}
}

// oauthBeta is the anthropic-beta opt-in Anthropic requires for OAuth
// (Claude Code subscription) tokens. Without it, a Bearer-authenticated
// request is rejected.
const oauthBeta = "oauth-2025-04-20"

// isOAuthToken reports whether key is a Claude Code OAuth token rather
// than a plain API key. OAuth tokens use the stable sk-ant-oat prefix
// and must be sent as a Bearer credential; API keys (sk-ant-api…) use
// the x-api-key header.
func isOAuthToken(key string) bool {
	return strings.HasPrefix(strings.TrimSpace(key), "sk-ant-oat")
}

// ensureBeta adds value to the comma-separated anthropic-beta header if
// it is not already present, preserving any client-supplied flags. The
// header is the documented place for opt-in features, and Anthropic
// tolerates multiple values; we dedup only to keep the header tidy.
func ensureBeta(h http.Header, value string) {
	existing := h.Get("anthropic-beta")
	if existing == "" {
		h.Set("anthropic-beta", value)
		return
	}
	for _, v := range strings.Split(existing, ",") {
		if strings.TrimSpace(v) == value {
			return
		}
	}
	h.Set("anthropic-beta", existing+","+value)
}
