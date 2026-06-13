// Package proxy implements the Anthropic Messages reverse proxy.
//
// The proxy is intentionally thin: it forwards every POST through Go's
// standard httputil.ReverseProxy, derives the upstream and stamps the
// auth header from the backend the resolver middleware stored on the
// request context, and disables response buffering so server-sent events
// stream as they arrive. Nothing about request or response bodies is
// inspected. Paths are not whitelisted — any path reaches the upstream,
// which is the authority on what it serves. The loopback-only bind
// (enforced at config load) is the security boundary, not a route table.
//
// Each backend carries its own upstream URL, so one proxy serves every
// pool: the director reads the resolved backend per request rather than
// a single construction-time upstream. A credential and its upstream
// always travel together on the context, so one pool's credential can
// never be sent to another pool's host.
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
// rewriting a response. It is the pool failover hook: an upstream 429
// becomes a 503 (switchable) or a Retry-After 429 (pool dry). A returned
// error surfaces as a 502, so implementations should return nil for the
// pass-through case. nil disables the hook.
type ResponseModifier func(*http.Response) error

// New builds the proxy http.Handler.
//
// The upstream and credential are not configured here: they are resolved
// per request from the backend that the resolver middleware stored on the
// request context, so one proxy serves every configured pool. The auth
// scheme is chosen from the credential's class:
//
//   - sk-ant-oat* (OAuth / Claude Code subscription) → "Authorization:
//     Bearer <token>" with the oauth-2025-04-20 beta flag;
//   - sk-ant-api* (Anthropic API key) → the x-api-key header;
//   - anything else (non-native Claude-compatible) → "Authorization:
//     Bearer <token>" without the beta flag.
//
// observer, if non-nil, is invoked once per upstream response for
// header-only inspection (see ResponseObserver). modifier, if non-nil,
// runs after the observer and may rewrite the response (see
// ResponseModifier).
func New(observer ResponseObserver, modifier ResponseModifier) (http.Handler, error) {
	rp := &httputil.ReverseProxy{}

	// The director re-derives scheme/host/path from the resolved
	// backend's own upstream URL so a malicious inbound Host header cannot
	// redirect traffic, and stamps the backend's credential so the inbound
	// selector is always replaced before it goes upstream.
	rp.Director = func(r *http.Request) {
		b, ok := backend.FromContext(r.Context())
		if !ok {
			// The resolver middleware guarantees a backend before the
			// proxy runs; if it is somehow absent, fail safe by stripping
			// every inbound credential so neither the selector nor a stray
			// client key reaches an upstream.
			stripInboundCredentials(r.Header)
			return
		}

		upstream, err := url.Parse(b.BaseURL)
		if err != nil || upstream.Scheme == "" || upstream.Host == "" {
			// BaseURL was validated at load, so this is defensive only.
			// Leave the request without a host (it will fail) and strip
			// credentials rather than forward to an unknown destination.
			stripInboundCredentials(r.Header)
			return
		}

		r.URL.Scheme = upstream.Scheme
		r.URL.Host = upstream.Host
		r.Host = upstream.Host
		r.URL.Path = joinPath(strings.TrimRight(upstream.Path, "/"), r.URL.Path)

		stampAuth(r.Header, b.Credential)
	}

	// ModifyResponse runs after headers are received but before the body
	// copy starts. The observer inspects headers only (its body-read
	// caveat still holds — reading the streaming body here would race the
	// copy loop). The modifier runs next and is the one hook allowed to
	// rewrite the response. The observer is best-effort (returns nothing);
	// only the modifier can surface an error, which becomes a 502.
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
	// accumulating in the response writer's buffer while a slow upstream
	// finishes the full payload. Without this, a streaming /v1/messages
	// response can be held until the upstream completes the entire
	// stream, which breaks Claude Code's incremental UI.
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

// stampAuth replaces any inbound credential with the resolved backend's,
// choosing the scheme from the credential's class. See New for the
// three-way mapping.
func stampAuth(h http.Header, credential string) {
	cred := strings.TrimSpace(credential)
	switch {
	case isOAuthToken(cred):
		// OAuth tokens authenticate over Bearer, not x-api-key, and
		// require the beta opt-in.
		h.Del("x-api-key")
		h.Set("Authorization", "Bearer "+cred)
		ensureBeta(h, oauthBeta)
	case isAPIKey(cred):
		// Anthropic API keys use x-api-key; drop the inbound selector that
		// arrived on Authorization so it never goes upstream.
		h.Del("Authorization")
		h.Set("x-api-key", cred)
	default:
		// Non-native Claude-compatible providers authenticate over Bearer
		// without the Anthropic beta flag.
		h.Del("x-api-key")
		h.Set("Authorization", "Bearer "+cred)
	}
}

// stripInboundCredentials removes any client-supplied credential headers,
// used on the defensive path where no backend resolved.
func stripInboundCredentials(h http.Header) {
	h.Del("Authorization")
	h.Del("x-api-key")
}

// joinPath concatenates a base path with a request path, ensuring
// exactly one slash between them and a leading slash on the result. For
// the Anthropic V1 surface, basePath is empty (the upstream lives at the
// root), but a non-native pool whose base URL carries a path prefix
// (e.g. https://host/anthropic) needs the join to preserve that prefix.
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

// isOAuthToken reports whether key is a Claude Code OAuth token. OAuth
// tokens use the stable sk-ant-oat prefix and must be sent as a Bearer
// credential with the beta flag.
func isOAuthToken(key string) bool {
	return strings.HasPrefix(key, "sk-ant-oat")
}

// isAPIKey reports whether key is an Anthropic API key (sk-ant-api…),
// which authenticates over the x-api-key header.
func isAPIKey(key string) bool {
	return strings.HasPrefix(key, "sk-ant-api")
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
