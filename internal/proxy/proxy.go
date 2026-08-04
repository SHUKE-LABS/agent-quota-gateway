// Package proxy implements the reverse proxy.
//
// The proxy is intentionally thin: it forwards every request through Go's
// standard httputil.ReverseProxy, derives the upstream and stamps the
// auth header from the backend the resolver middleware stored on the
// request context, and disables response buffering so server-sent events
// stream as they arrive. Nothing about request or response bodies is
// inspected. Neither paths nor methods are whitelisted — any method on
// any path reaches the upstream, which is the authority on what it
// serves. The loopback-only bind (enforced at config load) plus the
// resolver's selector check are the security boundary, not a route table.
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
	"github.com/shukebeta/agent-quota-gateway/internal/reqlog"
)

// ResponseObserver is invoked once per successful upstream round-trip,
// after the response status and headers are known and before the proxy
// writes the response back to the client. The body must not be read;
// doing so would race with the proxy's own copy loop and break
// streaming. nil is a valid value and disables the hook.
type ResponseObserver func(*http.Response)

// ResponseModifier runs in ModifyResponse after the observer and may
// mutate the response (status, headers, body) before the proxy streams
// it to the client — the supported httputil.ReverseProxy mechanism for
// rewriting a response. It is the pool response hook: an upstream 429
// becomes a 503 (switchable) or a Retry-After 429 (pool dry), and a native
// Anthropic 529 overload may become a same-member 503 with a fixed retry
// delay. A returned error surfaces as a 502, so implementations should return
// nil for the pass-through case. nil disables the hook.
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
		basePath := strings.TrimRight(upstream.Path, "/")
		reqPath := r.URL.Path
		// Normalize the version segment independently of the payload.
		// Root-mounted upstreams receive exactly one leading /v1; a base
		// URL ending in /v1 consumes inbound /v1.
		reqPath = normalizeRequestPath(basePath, reqPath)
		r.URL.Path = joinPath(basePath, reqPath)

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
	base := &http.Transport{
		IdleConnTimeout:    90 * time.Second,
		DisableCompression: true,
	}
	rp.Transport = reqlog.WrapTransport(base)

	return rp, nil
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

// normalizeLeadingV1 rewrites an inbound request path so it carries
// exactly one leading "/v1" segment. It runs only for root-mounted
// upstreams — the native Anthropic surface and every root-mounted
// Anthropic-compat vendor (Z.ai, MiniMax, Volcengine, …), all of which
// serve the V1 API from the host root.
//
// Clients disagree on who owns the "/v1" prefix when a reverse proxy
// sits in front of an API that already lives at /v1/*:
//   - Claude Code points its base URL at the gateway root and sends
//     /v1/messages (untouched here);
//   - OpenCode / Codex and SDKs that hardcode baseURL/v1 require the
//     operator to set the base URL with a /v1 suffix, so the prefix is
//     consumed there and the request arrives as /messages;
//   - a few SDKs add their own /v1 on top, producing /v1/v1/messages.
//
// Collapsing any run of leading /v1 segments to one — and prepending
// /v1 when none is present — makes all three reach the upstream's /v1
// surface (issue #157). The match is segment-aware: "/v1" counts only
// when followed by "/" or end-of-path, so "/v1beta/x" is a bare subpath
// (→ /v1/v1beta/x), not a version segment to collapse.
func normalizeLeadingV1(path string) string {
	rest := path
	for {
		if rest == "/v1" {
			rest = ""
			break
		}
		if strings.HasPrefix(rest, "/v1/") {
			rest = rest[len("/v1"):]
			continue
		}
		break
	}
	return "/v1" + rest
}

// normalizeRequestPath applies the version-prefix rules at the generic HTTP
// boundary. Root-mounted upstreams receive exactly one leading /v1 segment.
// When the configured base path already ends in /v1, inbound leading /v1
// segments are removed before joining, so /responses and /v1/responses agree.
// Other non-root prefixes retain their existing join behavior.
func normalizeRequestPath(basePath, path string) string {
	if basePath == "" {
		return normalizeLeadingV1(path)
	}
	trimmedBase := strings.TrimRight(basePath, "/")
	if !strings.HasSuffix(trimmedBase, "/v1") {
		return path
	}
	return trimLeadingV1(path)
}

func trimLeadingV1(path string) string {
	rest := path
	for {
		if rest == "/v1" {
			return "/"
		}
		if strings.HasPrefix(rest, "/v1/") {
			rest = rest[len("/v1"):]
			continue
		}
		return rest
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
