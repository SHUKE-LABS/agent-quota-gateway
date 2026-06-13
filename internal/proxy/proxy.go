// Package proxy implements the Anthropic Messages reverse proxy.
//
// The proxy is intentionally thin: it routes the V1 surface
// (/v1/messages and /v1/messages/count_tokens) through Go's standard
// httputil.ReverseProxy, sets the upstream auth header from config, and
// disables response buffering so server-sent events stream as they
// arrive. Nothing about request or response bodies is inspected.
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
)

// allowedPaths is the closed route table for V1. Requests outside this
// set receive 404 — we refuse to be an open relay for arbitrary upstream
// paths, both to keep the surface auditable and to avoid leaking the
// API key through routes the operator did not intend to expose.
var allowedPaths = map[string]bool{
	"/v1/messages":              true,
	"/v1/messages/count_tokens": true,
}

// allowedMethods is the HTTP method set V1 accepts on the routed paths.
// Anthropic's Messages surface is POST-only.
var allowedMethods = map[string]bool{
	http.MethodPost: true,
}

// ResponseObserver is invoked once per successful upstream round-trip,
// after the response status and headers are known and before the proxy
// writes the response back to the client. The body must not be read;
// doing so would race with the proxy's own copy loop and break
// streaming. nil is a valid value and disables the hook.
type ResponseObserver func(*http.Response)

// New builds the proxy http.Handler.
//
// baseURL must be a fully qualified upstream URL (e.g.
// "https://api.anthropic.com"). apiKey is forwarded as the x-api-key
// header on every request. observer, if non-nil, is invoked once per
// upstream response for header-only inspection (see ResponseObserver).
func New(baseURL, apiKey string, observer ResponseObserver) (http.Handler, error) {
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
	// redirect traffic, and stamp the auth header from the gateway's
	// own config so client-supplied x-api-key is replaced.
	basePath := strings.TrimRight(upstream.Path, "/")
	rp.Director = func(r *http.Request) {
		r.URL.Scheme = upstream.Scheme
		r.URL.Host = upstream.Host
		r.Host = upstream.Host
		r.URL.Path = joinPath(basePath, r.URL.Path)
		r.Header.Set("x-api-key", apiKey)
	}

	// ModifyResponse runs after headers are received but before the
	// body copy starts. Touching the body here would race with the
	// proxy's own streaming copy, so we hand the response (headers,
	// status, and resp.Request) to the observer and return nil.
	// Returning an error here would surface as a 502; the observer is
	// best-effort and must not break the request.
	if observer != nil {
		rp.ModifyResponse = func(resp *http.Response) error {
			observer(resp)
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
		if !allowedPaths[r.URL.Path] {
			http.NotFound(w, r)
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
