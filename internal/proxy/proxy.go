// Package proxy implements the Anthropic Messages reverse proxy.
//
// The proxy is intentionally thin: it routes the V1 surface
// (/v1/messages and /v1/messages/count_tokens) through Go's standard
// httputil.ReverseProxy, sets the upstream auth header from config, and
// disables response buffering so server-sent events stream as they
// arrive. Nothing about request or response bodies is inspected.
package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

// allowedPaths is the closed route table for V1. Requests outside this
// set receive 404 — we refuse to be an open relay for arbitrary upstream
// paths, both to keep the surface auditable and to avoid leaking the
// API key through routes the operator did not intend to expose.
var allowedPaths = map[string]bool{
	"/v1/messages":             true,
	"/v1/messages/count_tokens": true,
}

// allowedMethods is the HTTP method set V1 accepts on the routed paths.
// Anthropic's Messages surface is POST-only.
var allowedMethods = map[string]bool{
	http.MethodPost: true,
}

// New builds the proxy http.Handler.
//
// baseURL must be a fully qualified upstream URL (e.g.
// "https://api.anthropic.com"). apiKey is forwarded as the x-api-key
// header on every request.
func New(baseURL, apiKey string) (http.Handler, error) {
	upstream, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if upstream.Scheme == "" || upstream.Host == "" {
		return nil, errInvalidBaseURL
	}

	rp := httputil.NewSingleHostReverseProxy(upstream)

	// Director runs once per request, after the standard ReverseProxy
	// has copied the inbound URL. We re-derive scheme/host/path from the
	// configured upstream so a malicious inbound Host header cannot
	// redirect traffic, and stamp the auth header from the gateway's
	// own config so client-supplied x-api-key is replaced.
	origDirector := rp.Director
	rp.Director = func(r *http.Request) {
		origDirector(r)
		r.Host = upstream.Host
		r.URL.Scheme = upstream.Scheme
		r.URL.Host = upstream.Host
		// Preserve the inbound path; the standard director already
		// joined the upstream path with the inbound path.
		r.Header.Set("x-api-key", apiKey)
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

// errInvalidBaseURL is returned when the configured upstream URL has no
// scheme or host. We surface this at startup so misconfiguration is loud,
// not silent.
var errInvalidBaseURL = &configError{msg: "ANTHROPIC_BASE_URL must include scheme and host"}

type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }