// Package reqlog provides a debug middleware that dumps inbound requests to
// stderr when AQG_DEBUG_LOG_REQUESTS=1. It is intentionally off by default:
// request bodies may contain user messages. Enable only in dev/debug runs.
//
// Each dump prints a separator, then every header except Authorization and
// x-api-key (which must never be logged), then the raw body. The body is
// read, logged, and restored so downstream handlers see it unchanged.
package reqlog

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

// credentialHeaders are never logged — a misconfigured client could have put
// a real API key or session token there.
var credentialHeaders = map[string]bool{
	"authorization": true,
	"x-api-key":     true,
}

// debugEnabled caches the env check so WrapTransport and Middleware agree.
var debugEnabled = os.Getenv("AQG_DEBUG_LOG_REQUESTS") == "1"

// WrapTransport returns t unchanged when debug logging is off. When on, it
// wraps t so outbound request headers (after the director stamped them) are
// dumped to stderr before each upstream round-trip.
func WrapTransport(t http.RoundTripper) http.RoundTripper {
	if !debugEnabled {
		return t
	}
	return &debugTransport{inner: t}
}

type debugTransport struct{ inner http.RoundTripper }

func (d *debugTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n>>> outbound %s %s\n", r.Method, r.URL)
	names := make([]string, 0, len(r.Header))
	for name := range r.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if credentialHeaders[strings.ToLower(name)] {
			fmt.Fprintf(&sb, "  %s: [redacted]\n", name)
			continue
		}
		fmt.Fprintf(&sb, "  %s: %s\n", name, strings.Join(r.Header[name], ", "))
	}
	fmt.Fprint(os.Stderr, sb.String())
	return d.inner.RoundTrip(r)
}

// Middleware returns a handler that dumps each inbound request to stderr and
// then calls next. Returns next unchanged when debug logging is disabled.
func Middleware(next http.Handler) http.Handler {
	if !debugEnabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dump(r)
		next.ServeHTTP(w, r)
	})
}

func dump(r *http.Request) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "\n--- request %s %s %s ---\n", time.Now().UTC().Format("15:04:05.000Z"), r.Method, r.URL.Path)

	// Headers, sorted for stable diffs, credentials redacted.
	names := make([]string, 0, len(r.Header))
	for name := range r.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if credentialHeaders[strings.ToLower(name)] {
			fmt.Fprintf(&sb, "  %s: [redacted]\n", name)
			continue
		}
		fmt.Fprintf(&sb, "  %s: %s\n", name, strings.Join(r.Header[name], ", "))
	}

	// Body: read, log first 500 bytes, restore. Full body is intentionally
	// truncated — bodies contain conversation history and balloon journald.
	if r.Body != nil {
		body, err := io.ReadAll(r.Body)
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))
		if err != nil {
			fmt.Fprintf(&sb, "  [body read error: %v]\n", err)
		} else if len(body) > 0 {
			preview := body
			suffix := ""
			if len(preview) > 500 {
				preview = preview[:500]
				suffix = fmt.Sprintf("... (%d bytes total)", len(body))
			}
			fmt.Fprintf(&sb, "  body (%d bytes): %s%s\n", len(body), preview, suffix)
		}
	}

	fmt.Fprint(os.Stderr, sb.String())
}
