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

// Middleware returns a handler that dumps each inbound request to stderr and
// then calls next. Returns next unchanged when debug logging is disabled.
func Middleware(next http.Handler) http.Handler {
	if os.Getenv("AQG_DEBUG_LOG_REQUESTS") != "1" {
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

	// Body: read, log, restore.
	if r.Body != nil {
		body, err := io.ReadAll(r.Body)
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))
		if err != nil {
			fmt.Fprintf(&sb, "  [body read error: %v]\n", err)
		} else if len(body) > 0 {
			fmt.Fprintf(&sb, "  body (%d bytes):\n%s\n", len(body), body)
		}
	}

	fmt.Fprint(os.Stderr, sb.String())
}
