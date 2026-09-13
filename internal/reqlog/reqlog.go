// Package reqlog provides a debug middleware that dumps inbound requests to
// stderr when request logging is enabled. It is intentionally off by default:
// request bodies may contain user messages. Enable only in dev/debug runs.
//
// The enable gate is a runtime toggle (issue #301): the flag lives in
// aqg.json's "debug" section, seeded once from AQG_DEBUG_LOG_REQUESTS=1 on
// first bootstrap, and flipped live via POST /_gateway/debug or the UI —
// no restart. SetEnabled takes effect on the very next request.
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
	"sync/atomic"
	"time"
)

// credentialHeaders are never logged — a misconfigured client could have put
// a real API key or session token there.
var credentialHeaders = map[string]bool{
	"authorization": true,
	"x-api-key":     true,
}

// enabled is the live request-logging gate. An atomic (not the historical
// init-time env read) because the flag must flip on a running gateway
// without a restart (issue #301): Middleware and WrapTransport consult it
// per request / per round-trip, not once at wiring time. Zero value = off.
var enabled atomic.Bool

// SetEnabled turns request logging on or off for subsequent requests. Safe
// for concurrent use; callers outside tests are main (startup, from the
// resolved config) and the /_gateway/debug handler (live toggle).
func SetEnabled(v bool) { enabled.Store(v) }

// Enabled reports the current request-logging state.
func Enabled() bool { return enabled.Load() }

// debugOut is the test-injection point for the dump stream (issue #301's
// AC1 integration test). nil means "use os.Stderr at call time" — which
// matters because tests that redirect os.Stderr (captureStderr) must keep
// working: capturing os.Stderr once at init time would freeze the var to
// the original *os.File and silently bypass the pipe. sink() does the
// dynamic lookup so both paths land in the test's buffer.
var debugOut io.Writer

// SetDebugOutput redirects the dump stream. Restoring nil (or os.Stderr
// behaviour) is the caller's job; tests use a t.Cleanup. Production code
// never calls this — the dump goes to stderr unconditionally.
func SetDebugOutput(w io.Writer) { debugOut = w }

// sink returns the current dump target: the test-injected writer if
// non-nil, otherwise os.Stderr looked up at call time so a test's
// captureStderr-style redirect is honoured.
func sink() io.Writer {
	if debugOut != nil {
		return debugOut
	}
	return os.Stderr
}

// WrapTransport always wraps t; the wrapper consults the live flag before
// each upstream round-trip and, when on, dumps the outbound request headers
// (after the director stamped them) to stderr. Wrapping unconditionally is
// what makes the runtime toggle reach the transport the proxy already
// holds — a wiring-time short-circuit would freeze the boot state.
func WrapTransport(t http.RoundTripper) http.RoundTripper {
	return &debugTransport{inner: t}
}

type debugTransport struct{ inner http.RoundTripper }

func (d *debugTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !enabled.Load() {
		return d.inner.RoundTrip(r)
	}
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
	fmt.Fprint(sink(), sb.String())
	return d.inner.RoundTrip(r)
}

// Middleware returns a handler that dumps each inbound request to stderr and
// then calls next. The dump decision is made per request against the live
// flag, so SetEnabled flips it on a running gateway with no rewire (the
// handler chain is immutable once the server is listening — issue #301).
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if enabled.Load() {
			dump(r)
		}
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

	fmt.Fprint(sink(), sb.String())
}
