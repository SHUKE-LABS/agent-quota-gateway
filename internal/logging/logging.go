// Package logging emits one JSON line per request to stderr.
//
// Logs intentionally exclude request and response bodies and any header
// that carries credentials. This is a V1 hard constraint — the proxy is
// the credential boundary, and treating its logs as safe to share is
// what makes the loopback trust model work.
package logging

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"time"
)

// Middleware returns an http.Handler that emits one log line per request
// to stderr. Status, duration, method, path, and a request ID are
// recorded; bodies and authorization headers are never logged.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Capture the response status code via a thin wrapper so the
		// logger can see what the proxy actually emitted without
		// depending on net/http internals.
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)

		entry := map[string]any{
			"ts":        time.Now().UTC().Format(time.RFC3339Nano),
			"method":    r.Method,
			"path":      r.URL.Path,
			"status":    rw.status,
			"duration":  time.Since(start).String(),
			"request_id": requestID(r),
		}
		// Marshal errors are impossible with the field set above, but
		// the empty-error return keeps the call site clean.
		_ = json.NewEncoder(os.Stderr).Encode(entry)
	})
}

// statusRecorder is a minimal http.ResponseWriter that records the
// status code. We don't need the full http.ResponseController surface
// here — the proxy is responsible for SSE flushing, not this middleware.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap exposes the underlying ResponseWriter so http.NewResponseController
// can walk through this wrapper to find an http.Flusher (or any other
// optional interface) on the real writer. Without this, a logging wrapper
// silently breaks streaming: httputil.ReverseProxy.FlushInterval = -1 calls
// http.NewResponseController(w).Flush() per Write, and that controller
// stops at the first writer that lacks the interface — i.e. this one —
// so SSE frames accumulate in the response buffer instead of reaching
// the client chunk-by-chunk.
func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

// requestID returns the inbound X-Request-Id header if present, otherwise
// a fresh 16-hex-char random ID. The random form is generated once per
// request so uninstrumented traffic can still be correlated across the
// log stream; the sentinel form ("-") would have collapsed unrelated
// requests into a single bucket, which defeats the field's purpose.
func requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-Id"); id != "" {
		return id
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is exceptional; fall back to a time-based
		// label rather than the "-"" sentinel so log shape stays
		// stable for downstream consumers.
		return time.Now().UTC().Format("20060102T150405.000000000")
	}
	return hex.EncodeToString(b[:])
}