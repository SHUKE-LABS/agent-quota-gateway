// Package logging emits one JSON line per request to stderr.
//
// Logs intentionally exclude request and response bodies and any header
// that carries credentials. This is a V1 hard constraint — the proxy is
// the credential boundary, and treating its logs as safe to share is
// what makes the loopback trust model work.
package logging

import (
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

// requestID returns the inbound X-Request-Id header if present, otherwise
// a deterministic "-" so log consumers always see the same field shape.
func requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-Id"); id != "" {
		return id
	}
	return "-"
}