package activity

import (
	"net/http"
	"strings"
	"time"
)

// Middleware times each request and files it into store after the handler
// returns, recording only path/status/duration (the same fields the logging
// middleware emits — no bodies, headers, or credentials). It composes beside
// logging.Middleware in the request chain.
//
// /_gateway/* paths are excluded: they are the dashboard's own polling traffic
// (the activity panel refreshes on a timer), and folding that into the series
// would drown real upstream traffic in gateway self-chatter.
func Middleware(store *Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_gateway/") {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		store.Record(r.URL.Path, rw.status, time.Since(start), time.Now())
	})
}

// statusRecorder captures the emitted status without depending on net/http
// internals. It mirrors logging.statusRecorder — critically including Unwrap —
// so http.NewResponseController can walk through it to the real writer's
// http.Flusher; without Unwrap this wrapper would swallow SSE flushes and
// /v1/messages would buffer instead of streaming.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
