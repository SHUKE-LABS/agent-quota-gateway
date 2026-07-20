package activity

import (
	"net/http"
	"strings"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// Middleware times each request and files it into store after the handler
// returns, recording only path/status/duration (the same fields the logging
// middleware emits — no bodies, headers, or credentials). It composes beside
// logging.Middleware in the request chain.
//
// /_gateway/* paths are excluded: they are the dashboard's own polling traffic
// (the activity panel refreshes on a timer), and folding that into the series
// would drown real upstream traffic in gateway self-chatter.
//
// Unknown-selector rejections are excluded too (issue #230): a request that
// names no valid pool fails closed at the gateway boundary with a 403 and
// never reaches an upstream, so it is always an error and carries no
// upstream-health signal. We install a reached-pool marker into the context
// and record only if backend.Middleware flipped it — i.e. the request
// resolved to a valid pool (including an exhausted one, which is real signal).
func Middleware(store *Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_gateway/") {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		var reachedPool bool
		next.ServeHTTP(rw, r.WithContext(backend.WithReachedPoolMarker(r.Context(), &reachedPool)))
		if !reachedPool {
			// No valid pool named (unknown-selector 403: favicon, browser
			// probe, misconfigured client). Dropped from the series.
			return
		}
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
