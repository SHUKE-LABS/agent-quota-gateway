package backend

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// PoolRouter picks the backend an inbound request resolves to. The
// gateway fronts every pool with auto-rotation, so a request names a
// pool and the router returns that pool's current sticky backend.
//
// The interface is kept here so the resolver middleware can call it
// without the backend package importing the auto package (which itself
// depends on this one). The concrete implementation lives in
// internal/auto.
type PoolRouter interface {
	// Route returns the backend to serve a request for the named pool.
	// ok is false when the pool name is unknown (the middleware fails
	// closed with 403). When exhausted is true the whole pool is
	// rate-limited and the caller must emit 429 with the given
	// Retry-After (the wait until the soonest member resets); b is then
	// the soonest-resetting member the client's post-wait retry lands on.
	Route(pool string) (b Backend, retryAfter time.Duration, ok, exhausted bool)
}

// Middleware resolves the inbound selector to a backend and stores it on
// the request context for the proxy director and quota observer. It
// wraps only the proxy handler — the gateway's own /_gateway endpoints
// take no selector.
//
// The selector arrives as the Authorization bearer token: Claude Code
// puts ANTHROPIC_AUTH_TOKEN there, and here that value is a local *pool
// name*, not a credential. The router auto-rotates within that pool. An
// unknown pool fails closed with 403 and never reaches the upstream; an
// exhausted pool returns an honest 429 with a precise Retry-After. The
// selector value is deliberately never logged or echoed — a
// misconfigured client could have put a real token there, and we must
// not leak it.
func Middleware(router PoolRouter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Normalize the selector here so the router does a direct lookup
		// against the (already normalized) pool names: the client may send
		// any case, and ANTHROPIC_AUTH_TOKEN=AUTO must match pool "auto".
		selector := normalizeName(bearerToken(r.Header.Get("Authorization")))

		b, retryAfter, ok, exhausted := router.Route(selector)
		if !ok {
			writeForbidden(w)
			return
		}
		if exhausted {
			// Whole pool is rate-limited; there is nothing to switch to,
			// so be honest: 429 with the precise wait until the soonest
			// member resets.
			writeRateLimited(w, retryAfter)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithBackend(r.Context(), b)))
	})
}

// bearerToken extracts the token from an "Authorization: Bearer <tok>"
// header value. The scheme is matched case-insensitively per RFC 7235.
// A header without the bearer scheme yields "", which Route rejects.
func bearerToken(authHeader string) string {
	const scheme = "bearer "
	if len(authHeader) < len(scheme) || !strings.EqualFold(authHeader[:len(scheme)], scheme) {
		return ""
	}
	return strings.TrimSpace(authHeader[len(scheme):])
}

// writeForbidden emits the fail-closed response. The body is generic on
// purpose: it names neither the rejected selector nor the set of valid
// pools, so nothing about the gateway's configuration leaks to a client
// that guessed wrong.
func writeForbidden(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	_, _ = w.Write([]byte(`{"error":"unknown backend selector"}`))
}

// writeRateLimited emits the honest 429 returned when every member of a
// pool is exhausted. Retry-After carries the precise wait until the
// soonest member resets (ceiled to whole seconds, floored at 1 so a
// client never busy-loops on a zero/negative hint). The body is generic
// and leaks no nick.
func writeRateLimited(w http.ResponseWriter, retryAfter time.Duration) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(retryAfter)))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write([]byte(`{"error":"all backends rate-limited"}`))
}

// retryAfterSeconds converts a duration into the whole-second value an
// HTTP Retry-After header carries: ceiled (so we never advertise a wait
// shorter than reality) and floored at 1 (a client must wait at least a
// tick rather than retry instantly).
func retryAfterSeconds(d time.Duration) int {
	secs := int(math.Ceil(d.Seconds()))
	if secs < 1 {
		secs = 1
	}
	return secs
}
