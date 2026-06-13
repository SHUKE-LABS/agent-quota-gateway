package backend

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Middleware resolves the inbound selector to a backend and stores it on
// the request context for the proxy director and quota observer. It
// wraps only the proxy handler — the gateway's own /_gateway endpoints
// take no selector.
//
// The selector arrives as the Authorization bearer token: Claude Code
// puts ANTHROPIC_AUTH_TOKEN there, and here that value is a local
// backend name, not a credential. An unknown or missing selector fails
// closed with 403 and never reaches the upstream. The selector value is
// deliberately never logged or echoed — a misconfigured client could
// have put a real token there, and we must not leak it.
//
// The reserved selector "auto" routes through the supplied AutoResolver
// instead of the static registry: the gateway picks the sticky backend
// and flags the request so the proxy's response hook can fail over on a
// 429. When auto is nil (no resolver wired) the word "auto" has no
// special meaning and falls through to the registry, which fails closed
// because "auto" is a reserved nick that can never be configured.
func Middleware(reg *Registry, auto AutoResolver, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selector := bearerToken(r.Header.Get("Authorization"))

		if auto != nil && IsAutoSelector(selector) {
			b, retryAfter, exhausted := auto.ResolveAuto()
			if exhausted {
				// Whole pool is rate-limited; there is nothing to switch
				// to, so be honest: 429 with the precise wait until the
				// soonest backend resets.
				writeRateLimited(w, retryAfter)
				return
			}
			ctx := MarkAuto(WithBackend(r.Context(), b))
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		b, ok := reg.Resolve(selector)
		if !ok {
			writeForbidden(w)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithBackend(r.Context(), b)))
	})
}

// bearerToken extracts the token from an "Authorization: Bearer <tok>"
// header value. The scheme is matched case-insensitively per RFC 7235.
// A header without the bearer scheme yields "", which Resolve rejects.
func bearerToken(authHeader string) string {
	const scheme = "bearer "
	if len(authHeader) < len(scheme) || !strings.EqualFold(authHeader[:len(scheme)], scheme) {
		return ""
	}
	return strings.TrimSpace(authHeader[len(scheme):])
}

// writeForbidden emits the fail-closed response. The body is generic on
// purpose: it names neither the rejected selector nor the set of valid
// nicks, so nothing about the gateway's configuration leaks to a client
// that guessed wrong.
func writeForbidden(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	_, _ = w.Write([]byte(`{"error":"unknown backend selector"}`))
}

// writeRateLimited emits the honest 429 the auto selector returns when
// every pooled backend is exhausted. Retry-After carries the precise
// wait until the soonest backend resets (ceiled to whole seconds, floored
// at 1 so a client never busy-loops on a zero/negative hint). The body is
// generic and leaks no nick.
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
