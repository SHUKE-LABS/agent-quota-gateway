package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/shukebeta/agent-quota-gateway/internal/config"
)

// writeAllowMiddleware enforces SHARED_WRITE_ALLOW (issue #349): a
// non-GET/HEAD request under /_gateway from a peer outside the allowlist
// (loopback always passes) gets 403 before it reaches a handler, so pool
// state is never touched. GETs and every proxy path pass untouched — fleet
// clients on the same port still need /v1/*, /_aqg/w/<nick>/*, and
// GET /_gateway/quota, which is why a port-level network ACL cannot do this.
//
// The peer is the TCP RemoteAddr, never X-Forwarded-For / Forwarded: the
// gateway sits behind no proxy, so any such header is client-controlled and
// would let any tailnet node claim an allowed address.
//
// It must wrap the mux inside backend.WorkerNamespaceMiddleware: that
// middleware rewrites /_aqg/w/<nick>/_gateway/... to /_gateway/..., which
// the mux then serves as an admin route.
//
// A nil allowlist returns next unchanged — the pre-#349 behavior.
func writeAllowMiddleware(allow []netip.Prefix, next http.Handler) http.Handler {
	if len(allow) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead || !isGatewayPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if peerAllowed(allow, r.RemoteAddr) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "this address may not change gateway state (SHARED_WRITE_ALLOW)"})
	})
}

func isGatewayPath(path string) bool {
	return path == "/_gateway" || strings.HasPrefix(path, "/_gateway/")
}

// peerAllowed fails closed on a RemoteAddr it cannot parse.
func peerAllowed(allow []netip.Prefix, remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return config.MatchesWriteAllow(allow, peer)
}

// sharedWriteAllowSummary is the shared-mode startup line describing who may
// change gateway state, so an open admin surface is as loud as the bind.
func sharedWriteAllowSummary(allow []netip.Prefix) string {
	if len(allow) == 0 {
		return "/_gateway writes are open to every client that can reach this address; set SHARED_WRITE_ALLOW to restrict them"
	}
	entries := make([]string, 0, len(allow))
	for _, prefix := range allow {
		entries = append(entries, config.FormatWriteAllowEntry(prefix))
	}
	return "/_gateway writes restricted to loopback + " + strings.Join(entries, ", ")
}
