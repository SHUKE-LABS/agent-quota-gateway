package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/configfile"
)

// guardedHandler wires the SHARED_WRITE_ALLOW guard (issue #349) the way run()
// does — inside WorkerNamespaceMiddleware, directly around the mux — with the
// real disable handler (so a denied write can be checked against pool state)
// and stubs for the read and proxy routes the guard must leave alone.
func guardedHandler(t *testing.T, allow []netip.Prefix) (http.Handler, *auto.Pools) {
	t.Helper()
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-b")
	pools := loadPools(t)
	ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/quota", ok)
	mux.HandleFunc("/_gateway/pool", ok)
	mux.HandleFunc("/_gateway/clear", ok)
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/disable", disableMemberHandler(pools, configfile.PersistenceState{}))
	mux.HandleFunc("/", ok)
	return backend.WorkerNamespaceMiddleware(writeAllowMiddleware(allow, mux)), pools
}

func serveFrom(h http.Handler, method, path, remoteAddr string, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.RemoteAddr = remoteAddr
	for k, v := range header {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func memberADisabled(pools *auto.Pools) bool {
	return pools.CurrentRegistry().Spec().Pools["auto"].Members["a"].Disabled
}

var testWriteAllow = []netip.Prefix{netip.MustParsePrefix("100.64.1.2/32")}

const (
	allowedPeer = "100.64.1.2:51000"
	deniedPeer  = "100.64.9.9:51000"
	disablePath = "/_gateway/pool/auto/member/a/disable"
)

func TestWriteAllow_unsetLeavesWritesOpen(t *testing.T) {
	h, pools := guardedHandler(t, nil)
	if rec := serveFrom(h, http.MethodPost, disablePath, deniedPeer, nil); rec.Code != http.StatusOK {
		t.Fatalf("unset allowlist: status=%d, want 200", rec.Code)
	}
	if !memberADisabled(pools) {
		t.Fatal("unset allowlist: write did not apply")
	}
}

func TestWriteAllow_allowedPeerWrites(t *testing.T) {
	h, pools := guardedHandler(t, testWriteAllow)
	if rec := serveFrom(h, http.MethodPost, disablePath, allowedPeer, nil); rec.Code != http.StatusOK {
		t.Fatalf("allowed peer: status=%d, want 200", rec.Code)
	}
	if !memberADisabled(pools) {
		t.Fatal("allowed peer: write did not apply")
	}
}

func TestWriteAllow_loopbackAlwaysWrites(t *testing.T) {
	for _, peer := range []string{"127.0.0.1:51000", "[::1]:51000"} {
		h, pools := guardedHandler(t, testWriteAllow)
		if rec := serveFrom(h, http.MethodPost, disablePath, peer, nil); rec.Code != http.StatusOK {
			t.Fatalf("loopback %s: status=%d, want 200", peer, rec.Code)
		}
		if !memberADisabled(pools) {
			t.Fatalf("loopback %s: write did not apply", peer)
		}
	}
}

func TestWriteAllow_deniedPeerGets403AndStateUnchanged(t *testing.T) {
	h, pools := guardedHandler(t, testWriteAllow)
	rec := serveFrom(h, http.MethodPost, disablePath, deniedPeer, nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("denied peer: status=%d, want 403", rec.Code)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error == "" {
		t.Fatalf("denied peer: body %q is not a JSON {error} (the UI banner reads .error): %v", rec.Body.String(), err)
	}
	if memberADisabled(pools) {
		t.Fatal("denied peer: write reached the handler and changed pool state")
	}
}

// TestWriteAllow_forwardedHeaderDoesNotBypass: the gateway sits behind no
// proxy, so X-Forwarded-For / Forwarded are client-controlled and must not
// stand in for the TCP peer.
func TestWriteAllow_forwardedHeaderDoesNotBypass(t *testing.T) {
	h, pools := guardedHandler(t, testWriteAllow)
	header := http.Header{
		"X-Forwarded-For": {"100.64.1.2"},
		"X-Real-Ip":       {"100.64.1.2"},
		"Forwarded":       {"for=100.64.1.2"},
	}
	if rec := serveFrom(h, http.MethodPost, disablePath, deniedPeer, header); rec.Code != http.StatusForbidden {
		t.Fatalf("forwarded header: status=%d, want 403", rec.Code)
	}
	if memberADisabled(pools) {
		t.Fatal("forwarded header: write changed pool state")
	}
}

// TestWriteAllow_namespacedGatewayWriteDenied: WorkerNamespaceMiddleware
// rewrites /_aqg/w/<nick>/_gateway/... to /_gateway/..., which the mux serves
// as an admin route — the guard must see the rewritten path.
func TestWriteAllow_namespacedGatewayWriteDenied(t *testing.T) {
	h, pools := guardedHandler(t, testWriteAllow)
	if rec := serveFrom(h, http.MethodPost, "/_aqg/w/worker-1"+disablePath, deniedPeer, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("namespaced write: status=%d, want 403", rec.Code)
	}
	if memberADisabled(pools) {
		t.Fatal("namespaced write: changed pool state")
	}
	if rec := serveFrom(h, http.MethodDelete, "/_gateway/clear", deniedPeer, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("DELETE /_gateway/clear: status=%d, want 403", rec.Code)
	}
}

// TestWriteAllow_readsAndProxyUntouched: fleet clients outside the allowlist
// keep the reads and proxy paths they need on the same port.
func TestWriteAllow_readsAndProxyUntouched(t *testing.T) {
	h, _ := guardedHandler(t, testWriteAllow)
	cases := []struct{ method, path string }{
		{http.MethodGet, "/_gateway/quota?backend=auto"},
		{http.MethodGet, "/_gateway/pool"},
		{http.MethodHead, "/_gateway/pool"},
		{http.MethodPost, "/v1/messages"},
		{http.MethodPost, "/_aqg/w/worker-1/v1/messages"},
		{http.MethodPost, "/responses"},
	}
	for _, c := range cases {
		if rec := serveFrom(h, c.method, c.path, deniedPeer, nil); rec.Code != http.StatusOK {
			t.Errorf("%s %s from denied peer: status=%d, want 200", c.method, c.path, rec.Code)
		}
	}
}

func TestWriteAllow_unparseablePeerDenied(t *testing.T) {
	h, _ := guardedHandler(t, testWriteAllow)
	if rec := serveFrom(h, http.MethodPost, disablePath, "not-an-addr", nil); rec.Code != http.StatusForbidden {
		t.Fatalf("unparseable peer: status=%d, want 403", rec.Code)
	}
}
