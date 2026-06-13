package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/logging"
	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// TestIntegration_fullStack is the end-to-end smoke test for the gateway:
// it rebuilds the same mux `run()` wires (config is stubbed inline so the
// test does not depend on the ambient environment), points the proxy at
// a fake upstream that streams SSE events with Anthropic rate-limit
// headers, and asserts:
//
//   - streaming passthrough works (first SSE event arrives within 150ms)
//   - the quota snapshot captured from upstream headers is readable via
//     GET /_gateway/quota
//   - GET /_gateway/health returns 200 with {"status":"ok"}
//   - no credential headers or request body bytes appear in the stderr
//     log lines
//
// If `run()` ever changes shape (new handlers, new middleware), this
// test should change in lockstep — its job is to pin the wired surface,
// not the individual component behavior, which the per-package tests
// already cover.
func TestIntegration_fullStack(t *testing.T) {
	// Capture stderr so the logging middleware does not contaminate
	// the test runner output, and so we can grep the captured stream
	// for credential leakage.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	oldStderr := os.Stderr
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = oldStderr })

	var logBuf bytes.Buffer
	logDone := make(chan struct{})
	go func() {
		_, _ = logBuf.ReadFrom(r)
		close(logDone)
	}()

	// Fake upstream: streams three SSE events with rate-limit headers,
	// flushing between each so a buffered proxy would take the full
	// stream duration to surface the first event. It also records the
	// credential headers it received so the test can prove the gateway
	// swapped in the backend's real credential and dropped the selector.
	var gotUpstreamKey, gotUpstreamAuth string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUpstreamKey = r.Header.Get("x-api-key")
		gotUpstreamAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("anthropic-version", "2023-06-01")
		w.Header().Set("anthropic-ratelimit-unified-status", "allowed")
		w.Header().Set("anthropic-ratelimit-unified-5h-utilization", "0.25")
		w.Header().Set("anthropic-ratelimit-unified-5h-reset", "1781352600")
		w.Header().Set("anthropic-ratelimit-unified-7d-utilization", "0.07")
		w.Header().Set("anthropic-organization-id", "org_test123")
		w.WriteHeader(http.StatusOK)

		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("upstream ResponseWriter is not an http.Flusher")
		}
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "event: message\ndata: {\"chunk\":%d}\n\n", i)
			flusher.Flush()
			time.Sleep(100 * time.Millisecond)
		}
	})
	upSrv := httptest.NewServer(upstream)
	t.Cleanup(upSrv.Close)

	// Rebuild the wiring `run()` produces, minus the signal-driven
	// shutdown path (httptest.Server handles cleanup). The backend
	// "test-backend" owns the real upstream credential; the client only
	// ever sends its nick as a selector.
	t.Setenv("AQG_BACKEND_TEST_BACKEND", "real-upstream-credential")
	registry, err := backend.Load()
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}

	store := quota.NewStore()
	observer := func(resp *http.Response) {
		snap := quota.Extract(resp)
		if !snap.HasData() {
			return
		}
		key := defaultBackendKey
		if resp.Request != nil {
			if b, ok := backend.FromContext(resp.Request.Context()); ok {
				key = b.Nick
			}
		}
		store.Put(key, snap)
	}
	proxyHandler, err := proxy.New(upSrv.URL, observer, nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/health", healthHandler())
	mux.HandleFunc("/_gateway/quota", quotaHandler(store, nil))
	mux.Handle("/", backend.Middleware(registry, nil, proxyHandler))

	handler := logging.Middleware(mux)
	gw := httptest.NewServer(handler)
	t.Cleanup(gw.Close)

	// 1. Streaming passthrough. The client names the backend by putting
	// its nick in the bearer token (where Claude Code puts
	// ANTHROPIC_AUTH_TOKEN) and sends a stray x-api-key the gateway must
	// drop.
	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader(`{"secret":"prompt-body-should-not-leak"}`))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "client-supplied-attacker-key")
	req.Header.Set("Authorization", "Bearer test-backend")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var firstAt time.Duration
	for scanner.Scan() {
		line := scanner.Text()
		if firstAt == 0 && strings.HasPrefix(line, "event: message") {
			firstAt = time.Since(start)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if firstAt == 0 {
		t.Fatal("never received first SSE event")
	}
	if firstAt > 150*time.Millisecond {
		t.Errorf("first event arrived at %v; proxy appears to buffer (want < 150ms)", firstAt)
	}

	// 1b. The gateway must have swapped in the backend's real credential
	// (an API key here → x-api-key) and dropped the inbound selector that
	// arrived on Authorization.
	if gotUpstreamKey != "real-upstream-credential" {
		t.Errorf("upstream x-api-key = %q, want real-upstream-credential", gotUpstreamKey)
	}
	if gotUpstreamAuth != "" {
		t.Errorf("upstream Authorization = %q, want empty (selector must not reach upstream)", gotUpstreamAuth)
	}

	// 2. Quota snapshot readable via /_gateway/quota under the resolved
	// backend nick.
	quotaResp, err := http.Get(gw.URL + "/_gateway/quota?backend=test-backend")
	if err != nil {
		t.Fatalf("quota get: %v", err)
	}
	defer quotaResp.Body.Close()
	if quotaResp.StatusCode != http.StatusOK {
		t.Fatalf("quota status = %d, want 200", quotaResp.StatusCode)
	}
	var snap quota.Snapshot
	if err := json.NewDecoder(quotaResp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode quota: %v", err)
	}
	if snap.Backend != "test-backend" {
		t.Errorf("backend = %q, want test-backend", snap.Backend)
	}
	if snap.UnifiedStatus != "allowed" {
		t.Errorf("unified_status = %q, want allowed", snap.UnifiedStatus)
	}
	if snap.Unified5hUtilization == nil || *snap.Unified5hUtilization != 0.25 {
		t.Errorf("unified_5h_utilization = %v, want 0.25", snap.Unified5hUtilization)
	}
	if snap.Unified5hReset == nil || !snap.Unified5hReset.Equal(time.Unix(1781352600, 0).UTC()) {
		t.Errorf("unified_5h_reset = %v, want %v", snap.Unified5hReset, time.Unix(1781352600, 0).UTC())
	}
	if snap.Unified7dUtilization == nil || *snap.Unified7dUtilization != 0.07 {
		t.Errorf("unified_7d_utilization = %v, want 0.07", snap.Unified7dUtilization)
	}
	if snap.OrgID != "org_test123" {
		t.Errorf("org_id = %q, want org_test123", snap.OrgID)
	}
	if snap.AsOf.IsZero() {
		t.Errorf("as_of is zero; gateway should stamp it")
	}

	// 3. Health endpoint.
	healthResp, err := http.Get(gw.URL + "/_gateway/health")
	if err != nil {
		t.Fatalf("health get: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", healthResp.StatusCode)
	}
	if ct := healthResp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("health content-type = %q, want application/json", ct)
	}
	body, err := io.ReadAll(healthResp.Body)
	if err != nil {
		t.Fatalf("health read: %v", err)
	}
	if got := strings.TrimSpace(string(body)); got != `{"status":"ok"}` {
		t.Errorf("health body = %q, want {\"status\":\"ok\"}", got)
	}

	// 4. Stop capturing stderr and assert nothing leaked.
	w.Close()
	<-logDone
	logs := logBuf.String()
	for _, banned := range []string{
		"prompt-body-should-not-leak",
		"client-supplied-attacker-key",
		"real-upstream-credential",
	} {
		if strings.Contains(logs, banned) {
			t.Errorf("stderr leaked %q\nlogs: %s", banned, logs)
		}
	}
}

// TestIntegration_autoFailover drives the full wired stack: an `auto`
// request routed to a backend whose upstream 429s comes back to the
// client as a 503 (switchable), the sticky pointer advances, and the
// client's retry lands on the healthy backend and succeeds — all without
// the gateway buffering the request body. It also confirms the auto quota
// view follows the switch.
func TestIntegration_autoFailover(t *testing.T) {
	// Upstream 429s the first backend's credential (with a future reset)
	// and serves 200 for the second.
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("x-api-key") {
		case "cred-a":
			w.Header().Set("anthropic-ratelimit-unified-status", "rejected")
			w.Header().Set("anthropic-ratelimit-unified-reset", fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()))
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error"}}`))
		default:
			w.Header().Set("anthropic-ratelimit-unified-status", "allowed")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	})
	upSrv := httptest.NewServer(upstream)
	t.Cleanup(upSrv.Close)

	// Two backends; sorted nicks => ["acct-a","acct-b"], start at index 0.
	t.Setenv("AQG_BACKEND_ACCT_A", "cred-a")
	t.Setenv("AQG_BACKEND_ACCT_B", "cred-b")
	registry, err := backend.Load()
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	autoCtl := auto.NewController(registry, 0, nil, io.Discard)

	store := quota.NewStore()
	observer := func(resp *http.Response) {
		snap := quota.Extract(resp)
		if !snap.HasData() {
			return
		}
		key := defaultBackendKey
		if resp.Request != nil {
			if b, ok := backend.FromContext(resp.Request.Context()); ok {
				key = b.Nick
			}
		}
		store.Put(key, snap)
	}
	proxyHandler, err := proxy.New(upSrv.URL, observer, autoCtl.ModifyResponse)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/quota", quotaHandler(store, autoCtl))
	mux.Handle("/", backend.Middleware(registry, autoCtl, proxyHandler))
	gw := httptest.NewServer(logging.Middleware(mux))
	t.Cleanup(gw.Close)

	autoPost := func() *http.Response {
		req, _ := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader("{}"))
		req.Header.Set("Authorization", "Bearer auto")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		return resp
	}

	// 1. First auto request hits acct-a → upstream 429 → client sees 503.
	resp1 := autoPost()
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("first auto request status=%d, want 503 (switchable)", resp1.StatusCode)
	}
	if ra := resp1.Header.Get("Retry-After"); ra == "" {
		t.Errorf("503 missing Retry-After")
	}
	if got := autoCtl.Current(); got != "acct-b" {
		t.Fatalf("after 429, currentAuto=%q, want acct-b", got)
	}

	// 2. The client's retry lands on acct-b and succeeds.
	resp2 := autoPost()
	body, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("retry status=%d, want 200 on the switched backend", resp2.StatusCode)
	}
	if !strings.Contains(string(body), `"ok":true`) {
		t.Errorf("retry body=%q, want upstream success payload", body)
	}

	// 3. The auto quota view follows the switch.
	qResp, err := http.Get(gw.URL + "/_gateway/quota?backend=auto")
	if err != nil {
		t.Fatalf("quota get: %v", err)
	}
	defer qResp.Body.Close()
	var view map[string]any
	if err := json.NewDecoder(qResp.Body).Decode(&view); err != nil {
		t.Fatalf("decode quota: %v", err)
	}
	if view["active_backend"] != "acct-b" {
		t.Errorf("active_backend=%v, want acct-b", view["active_backend"])
	}
}

// TestQuotaHandler_autoViewAddsActiveBackend proves the quota endpoint's
// `auto` path returns the active sticky backend's snapshot with an
// active_backend field naming it — so a consumer asking `auto` needs zero
// knowledge of pool membership.
func TestQuotaHandler_autoViewAddsActiveBackend(t *testing.T) {
	t.Setenv("AQG_BACKEND_ACCT_ONE", "cred-one")
	registry, err := backend.Load()
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	autoCtl := auto.NewController(registry, 0, nil, io.Discard) // start at acct-one

	store := quota.NewStore()
	util := 0.42
	store.Put("acct-one", quota.Snapshot{UnifiedStatus: "allowed", Unified5hUtilization: &util})

	srv := httptest.NewServer(quotaHandler(store, autoCtl))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/_gateway/quota?backend=auto")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["active_backend"] != "acct-one" {
		t.Errorf("active_backend=%v, want acct-one", got["active_backend"])
	}
	if got["backend"] != "acct-one" {
		t.Errorf("backend=%v, want acct-one (snapshot promoted into the view)", got["backend"])
	}
	if got["unified_status"] != "allowed" {
		t.Errorf("unified_status=%v, want allowed", got["unified_status"])
	}
	if got["unified_5h_utilization"] != 0.42 {
		t.Errorf("unified_5h_utilization=%v, want 0.42", got["unified_5h_utilization"])
	}
}

// TestHealthHandler_methodGuard pins the GET-only contract on
// /_gateway/health. The README documents the endpoint as GET, but the
// handler used to accept any verb and return 200 — same shape as the
// GET response, which let a client that learned POST-on-health then
// trip on quota's 405. healthHandler and quotaHandler must agree, so
// this test fires POST/PUT/DELETE/OPTIONS and asserts 405 + Allow: GET
// (matching quotaHandler's policy).
func TestHealthHandler_methodGuard(t *testing.T) {
	srv := httptest.NewServer(healthHandler())
	defer srv.Close()

	// Sanity: GET still works.
	getResp, err := http.Get(srv.URL + "/_gateway/health")
	if err != nil {
		t.Fatalf("health GET: %v", err)
	}
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("health GET status = %d, want 200", getResp.StatusCode)
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions} {
		req, err := http.NewRequest(method, srv.URL+"/_gateway/health", nil)
		if err != nil {
			t.Fatalf("NewRequest %s: %v", method, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s status = %d, want 405", method, resp.StatusCode)
		}
		if allow := resp.Header.Get("Allow"); allow != http.MethodGet {
			t.Errorf("%s Allow header = %q, want %q", method, allow, http.MethodGet)
		}
		resp.Body.Close()
	}
}
