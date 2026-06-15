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
	"sync"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/logging"
	"github.com/shukebeta/agent-quota-gateway/internal/persist"
	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// poolView decodes a /_gateway/quota?backend=<pool> response: the active
// member's snapshot plus the active_backend field.
type poolView struct {
	quota.Snapshot
	ActiveBackend string `json:"active_backend"`
}

// mkObserver mirrors the observer run() wires: it files snapshots that
// carry quota data under the resolved backend's quota key.
func mkObserver(store *quota.Store) proxy.ResponseObserver {
	return func(resp *http.Response) {
		snap := quota.Extract(resp)
		if !snap.HasData() {
			return
		}
		key := defaultBackendKey
		if resp.Request != nil {
			if b, ok := backend.FromContext(resp.Request.Context()); ok {
				key = b.QuotaKey()
			}
		}
		store.Put(key, snap)
	}
}

// TestIntegration_fullStack is the end-to-end smoke test for the gateway:
// it rebuilds the same mux `run()` wires (config is stubbed inline so the
// test does not depend on the ambient environment), points a single-member
// "auto" pool at a fake upstream that streams SSE events with Anthropic
// rate-limit headers, and asserts:
//
//   - streaming passthrough works (first SSE event arrives within 150ms)
//   - the gateway swaps in the backend's real credential and drops the selector
//   - the quota snapshot is readable via GET /_gateway/quota?backend=auto
//   - GET /_gateway/health returns 200 with {"status":"ok"}
//   - no credential headers or request body bytes appear in the stderr log
func TestIntegration_fullStack(t *testing.T) {
	// Capture stderr so the logging middleware does not contaminate the
	// test runner output, and so we can grep the captured stream for
	// credential leakage.
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
	// flushing between each. It records the credential headers it received
	// so the test can prove the gateway swapped in the backend's real
	// credential and dropped the selector.
	const realCred = "sk-ant-api-real-upstream-cred"
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

	// Rebuild the wiring `run()` produces. The pool default upstream is the
	// fake server. The backend "test-backend" in pool "auto" owns the real
	// upstream credential; the client only ever sends the pool name.
	t.Setenv("AQG_POOL_AUTO_BACKEND_TEST_BACKEND", realCred)
	registry, err := backend.Load(upSrv.URL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := auto.NewPools(registry, nil, nil, io.Discard)

	store := quota.NewStore()
	proxyHandler, err := proxy.New(mkObserver(store), pools.ModifyResponse)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/health", healthHandler())
	mux.HandleFunc("/_gateway/quota", quotaHandler(store, pools))
	mux.Handle("/", backend.Middleware(pools, proxyHandler))

	handler := logging.Middleware(mux)
	gw := httptest.NewServer(handler)
	t.Cleanup(gw.Close)

	// 1. Streaming passthrough. The client names the pool by putting it in
	// the bearer token (where Claude Code puts ANTHROPIC_AUTH_TOKEN) and
	// sends a stray x-api-key the gateway must drop.
	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader(`{"secret":"prompt-body-should-not-leak"}`))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "client-supplied-attacker-key")
	req.Header.Set("Authorization", "Bearer auto")

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
	// (an API key here → x-api-key) and dropped the inbound selector.
	if gotUpstreamKey != realCred {
		t.Errorf("upstream x-api-key = %q, want %q", gotUpstreamKey, realCred)
	}
	if gotUpstreamAuth != "" {
		t.Errorf("upstream Authorization = %q, want empty (selector must not reach upstream)", gotUpstreamAuth)
	}

	// 2. Quota snapshot readable via /_gateway/quota?backend=auto, filed
	// under the active member's quota key with its nick surfaced.
	quotaResp, err := http.Get(gw.URL + "/_gateway/quota?backend=auto")
	if err != nil {
		t.Fatalf("quota get: %v", err)
	}
	defer quotaResp.Body.Close()
	if quotaResp.StatusCode != http.StatusOK {
		t.Fatalf("quota status = %d, want 200", quotaResp.StatusCode)
	}
	var view poolView
	if err := json.NewDecoder(quotaResp.Body).Decode(&view); err != nil {
		t.Fatalf("decode quota: %v", err)
	}
	if view.ActiveBackend != "test-backend" {
		t.Errorf("active_backend = %q, want test-backend", view.ActiveBackend)
	}
	if view.Backend != "auto/test-backend" {
		t.Errorf("backend = %q, want auto/test-backend (pool-qualified quota key)", view.Backend)
	}
	if view.UnifiedStatus != "allowed" {
		t.Errorf("unified_status = %q, want allowed", view.UnifiedStatus)
	}
	if view.Unified5hUtilization == nil || *view.Unified5hUtilization != 0.25 {
		t.Errorf("unified_5h_utilization = %v, want 0.25", view.Unified5hUtilization)
	}
	if view.Unified5hReset == nil || !view.Unified5hReset.Equal(time.Unix(1781352600, 0).UTC()) {
		t.Errorf("unified_5h_reset = %v, want %v", view.Unified5hReset, time.Unix(1781352600, 0).UTC())
	}
	if view.Unified7dUtilization == nil || *view.Unified7dUtilization != 0.07 {
		t.Errorf("unified_7d_utilization = %v, want 0.07", view.Unified7dUtilization)
	}
	if view.OrgID != "org_test123" {
		t.Errorf("org_id = %q, want org_test123", view.OrgID)
	}
	if view.AsOf.IsZero() {
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
		realCred,
	} {
		if strings.Contains(logs, banned) {
			t.Errorf("stderr leaked %q\nlogs: %s", banned, logs)
		}
	}
}

// TestIntegration_autoFailover drives the full wired stack: a request to a
// pool whose first member's upstream 429s comes back to the client as a
// 503 (switchable), the sticky pointer advances, and the client's retry
// lands on the healthy member and succeeds. The upstream 429s the first
// distinct credential it sees so the test is robust to the pool's random
// start member. It also confirms the quota view follows the switch.
func TestIntegration_autoFailover(t *testing.T) {
	var mu sync.Mutex
	firstKey := ""
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("x-api-key")
		mu.Lock()
		if firstKey == "" {
			firstKey = key
		}
		is429 := key == firstKey
		mu.Unlock()
		if is429 {
			w.Header().Set("anthropic-ratelimit-unified-status", "rejected")
			w.Header().Set("anthropic-ratelimit-unified-reset", fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()))
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error"}}`))
			return
		}
		w.Header().Set("anthropic-ratelimit-unified-status", "allowed")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	upSrv := httptest.NewServer(upstream)
	t.Cleanup(upSrv.Close)

	// Two API-key members in pool "auto".
	t.Setenv("AQG_POOL_AUTO_BACKEND_ACCT_A", "sk-ant-api-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_ACCT_B", "sk-ant-api-b")
	registry, err := backend.Load(upSrv.URL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := auto.NewPools(registry, nil, nil, io.Discard)

	store := quota.NewStore()
	proxyHandler, err := proxy.New(mkObserver(store), pools.ModifyResponse)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/quota", quotaHandler(store, pools))
	mux.Handle("/", backend.Middleware(pools, proxyHandler))
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

	// 1. First request hits the start member → upstream 429 → client 503.
	resp1 := autoPost()
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("first auto request status=%d, want 503 (switchable)", resp1.StatusCode)
	}
	if ra := resp1.Header.Get("Retry-After"); ra == "" {
		t.Errorf("503 missing Retry-After")
	}
	if cur, _ := pools.Current("auto"); cur.Nick == "" {
		t.Fatal("no active backend after switch")
	}

	// 2. The client's retry lands on the healthy member and succeeds.
	resp2 := autoPost()
	body, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("retry status=%d, want 200 on the switched backend", resp2.StatusCode)
	}
	if !strings.Contains(string(body), `"ok":true`) {
		t.Errorf("retry body=%q, want upstream success payload", body)
	}

	// 3. The quota view follows the switch: active_backend names the
	// current member, which is the one that served the 200.
	qResp, err := http.Get(gw.URL + "/_gateway/quota?backend=auto")
	if err != nil {
		t.Fatalf("quota get: %v", err)
	}
	defer qResp.Body.Close()
	var qview poolView
	if err := json.NewDecoder(qResp.Body).Decode(&qview); err != nil {
		t.Fatalf("decode quota: %v", err)
	}
	cur, _ := pools.Current("auto")
	if qview.ActiveBackend != cur.Nick {
		t.Errorf("active_backend=%q, want %q (the switched-to member)", qview.ActiveBackend, cur.Nick)
	}
	if qview.ActiveBackend != "acct-a" && qview.ActiveBackend != "acct-b" {
		t.Errorf("active_backend=%q, want one of the configured nicks", qview.ActiveBackend)
	}
}

// TestQuotaHandler_poolViewAddsActiveBackend proves the quota endpoint's
// pool path returns the active member's snapshot with an active_backend
// field naming it — so a consumer asking for a pool needs zero knowledge
// of pool membership.
func TestQuotaHandler_poolViewAddsActiveBackend(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_ACCT_ONE", "sk-ant-oat-one")
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := auto.NewPools(registry, nil, nil, io.Discard) // single member → deterministic

	store := quota.NewStore()
	util := 0.42
	store.Put("auto/acct-one", quota.Snapshot{UnifiedStatus: "allowed", Unified5hUtilization: &util})

	srv := httptest.NewServer(quotaHandler(store, pools))
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
	if got["backend"] != "auto/acct-one" {
		t.Errorf("backend=%v, want auto/acct-one (pool-qualified key promoted into the view)", got["backend"])
	}
	if got["unified_status"] != "allowed" {
		t.Errorf("unified_status=%v, want allowed", got["unified_status"])
	}
	if got["unified_5h_utilization"] != 0.42 {
		t.Errorf("unified_5h_utilization=%v, want 0.42", got["unified_5h_utilization"])
	}

	// The pool query is case-insensitive, matching the routing path.
	upper, err := http.Get(srv.URL + "/_gateway/quota?backend=AUTO")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer upper.Body.Close()
	var gotUpper map[string]any
	if err := json.NewDecoder(upper.Body).Decode(&gotUpper); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotUpper["active_backend"] != "acct-one" {
		t.Errorf("?backend=AUTO active_backend=%v, want acct-one (case-insensitive)", gotUpper["active_backend"])
	}
}

// TestQuotaHandler_unknownPoolEmptySnapshot proves an unknown pool (or a
// missing param) returns 200 with an empty snapshot rather than an error.
func TestQuotaHandler_unknownPoolEmptySnapshot(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-oat-a")
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := auto.NewPools(registry, nil, nil, io.Discard)
	srv := httptest.NewServer(quotaHandler(quota.NewStore(), pools))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/_gateway/quota?backend=nope")
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
	if got["unified_status"] != nil {
		t.Errorf("unknown pool returned quota data: %v", got)
	}
	if _, hasActive := got["active_backend"]; hasActive {
		t.Errorf("unknown pool view should not carry active_backend: %v", got)
	}
}

// TestHealthHandler_methodGuard pins the GET-only contract on
// /_gateway/health: GET works, other verbs get 405 + Allow: GET.
func TestHealthHandler_methodGuard(t *testing.T) {
	srv := httptest.NewServer(healthHandler())
	defer srv.Close()

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

// /_gateway/quota: GET works, other verbs get 405 + Allow: GET — mirrors the
// health guard so the documented Allow contract is enforced for both
// gateway endpoints.
func TestQuotaHandler_methodGuard(t *testing.T) {
	srv := httptest.NewServer(quotaHandler(quota.NewStore(), nil))
	defer srv.Close()

	getResp, err := http.Get(srv.URL + "/_gateway/quota?backend=auto")
	if err != nil {
		t.Fatalf("quota GET: %v", err)
	}
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("quota GET status = %d, want 200", getResp.StatusCode)
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions} {
		req, err := http.NewRequest(method, srv.URL+"/_gateway/quota", nil)
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

// TestPoolHandler_singlePool verifies /_gateway/pool?pool=<name> returns the
// single-pool JSON shape with correct status values.
func TestPoolHandler_singlePool(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_ACCT_ONE", "sk-ant-oat-one")
	t.Setenv("AQG_POOL_AUTO_BACKEND_ACCT_TWO", "sk-ant-oat-two")
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	store := quota.NewStore()
	util := 0.9
	store.Put("auto/acct-one", quota.Snapshot{UnifiedStatus: "allowed", Unified5hUtilization: &util})
	pools := auto.NewPools(registry, nil, nil, io.Discard)

	srv := httptest.NewServer(poolHandler(store, pools))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/_gateway/pool?pool=auto")
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
	if got["pool"] != "auto" {
		t.Errorf("pool=%v, want auto", got["pool"])
	}
	if got["active"] == "" {
		t.Error("active field is empty")
	}
	members, ok := got["members"].([]any)
	if !ok || len(members) == 0 {
		t.Fatalf("members=%v, want non-empty array", got["members"])
	}
	activeNick, _ := got["active"].(string)
	if activeNick == "" {
		t.Fatal("active field is empty")
	}
	foundActive := false
	for _, m := range members {
		mm := m.(map[string]any)
		nick := mm["nick"].(string)
		if nick == activeNick {
			foundActive = true
			if mm["status"] != "active" {
				t.Errorf("member %q status=%v, want active", nick, mm["status"])
			}
		}
		// exhausted_until must be present in the JSON (null or a string), not absent.
		if _, hasKey := mm["exhausted_until"]; !hasKey {
			t.Errorf("member %q missing exhausted_until key", nick)
		}
	}
	if !foundActive {
		t.Errorf("active member %q not found in members list", activeNick)
	}
}

// TestPoolHandler_allPools verifies /_gateway/pool (no param) returns an array.
func TestPoolHandler_allPools(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-b")
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := auto.NewPools(registry, nil, nil, io.Discard)

	srv := httptest.NewServer(poolHandler(quota.NewStore(), pools))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/_gateway/pool")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	var got []any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("expected at least one pool in response")
	}
	// Find the "auto" pool in the response (there may be more in an env with
	// live credentials set, matching the pattern of existing tests).
	var found bool
	for _, p := range got {
		if pm, ok := p.(map[string]any); ok && pm["pool"] == "auto" {
			found = true
			if _, hasMem := pm["members"]; !hasMem {
				t.Error("auto pool entry missing members field")
			}
			break
		}
	}
	if !found {
		t.Errorf("auto pool not found in response: %v", got)
	}
}

// TestPoolHandler_unknownPool verifies /_gateway/pool?pool=<unknown> returns 404.
func TestPoolHandler_unknownPool(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := auto.NewPools(registry, nil, nil, io.Discard)

	srv := httptest.NewServer(poolHandler(quota.NewStore(), pools))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/_gateway/pool?pool=nonexistent")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", resp.StatusCode)
	}
}

// TestPoolHandler_methodGuard verifies non-GET returns 405 with Allow: GET.
func TestPoolHandler_methodGuard(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := auto.NewPools(registry, nil, nil, io.Discard)

	srv := httptest.NewServer(poolHandler(quota.NewStore(), pools))
	t.Cleanup(srv.Close)

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req, err := http.NewRequest(method, srv.URL+"/_gateway/pool", nil)
		if err != nil {
			t.Fatalf("NewRequest %s: %v", method, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s status=%d, want 405", method, resp.StatusCode)
		}
		if allow := resp.Header.Get("Allow"); allow != http.MethodGet {
			t.Errorf("%s Allow=%q, want GET", method, allow)
		}
	}
}

// TestPersist_roundTrip verifies that persisted state survives a simulated
// restart: write state to a file, reload it, and confirm sticky + snapshots
// are restored.
func TestPersist_roundTrip(t *testing.T) {
	dir := t.TempDir()
	stateFile := dir + "/state.json"

	t.Setenv("AQG_POOL_AUTO_BACKEND_CCW", "sk-ant-ccw")
	t.Setenv("AQG_POOL_AUTO_BACKEND_CCH", "sk-ant-cch")
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	store := quota.NewStore()
	util5h := 0.55
	store.Put("auto/ccw", quota.Snapshot{UnifiedStatus: "allowed", Unified5hUtilization: &util5h})
	pools := auto.NewPools(registry, nil, nil, io.Discard)

	// Build and write persisted state with ccw as sticky.
	ps := persist.GatewayState{
		Pools:     pools.PersistState(),
		Snapshots: store.Snapshot(),
	}
	if poolState, ok := ps.Pools["auto"]; ok {
		poolState.Sticky = "ccw"
		ps.Pools["auto"] = poolState
	}
	data, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(stateFile, data, 0600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	// Simulate restart: load state, restore into fresh pools.
	loaded, err := persist.Load(stateFile)
	if err != nil {
		t.Fatalf("persist.Load: %v", err)
	}
	store2 := quota.NewStore()
	for key, snap := range loaded.Snapshots {
		store2.Put(key, snap)
	}
	pools2 := auto.NewPools(registry, store2, nil, io.Discard)
	pools2.LoadPersistState(loaded.Pools)

	srv := httptest.NewServer(quotaHandler(store2, pools2))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/_gateway/quota?backend=auto")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["active_backend"] != "ccw" {
		t.Errorf("active_backend=%v, want ccw (sticky should survive restart)", got["active_backend"])
	}
	if got["unified_5h_utilization"] != 0.55 {
		t.Errorf("unified_5h_utilization=%v, want 0.55 (snapshot should survive restart)", got["unified_5h_utilization"])
	}
}

// TestPersist_missingFileStartsFresh verifies that a missing state file is
// not an error and the gateway starts normally.
func TestPersist_missingFileStartsFresh(t *testing.T) {
	state, err := persist.Load("/tmp/does-not-exist-agq-test-state.json")
	if err != nil {
		t.Fatalf("Load missing file: want nil error, got %v", err)
	}
	if len(state.Pools) != 0 || len(state.Snapshots) != 0 {
		t.Errorf("expected empty state for missing file, got %+v", state)
	}
}

// TestPersist_corruptFileStartsFresh verifies an unparseable file logs and
// starts fresh rather than failing startup.
func TestPersist_corruptFileStartsFresh(t *testing.T) {
	dir := t.TempDir()
	stateFile := dir + "/corrupt.json"
	if err := os.WriteFile(stateFile, []byte("not json {{{"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	state, err := persist.Load(stateFile)
	if err != nil {
		t.Fatalf("Load corrupt file: want nil error, got %v", err)
	}
	if len(state.Pools) != 0 || len(state.Snapshots) != 0 {
		t.Errorf("expected empty state for corrupt file, got %+v", state)
	}
}
