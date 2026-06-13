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
//   - streaming passthrough works (first SSE event arrives within 250ms)
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
	// stream duration to surface the first event.
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("anthropic-version", "2023-06-01")
		w.Header().Set("anthropic-ratelimit-requests-limit", "1000")
		w.Header().Set("anthropic-ratelimit-requests-remaining", "997")
		w.Header().Set("anthropic-ratelimit-tokens-limit", "80000")
		w.Header().Set("anthropic-ratelimit-tokens-remaining", "79412")
		w.Header().Set("anthropic-organization-id", "org_test123")
		w.WriteHeader(http.StatusOK)

		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("upstream ResponseWriter is not an http.Flusher")
		}
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "event: message\ndata: {\"chunk\":%d}\n\n", i)
			flusher.Flush()
			time.Sleep(50 * time.Millisecond)
		}
	})
	upSrv := httptest.NewServer(upstream)
	t.Cleanup(upSrv.Close)

	// Rebuild the wiring `run()` produces, minus the real config.Load
	// (which reads the environment) and the signal-driven shutdown
	// path (httptest.Server handles cleanup).
	store := quota.NewStore()
	observer := func(resp *http.Response) {
		snap := quota.Extract(resp)
		if !snap.HasData() {
			return
		}
		key := defaultBackendKey
		if resp.Request != nil {
			if v := resp.Request.Header.Get(backendHeader); v != "" {
				key = v
			}
		}
		store.Put(key, snap)
	}
	proxyHandler, err := proxy.New(upSrv.URL, "test-api-key", observer)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/health", healthHandler())
	mux.HandleFunc("/_gateway/quota", quotaHandler(store))
	mux.Handle("/", proxyHandler)

	handler := logging.Middleware(mux)
	gw := httptest.NewServer(handler)
	t.Cleanup(gw.Close)

	// 1. Streaming passthrough.
	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader(`{"secret":"prompt-body-should-not-leak"}`))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "client-supplied-attacker-key")
	req.Header.Set("Authorization", "Bearer secret-token-should-not-leak")
	req.Header.Set("X-Mux-Backend-Nick", "test-backend")

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
	if firstAt > 250*time.Millisecond {
		t.Errorf("first event arrived at %v; proxy appears to buffer (want < 250ms)", firstAt)
	}

	// 2. Quota snapshot readable via /_gateway/quota with the backend
	// key the request named.
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
	if snap.RequestsLimit == nil || *snap.RequestsLimit != 1000 {
		t.Errorf("requests_limit = %v, want 1000", snap.RequestsLimit)
	}
	if snap.RequestsRemaining == nil || *snap.RequestsRemaining != 997 {
		t.Errorf("requests_remaining = %v, want 997", snap.RequestsRemaining)
	}
	if snap.TokensLimit == nil || *snap.TokensLimit != 80000 {
		t.Errorf("tokens_limit = %v, want 80000", snap.TokensLimit)
	}
	if snap.TokensRemaining == nil || *snap.TokensRemaining != 79412 {
		t.Errorf("tokens_remaining = %v, want 79412", snap.TokensRemaining)
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
		"secret-token-should-not-leak",
	} {
		if strings.Contains(logs, banned) {
			t.Errorf("stderr leaked %q\nlogs: %s", banned, logs)
		}
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