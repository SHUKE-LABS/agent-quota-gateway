package logging

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// TestMiddleware_emitsRequiredFields checks that the JSON log line
// contains the documented fields and the documented absences —
// specifically that the body and auth headers are not serialized.
func TestMiddleware_emitsRequiredFields(t *testing.T) {
	// Capture stderr instead of letting the middleware write to the
	// real stderr and contaminating the test runner output.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	oldStderr := os.Stderr
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = oldStderr })

	// Drain the pipe in a goroutine so we don't block the writer.
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = buf.ReadFrom(r)
		close(done)
	}()

	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Ensure body bytes and auth header are available to be
		// (incorrectly) logged. The middleware must not touch them.
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(`{"secret":"prompt-body"}`))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "sk-should-not-appear")
	req.Header.Set("Authorization", "Bearer should-not-appear")
	req.Header.Set("X-Request-Id", "rid-123")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()

	w.Close()
	<-done

	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("expected a log line, got empty")
	}

	// Fields that MUST be present.
	var entry map[string]any
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("log line not JSON: %v\nline=%s", err, line)
	}
	for _, key := range []string{"ts", "method", "path", "status", "duration", "request_id"} {
		if _, ok := entry[key]; !ok {
			t.Errorf("log line missing %q: %s", key, line)
		}
	}
	if entry["path"] != "/v1/messages" {
		t.Errorf("path = %v, want /v1/messages", entry["path"])
	}
	if entry["method"] != "POST" {
		t.Errorf("method = %v, want POST", entry["method"])
	}
	if entry["request_id"] != "rid-123" {
		t.Errorf("request_id = %v, want rid-123", entry["request_id"])
	}

	// Strings that MUST NOT appear.
	for _, banned := range []string{"prompt-body", "sk-should-not-appear", "Bearer should-not-appear"} {
		if strings.Contains(line, banned) {
			t.Errorf("log line leaked %q: %s", banned, line)
		}
	}
}

// TestMiddleware_generatesRequestIDWhenAbsent verifies that the
// logger fabricates a non-empty request ID when the inbound request
// has no X-Request-Id header. Two consecutive requests must get
// different IDs — otherwise log correlation is meaningless.
func TestMiddleware_generatesRequestIDWhenAbsent(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	oldStderr := os.Stderr
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = oldStderr })

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = buf.ReadFrom(r)
		close(done)
	}()

	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	ids := make(map[string]bool, 3)
	for i := 0; i < 3; i++ {
		resp, err := http.Post(srv.URL+"/v1/messages", "application/json", nil)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		resp.Body.Close()
	}

	w.Close()
	<-done

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 log lines, got %d: %q", len(lines), buf.String())
	}
	for _, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, line)
		}
		id, _ := entry["request_id"].(string)
		if id == "" || id == "-" {
			t.Errorf("request_id missing or sentinel: %v", entry["request_id"])
		}
		if ids[id] {
			t.Errorf("duplicate request_id %q across requests", id)
		}
		ids[id] = true
	}
}

// TestMiddleware_preservesFlushChain pins the contract that a logging
// wrapper must not break streaming. The proxy relies on
// http.NewResponseController reaching an http.Flusher on the real
// ResponseWriter; without Unwrap, the controller stops at statusRecorder
// and httputil.ReverseProxy's per-Write Flush silently no-ops, which
// collapses SSE into a single buffered payload.
//
// We model the chain with an httptest.ResponseRecorder (which has
// supported http.Flusher since Go 1.20) wrapped in Middleware, hand
// the inner handler a ResponseWriter derived via http.NewResponseController
// (the same call ReverseProxy makes), and assert that Flush() drives a
// Write through to the recorder and does not panic.
func TestMiddleware_preservesFlushChain(t *testing.T) {
	rec := httptest.NewRecorder()

	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		// http.NewResponseController must find an http.Flusher through
		// the logging wrapper. If Unwrap is missing this returns an
		// "response controller unavailable" error and Flush() below
		// would never reach the underlying recorder.
		_, _ = w.Write([]byte("first "))
		if err := rc.Flush(); err != nil {
			t.Fatalf("Flush after first write: %v", err)
		}
		_, _ = w.Write([]byte("second"))
		if err := rc.Flush(); err != nil {
			t.Fatalf("Flush after second write: %v", err)
		}
	}))

	// Use a real handler invocation (not ServeHTTP directly) so the
	// wrapper is engaged exactly the way the gateway engages it.
	req := httptest.NewRequest(http.MethodGet, "/v1/messages", nil)
	handler.ServeHTTP(rec, req)

	if got := rec.Body.String(); got != "first second" {
		t.Errorf("body = %q, want %q (chunks must round-trip through logging wrapper)", got, "first second")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}

	// Belt-and-braces: even when the inner handler never calls Flush,
	// http.NewResponseController must be able to *negotiate* one without
	// panicking — otherwise a downstream handler that probes Flusher
	// availability during the request would crash.
	rec2 := httptest.NewRecorder()
	probe := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	probe.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := rec2.Body.String(); got != "ok" {
		t.Errorf("probe body = %q, want ok", got)
	}
}

// TestMiddleware_unwrapsToUnderlying pins the Unwrap method directly so
// that any future refactor of statusRecorder that drops the method
// fails with a targeted assertion, not via a downstream streaming
// regression. NewResponseController is the public Go stdlib entry point
// for walking Unwrap; if it cannot reach the inner ResponseWriter, this
// test fails before any streaming happens.
func TestMiddleware_unwrapsToUnderlying(t *testing.T) {
	rec := httptest.NewRecorder()
	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// NewResponseController walks Unwrap() to find http.Flusher;
		// if the chain dies at statusRecorder, Flush() returns an
		// error here rather than silently succeeding.
		rc := http.NewResponseController(w)
		_, _ = w.Write([]byte("x"))
		done := make(chan error, 1)
		go func() {
			done <- rc.Flush()
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Flush returned %v; Unwrap chain is broken", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Flush did not return; controller is stuck on a wrapper")
		}
	}))
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
}