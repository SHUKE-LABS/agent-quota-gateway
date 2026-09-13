package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/configfile"
	"github.com/shukebeta/agent-quota-gateway/internal/reqlog"
)

// doDebug dispatches a synthetic request at the /_gateway/debug handler
// directly and decodes the body. dirtyCount is incremented inside the
// markDirty closure the handler runs, so the test sees every successful
// POST's call to configWriter.MarkDirty — the contract issue #301 depends
// on (the toggle persists across restart because the writer flushes).
func doDebug(t *testing.T, pers configfile.PersistenceState, method string, body any, dirtyCount *int) *httptest.ResponseRecorder {
	t.Helper()
	var md func()
	if dirtyCount != nil {
		md = func() { *dirtyCount++ }
	} else {
		md = func() {}
	}
	rec := httptest.NewRecorder()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, "/_gateway/debug", &buf)
	req.Header.Set("Content-Type", "application/json")
	debugHandler(pers, md)(rec, req)
	return rec
}

func setDebug(t *testing.T, v bool) {
	t.Helper()
	old := reqlog.Enabled()
	reqlog.SetEnabled(v)
	t.Cleanup(func() { reqlog.SetEnabled(old) })
}

// TestDebugEndpoint_get reports the live flag — the panel's load path.
// The handler consults reqlog directly so the test seeds reqlog to
// verify the read reaches it.
func TestDebugEndpoint_get(t *testing.T) {
	setDebug(t, true)
	rec := doDebug(t, persistedCleanState(), http.MethodGet, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET code = %d, want 200", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("GET Content-Type = %q", rec.Header().Get("Content-Type"))
	}
	if got := rec.Header().Get("X-AQG-Persistence"); got != "persisted" {
		t.Errorf("persistence header = %q, want persisted", got)
	}
	var body map[string]bool
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if !body["log_requests"] {
		t.Errorf("GET body.log_requests = false, want true")
	}

	setDebug(t, false)
	rec = doDebug(t, persistedCleanState(), http.MethodGet, nil, nil)
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["log_requests"] {
		t.Errorf("GET body.log_requests = true, want false")
	}
}

// TestDebugEndpoint_postFlipsAndMarksDirty is AC1: the toggle flips the
// live flag and schedules a config-file flush, so the new value survives
// a restart in file mode.
func TestDebugEndpoint_postFlipsAndMarksDirty(t *testing.T) {
	setDebug(t, false)
	var dirtyCalls int

	on := true
	rec := doDebug(t, persistedCleanState(), http.MethodPost, map[string]any{"log_requests": on}, &dirtyCalls)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST code = %d, want 200", rec.Code)
	}
	if !reqlog.Enabled() {
		t.Error("reqlog.Enabled() = false after POST log_requests:true")
	}
	if dirtyCalls != 1 {
		t.Errorf("markDirty calls = %d, want 1", dirtyCalls)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["log_requests"] != true {
		t.Errorf("response body.log_requests = %v, want true", body["log_requests"])
	}
	if _, ok := body["persistence"]; ok {
		t.Errorf("persisted clean response carries a persistence body field: %v", body)
	}

	// Flip back off — second POST must also mark dirty (not no-op)
	off := false
	rec = doDebug(t, persistedCleanState(), http.MethodPost, map[string]any{"log_requests": off}, &dirtyCalls)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST(off) code = %d", rec.Code)
	}
	if reqlog.Enabled() {
		t.Error("reqlog.Enabled() = true after POST log_requests:false")
	}
	if dirtyCalls != 2 {
		t.Errorf("markDirty calls after second POST = %d, want 2", dirtyCalls)
	}
}

// TestDebugEndpoint_envOnlyBodyField mirrors the mutation-family contract:
// every successful runtime mutation response carries the persistence body
// field in env-only mode so an API caller sees the in-memory-only state
// directly (issue #246).
func TestDebugEndpoint_envOnlyBodyField(t *testing.T) {
	setDebug(t, false)
	rec := doDebug(t, envOnlyState(), http.MethodPost, map[string]any{"log_requests": true}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST code = %d", rec.Code)
	}
	if got := rec.Header().Get("X-AQG-Persistence"); got != "env_only" {
		t.Errorf("env-only header = %q, want env_only", got)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["persistence"] != "env_only" {
		t.Errorf("env-only body field = %v, want env_only", body["persistence"])
	}
	if body["log_requests"] != true {
		t.Errorf("log_requests = %v, want true", body["log_requests"])
	}
}

// TestDebugEndpoint_postRequiresField covers the *bool requirement: a
// missing log_requests would silently read as "off" and corrupt operator
// intent (issue #301 review). The handler must reject explicitly.
func TestDebugEndpoint_postRequiresField(t *testing.T) {
	setDebug(t, true) // pre-state must NOT flip
	rec := doDebug(t, persistedCleanState(), http.MethodPost, map[string]any{}, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST(empty) code = %d, want 400", rec.Code)
	}
	if reqlog.Enabled() != true {
		t.Error("reqlog flipped on empty body; required-field gate failed")
	}
}

// TestDebugEndpoint_postRejectsMalformedJSON — the body decoder mirrors
// addMember's policy: a truncated body is a real decode failure and
// returns 400, never silently no-ops.
func TestDebugEndpoint_postRejectsMalformedJSON(t *testing.T) {
	setDebug(t, false)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/_gateway/debug", bytes.NewReader([]byte(`{"log_requests":`)))
	req.Header.Set("Content-Type", "application/json")
	debugHandler(persistedCleanState(), func() {})(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("truncated body code = %d, want 400", rec.Code)
	}
	if reqlog.Enabled() {
		t.Error("reqlog flipped on malformed body")
	}
}

// TestDebugEndpoint_methodNotAllowed — only GET and POST are accepted.
// The Allow header advertises both so an operator hitting PUT/DELETE sees
// the supported verbs.
func TestDebugEndpoint_methodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := doDebug(t, persistedCleanState(), method, nil, nil)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s code = %d, want 405", method, rec.Code)
		}
		if got := rec.Header().Get("Allow"); got != "GET, POST" {
			t.Errorf("%s Allow = %q, want GET, POST", method, got)
		}
	}
}

// Note: the end-to-end "wired while off, SetEnabled flips the very next
// request" behavior is covered in internal/reqlog's TestMiddleware_hotToggle
// and TestWrapTransport_hotToggle. The handler test above only owns the
// handler-shape contract (status codes, headers, persistence signals,
// markDirty).

// safeBuffer serializes writes/reads across the HTTP server goroutines and
// the test goroutine. Writers are the request handlers running under the
// httptest server; the reader is the test goroutine between assertions.
// Plain bytes.Buffer is not safe for concurrent use, so this wrapper is.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *safeBuffer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.Reset()
}

// TestDebugEndpoint_liveGatewayHotToggle is AC1 end-to-end (issue #301):
// a real httptest server wired as run() wires it (reqlog.Middleware wrapping
// a mux that owns /_gateway/debug) must obey SetEnabled on the very next
// inbound request, in both directions, with no rewire. This catches a
// routing or middleware wiring regression that the per-package unit tests
// (reqlog's hot-toggle, the handler's status codes) cannot — they exercise
// the components separately. A captured dump proves the middleware consulted
// the live flag through the actual request path.
func TestDebugEndpoint_liveGatewayHotToggle(t *testing.T) {
	var captured safeBuffer
	reqlog.SetDebugOutput(&captured)
	t.Cleanup(func() { reqlog.SetDebugOutput(nil) })

	setDebug(t, false)
	markDirty := func() {}

	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/debug", debugHandler(persistedCleanState(), markDirty))
	mux.HandleFunc("/v1/messages", func(w http.ResponseWriter, r *http.Request) {
		// Drain so the middleware's restore path stays honest.
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(reqlog.Middleware(mux))
	defer srv.Close()

	postDebug := func(v bool) {
		t.Helper()
		body := fmt.Sprintf(`{"log_requests":%v}`, v)
		resp, err := http.Post(srv.URL+"/_gateway/debug", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST debug(%v): %v", v, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST debug(%v) code = %d, want 200", v, resp.StatusCode)
		}
	}

	hitUpstream := func(label string) {
		t.Helper()
		resp, err := http.Post(srv.URL+"/v1/messages", "application/json", strings.NewReader("hello-body"))
		if err != nil {
			t.Fatalf("POST upstream (%s): %v", label, err)
		}
		resp.Body.Close()
	}

	// 1. Pre-toggle: the request must not dump. The "outbound" arm is
	// the WrapTransport path, not exercised here (no real upstream in
	// this test); the "inbound" arm is Middleware. With logging off,
	// Middleware short-circuits and the dump sink stays empty.
	hitUpstream("pre-toggle")
	if got := captured.String(); strings.Contains(got, "hello-body") {
		t.Errorf("pre-toggle dump should be empty, got: %q", got)
	}

	// 2. Flip on through the live endpoint — this is the moment under test.
	postDebug(true)
	if !reqlog.Enabled() {
		t.Fatal("POST /_gateway/debug true did not flip the live flag")
	}

	// 3. The very next request must dump. Captured via the injected sink,
	// proving the handler chain (reqlog → mux → handler) consults the
	// atomic on the request path the live toggle reached.
	captured.Reset()
	hitUpstream("post-enable")
	if got := captured.String(); !strings.Contains(got, "hello-body") {
		t.Errorf("post-enable dump missing body, got: %q", got)
	}

	// 4. Flip off through the same endpoint — same atomic, same chain.
	postDebug(false)
	if reqlog.Enabled() {
		t.Fatal("POST /_gateway/debug false did not flip the live flag")
	}

	// 5. The next request must not dump.
	captured.Reset()
	hitUpstream("post-disable")
	if got := captured.String(); strings.Contains(got, "hello-body") {
		t.Errorf("post-disable dump should be empty, got: %q", got)
	}
}