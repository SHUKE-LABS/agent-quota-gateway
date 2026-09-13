package reqlog

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// setEnabled overrides the live gate for the duration of a test, restoring it
// afterwards. SetEnabled/Enabled are the public surface the /_gateway/debug
// handler uses (issue #301); tests go through them too so the toggle path
// itself stays covered.
func setEnabled(t *testing.T, v bool) {
	t.Helper()
	old := Enabled()
	SetEnabled(v)
	t.Cleanup(func() { SetEnabled(old) })
}

// captureStderr redirects os.Stderr through a pipe while fn runs and returns
// what was written. The package writes dumps directly to os.Stderr.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	fn()
	w.Close()
	os.Stderr = old
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stderr: %v", err)
	}
	return string(out)
}

// recordingTransport is a stub RoundTripper that records the request it saw.
type recordingTransport struct {
	seen   *http.Request
	called bool
}

func (rt *recordingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rt.called = true
	rt.seen = r
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

// TestWrapTransport_disabledPassesThrough verifies the off path: the wrapper
// is still installed (it must be, for the runtime toggle to reach it) but
// consults the flag per round-trip and stays silent when off.
func TestWrapTransport_disabledPassesThrough(t *testing.T) {
	setEnabled(t, false)
	inner := &recordingTransport{}
	rt := WrapTransport(inner)

	req, _ := http.NewRequest(http.MethodGet, "https://upstream.example/v1", nil)
	out := captureStderr(t, func() {
		if _, err := rt.RoundTrip(req); err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
	})
	if !inner.called {
		t.Error("inner RoundTripper was not called")
	}
	if out != "" {
		t.Errorf("disabled transport should dump nothing, got:\n%s", out)
	}
}

func TestWrapTransport_enabledRedactsAndDelegates(t *testing.T) {
	setEnabled(t, true)
	inner := &recordingTransport{}
	rt := WrapTransport(inner)

	req, _ := http.NewRequest(http.MethodGet, "https://upstream.example/v1", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("X-Api-Key", "sk-secret")
	req.Header.Set("X-Custom", "passthrough")

	out := captureStderr(t, func() {
		if _, err := rt.RoundTrip(req); err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
	})

	if !inner.called {
		t.Error("inner RoundTripper was not called")
	}
	if strings.Contains(out, "secret-token") || strings.Contains(out, "sk-secret") {
		t.Errorf("credential leaked into dump:\n%s", out)
	}
	for _, want := range []string{"Authorization: [redacted]", "X-Api-Key: [redacted]", "X-Custom: passthrough"} {
		if !strings.Contains(out, want) {
			t.Errorf("dump missing %q:\n%s", want, out)
		}
	}
}

// TestWrapTransport_hotToggle is the issue #301 core behavior for the
// outbound path: a transport wired while logging is off starts dumping the
// moment SetEnabled(true) lands, without any rewire, and stops again on
// SetEnabled(false).
func TestWrapTransport_hotToggle(t *testing.T) {
	setEnabled(t, false)
	inner := &recordingTransport{}
	rt := WrapTransport(inner)
	req, _ := http.NewRequest(http.MethodGet, "https://upstream.example/v1", nil)

	out := captureStderr(t, func() {
		_, _ = rt.RoundTrip(req)
	})
	if out != "" {
		t.Fatalf("pre-toggle round-trip should dump nothing, got:\n%s", out)
	}

	out = captureStderr(t, func() {
		SetEnabled(true)
		_, _ = rt.RoundTrip(req)
	})
	if !strings.Contains(out, ">>> outbound GET") {
		t.Errorf("post-enable round-trip should dump, got:\n%s", out)
	}

	out = captureStderr(t, func() {
		SetEnabled(false)
		_, _ = rt.RoundTrip(req)
	})
	if out != "" {
		t.Errorf("post-disable round-trip should dump nothing, got:\n%s", out)
	}
}

// TestMiddleware_disabledPassesThrough verifies the off path: middleware is
// always installed (the chain cannot be rewired once the server listens) and
// dumps nothing while the flag is off.
func TestMiddleware_disabledPassesThrough(t *testing.T) {
	setEnabled(t, false)
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader([]byte("hello-body")))
	out := captureStderr(t, func() {
		Middleware(next).ServeHTTP(httptest.NewRecorder(), req)
	})
	if out != "" {
		t.Errorf("disabled middleware should dump nothing, got:\n%s", out)
	}
}

// TestMiddleware_hotToggle is the issue #301 core behavior for the inbound
// path: a handler chain wired while logging is off obeys SetEnabled on the
// very next request, in both directions.
func TestMiddleware_hotToggle(t *testing.T) {
	setEnabled(t, false)
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	h := Middleware(next)

	newReq := func() *http.Request {
		return httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader([]byte("hello-body")))
	}

	out := captureStderr(t, func() {
		h.ServeHTTP(httptest.NewRecorder(), newReq())
	})
	if out != "" {
		t.Fatalf("pre-toggle request should dump nothing, got:\n%s", out)
	}

	out = captureStderr(t, func() {
		SetEnabled(true)
		h.ServeHTTP(httptest.NewRecorder(), newReq())
	})
	if !strings.Contains(out, "hello-body") {
		t.Errorf("post-enable request should dump, got:\n%s", out)
	}

	out = captureStderr(t, func() {
		SetEnabled(false)
		h.ServeHTTP(httptest.NewRecorder(), newReq())
	})
	if out != "" {
		t.Errorf("post-disable request should dump nothing, got:\n%s", out)
	}
}

func TestMiddleware_enabledRedactsAndRestoresBody(t *testing.T) {
	setEnabled(t, true)

	var gotBody string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader([]byte("hello-body")))
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("X-Custom", "passthrough")

	out := captureStderr(t, func() {
		Middleware(next).ServeHTTP(httptest.NewRecorder(), req)
	})

	if gotBody != "hello-body" {
		t.Errorf("downstream body = %q, want it restored to %q", gotBody, "hello-body")
	}
	if strings.Contains(out, "secret-token") {
		t.Errorf("credential leaked into dump:\n%s", out)
	}
	if !strings.Contains(out, "Authorization: [redacted]") {
		t.Errorf("dump missing redacted Authorization:\n%s", out)
	}
	if !strings.Contains(out, "X-Custom: passthrough") {
		t.Errorf("dump missing passthrough header:\n%s", out)
	}
	if !strings.Contains(out, "hello-body") {
		t.Errorf("dump missing body preview:\n%s", out)
	}
}

func TestMiddleware_enabledTruncatesLargeBody(t *testing.T) {
	setEnabled(t, true)

	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	large := strings.Repeat("x", 600)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader([]byte(large)))

	out := captureStderr(t, func() {
		Middleware(next).ServeHTTP(httptest.NewRecorder(), req)
	})

	if !strings.Contains(out, "600 bytes total)") {
		t.Errorf("large body should report total size with truncation suffix:\n%s", out)
	}
	// Only the first 500 bytes are previewed, so the full 600-char run must not appear.
	if strings.Contains(out, large) {
		t.Errorf("full body should be truncated in the dump:\n%s", out)
	}
}
