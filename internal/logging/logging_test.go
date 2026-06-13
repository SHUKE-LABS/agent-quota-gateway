package logging

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
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