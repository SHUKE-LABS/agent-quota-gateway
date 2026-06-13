package proxy_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
)

// newGateway spins up a fake upstream plus a proxy that targets it.
// The fake upstream records the headers it saw, returns a configurable
// response, and (for the streaming test) flushes each chunk on a delay.
func newGateway(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *httptest.Server) {
	t.Helper()
	upstream := httptest.NewServer(handler)
	t.Cleanup(upstream.Close)

	gw, err := proxy.New(upstream.URL, "test-api-key")
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	gwSrv := httptest.NewServer(gw)
	t.Cleanup(gwSrv.Close)
	return gwSrv, upstream
}

func TestProxy_messagesStreamsWithoutBuffering(t *testing.T) {
	// Upstream writes three SSE events with a 100ms gap between them.
	// If the proxy buffers the full response, the client will not see
	// the first event until ~300ms after the request starts.
	var writes atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("anthropic-version", "2023-06-01")
		w.WriteHeader(http.StatusOK)

		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("upstream ResponseWriter is not an http.Flusher")
		}

		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "event: message\ndata: {\"chunk\":%d}\n\n", i)
			flusher.Flush()
			writes.Add(1)
			time.Sleep(100 * time.Millisecond)
		}
	})
	gw, _ := newGateway(t, upstream)

	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Read events line-by-line and time-stamp the first arrival. We
	// give the request a generous 250ms budget; a buffered proxy would
	// need 300ms+ to surface the first event.
	start := time.Now()
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
}

func TestProxy_messagesForwardsAPIKey(t *testing.T) {
	var got string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("x-api-key")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, strings.NewReader(`{"ok":true}`))
	})
	gw, _ := newGateway(t, upstream)

	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	// The client sets a placeholder; the proxy must replace it with
	// the configured value, not pass the client header through.
	req.Header.Set("x-api-key", "client-supplied-attacker-key")
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("anthropic-beta", "prompt-caching-2024-07-31")
	req.Header.Set("X-Custom-Header", "keep-me")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got != "test-api-key" {
		t.Errorf("upstream x-api-key = %q, want test-api-key", got)
	}
}

func TestProxy_messagesPreservesAnthropicHeaders(t *testing.T) {
	var gotVersion, gotBeta, gotCustom string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotVersion = r.Header.Get("anthropic-version")
		gotBeta = r.Header.Get("anthropic-beta")
		gotCustom = r.Header.Get("X-Custom-Header")
		w.WriteHeader(http.StatusOK)
	})
	gw, _ := newGateway(t, upstream)

	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("anthropic-beta", "prompt-caching-2024-07-31")
	req.Header.Set("X-Custom-Header", "keep-me")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()

	if gotVersion != "2023-06-01" {
		t.Errorf("anthropic-version lost: %q", gotVersion)
	}
	if gotBeta != "prompt-caching-2024-07-31" {
		t.Errorf("anthropic-beta lost: %q", gotBeta)
	}
	if gotCustom != "keep-me" {
		t.Errorf("X-Custom-Header lost: %q", gotCustom)
	}
}

func TestProxy_countTokensJSONRoundTrip(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages/count_tokens" {
			t.Errorf("upstream saw path %q, want /v1/messages/count_tokens", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"input_tokens": 42})
	})
	gw, _ := newGateway(t, upstream)

	resp, err := http.Post(gw.URL+"/v1/messages/count_tokens", "application/json", strings.NewReader(`{"model":"claude-haiku-4-5-20251001","messages":[]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["input_tokens"] != 42 {
		t.Errorf("input_tokens = %d, want 42", got["input_tokens"])
	}
}

func TestProxy_errorStatusPropagates(t *testing.T) {
	cases := []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
	}
	for _, code := range cases {
		t.Run(fmt.Sprintf("status_%d", code), func(t *testing.T) {
			upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, http.StatusText(code), code)
			})
			gw, _ := newGateway(t, upstream)

			resp, err := http.Post(gw.URL+"/v1/messages", "application/json", bytes.NewReader([]byte("{}")))
			if err != nil {
				t.Fatalf("post: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != code {
				t.Errorf("status = %d, want %d", resp.StatusCode, code)
			}
		})
	}
}

func TestProxy_unknownPathReturns404(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("upstream should not have been called for unknown path; got %s", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	})
	gw, _ := newGateway(t, upstream)

	resp, err := http.Post(gw.URL+"/v1/unknown", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestProxy_disallowedMethodReturns405(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("upstream should not have been called for non-POST; got %s", r.Method)
	})
	gw, _ := newGateway(t, upstream)

	resp, err := http.Get(gw.URL + "/v1/messages")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}