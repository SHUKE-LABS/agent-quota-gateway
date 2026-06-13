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

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
)

// testAPIKey is an Anthropic API key (sk-ant-api prefix), which the proxy
// stamps via the x-api-key header.
const testAPIKey = "sk-ant-api-test-key"

// injectBackend wraps a handler so every request arrives with b on its
// context, standing in for the resolver middleware the real gateway runs
// in front of the proxy.
func injectBackend(b backend.Backend, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(backend.WithBackend(r.Context(), b)))
	})
}

// newGateway spins up a fake upstream plus a proxy, fronted by a backend
// whose BaseURL targets that upstream and whose credential is an API key
// (so the proxy uses the x-api-key scheme). The fake upstream records the
// headers it saw, returns a configurable response, and (for the streaming
// test) flushes each chunk on a delay.
func newGateway(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *httptest.Server) {
	t.Helper()
	upstream := httptest.NewServer(handler)
	t.Cleanup(upstream.Close)

	gw, err := proxy.New(nil, nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	b := backend.Backend{Pool: "api", Nick: "default", Credential: testAPIKey, BaseURL: upstream.URL}
	gwSrv := httptest.NewServer(injectBackend(b, gw))
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
	if got != testAPIKey {
		t.Errorf("upstream x-api-key = %q, want %q", got, testAPIKey)
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

// TestProxy_unknownPathForwardsToUpstream proves the proxy no longer
// whitelists paths: an arbitrary path (here a plausible future Anthropic
// endpoint) reaches the upstream with the auth header stamped, and the
// upstream's response is returned verbatim. The upstream — not a closed
// route table in the gateway — is the authority on what it serves.
func TestProxy_unknownPathForwardsToUpstream(t *testing.T) {
	var gotPath, gotKey string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKey = r.Header.Get("x-api-key")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, strings.NewReader(`{"data":["claude-opus-4-8"]}`))
	})
	gw, _ := newGateway(t, upstream)

	resp, err := http.Post(gw.URL+"/v1/models", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (unknown path must forward to upstream)", resp.StatusCode)
	}
	if gotPath != "/v1/models" {
		t.Errorf("upstream saw path %q, want /v1/models", gotPath)
	}
	if gotKey != testAPIKey {
		t.Errorf("upstream x-api-key = %q, want %q", gotKey, testAPIKey)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), "claude-opus-4-8") {
		t.Errorf("body = %q, want upstream payload forwarded", string(body))
	}
}

// TestProxy_perBackendBaseURLAndPathPrefix proves the upstream is taken
// from the resolved backend (not a single construction-time URL) and that
// a base URL carrying a path prefix (a non-native pool, e.g.
// https://host/anthropic) has that prefix preserved on the forwarded
// request.
func TestProxy_perBackendBaseURLAndPathPrefix(t *testing.T) {
	var gotPath string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
	upSrv := httptest.NewServer(upstream)
	t.Cleanup(upSrv.Close)

	gw, err := proxy.New(nil, nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	b := backend.Backend{Pool: "z-ai", Nick: "x", Credential: "znative", BaseURL: upSrv.URL + "/anthropic"}
	gwSrv := httptest.NewServer(injectBackend(b, gw))
	t.Cleanup(gwSrv.Close)

	resp, err := http.Post(gwSrv.URL+"/v1/messages", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()

	if gotPath != "/anthropic/v1/messages" {
		t.Errorf("upstream saw path %q, want /anthropic/v1/messages (pool base-URL prefix preserved)", gotPath)
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

// TestProxy_observerFiresWithResponse confirms the ModifyResponse hook
// runs once per upstream round-trip and receives a response whose Header
// set and Request still reflect what the upstream sent and what the
// client originally asked for. This is the integration point quota
// capture relies on; if it ever stops firing, the snapshot cache goes
// silently stale.
func TestProxy_observerFiresWithResponse(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("anthropic-ratelimit-unified-5h-utilization", "0.42")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	upSrv := httptest.NewServer(upstream)
	t.Cleanup(upSrv.Close)

	var seenHeader, seenKey string
	var calls atomic.Int32
	observer := func(resp *http.Response) {
		calls.Add(1)
		seenHeader = resp.Header.Get("anthropic-ratelimit-unified-5h-utilization")
		if resp.Request != nil {
			if b, ok := backend.FromContext(resp.Request.Context()); ok {
				seenKey = b.QuotaKey()
			}
		}
	}

	gw, err := proxy.New(observer, nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	b := backend.Backend{Pool: "auto", Nick: "mybackend", Credential: testAPIKey, BaseURL: upSrv.URL}
	gwSrv := httptest.NewServer(injectBackend(b, gw))
	t.Cleanup(gwSrv.Close)

	req, _ := http.NewRequest(http.MethodPost, gwSrv.URL+"/v1/messages", strings.NewReader("{}"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()

	if got := calls.Load(); got != 1 {
		t.Errorf("observer call count = %d, want 1", got)
	}
	if seenHeader != "0.42" {
		t.Errorf("observer saw unified-5h-utilization = %q, want 0.42", seenHeader)
	}
	if seenKey != "auto/mybackend" {
		t.Errorf("observer saw quota key = %q, want auto/mybackend (resolved backend must reach ModifyResponse via context)", seenKey)
	}
}

// TestProxy_observerNotCalledForRejectedRequests proves the hook does
// not fire for requests the proxy rejects before they reach the
// upstream. A non-POST method → no observer call, since there is no
// upstream response to inspect.
func TestProxy_observerNotCalledForRejectedRequests(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("upstream must not be hit; got %s %s", r.Method, r.URL.Path)
	})
	upSrv := httptest.NewServer(upstream)
	t.Cleanup(upSrv.Close)

	var calls atomic.Int32
	gw, err := proxy.New(func(*http.Response) { calls.Add(1) }, nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	b := backend.Backend{Pool: "api", Nick: "default", Credential: testAPIKey, BaseURL: upSrv.URL}
	gwSrv := httptest.NewServer(injectBackend(b, gw))
	t.Cleanup(gwSrv.Close)

	resp, err := http.Get(gwSrv.URL + "/v1/messages")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()

	if got := calls.Load(); got != 0 {
		t.Errorf("observer call count = %d, want 0 (rejected requests must not invoke observer)", got)
	}
}

// oauthBetaValue mirrors the proxy's internal oauthBeta constant; the
// external test package can't reach the unexported one.
const oauthBetaValue = "oauth-2025-04-20"

// newGatewayWithKey is like newGateway but lets the test choose the
// backend credential so the three auth schemes can be exercised.
func newGatewayWithKey(t *testing.T, key string, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	upstream := httptest.NewServer(handler)
	t.Cleanup(upstream.Close)

	gw, err := proxy.New(nil, nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	b := backend.Backend{Pool: "test", Nick: "test", Credential: key, BaseURL: upstream.URL}
	gwSrv := httptest.NewServer(injectBackend(b, gw))
	t.Cleanup(gwSrv.Close)
	return gwSrv
}

func TestProxy_oauthTokenUsesBearer(t *testing.T) {
	const token = "sk-ant-oat01-secret-token"
	var gotAuth, gotKey, gotBeta string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotKey = r.Header.Get("x-api-key")
		gotBeta = r.Header.Get("anthropic-beta")
		w.WriteHeader(http.StatusOK)
	})
	gw := newGatewayWithKey(t, token, upstream)

	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	// A client placeholder x-api-key must be dropped, not forwarded.
	req.Header.Set("x-api-key", "client-placeholder")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()

	if gotAuth != "Bearer "+token {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer "+token)
	}
	if gotKey != "" {
		t.Errorf("x-api-key = %q, want empty (OAuth tokens must not be sent as x-api-key)", gotKey)
	}
	if gotBeta != oauthBetaValue {
		t.Errorf("anthropic-beta = %q, want %q", gotBeta, oauthBetaValue)
	}
}

// TestProxy_nonNativeTokenUsesBearerWithoutBeta proves a credential that
// is neither an OAuth token nor an API key (a non-native Claude-compatible
// provider's key) is sent as a plain Bearer with no anthropic-beta flag.
func TestProxy_nonNativeTokenUsesBearerWithoutBeta(t *testing.T) {
	const token = "znative-compatible-key"
	var gotAuth, gotKey, gotBeta string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotKey = r.Header.Get("x-api-key")
		gotBeta = r.Header.Get("anthropic-beta")
		w.WriteHeader(http.StatusOK)
	})
	gw := newGatewayWithKey(t, token, upstream)

	req, _ := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader("{}"))
	req.Header.Set("x-api-key", "client-placeholder")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()

	if gotAuth != "Bearer "+token {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer "+token)
	}
	if gotKey != "" {
		t.Errorf("x-api-key = %q, want empty (Bearer providers must not get x-api-key)", gotKey)
	}
	if gotBeta != "" {
		t.Errorf("anthropic-beta = %q, want empty (no oauth beta for non-native providers)", gotBeta)
	}
}

func TestProxy_oauthTokenPreservesClientBeta(t *testing.T) {
	const token = "sk-ant-oat01-secret-token"
	var gotBeta string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBeta = r.Header.Get("anthropic-beta")
		w.WriteHeader(http.StatusOK)
	})
	gw := newGatewayWithKey(t, token, upstream)

	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("anthropic-beta", "prompt-caching-2024-07-31")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()

	want := "prompt-caching-2024-07-31," + oauthBetaValue
	if gotBeta != want {
		t.Errorf("anthropic-beta = %q, want %q (client beta preserved, oauth flag appended once)", gotBeta, want)
	}
}

func TestProxy_oauthTokenDoesNotDuplicateBeta(t *testing.T) {
	const token = "sk-ant-oat01-secret-token"
	var gotBeta string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBeta = r.Header.Get("anthropic-beta")
		w.WriteHeader(http.StatusOK)
	})
	gw := newGatewayWithKey(t, token, upstream)

	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/messages", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	// Client already sent the oauth flag; it must not be duplicated.
	req.Header.Set("anthropic-beta", oauthBetaValue)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()

	if gotBeta != oauthBetaValue {
		t.Errorf("anthropic-beta = %q, want %q (no duplication)", gotBeta, oauthBetaValue)
	}
}
