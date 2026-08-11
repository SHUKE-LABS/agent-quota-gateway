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

// sseAttempt is the outcome of one streaming exchange through the proxy.
// firstAt is zero when no event ever arrived.
type sseAttempt struct {
	firstAt time.Duration
	events  int
	writes  int32
	scanErr error
}

// streamSSEOnce runs a single streaming exchange: a fake upstream writes three
// SSE events with a 100ms gap between them, and the client reads the proxied
// body to EOF, time-stamping the first event. Each call builds its own gateway
// and its own write counter, so attempts never pool their counts.
func streamSSEOnce(t *testing.T) sseAttempt {
	t.Helper()

	var writes atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		// Not t.Fatal: this runs on the httptest server's goroutine, and
		// FailNow off the test goroutine is undefined. httptest's
		// ResponseWriter always implements Flusher, so this only guards
		// against a future change to the harness.
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("upstream ResponseWriter is not an http.Flusher")
			return
		}

		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "event: response.output_text.delta\ndata: {\"chunk\":%d}\n\n", i)
			flusher.Flush()
			writes.Add(1)
			// Sleeping after the final write as well keeps the script
			// 300ms long, which is what makes the caller's 250ms budget
			// discriminate: drop this and a fully buffered response
			// would surface its first event at ~200ms and pass.
			time.Sleep(100 * time.Millisecond)
		}
	})
	gw, _ := newGateway(t, upstream)

	// Deliberately bodyless. net/http defers the inbound connection's
	// background read until the request body hits EOF, which for a
	// request with a body lands in the middle of the streamed response
	// and, under CPU contention, cancels the request context and kills
	// the upstream connection mid-stream. A bodyless request starts that
	// read up front instead (net/http/server.go, registerOnHitEOF vs the
	// direct startBackgroundRead call). Measured with a stdlib-only
	// reverse proxy, 200 runs each under saturating load: with a body
	// 11-19 aborts, bodyless POST 0, bodyless GET 0.
	//
	// Nothing in the flush contract depends on the request body, and
	// body passthrough is asserted by
	// TestProxy_responsesJSONPassesThroughOpaque.
	req, err := http.NewRequest(http.MethodPost, gw.URL+"/v1/responses", nil)
	if err != nil {
		t.Fatalf("req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Time from sending the request, not from the headers coming back. A
	// proxy that buffers the whole upstream response before returning
	// anything delays its headers too, so a clock started after Do would
	// begin only once that delay had already elapsed and would measure
	// ~0ms to the first event.
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

	out := sseAttempt{}
	for scanner.Scan() {
		if !strings.HasPrefix(scanner.Text(), "event: response.output_text.delta") {
			continue
		}
		if out.firstAt == 0 {
			out.firstAt = time.Since(start)
		}
		out.events++
	}
	out.scanErr = scanner.Err()
	out.writes = writes.Load()
	return out
}

// TestProxy_responsesStreamsWithoutBuffering proves the proxy surfaces SSE
// frames as they arrive instead of holding them until the upstream finishes.
// The upstream takes 300ms to write three events, so a buffering proxy cannot
// deliver the first one inside the 250ms budget.
//
// The request is bodyless to dodge a net/http race that has nothing to do with
// this gateway: see streamSSEOnce. Under CPU saturation the upstream connection
// was closed mid-body, ReverseProxy logged a read error and panicked with
// http.ErrAbortHandler, and the client's scanner saw `unexpected EOF` after a
// single event. A pure-stdlib httputil.NewSingleHostReverseProxy with no
// gateway code in the path reproduced it identically (issue #294).
//
// The bounded retry is a safety net for whatever residual environmental abort
// remains. It is not the primary fix, and it cannot be: the aborts are
// correlated inside a contention burst, so a run that hits one usually hits it
// on every attempt.
//
// Both regression directions still fail deterministically: a buffering proxy
// blows the 250ms budget on every attempt with no scan error, and a genuinely
// truncating one aborts on every attempt.
//
// Note this test does not guard `rp.FlushInterval = -1`. The stdlib forces
// immediate flushing for any text/event-stream response and for any response of
// unknown length (httputil.ReverseProxy.flushInterval), and this upstream is
// both, so the field is never consulted here.
func TestProxy_responsesStreamsWithoutBuffering(t *testing.T) {
	const attempts = 3

	var got sseAttempt
	aborted := make([]sseAttempt, 0, attempts)
	for i := 0; i < attempts; i++ {
		got = streamSSEOnce(t)
		if got.scanErr == nil {
			break
		}
		// Never retry silently: a retry that stops being rare is worth
		// seeing in the log before it becomes a failure.
		t.Logf("attempt %d/%d cut short, retrying: %v (events=%d writes=%d firstAt=%v)",
			i+1, attempts, got.scanErr, got.events, got.writes, got.firstAt)
		aborted = append(aborted, got)
	}

	if got.scanErr != nil {
		// Every attempt was cut short. That is the signature of a real
		// truncation regression, so it stays loud and reports the
		// counters — the original flake was undiagnosable without them.
		for i, a := range aborted {
			t.Errorf("attempt %d/%d: scan: %v (events=%d writes=%d firstAt=%v)",
				i+1, attempts, a.scanErr, a.events, a.writes, a.firstAt)
		}
		t.Fatalf("all %d streaming attempts were cut short mid-body", attempts)
	}

	if got.firstAt == 0 {
		t.Fatalf("never received first SSE event (events=%d writes=%d)", got.events, got.writes)
	}
	if got.firstAt > 250*time.Millisecond {
		t.Errorf("first event arrived at %v; proxy appears to buffer (want < 250ms)", got.firstAt)
	}
	if got.events != 3 || got.writes != 3 {
		t.Errorf("streamed events = %d, upstream writes = %d, want 3 each", got.events, got.writes)
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

// TestProxy_normalizeRequestPath covers the generic /v1 prefix normalization
// (issue #157): root-mounted upstreams receive exactly one leading /v1
// segment, while a base URL ending in /v1 consumes inbound /v1 segments before
// joining. Other base-path prefixes retain their existing join behavior.
func TestProxy_normalizeRequestPath(t *testing.T) {
	cases := []struct {
		name     string
		basePath string // appended to the fake upstream URL; "" = root-mounted
		inbound  string
		want     string
	}{
		// 1. Claude Code's working path is untouched (regression guard).
		{"native_single_v1", "", "/v1/messages", "/v1/messages"},
		// 2-3. SDKs that double / triple the prefix collapse to one.
		{"native_double_v1", "", "/v1/v1/messages", "/v1/messages"},
		{"native_triple_v1", "", "/v1/v1/v1/messages", "/v1/messages"},
		// 4. OpenCode / Codex consume /v1 from the base URL → gains it back.
		{"native_bare_messages", "", "/messages", "/v1/messages"},
		{"native_bare_responses", "", "/responses", "/v1/responses"},
		{"native_responses", "", "/v1/responses", "/v1/responses"},
		// 5. A bare subpath gains the prefix.
		{"native_bare_subpath", "", "/some/random/path", "/v1/some/random/path"},
		// 6. Only the duplicate prefix collapses; the suffix passes through.
		{"native_double_v1_subpath", "", "/v1/v1/some/other/path", "/v1/some/other/path"},
		// 8. A root-mounted Anthropic-compat vendor (not native) gets the
		// same fix — locks in the wide scope.
		{"compat_root_bare_messages", "", "/messages", "/v1/messages"},
		// A non-version base path is preserved and no /v1 rule fires.
		{"non_root_unchanged", "/anthropic", "/v1/messages", "/anthropic/v1/messages"},
		// A versioned base path accepts both client spellings without
		// duplicating /v1, including the Responses endpoint.
		{"versioned_base_bare_responses", "/api/v1", "/responses", "/api/v1/responses"},
		{"versioned_base_responses", "/api/v1", "/v1/responses", "/api/v1/responses"},
		{"versioned_base_double_responses", "/api/v1", "/v1/v1/responses", "/api/v1/responses"},
		{"versioned_base_messages", "/api/v1", "/v1/messages", "/api/v1/messages"},
		// 9. Segment-aware boundary: a leading token that merely starts
		// with "v1" is not the /v1 segment, so it gains the prefix rather
		// than collapsing. "v1beta" is synthetic (no Anthropic-compat
		// surface serves it today); the case pins the boundary the design
		// depends on (issue #157) against a future over-broad prefix check.
		{"native_v1beta_boundary", "", "/v1beta/x", "/v1/v1beta/x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
			b := backend.Backend{Pool: "p", Nick: "n", Credential: testAPIKey, BaseURL: upSrv.URL + tc.basePath}
			gwSrv := httptest.NewServer(injectBackend(b, gw))
			t.Cleanup(gwSrv.Close)

			resp, err := http.Post(gwSrv.URL+tc.inbound, "application/json", strings.NewReader("{}"))
			if err != nil {
				t.Fatalf("post: %v", err)
			}
			resp.Body.Close()

			if gotPath != tc.want {
				t.Errorf("upstream saw path %q, want %q", gotPath, tc.want)
			}
		})
	}
}

// TestProxy_responsesJSONPassesThroughOpaque proves that an OpenAI-compatible
// Responses request is only an HTTP exchange at the gateway boundary: the
// path is joined, the configured Bearer credential replaces the selector, and
// the method, query, headers, body, status, and response bytes are untouched.
func TestProxy_responsesJSONPassesThroughOpaque(t *testing.T) {
	const credential = "responses-provider-secret"
	const requestBody = `{"model":"opaque-model","input":[{"role":"user","content":"hello"}]}`
	const responseBody = `{"id":"resp_opaque","output":[{"type":"message"}]}`

	tests := []string{"/responses", "/v1/responses"}
	for _, inbound := range tests {
		t.Run(strings.TrimPrefix(inbound, "/"), func(t *testing.T) {
			var gotMethod, gotPath, gotQuery, gotAuth, gotAPIKey, gotCustom, gotBody string
			upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				gotPath = r.URL.Path
				gotQuery = r.URL.RawQuery
				gotAuth = r.Header.Get("Authorization")
				gotAPIKey = r.Header.Get("x-api-key")
				gotCustom = r.Header.Get("X-Opaque-Header")
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read request body: %v", err)
				}
				gotBody = string(body)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, responseBody)
			})
			upSrv := httptest.NewServer(upstream)
			t.Cleanup(upSrv.Close)

			gw, err := proxy.New(nil, nil)
			if err != nil {
				t.Fatalf("proxy.New: %v", err)
			}
			b := backend.Backend{
				Pool:       "responses",
				Nick:       "provider",
				Credential: credential,
				BaseURL:    upSrv.URL + "/api/v1",
			}
			gwSrv := httptest.NewServer(injectBackend(b, gw))
			t.Cleanup(gwSrv.Close)

			req, err := http.NewRequest(http.MethodPost, gwSrv.URL+inbound+"?stream=false", strings.NewReader(requestBody))
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			req.Header.Set("Authorization", "Bearer client-pool-selector")
			req.Header.Set("x-api-key", "client-placeholder")
			req.Header.Set("X-Opaque-Header", "preserve-me")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusCreated {
				t.Errorf("status = %d, want 201", resp.StatusCode)
			}
			if got := resp.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response body: %v", err)
			}
			if string(body) != responseBody {
				t.Errorf("response body = %q, want %q", body, responseBody)
			}
			if gotMethod != http.MethodPost {
				t.Errorf("method = %q, want POST", gotMethod)
			}
			if gotPath != "/api/v1/responses" {
				t.Errorf("upstream path = %q, want /api/v1/responses", gotPath)
			}
			if gotQuery != "stream=false" {
				t.Errorf("query = %q, want stream=false", gotQuery)
			}
			if gotAuth != "Bearer "+credential {
				t.Errorf("Authorization = %q, want Bearer credential", gotAuth)
			}
			if gotAPIKey != "" {
				t.Errorf("x-api-key = %q, want empty", gotAPIKey)
			}
			if gotCustom != "preserve-me" {
				t.Errorf("opaque header = %q, want preserve-me", gotCustom)
			}
			if gotBody != requestBody {
				t.Errorf("request body = %q, want %q", gotBody, requestBody)
			}
		})
	}
}

// TestProxy_nonPOSTMethodReachesUpstream confirms the proxy forwards
// non-POST methods (the POST-only gate was lifted in #141): a GET with a
// valid selector reaches the upstream, carrying the backend's stamped
// credential, instead of being rejected with 405. The upstream — not the
// gateway — is the authority on which methods a path serves.
func TestProxy_nonPOSTMethodReachesUpstream(t *testing.T) {
	var gotMethod, gotKey string
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotKey = r.Header.Get("x-api-key")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, strings.NewReader(`{"ok":true}`))
	})
	gw, _ := newGateway(t, upstream)

	resp, err := http.Get(gw.URL + "/v1/models")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (GET must reach upstream)", resp.StatusCode)
	}
	if gotMethod != http.MethodGet {
		t.Errorf("upstream saw method %q, want GET", gotMethod)
	}
	if gotKey != testAPIKey {
		t.Errorf("upstream x-api-key = %q, want %q (credential must be stamped on GET)", gotKey, testAPIKey)
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
	if seenKey != "mybackend" {
		t.Errorf("observer saw quota key = %q, want mybackend (quota key is the nick alone)", seenKey)
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
