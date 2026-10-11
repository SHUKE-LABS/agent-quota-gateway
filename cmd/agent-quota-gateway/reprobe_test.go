package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// upstreamRecord captures what a reprobe sent upstream.
type upstreamRecord struct {
	mu       sync.Mutex
	method   string
	path     string
	auth     string
	beta     string
	version  string
	body     map[string]any
	requests int
}

func (u *upstreamRecord) snapshot() upstreamRecord {
	u.mu.Lock()
	defer u.mu.Unlock()
	return upstreamRecord{method: u.method, path: u.path, auth: u.auth, beta: u.beta, version: u.version, body: u.body, requests: u.requests}
}

// fakeUpstream serves every request with status and headers, recording it.
func fakeUpstream(t *testing.T, status int, headers http.Header) (*httptest.Server, *upstreamRecord) {
	t.Helper()
	rec := &upstreamRecord{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.method, rec.path = r.Method, r.URL.Path
		rec.auth, rec.beta, rec.version = r.Header.Get("Authorization"), r.Header.Get("anthropic-beta"), r.Header.Get("anthropic-version")
		rec.body = nil
		_ = json.Unmarshal(raw, &rec.body)
		rec.requests++
		rec.mu.Unlock()
		for k, vs := range headers {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

// redirectTransport sends every request to target, keeping the original
// path, so a chatgpt.com base URL (the Codex gate is host-based) reaches the
// test server.
type redirectTransport struct{ target *url.URL }

func (rt redirectTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.URL.Scheme, r.URL.Host = rt.target.Scheme, rt.target.Host
	return http.DefaultTransport.RoundTrip(r)
}

// reprobeServer builds a one-pool "auto" gateway whose member "a" points at
// baseURL with credential cred, and serves the reprobe route through a mux
// carrying the proxy catch-all, as run() wires it.
func reprobeServer(t *testing.T, store *quota.Store, baseURL, cred string, client *http.Client) (*httptest.Server, *auto.Pools) {
	t.Helper()
	scrubPoolEnv(t)
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", cred)
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-api-other")
	registry, err := backend.Load(baseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := auto.NewPools(registry, store, nil, io.Discard)
	if client == nil {
		client = newReprobeClient()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/pool/{name}/member/{nick}/reprobe", reprobeHandler(pools, reprobeQuery(store, client, time.Now)))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("reprobe fell through to the add-member route")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("reprobe fell through to the proxy catch-all: %s %s", r.Method, r.URL.Path)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, pools
}

func postReprobe(t *testing.T, gw *httptest.Server, pool, nick string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(gw.URL+"/_gateway/pool/"+pool+"/member/"+nick+"/reprobe", "", nil)
	if err != nil {
		t.Fatalf("POST reprobe: %v", err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func unifiedHeaders(status5h, status7d string, util5h, util7d float64, reset5h, reset7d time.Time) http.Header {
	h := http.Header{}
	h.Set(quota.HeaderUnifiedStatus, status7d)
	h.Set(quota.HeaderUnified5hStatus, status5h)
	h.Set(quota.HeaderUnified5hUtilization, strconv.FormatFloat(util5h, 'f', -1, 64))
	h.Set(quota.HeaderUnified5hReset, strconv.FormatInt(reset5h.Unix(), 10))
	h.Set(quota.HeaderUnified7dStatus, status7d)
	h.Set(quota.HeaderUnified7dUtilization, strconv.FormatFloat(util7d, 'f', -1, 64))
	h.Set(quota.HeaderUnified7dReset, strconv.FormatInt(reset7d.Unix(), 10))
	return h
}

// staleRejected7d is the #350 field snapshot: a 7d rejected window days out.
func staleRejected7d(now time.Time) quota.Snapshot {
	util, reset := 1.0, now.Add(72*time.Hour)
	return quota.Snapshot{
		UnifiedStatus:        "rejected",
		Unified7dStatus:      "rejected",
		Unified7dUtilization: &util,
		Unified7dReset:       &reset,
		AsOf:                 now.Add(-time.Minute),
	}
}

// TestReprobe_anthropicHealthy: an Anthropic member stranded by a stale 7d
// rejected snapshot is re-queried with one minimal stamped Messages call; the
// allowed headers are merged and the member reports healthy.
func TestReprobe_anthropicHealthy(t *testing.T) {
	now := time.Now().UTC()
	upstream, rec := fakeUpstream(t, http.StatusOK,
		unifiedHeaders("allowed", "allowed", 0.01, 0, now.Add(5*time.Hour), now.Add(7*24*time.Hour)))
	store := quota.NewStore()
	store.Put("a", staleRejected7d(now))
	gw, pools := reprobeServer(t, store, upstream.URL, "sk-ant-oat01-test", nil)

	status, body := postReprobe(t, gw, "auto", "a")
	if status != http.StatusOK {
		t.Fatalf("status = %d body=%v, want 200", status, body)
	}
	if body["healthy"] != true || body["pool"] != "auto" || body["nick"] != "a" {
		t.Errorf("body = %v, want healthy auto/a", body)
	}
	if body["unified_7d_utilization"] != 0.0 || body["unified_status"] != "allowed" {
		t.Errorf("body = %v, want the fresh allowed 7d window", body)
	}
	got := rec.snapshot()
	if got.method != http.MethodPost || got.path != "/v1/messages" {
		t.Errorf("upstream got %s %s, want POST /v1/messages", got.method, got.path)
	}
	if got.auth != "Bearer sk-ant-oat01-test" || got.beta == "" || got.version == "" {
		t.Errorf("upstream auth=%q beta=%q version=%q, want stamped OAuth credential", got.auth, got.beta, got.version)
	}
	if got.body["max_tokens"] != 1.0 {
		t.Errorf("upstream body max_tokens = %v, want 1", got.body["max_tokens"])
	}
	ps, _ := pools.PoolStatus("auto", store, nil)
	for _, m := range ps.Members {
		if m.Nick == "a" && m.Status == "exhausted" {
			t.Errorf("member a still exhausted after healthy reprobe")
		}
	}
}

// TestReprobe_anthropicStillRejected: a fresh 429 carrying rejected headers
// is a successful reprobe that confirms exhaustion — merged honestly,
// healthy:false, the upstream's reset reported.
func TestReprobe_anthropicStillRejected(t *testing.T) {
	now := time.Now().UTC()
	reset7d := now.Add(30 * time.Hour).Truncate(time.Second)
	upstream, _ := fakeUpstream(t, http.StatusTooManyRequests,
		unifiedHeaders("allowed", "rejected", 0.2, 1, now.Add(5*time.Hour), reset7d))
	store := quota.NewStore()
	store.Put("a", staleRejected7d(now))
	gw, _ := reprobeServer(t, store, upstream.URL, "sk-ant-api03-test", nil)

	status, body := postReprobe(t, gw, "auto", "a")
	if status != http.StatusOK || body["healthy"] != false {
		t.Fatalf("status = %d body=%v, want 200 healthy:false", status, body)
	}
	if got := store.Get("a").Unified7dReset; got == nil || !got.Equal(reset7d) {
		t.Errorf("stored 7d reset = %v, want upstream's %v", got, reset7d)
	}
}

// TestReprobe_credentialRejected: 401/403 is a 502 and the stored snapshot
// is left exactly as it was.
func TestReprobe_credentialRejected(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			now := time.Now().UTC()
			upstream, _ := fakeUpstream(t, code, nil)
			store := quota.NewStore()
			store.Put("a", staleRejected7d(now))
			before := store.Get("a")
			gw, _ := reprobeServer(t, store, upstream.URL, "sk-ant-api03-test", nil)

			status, body := postReprobe(t, gw, "auto", "a")
			if status != http.StatusBadGateway || body["error"] != "credential rejected" {
				t.Errorf("status = %d body=%v, want 502 credential rejected", status, body)
			}
			if after := store.Get("a"); !reflect.DeepEqual(before, after) {
				t.Errorf("snapshot changed on credential rejection")
			}
		})
	}
}

// staleCodexFull is a stranded Codex snapshot: x-codex-* windows carry no
// status, so a full weekly window (utilization 1.0, reset days out) is the
// exhaustion signal.
func staleCodexFull(now time.Time) quota.Snapshot {
	util, reset := 1.0, now.Add(72*time.Hour)
	return quota.Snapshot{Unified7dUtilization: &util, Unified7dReset: &reset, AsOf: now.Add(-time.Minute)}
}

// TestReprobe_codex: a chatgpt.com member is re-queried with the Codex hello
// request and its x-codex-* windows; an empty 2xx is rejected without
// touching the prior snapshot.
func TestReprobe_codex(t *testing.T) {
	now := time.Now().UTC()
	h := http.Header{}
	h.Set(quota.HeaderCodexPrimaryUsedPercent, "3")
	h.Set(quota.HeaderCodexPrimaryResetAt, strconv.FormatInt(now.Add(5*time.Hour).Unix(), 10))
	h.Set(quota.HeaderCodexSecondaryUsedPercent, "1")
	h.Set(quota.HeaderCodexSecondaryResetAt, strconv.FormatInt(now.Add(7*24*time.Hour).Unix(), 10))
	upstream, rec := fakeUpstream(t, http.StatusOK, h)
	target, _ := url.Parse(upstream.URL)
	client := &http.Client{Transport: redirectTransport{target: target}}

	store := quota.NewStore()
	store.Put("a", staleCodexFull(now))
	gw, _ := reprobeServer(t, store, "https://chatgpt.com/backend-api/codex", "codex-token", client)

	status, body := postReprobe(t, gw, "auto", "a")
	if status != http.StatusOK || body["healthy"] != true {
		t.Fatalf("status = %d body=%v, want 200 healthy", status, body)
	}
	if got := rec.snapshot(); got.method != http.MethodPost || got.path != "/backend-api/codex/responses" {
		t.Errorf("upstream got %s %s, want POST /backend-api/codex/responses", got.method, got.path)
	}

	empty, _ := fakeUpstream(t, http.StatusOK, nil)
	emptyTarget, _ := url.Parse(empty.URL)
	store2 := quota.NewStore()
	store2.Put("a", staleCodexFull(now))
	before := store2.Get("a")
	gw2, _ := reprobeServer(t, store2, "https://chatgpt.com/backend-api/codex", "codex-token",
		&http.Client{Transport: redirectTransport{target: emptyTarget}})
	status, body = postReprobe(t, gw2, "auto", "a")
	if status != http.StatusBadGateway || body["error"] != "no quota signal" {
		t.Errorf("empty 2xx: status = %d body=%v, want 502 no quota signal", status, body)
	}
	if after := store2.Get("a"); !reflect.DeepEqual(before, after) {
		t.Errorf("empty 2xx changed the prior snapshot")
	}
}

// TestReprobe_pollerProvider: a poller-registered member is re-queried
// through poller.Probe's quota endpoint, not a Messages call.
func TestReprobe_pollerProvider(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		fmt.Fprint(w, `{"data":{"limits":[{"type":"TOKENS_LIMIT","percentage":10,"nextResetTime":4102444800000}]}}`)
	}))
	t.Cleanup(upstream.Close)
	poller.WithTestProviderForTest(t, upstream.URL, poller.HostURLForTest("/api/monitor/usage/quota/limit"), poller.RawAuthForTest, poller.ParseZhipuForTest)

	store := quota.NewStore()
	gw, _ := reprobeServer(t, store, upstream.URL, "zai-key", nil)
	status, body := postReprobe(t, gw, "auto", "a")
	if status != http.StatusOK || body["healthy"] != true {
		t.Fatalf("status = %d body=%v, want 200 healthy", status, body)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 || paths[0] != "/api/monitor/usage/quota/limit" {
		t.Errorf("upstream paths = %v, want one quota-endpoint request", paths)
	}
}

// TestReprobe_errorContract covers the request-validation statuses and the
// 405 for a non-POST (which must not fall through to the proxy catch-all).
func TestReprobe_errorContract(t *testing.T) {
	upstream, rec := fakeUpstream(t, http.StatusOK, nil)
	store := quota.NewStore()
	gw, pools := reprobeServer(t, store, upstream.URL, "sk-ant-api03-test", nil)
	if _, err := pools.SetMemberDisabled("auto", "b", true); err != nil {
		t.Fatalf("SetMemberDisabled: %v", err)
	}

	for _, tc := range []struct {
		pool, nick string
		want       int
		err        string
	}{
		{"nope", "a", http.StatusNotFound, "pool not found"},
		{"auto", "ghost", http.StatusBadRequest, "unknown nick: ghost"},
		{"auto", "b", http.StatusConflict, "member disabled"},
	} {
		status, body := postReprobe(t, gw, tc.pool, tc.nick)
		if status != tc.want || body["error"] != tc.err {
			t.Errorf("%s/%s: status = %d body=%v, want %d %q", tc.pool, tc.nick, status, body, tc.want, tc.err)
		}
	}

	resp, err := http.Get(gw.URL + "/_gateway/pool/auto/member/a/reprobe")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != http.MethodPost {
		t.Errorf("GET: status = %d Allow=%q, want 405 Allow: POST", resp.StatusCode, resp.Header.Get("Allow"))
	}
	if n := rec.snapshot().requests; n != 0 {
		t.Errorf("upstream saw %d requests, want 0", n)
	}
}
