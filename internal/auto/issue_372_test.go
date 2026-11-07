package auto

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

func authResponse(b backend.Backend, status int) *http.Response {
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	ctx := backend.WithBackend(req.Context(), b)
	ctx = backend.WithInboundRequest(ctx, req.Method, req.URL.Path)
	req = req.WithContext(ctx)
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Encoding":                  []string{"gzip"},
			"Content-Type":                      []string{"text/plain"},
			quota.HeaderUnified5hStatus:         []string{"rejected"},
			quota.HeaderCodexPrimaryUsedPercent: []string{"100"},
			"X-Upstream-Test":                   []string{"preserved"},
		},
		Request:       req,
		Body:          io.NopCloser(strings.NewReader("upstream credential body")),
		ContentLength: int64(len("upstream credential body")),
	}
}

func rejectingCredentialProbe(context.Context, backend.Backend) CredentialVerdict {
	return CredentialRejected
}

func acceptedCredentialProbe(context.Context, backend.Backend) CredentialVerdict {
	return CredentialAccepted
}

func TestCredentialProbeAcceptedKeepsPlainAndCodexMembersServing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(*testing.T) (*Pools, backend.Backend)
		status int
	}{
		{
			name: "plain",
			setup: func(t *testing.T) (*Pools, backend.Backend) {
				p := NewPools(testRegistry(t, "plain"), nil, nil, io.Discard)
				b, _ := p.CurrentRegistry().ResolveIn("auto", "plain")
				return p, b
			},
			status: http.StatusForbidden,
		},
		{
			name: "codex",
			setup: func(t *testing.T) (*Pools, backend.Backend) {
				scrubPoolEnv(t)
				t.Setenv(backend.EnvPrefix+"AUTO_BASE_URL", "https://chatgpt.com/backend-api/codex")
				t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_CODEX", "codex-secret")
				reg, err := backend.Load(testDefaultBaseURL)
				if err != nil {
					t.Fatalf("backend.Load: %v", err)
				}
				p := NewPools(reg, nil, nil, io.Discard)
				b, _ := p.CurrentRegistry().ResolveIn("auto", "codex")
				return p, b
			},
			status: http.StatusForbidden,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, b := tc.setup(t)
			p.SetCredentialProbe(acceptedCredentialProbe)
			resp := authResponse(b, tc.status)
			if err := p.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse: %v", err)
			}
			if resp.StatusCode != tc.status {
				t.Fatalf("status=%d, want original %d", resp.StatusCode, tc.status)
			}
			var body struct {
				Error              string `json:"error"`
				Method             string `json:"method"`
				Path               string `json:"path"`
				UpstreamStatus     int    `json:"upstream_status"`
				CredentialVerified bool   `json:"credential_verified"`
				Verdict            string `json:"credential_verdict"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("diagnostic JSON: %v", err)
			}
			if body.Method != http.MethodPost || body.Path != "/v1/responses" || body.UpstreamStatus != tc.status || !body.CredentialVerified || body.Verdict != "accepted" || !strings.Contains(body.Error, "base URL") {
				t.Errorf("diagnostic=%+v, want request details and verified guidance", body)
			}
			if got := resp.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type=%q, want application/json", got)
			}
			if got := resp.Header.Get("Content-Encoding"); got != "" {
				t.Errorf("Content-Encoding=%q, want removed after body replacement", got)
			}
			if got := resp.Header.Get(quota.HeaderUnified5hStatus); got != "" || resp.Header.Get(quota.HeaderCodexPrimaryUsedPercent) != "" {
				t.Errorf("quota headers leaked after diagnostic: %v", resp.Header)
			}
			c := p.byPool["auto"]
			c.mu.Lock()
			_, exhausted := c.exhausted[b.Nick]
			_, parked := c.credentialPark[b.Nick]
			current := c.curNick
			c.mu.Unlock()
			if exhausted || parked || current != b.Nick {
				t.Errorf("state exhausted=%v credentialPark=%v current=%q, want serving %q", exhausted, parked, current, b.Nick)
			}
		})
	}
}

func TestCredentialProbeAcceptedDoesNotEmptyTwoMemberPool(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(testRegistry(t, "a", "b"), nil, clock.now, io.Discard)
	p.SetCredentialProbe(acceptedCredentialProbe)
	for _, nick := range []string{"a", "b"} {
		b, _ := p.CurrentRegistry().ResolveIn("auto", nick)
		resp := authResponse(b, http.StatusForbidden)
		if err := p.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse %s: %v", nick, err)
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s status=%d, want 403", nick, resp.StatusCode)
		}
	}
	c := p.byPool["auto"]
	c.mu.Lock()
	for _, nick := range []string{"a", "b"} {
		if _, parked := c.exhausted[nick]; parked {
			t.Errorf("%s unexpectedly exhausted after client rejection", nick)
		}
		if _, parked := c.credentialPark[nick]; parked {
			t.Errorf("%s unexpectedly credential-parked after client rejection", nick)
		}
	}
	c.mu.Unlock()
	if b, _, ok, exhausted := p.Route("auto"); !ok || exhausted || b.Nick != "a" {
		t.Fatalf("route after client rejections=%q ok=%v exhausted=%v, want serving a", b.Nick, ok, exhausted)
	}

	// Exercise the actual resolver boundary after the two bad client paths:
	// a valid follow-up request must still reach the selected fake upstream.
	gateway := backend.Middleware(p, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := backend.FromContext(r.Context())
		if !ok || b.Nick != "a" {
			t.Errorf("follow-up upstream backend=(%q, ok=%v), want a", b.Nick, ok)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("healthy"))
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer auto")
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "healthy" {
		t.Fatalf("valid follow-up response=%d %q, want 200 healthy", rec.Code, rec.Body.String())
	}
}

func TestCredentialProbeRejectedPreservesFailover(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(testRegistry(t, "a", "b"), nil, clock.now, io.Discard)
	p.SetCredentialProbe(rejectingCredentialProbe)
	b, _ := p.CurrentRegistry().ResolveIn("auto", "a")
	resp := authResponse(b, http.StatusUnauthorized)
	if err := p.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want switch 503", resp.StatusCode)
	}
	if got, _, _, exhausted := p.Route("auto"); exhausted || got.Nick != "b" {
		t.Fatalf("route=%q exhausted=%v, want b serving", got.Nick, exhausted)
	}
}

func TestCredentialProbeCoalescesAndCachesServingVerdicts(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(testRegistry(t, "a"), nil, clock.now, io.Discard)
	var calls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	p.SetCredentialProbe(func(context.Context, backend.Backend) CredentialVerdict {
		calls.Add(1)
		if calls.Load() == 1 {
			close(entered)
			<-release
		}
		return CredentialAccepted
	})
	b, _ := p.CurrentRegistry().ResolveIn("auto", "a")
	const concurrent = 8
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := authResponse(b, http.StatusForbidden)
			if err := p.ModifyResponse(resp); err != nil {
				t.Errorf("ModifyResponse: %v", err)
			}
		}()
	}
	<-entered
	close(release)
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("probe calls=%d, want exactly one", got)
	}
	resp := authResponse(b, http.StatusForbidden)
	if err := p.ModifyResponse(resp); err != nil {
		t.Fatalf("cached ModifyResponse: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("cached probe calls=%d, want one", got)
	}
	clock.advance(credentialProbeCooldown)
	resp = authResponse(b, http.StatusForbidden)
	if err := p.ModifyResponse(resp); err != nil {
		t.Fatalf("expired ModifyResponse: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expired probe calls=%d, want two", got)
	}
}

func TestCredentialProbeInconclusiveDoesNotParkOrWriteStore(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	before := quota.Snapshot{Backend: "a", Unified5hStatus: "allowed", AsOf: clock.now()}
	store.Put("a", before)
	p := NewPools(testRegistry(t, "a"), store, clock.now, io.Discard)
	calls := 0
	p.SetCredentialProbe(func(context.Context, backend.Backend) CredentialVerdict {
		calls++
		return CredentialInconclusive
	})
	b, _ := p.CurrentRegistry().ResolveIn("auto", "a")
	resp := authResponse(b, http.StatusUnauthorized)
	if err := p.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized || !strings.Contains(body, "could not be verified") {
		t.Fatalf("response status/body=%d/%q, want original status and inconclusive diagnostic", resp.StatusCode, body)
	}
	if calls != 1 {
		t.Fatalf("initial inconclusive probe calls=%d, want 1", calls)
	}
	resp = authResponse(b, http.StatusUnauthorized)
	if err := p.ModifyResponse(resp); err != nil {
		t.Fatalf("cached ModifyResponse: %v", err)
	}
	_ = readBody(t, resp)
	if calls != 1 {
		t.Fatalf("inconclusive probe calls within cooldown=%d, want 1", calls)
	}
	clock.advance(credentialProbeCooldown)
	resp = authResponse(b, http.StatusUnauthorized)
	if err := p.ModifyResponse(resp); err != nil {
		t.Fatalf("expired ModifyResponse: %v", err)
	}
	_ = readBody(t, resp)
	if calls != 2 {
		t.Fatalf("inconclusive probe calls after cooldown=%d, want 2", calls)
	}
	if after := store.Get("a"); after != before {
		t.Errorf("store changed on credential probe: before=%+v after=%+v", before, after)
	}
	c := p.byPool["auto"]
	c.mu.Lock()
	_, exhausted := c.exhausted["a"]
	_, parked := c.credentialPark["a"]
	c.mu.Unlock()
	if exhausted || parked {
		t.Errorf("inconclusive probe parked member: exhausted=%v credentialPark=%v", exhausted, parked)
	}
}

func TestCredentialDecisionLogHasRoutingDetailsWithoutSecrets(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf strings.Builder
	p := NewPools(testRegistry(t, "a"), nil, clock.now, &logBuf)
	p.SetCredentialProbe(acceptedCredentialProbe)
	b, _ := p.CurrentRegistry().ResolveIn("auto", "a")
	resp := authResponse(b, http.StatusForbidden)
	if err := p.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	log := logBuf.String()
	for _, want := range []string{"pool=\"auto\"", "nick=\"a\"", "method=\"POST\"", "path=\"/v1/responses\"", "upstream_status=403", "verdict=accepted"} {
		if !strings.Contains(log, want) {
			t.Errorf("log %q missing %q", log, want)
		}
	}
	for _, secret := range []string{"cred-a", "upstream credential body"} {
		if strings.Contains(log, secret) {
			t.Errorf("log leaked %q: %q", secret, log)
		}
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return string(b)
}
