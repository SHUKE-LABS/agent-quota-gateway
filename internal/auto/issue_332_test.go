package auto

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

func TestCredentialDryPoolForwardsRepeatedAuthFailure(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(testRegistry(t, "solo"), nil, clock.now, io.Discard)
	upstreamCalls := 0
	upstreamBody := `{"error":"invalid_api_key"}`
	gateway := backend.Middleware(p, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		resp := &http.Response{
			StatusCode: http.StatusUnauthorized,
			Header: http.Header{
				"Content-Type":    []string{"application/json"},
				"X-Upstream-Test": []string{"preserved"},
			},
			Request: r,
			Body:    io.NopCloser(strings.NewReader(upstreamBody)),
		}
		if err := p.ModifyResponse(resp); err != nil {
			t.Errorf("ModifyResponse: %v", err)
			return
		}
		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))

	doRequest := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader("{}"))
		r.Header.Set("Authorization", "Bearer auto")
		w := httptest.NewRecorder()
		gateway.ServeHTTP(w, r)
		return w
	}

	first := doRequest()
	if first.Code != http.StatusUnauthorized || first.Body.String() != upstreamBody {
		t.Fatalf("first response = %d %q, want upstream 401 %q", first.Code, first.Body.String(), upstreamBody)
	}
	if got := first.Header().Get("X-Upstream-Test"); got != "preserved" {
		t.Fatalf("first X-Upstream-Test=%q, want preserved", got)
	}

	second := doRequest()
	if second.Code != http.StatusUnauthorized || second.Body.String() != upstreamBody {
		t.Fatalf("second response = %d %q, want upstream 401 %q", second.Code, second.Body.String(), upstreamBody)
	}
	if got := second.Header().Get("X-Upstream-Test"); got != "preserved" {
		t.Errorf("second X-Upstream-Test=%q, want preserved", got)
	}
	if got := second.Header().Get("Retry-After"); got == "" {
		t.Errorf("second Retry-After is empty, want existing auth-park wait")
	}
	if upstreamCalls != 2 {
		t.Errorf("upstream calls=%d, want 2 (credential-only dry pool must forward the second request)", upstreamCalls)
	}
}

func TestCredentialRejectionStillFailsOverToHealthyMember(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerPriorityRegistry(t, 1, "a,b", "a", "b"), nil, clock.now, io.Discard)
	var upstreamNicks []string
	gateway := backend.Middleware(p, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := backend.FromContext(r.Context())
		if !ok {
			t.Error("upstream request missing backend context")
			return
		}
		upstreamNicks = append(upstreamNicks, b.Nick)
		status, body := http.StatusOK, "healthy"
		if b.Nick == "a" {
			status, body = http.StatusUnauthorized, "rejected"
		}
		resp := &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Request:    r,
			Body:       io.NopCloser(strings.NewReader(body)),
		}
		if err := p.ModifyResponse(resp); err != nil {
			t.Errorf("ModifyResponse: %v", err)
			return
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	doRequest := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		r.Header.Set("Authorization", "Bearer auto")
		w := httptest.NewRecorder()
		gateway.ServeHTTP(w, r)
		return w
	}

	if first := doRequest(); first.Code != http.StatusServiceUnavailable {
		t.Fatalf("first response=%d, want existing failover 503", first.Code)
	}
	if second := doRequest(); second.Code != http.StatusOK || second.Body.String() != "healthy" {
		t.Fatalf("second response=%d %q, want healthy 200", second.Code, second.Body.String())
	}
	if strings.Join(upstreamNicks, ",") != "a,b" {
		t.Errorf("upstream member sequence=%v, want [a b] with no retry to parked a", upstreamNicks)
	}
}

func TestCredentialDryPoolRecoveryClearsSiblingParks(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "A_BACKEND_SHARED": "cred-shared",
		backend.EnvPrefix + "B_BACKEND_SHARED": "cred-shared",
	})
	shared, ok := p.CurrentRegistry().ResolveIn("a", "shared")
	if !ok {
		t.Fatal("shared backend missing from pool a")
	}
	if err := p.ModifyResponse(respAuth(shared, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse 401: %v", err)
	}

	for _, pool := range []string{"a", "b"} {
		b, _, ok, exhausted := p.Route(pool)
		if !ok || exhausted || b.Nick != "shared" {
			t.Fatalf("Route(%s) after 401 = %q ok=%v exhausted=%v, want shared/true/false", pool, b.Nick, ok, exhausted)
		}
	}

	recovered := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Request: httptest.NewRequest(http.MethodPost, "/v1/messages", nil).
			WithContext(backend.WithBackend(context.Background(), shared)),
		Body: io.NopCloser(strings.NewReader(`{"ok":true}`)),
	}
	if err := p.ModifyResponse(recovered); err != nil {
		t.Fatalf("ModifyResponse 200: %v", err)
	}
	if recovered.StatusCode != http.StatusOK {
		t.Fatalf("recovered response status=%d, want 200", recovered.StatusCode)
	}

	for _, pool := range []string{"a", "b"} {
		status, ok := p.PoolStatus(pool, quota.NewStore(), nil)
		if !ok {
			t.Fatalf("PoolStatus(%s) missing", pool)
		}
		if memberStatus(status, "shared") != "serving" || memberParked(status, "shared") {
			t.Errorf("pool %s shared member status=%q parked=%v, want serving/false", pool, memberStatus(status, "shared"), memberParked(status, "shared"))
		}
		for _, member := range status.Members {
			if member.Nick == "shared" && member.ExhaustedUntil != nil {
				t.Errorf("pool %s exhausted_until=%v, want null", pool, member.ExhaustedUntil)
			}
		}
		c := p.byPool[pool]
		c.mu.Lock()
		_, hasCredentialPark := c.credentialPark["shared"]
		c.mu.Unlock()
		if hasCredentialPark {
			t.Errorf("pool %s retained shared credential park after upstream recovery", pool)
		}
	}
}

func TestCredentialAuthRecoveryReanchorsUnavailableStickyAcrossPools(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	scrubPoolEnv(t)
	for key, value := range map[string]string{
		backend.EnvPrefix + "A_BACKEND_AONLY":  "cred-aonly",
		backend.EnvPrefix + "A_BACKEND_SHARED": "cred-shared",
		backend.EnvPrefix + "B_BACKEND_BONLY":  "cred-bonly",
		backend.EnvPrefix + "B_BACKEND_SHARED": "cred-shared",
	} {
		t.Setenv(key, value)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	spec := reg.Spec()
	poolA := spec.Pools["a"]
	poolA.Priority = []string{"shared", "aonly"}
	spec.Pools["a"] = poolA
	poolB := spec.Pools["b"]
	poolB.Priority = []string{"shared", "bonly"}
	spec.Pools["b"] = poolB
	reg, err = backend.BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	p := NewPools(reg, nil, clock.now, io.Discard)
	reset := clock.now().Add(time.Hour)
	for pool, parkedNick := range map[string]string{"a": "aonly", "b": "bonly"} {
		c := p.byPool[pool]
		c.mu.Lock()
		for _, nick := range []string{"shared", parkedNick} {
			c.exhausted[nick] = reset
			c.credentialPark[nick] = credentialParkEntry{reset: reset, authRejected: true}
		}
		c.setActiveMemberLocked(parkedNick)
		c.mu.Unlock()
	}

	shared, ok := reg.ResolveIn("a", "shared")
	if !ok {
		t.Fatal("shared backend missing from pool a")
	}
	recovered := respAuth(shared, http.StatusOK)
	if err := p.ModifyResponse(recovered); err != nil {
		t.Fatalf("ModifyResponse recovered 200: %v", err)
	}
	for pool, c := range p.byPool {
		if got := c.Current(); got != "shared" {
			t.Errorf("pool %s active=%q after auth recovery, want priority-first recovered member shared", pool, got)
		}
	}
}

func TestCredentialDryPoolRecoveryPreservesQuotaPark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "A_BACKEND_SHARED": "cred-shared",
		backend.EnvPrefix + "B_BACKEND_SHARED": "cred-shared",
	})
	shared, _ := p.CurrentRegistry().ResolveIn("a", "shared")
	if err := p.ModifyResponse(respAuth(shared, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse 401: %v", err)
	}

	quotaReset := clock.now().Add(7 * time.Hour)
	origin := p.byPool["a"]
	origin.mu.Lock()
	origin.exhausted["shared"] = quotaReset
	origin.mu.Unlock()
	sibling := p.byPool["b"]
	sibling.mu.Lock()
	sibling.exhausted["shared"] = quotaReset
	sibling.credentialPark["shared"] = credentialParkEntry{reset: quotaReset, windowFact: false}
	sibling.mu.Unlock()

	recovered := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Request: httptest.NewRequest(http.MethodPost, "/v1/messages", nil).
			WithContext(backend.WithBackend(context.Background(), shared)),
		Body: io.NopCloser(strings.NewReader("ok")),
	}
	if err := p.ModifyResponse(recovered); err != nil {
		t.Fatalf("ModifyResponse 200: %v", err)
	}

	for pool, c := range map[string]*Controller{"a": origin, "b": sibling} {
		c.mu.Lock()
		reset, hasQuotaPark := c.exhausted["shared"]
		entry, hasCredentialPark := c.credentialPark["shared"]
		c.mu.Unlock()
		if !hasQuotaPark || !reset.Equal(quotaReset) {
			t.Errorf("pool %s quota exhausted reset=%v present=%v, want preserved %v", pool, reset, hasQuotaPark, quotaReset)
		}
		if pool == "a" && hasCredentialPark {
			t.Errorf("origin pool retained auth credential park")
		}
		if pool == "b" && (!hasCredentialPark || entry.authRejected) {
			t.Errorf("sibling credential park=%+v present=%v, want quota fallback preserved", entry, hasCredentialPark)
		}
		if _, _, ok, exhausted := p.Route(pool); !ok || !exhausted {
			t.Errorf("Route(%s) after recovery with quota park ok=%v exhausted=%v, want true/true", pool, ok, exhausted)
		}
	}
}

func TestCredentialDryPoolAuthRetry429ReplacesAuthParkWithQuotaPark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "A_BACKEND_SHARED": "cred-shared",
		backend.EnvPrefix + "B_BACKEND_SHARED": "cred-shared",
	})
	shared, _ := p.CurrentRegistry().ResolveIn("a", "shared")
	if err := p.ModifyResponse(respAuth(shared, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse 401: %v", err)
	}
	retryBackend, _, ok, exhausted := p.Route("a")
	if !ok || exhausted || retryBackend.Nick != "shared" {
		t.Fatalf("auth-only retry route=%q ok=%v exhausted=%v, want shared/true/false", retryBackend.Nick, ok, exhausted)
	}

	quotaReset := clock.now().Add(time.Hour)
	retry := resp429(retryBackend, clock, time.Hour)
	if err := p.ModifyResponse(retry); err != nil {
		t.Fatalf("ModifyResponse retry 429: %v", err)
	}
	if retry.StatusCode != http.StatusServiceUnavailable || retry.Header.Get("Retry-After") != "3600" {
		t.Fatalf("retry response=%d Retry-After=%q, want dry-pool 503 with the quota reset", retry.StatusCode, retry.Header.Get("Retry-After"))
	}

	for pool, c := range map[string]*Controller{"a": p.byPool["a"], "b": p.byPool["b"]} {
		c.mu.Lock()
		entry, hasCredentialPark := c.credentialPark["shared"]
		reset, hasQuotaPark := c.exhausted["shared"]
		c.mu.Unlock()
		if hasCredentialPark && entry.authRejected {
			t.Errorf("pool %s retained auth park after retry 429", pool)
		}
		if pool == "a" && (!hasQuotaPark || !reset.Equal(quotaReset)) {
			t.Errorf("origin quota park reset=%v present=%v, want %v", reset, hasQuotaPark, quotaReset)
		}
		if pool == "b" && hasCredentialPark {
			t.Errorf("sibling retained auth credential park after retry 429: %+v", entry)
		}
	}
	if got, wait, ok, exhausted := p.Route("a"); !ok || !exhausted || got.Nick != "shared" || wait != time.Hour {
		t.Errorf("Route after retry 429=%q wait=%s ok=%v exhausted=%v, want quota-parked shared for 1h", got.Nick, wait, ok, exhausted)
	}
}

func TestCredentialDryPoolKeepsQuotaFallback503(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := codexController(t, clock, io.Discard, nil, "solo")
	b := c.resolve(t, "solo")
	resp := resp429Codex(b, clock,
		codexWin{percent: "100", minutes: "300"},
		codexWin{},
		quota.CodexReachedTypeUsageLimit,
	)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse Codex fallback 429: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("Codex quota fallback response=%d Retry-After=%q, want synthetic 503 with retry hint", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	c.mu.Lock()
	entry, ok := c.credentialPark["solo"]
	c.mu.Unlock()
	if !ok || entry.windowFact || entry.authRejected {
		t.Errorf("Codex fallback park=%+v present=%v, want windowFact=false authRejected=false", entry, ok)
	}
	if _, _, exhausted := c.ResolveAuto(); !exhausted {
		t.Error("Codex quota fallback dry pool returned a real-request route, want synthetic 503 path")
	}
}

func TestCredentialDryPoolKeepsQuotaFallback503AlongsideAuthPark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := codexController(t, clock, io.Discard, nil, "quota", "auth")
	if err := c.ModifyResponse(respAuth(c.resolve(t, "auth"), http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse auth 401: %v", err)
	}

	fallback := resp429Codex(c.resolve(t, "quota"), clock,
		codexWin{percent: "100", minutes: "300"},
		codexWin{},
		quota.CodexReachedTypeUsageLimit,
	)
	if err := c.ModifyResponse(fallback); err != nil {
		t.Fatalf("ModifyResponse quota fallback 429: %v", err)
	}
	if fallback.StatusCode != http.StatusServiceUnavailable || fallback.Header.Get("Retry-After") == "" {
		t.Fatalf("mixed fallback response=%d Retry-After=%q, want synthetic 503 with retry hint", fallback.StatusCode, fallback.Header.Get("Retry-After"))
	}
	c.mu.Lock()
	quotaPark, hasQuotaPark := c.credentialPark["quota"]
	authPark, hasAuthPark := c.credentialPark["auth"]
	c.mu.Unlock()
	if !hasQuotaPark || quotaPark.windowFact || quotaPark.authRejected {
		t.Errorf("quota fallback park=%+v present=%v, want windowFact=false authRejected=false", quotaPark, hasQuotaPark)
	}
	if !hasAuthPark || !authPark.authRejected {
		t.Errorf("auth park=%+v present=%v, want authRejected=true", authPark, hasAuthPark)
	}
	if _, _, exhausted := c.ResolveAuto(); !exhausted {
		t.Error("mixed auth/fallback quota pool returned a real-request route, want synthetic 503 path")
	}
}

func TestCredentialDryPoolKeepsPreciseQuota503AlongsideAuthPark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b")
	if err := c.ModifyResponse(respAuth(c.resolve(t, "a"), http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse 401: %v", err)
	}
	resp := resp429(c.resolve(t, "b"), clock, time.Hour)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse 429: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "3600" {
		t.Errorf("mixed pool response=%d Retry-After=%q, want synthetic 503 with quota reset 3600", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if _, _, exhausted := c.ResolveAuto(); !exhausted {
		t.Error("mixed quota/auth dry pool returned a real-request route, want synthetic 503 path")
	}
}

func TestCredentialDryPoolAuthCausePersistsAndLegacyDoesNotRetry(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(testRegistry(t, "solo"), nil, clock.now, io.Discard)
	b, _ := p.CurrentRegistry().ResolveIn("auto", "solo")
	if err := p.ModifyResponse(respAuth(b, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse 401: %v", err)
	}
	saved := p.PersistState()
	if !saved["auto"].CredentialPark["solo"].AuthRejected {
		t.Fatal("persisted auth park has AuthRejected=false")
	}

	reloaded := NewPools(testRegistry(t, "solo"), nil, clock.now, io.Discard)
	reloaded.LoadPersistState(saved)
	if got, _, ok, exhausted := reloaded.Route("auto"); !ok || exhausted || got.Nick != "solo" {
		t.Fatalf("restored auth route=%q ok=%v exhausted=%v, want solo/true/false", got.Nick, ok, exhausted)
	}

	reset := clock.now().Add(time.Hour)
	legacyJSON, err := json.Marshal(map[string]any{"reset": reset, "window_fact": false})
	if err != nil {
		t.Fatal(err)
	}
	var legacy CredentialParkPersist
	if err := json.Unmarshal(legacyJSON, &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.AuthRejected {
		t.Fatal("legacy entry without auth_rejected decoded as an auth park")
	}
	legacyPool := NewPools(testRegistry(t, "solo"), nil, clock.now, io.Discard)
	legacyPool.LoadPersistState(map[string]PoolPersistState{"auto": {
		Exhausted:      map[string]time.Time{"solo": reset},
		CredentialPark: map[string]CredentialParkPersist{"solo": legacy},
	}})
	if _, _, ok, exhausted := legacyPool.Route("auto"); !ok || !exhausted {
		t.Errorf("legacy restored route ok=%v exhausted=%v, want synthetic 503 path", ok, exhausted)
	}
}

func TestCredentialDryPoolWorkerRoutesUseSameAuthOnlyRule(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			p := NewPools(workerRegistryWithConcurrency(t, concurrency, "a", "b"), nil, clock.now, io.Discard)
			reg := p.CurrentRegistry()
			for _, nick := range []string{"a", "b"} {
				b, _ := reg.ResolveIn("auto", nick)
				if err := p.ModifyResponse(respAuth(b, http.StatusUnauthorized)); err != nil {
					t.Fatalf("ModifyResponse %s 401: %v", nick, err)
				}
			}
			var upstreamNicks []string
			gateway := backend.WorkerNamespaceMiddleware(backend.Middleware(p, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, ok := backend.FromContext(r.Context())
				if !ok {
					t.Error("namespaced upstream request missing backend context")
					return
				}
				upstreamNicks = append(upstreamNicks, b.Nick)
				resp := &http.Response{
					StatusCode: http.StatusUnauthorized,
					Header:     http.Header{"X-Upstream-Test": []string{"preserved"}},
					Request:    r,
					Body:       io.NopCloser(strings.NewReader("worker auth rejected")),
				}
				if err := p.ModifyResponse(resp); err != nil {
					t.Errorf("ModifyResponse worker 401: %v", err)
					return
				}
				w.Header().Set("X-Upstream-Test", resp.Header.Get("X-Upstream-Test"))
				w.Header().Set("Retry-After", resp.Header.Get("Retry-After"))
				w.WriteHeader(resp.StatusCode)
				_, _ = io.Copy(w, resp.Body)
			})))
			for _, worker := range []string{"worker-one", "worker-two"} {
				r := httptest.NewRequest(http.MethodPost, "/_aqg/w/"+worker+"/v1/messages", nil)
				r.Header.Set("Authorization", "Bearer auto")
				w := httptest.NewRecorder()
				gateway.ServeHTTP(w, r)
				if w.Code != http.StatusUnauthorized || w.Body.String() != "worker auth rejected" {
					t.Errorf("worker %s response=%d %q, want upstream 401/body", worker, w.Code, w.Body.String())
				}
				if w.Header().Get("X-Upstream-Test") != "preserved" || w.Header().Get("Retry-After") == "" {
					t.Errorf("worker %s headers=%v, want upstream header and Retry-After", worker, w.Header())
				}
			}
			if strings.Join(upstreamNicks, ",") != "a,a" {
				t.Errorf("namespaced upstream member sequence=%v, want [a a]", upstreamNicks)
			}
		})
	}
}

func TestCredentialDryPoolWorkerRecoveryRoutesNormally(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			p := NewPools(workerPriorityRegistry(t, concurrency, "a,b", "a", "b"), nil, clock.now, io.Discard)
			reg := p.CurrentRegistry()
			for _, nick := range []string{"a", "b"} {
				b, _ := reg.ResolveIn("auto", nick)
				if err := p.ModifyResponse(respAuth(b, http.StatusUnauthorized)); err != nil {
					t.Fatalf("ModifyResponse %s 401: %v", nick, err)
				}
			}

			var upstreamNicks []string
			gateway := backend.WorkerNamespaceMiddleware(backend.Middleware(p, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, ok := backend.FromContext(r.Context())
				if !ok {
					t.Error("namespaced upstream request missing backend context")
					return
				}
				upstreamNicks = append(upstreamNicks, b.Nick)
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Request:    r,
					Body:       io.NopCloser(strings.NewReader("recovered")),
				}
				if err := p.ModifyResponse(resp); err != nil {
					t.Errorf("ModifyResponse worker 200: %v", err)
					return
				}
				w.WriteHeader(resp.StatusCode)
				_, _ = io.Copy(w, resp.Body)
			})))
			doRequest := func() *httptest.ResponseRecorder {
				r := httptest.NewRequest(http.MethodPost, "/_aqg/w/worker-one/v1/messages", nil)
				r.Header.Set("Authorization", "Bearer auto")
				w := httptest.NewRecorder()
				gateway.ServeHTTP(w, r)
				return w
			}

			if first := doRequest(); first.Code != http.StatusOK || first.Body.String() != "recovered" {
				t.Fatalf("auth-only worker retry response=%d %q, want recovered 200", first.Code, first.Body.String())
			}
			status, ok := p.PoolStatus("auto", quota.NewStore(), nil)
			if !ok {
				t.Fatal("PoolStatus(auto) missing")
			}
			if memberStatus(status, "a") != "serving" || memberParked(status, "a") {
				t.Errorf("recovered worker member status=%q parked=%v, want serving/false", memberStatus(status, "a"), memberParked(status, "a"))
			}
			for _, member := range status.Members {
				if member.Nick == "a" && member.ExhaustedUntil != nil {
					t.Errorf("recovered worker exhausted_until=%v, want null", member.ExhaustedUntil)
				}
			}

			c := p.byPool["auto"]
			c.mu.Lock()
			_, stillParked := c.credentialPark["a"]
			c.mu.Unlock()
			if stillParked {
				t.Fatal("recovered worker member still has a credential park before the next request")
			}
			if second := doRequest(); second.Code != http.StatusOK || second.Body.String() != "recovered" {
				t.Fatalf("normal worker request response=%d %q, want 200", second.Code, second.Body.String())
			}
			if strings.Join(upstreamNicks, ",") != "a,a" {
				t.Errorf("worker upstream member sequence=%v, want [a a]", upstreamNicks)
			}
			c.mu.Lock()
			assigned := c.workerAffinity["worker-one"]
			c.mu.Unlock()
			if concurrency > 1 && assigned != "a" {
				t.Errorf("worker assignment after recovery=%q, want a", assigned)
			}
			if concurrency == 1 && assigned != "" {
				t.Errorf("concurrency-one worker unexpectedly has affinity %q", assigned)
			}
		})
	}
}

func TestCredentialDryPoolUsesPriorityBeforeSortedNicks(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerPriorityRegistry(t, 2, "b,a", "a", "b"), nil, clock.now, io.Discard)
	reg := p.CurrentRegistry()
	for _, nick := range []string{"a", "b"} {
		b, _ := reg.ResolveIn("auto", nick)
		if err := p.ModifyResponse(respAuth(b, http.StatusUnauthorized)); err != nil {
			t.Fatalf("ModifyResponse %s 401: %v", nick, err)
		}
	}
	if got, _, exhausted := p.byPool["auto"].ResolveAuto(); exhausted || got.Nick != "b" {
		t.Fatalf("ResolveAuto()=%q exhausted=%v, want priority-first b/false", got.Nick, exhausted)
	}
}
