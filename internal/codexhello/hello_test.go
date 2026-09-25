package codexhello

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type redirectTransport struct {
	target *url.URL
	base   http.RoundTripper
	seen   func(*http.Request)
}

func (t redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.seen != nil {
		t.seen(req)
	}
	clone := req.Clone(req.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	clone.Host = t.target.Host
	return t.base.RoundTrip(clone)
}

func testClient(t *testing.T, handler http.Handler, seen func(*http.Request)) (*httptest.Server, *http.Client) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return server, &http.Client{Transport: redirectTransport{target: target, base: http.DefaultTransport, seen: seen}}
}

func codexMember(pool, nick, credential, baseURL string) backend.Backend {
	return backend.Backend{Pool: pool, Nick: nick, Credential: credential, BaseURL: baseURL}
}

func expiredSnapshot(reset, asOf time.Time) quota.Snapshot {
	weekly := reset
	used := 0.4
	return quota.Snapshot{Unified7dReset: &weekly, Unified7dUtilization: &used, AsOf: asOf}
}

func testClock(now time.Time) func() time.Time { return func() time.Time { return now } }

func TestTickEligibilityUsesCodexHostAndStoredWeeklyReset(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	reset := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	var requests atomic.Int32
	_, client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}), nil)

	cases := []struct {
		name    string
		member  backend.Backend
		snap    *quota.Snapshot
		wantReq int32
	}{
		{
			name:    "Codex in mixed pool fires",
			member:  codexMember("mixed", "seat", "token", "https://chatgpt.com/backend-api/codex"),
			snap:    ptrSnapshot(expiredSnapshot(reset, reset.Add(-time.Minute))),
			wantReq: 1,
		},
		{
			name:    "codex-named pool with another host is ineligible",
			member:  codexMember("codex", "seat", "token", "https://example.com/api"),
			snap:    ptrSnapshot(expiredSnapshot(reset, reset.Add(-time.Minute))),
			wantReq: 0,
		},
		{
			name:    "future weekly reset",
			member:  codexMember("mixed", "future", "token", "https://chatgpt.com/backend-api/codex"),
			snap:    ptrSnapshot(expiredSnapshot(future, now)),
			wantReq: 0,
		},
		{
			name:    "AsOf equals reset",
			member:  codexMember("mixed", "equal", "token", "https://chatgpt.com/backend-api/codex"),
			snap:    ptrSnapshot(expiredSnapshot(reset, reset)),
			wantReq: 0,
		},
		{
			name:    "AsOf newer than reset",
			member:  codexMember("mixed", "newer", "token", "https://chatgpt.com/backend-api/codex"),
			snap:    ptrSnapshot(expiredSnapshot(reset, now)),
			wantReq: 0,
		},
		{
			name: "disabled member",
			member: func() backend.Backend {
				b := codexMember("mixed", "disabled", "token", "https://chatgpt.com/backend-api/codex")
				b.Disabled = true
				return b
			}(),
			snap:    ptrSnapshot(expiredSnapshot(reset, reset.Add(-time.Minute))),
			wantReq: 0,
		},
		{
			name:    "no stored snapshot",
			member:  codexMember("mixed", "empty", "token", "https://chatgpt.com/backend-api/codex"),
			wantReq: 0,
		},
		{
			name:    "weekly reset required even when five-hour data is stale",
			member:  codexMember("mixed", "five-hour-only", "token", "https://chatgpt.com/backend-api/codex"),
			snap:    ptrSnapshot(quota.Snapshot{Unified5hReset: &reset, AsOf: reset.Add(-time.Minute)}),
			wantReq: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := quota.NewStore()
			if tc.snap != nil {
				store.Put(tc.member.QuotaKey(), *tc.snap)
			}
			s := New(Config{Members: func() []backend.Backend { return []backend.Backend{tc.member} }, Store: store, Client: client, Now: testClock(now), Log: io.Discard})
			s.tick(context.Background())
			if got := requests.Load(); got != tc.wantReq {
				t.Errorf("requests=%d, want %d", got, tc.wantReq)
			}
			requests.Store(0)
		})
	}
}

func ptrSnapshot(s quota.Snapshot) *quota.Snapshot { return &s }

func TestTickReadsCurrentMembershipAfterRuntimeRemoval(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	reset := now.Add(-time.Hour)
	var requests atomic.Int32
	_, client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}), nil)
	member := codexMember("mixed", "seat", "token", "https://chatgpt.com/backend-api/codex")
	currentMembers := []backend.Backend{member}
	store := quota.NewStore()
	store.Put("seat", expiredSnapshot(reset, reset.Add(-time.Minute)))
	s := New(Config{Members: func() []backend.Backend { return currentMembers }, Store: store, Client: client, Now: testClock(now), Log: io.Discard})
	// The same callback observes the copy-on-write registry after removal.
	currentMembers = nil
	s.tick(context.Background())
	if got := requests.Load(); got != 0 {
		t.Errorf("requests=%d after runtime removal, want 0", got)
	}
}

func TestHelloUsesProxyAuthBodyAndMemberResponsesPath(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	reset := now.Add(-time.Hour)
	for _, tc := range []struct {
		name       string
		credential string
		wantAuth   string
		wantAPIKey string
		wantBeta   string
	}{
		{"oauth", "sk-ant-oat-seat", "Bearer sk-ant-oat-seat", "", "oauth-2025-04-20"},
		{"api key", "sk-ant-api-seat", "", "sk-ant-api-seat", ""},
		{"other", "codex-seat", "Bearer codex-seat", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotMethod, gotPath, gotBody, gotAuth, gotAPIKey, gotBeta, gotType string
			server, client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
				}
				gotMethod, gotPath, gotBody = r.Method, r.URL.Path, string(body)
				gotAuth, gotAPIKey = r.Header.Get("Authorization"), r.Header.Get("x-api-key")
				gotBeta, gotType = r.Header.Get("anthropic-beta"), r.Header.Get("Content-Type")
				w.Header().Set(quota.HeaderCodexSecondaryUsedPercent, "30")
				w.Header().Set(quota.HeaderCodexSecondaryResetAt, strconv.FormatInt(time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC).Unix(), 10))
				w.WriteHeader(http.StatusOK)
			}), nil)
			defer server.Close()
			member := codexMember("mixed", "seat", tc.credential, "https://chatgpt.com/backend-api/codex/")
			store := quota.NewStore()
			store.Put("seat", expiredSnapshot(reset, reset.Add(-time.Minute)))
			s := New(Config{Members: func() []backend.Backend { return []backend.Backend{member} }, Store: store, Client: client, Now: testClock(now), Log: io.Discard})
			s.tick(context.Background())
			if gotMethod != http.MethodPost || gotPath != "/backend-api/codex/responses" {
				t.Errorf("request %s %q, want POST /backend-api/codex/responses", gotMethod, gotPath)
			}
			if gotBody != requestBody {
				t.Errorf("body=%q, want %q", gotBody, requestBody)
			}
			if gotAuth != tc.wantAuth || gotAPIKey != tc.wantAPIKey || gotBeta != tc.wantBeta {
				t.Errorf("auth headers Authorization=%q x-api-key=%q beta=%q", gotAuth, gotAPIKey, gotBeta)
			}
			if gotType != "application/json" {
				t.Errorf("Content-Type=%q, want application/json", gotType)
			}
		})
	}
}

func TestConcurrentTicksDeduplicateSharedAccountAndAllowLaterReset(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	reset := now.Add(-time.Hour)
	var requests atomic.Int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	finished := make(chan struct{}, 12)
	server, client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}), nil)
	defer server.Close()
	members := []backend.Backend{
		codexMember("alpha", "shared", "token", "https://chatgpt.com/backend-api/codex"),
		codexMember("beta", "shared", "token", "https://chatgpt.com/backend-api/codex"),
	}
	store := quota.NewStore()
	store.Put("shared", expiredSnapshot(reset, reset.Add(-time.Minute)))
	currentNow := now
	s := New(Config{Members: func() []backend.Backend { return members }, Store: store, Client: client, Now: func() time.Time { return currentNow }, Log: io.Discard})

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.tick(context.Background())
			finished <- struct{}{}
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("hello request did not start")
	}
	for i := 0; i < 11; i++ {
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("concurrent ticks did not finish while the first request was in flight")
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("concurrent requests=%d, want 1", got)
	}
	close(release)
	wg.Wait()

	// A repeated tick is suppressed for the same reset. A distinct later
	// reset for the same account gets exactly one new attempt.
	s.tick(context.Background())
	if got := requests.Load(); got != 1 {
		t.Fatalf("repeat requests=%d, want 1", got)
	}
	nextReset := reset.Add(7 * 24 * time.Hour)
	store.Put("shared", expiredSnapshot(nextReset, nextReset.Add(-time.Minute)))
	// The service uses the injected clock, so move it to after the new reset.
	currentNow = nextReset.Add(time.Minute)
	s.tick(context.Background())
	if got := requests.Load(); got != 2 {
		t.Fatalf("later-reset requests=%d, want 2", got)
	}
}

func TestSuccessfulHelloMergesQuotaAndMarksOriginPool(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	reset := now.Add(-time.Hour)
	server, client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(quota.HeaderCodexPrimaryUsedPercent, "10")
		w.Header().Set(quota.HeaderCodexPrimaryResetAt, "1790500000")
		w.Header().Set(quota.HeaderCodexSecondaryUsedPercent, "20")
		w.Header().Set(quota.HeaderCodexSecondaryResetAt, "1791000000")
		w.WriteHeader(http.StatusOK)
	}), nil)
	defer server.Close()
	members := []backend.Backend{
		codexMember("alpha", "shared", "token", "https://chatgpt.com/backend-api/codex"),
		codexMember("beta", "shared", "token", "https://chatgpt.com/backend-api/codex"),
	}
	store := quota.NewStore()
	store.Put("shared", expiredSnapshot(reset, reset.Add(-time.Minute)))
	var markedMu sync.Mutex
	var marked []string
	s := New(Config{
		Members: func() []backend.Backend { return members }, Store: store, Client: client, Now: testClock(now), Log: io.Discard,
		MarkLocal: func(poolName, nick string) {
			markedMu.Lock()
			marked = append(marked, poolName+"/"+nick)
			markedMu.Unlock()
		},
	})
	s.tick(context.Background())
	snap := store.Get("shared")
	if snap.Unified5hUtilization == nil || *snap.Unified5hUtilization != .1 {
		t.Errorf("primary utilization=%v, want .1", snap.Unified5hUtilization)
	}
	if snap.Unified7dUtilization == nil || *snap.Unified7dUtilization != .2 {
		t.Errorf("secondary utilization=%v, want .2", snap.Unified7dUtilization)
	}
	if snap.Unified7dReset == nil || !snap.Unified7dReset.Equal(time.Unix(1791000000, 0).UTC()) {
		t.Errorf("secondary reset=%v, want response reset", snap.Unified7dReset)
	}
	if !snap.AsOf.Equal(now) {
		t.Errorf("AsOf=%v, want %v", snap.AsOf, now)
	}
	markedMu.Lock()
	defer markedMu.Unlock()
	if strings.Join(marked, ",") != "alpha/shared" {
		t.Errorf("marked pools=%v, want only the pool that supplied the hello", marked)
	}
}

func TestSuccessfulHelloAppearsInPoolStatusImmediately(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	reset := now.Add(-time.Hour)
	server, client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(quota.HeaderCodexSecondaryUsedPercent, "35")
		w.Header().Set(quota.HeaderCodexSecondaryResetAt, "1791000000")
		w.WriteHeader(http.StatusOK)
	}), nil)
	defer server.Close()
	reg, err := backend.BuildFromSpec(backend.Spec{Pools: map[string]backend.PoolSpec{
		"mixed": {
			BaseURL: "https://chatgpt.com/backend-api/codex",
			Members: map[string]backend.MemberSpec{"seat": {Credential: "token"}},
		},
	}}, "")
	if err != nil {
		t.Fatal(err)
	}
	store := quota.NewStore()
	store.Put("seat", expiredSnapshot(reset, reset.Add(-time.Minute)))
	pools := auto.NewPools(reg, store, testClock(now), io.Discard)
	service := New(Config{
		Members: func() []backend.Backend {
			var members []backend.Backend
			current := pools.CurrentRegistry()
			for _, poolName := range current.PoolNames() {
				for _, nick := range current.PoolNicks(poolName) {
					if member, ok := current.ResolveIn(poolName, nick); ok {
						members = append(members, member)
					}
				}
			}
			return members
		},
		MarkLocal: pools.MarkLocalSnapshot, Store: store, Client: client, Now: testClock(now), Log: io.Discard,
	})
	service.tick(context.Background())
	status, ok := pools.PoolStatus("mixed", store, nil)
	if !ok {
		t.Fatal("PoolStatus(mixed) not found")
	}
	for _, member := range status.Members {
		if member.Nick == "seat" {
			if member.Snapshot == nil || member.Snapshot.Unified7dUtilization == nil || *member.Snapshot.Unified7dUtilization != .35 {
				t.Fatalf("pool status snapshot=%+v, want returned weekly quota", member.Snapshot)
			}
			return
		}
	}
	t.Fatal("pool status did not include seat")
}

func TestEmpty2xxDoesNotAdvanceSnapshotOrRetryAttempt(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	reset := now.Add(-time.Hour)
	var requests atomic.Int32
	server, client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}), nil)
	defer server.Close()
	member := codexMember("mixed", "seat", "token", "https://chatgpt.com/backend-api/codex")
	store := quota.NewStore()
	before := expiredSnapshot(reset, reset.Add(-time.Minute))
	store.Put("seat", before)
	s := New(Config{Members: func() []backend.Backend { return []backend.Backend{member} }, Store: store, Client: client, Now: testClock(now), Log: io.Discard})
	s.tick(context.Background())
	s.tick(context.Background())
	if got := requests.Load(); got != 1 {
		t.Errorf("requests=%d, want one attempted request", got)
	}
	after := store.Get("seat")
	if !after.AsOf.Equal(before.AsOf) || after.Unified7dReset == nil || !after.Unified7dReset.Equal(reset) {
		t.Errorf("empty 2xx changed snapshot: before=%+v after=%+v", before, after)
	}
}

func TestFailureLogsOncePreservesSnapshotAndConsumesReset(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	reset := now.Add(-time.Hour)
	for _, tc := range []struct {
		name      string
		transport http.RoundTripper
		status    int
	}{
		{name: "bad request", status: http.StatusBadRequest},
		{name: "transport error", transport: roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("connection reset") })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server, client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(tc.status)
			}), nil)
			defer server.Close()
			if tc.transport != nil {
				client.Transport = tc.transport
			}
			member := codexMember("mixed", "seat", "token", "https://chatgpt.com/backend-api/codex")
			store := quota.NewStore()
			before := expiredSnapshot(reset, reset.Add(-time.Minute))
			store.Put("seat", before)
			var logs strings.Builder
			s := New(Config{Members: func() []backend.Backend { return []backend.Backend{member} }, Store: store, Client: client, Now: testClock(now), Log: &logs})
			s.tick(context.Background())
			s.tick(context.Background())
			after := store.Get("seat")
			if !after.AsOf.Equal(before.AsOf) || after.Unified7dReset == nil || !after.Unified7dReset.Equal(reset) {
				t.Errorf("failure changed snapshot: before=%+v after=%+v", before, after)
			}
			if strings.Count(logs.String(), "\n") != 1 {
				t.Errorf("log lines=%q, want exactly one", logs.String())
			}
			if tc.transport == nil && requests.Load() != 1 {
				t.Errorf("HTTP requests=%d, want one", requests.Load())
			}
		})
	}
}

func TestHelloUsesInjectableThirtySecondDeadline(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	reset := now.Add(-time.Hour)
	const timeout = 125 * time.Millisecond
	var remaining time.Duration
	server, client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), func(req *http.Request) {
		deadline, ok := req.Context().Deadline()
		if !ok {
			t.Error("request has no deadline")
		} else {
			remaining = time.Until(deadline)
		}
	})
	defer server.Close()
	member := codexMember("mixed", "seat", "token", "https://chatgpt.com/backend-api/codex")
	store := quota.NewStore()
	store.Put("seat", expiredSnapshot(reset, reset.Add(-time.Minute)))
	s := New(Config{Members: func() []backend.Backend { return []backend.Backend{member} }, Store: store, Client: client, Timeout: timeout, Now: testClock(now), Log: io.Discard})
	s.tick(context.Background())
	if remaining <= 0 || remaining > timeout {
		t.Errorf("remaining deadline=%v, want >0 and <=%v", remaining, timeout)
	}
	if got := New(Config{}).timeout; got != requestTimeout {
		t.Errorf("default timeout=%v, want %v", got, requestTimeout)
	}
}

func TestTickReturnsEarliestKnownReset(t *testing.T) {
	now := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	firstReset, secondReset := now.Add(2*time.Hour), now.Add(4*time.Hour)
	first := codexMember("mixed", "first", "token", "https://chatgpt.com/backend-api/codex")
	second := codexMember("mixed", "second", "token", "https://chatgpt.com/backend-api/codex")
	store := quota.NewStore()
	store.Put("first", expiredSnapshot(firstReset, now))
	store.Put("second", expiredSnapshot(secondReset, now))
	s := New(Config{Members: func() []backend.Backend { return []backend.Backend{second, first} }, Store: store, Now: testClock(now), Log: io.Discard})
	if got := s.tick(context.Background()); !got.Equal(firstReset) {
		t.Errorf("next reset=%v, want %v", got, firstReset)
	}
}

func TestResponsesURLAppendsPathWithoutForwardingBaseCredentialsOrQuery(t *testing.T) {
	got, err := responsesURL("https://user:secret@chatgpt.com/backend%2Fcodex/?token=secret#fragment")
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://chatgpt.com/backend%2Fcodex/responses" {
		t.Errorf("responsesURL=%q, want escaped member path without URL credentials/query", got)
	}
}
