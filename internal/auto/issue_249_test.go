package auto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// Issue #249: a z.ai 429 is absorbed as the 1302 throttle only while the
// pool's poller liveness vouches for the member; a blind poller turns it into
// a park-and-switch whose own triggering snapshot cannot retire it.

const issue249ZaiURL = "https://api.z.ai/api/anthropic"

type issue249RoundTrip func(*http.Request) (*http.Response, error)

func (f issue249RoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// issue249QuotaClient answers every request with a z.ai quota-endpoint body
// at the given 5h percentage, so neither recovery probes nor poller ticks
// ever reach the real network.
func issue249QuotaClient(percentage int) *http.Client {
	return &http.Client{Transport: issue249RoundTrip(func(r *http.Request) (*http.Response, error) {
		body := `{"data":{"limits":[{"type":"TOKENS_LIMIT","percentage":` + strconv.Itoa(percentage) + `,"nextResetTime":4102444800000}]}}`
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
}

func issue249FailingClient() *http.Client {
	return &http.Client{Transport: issue249RoundTrip(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("quota endpoint unreachable")
	})}
}

// issue249Pools builds a z.ai pool "auto" with the given members (and extra
// env), a shared store, and a failing probe client on every controller.
func issue249Pools(t *testing.T, clock *fixedClock, logOut io.Writer, extra map[string]string, nicks ...string) (*Pools, *quota.Store) {
	t.Helper()
	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"AUTO_BASE_URL", issue249ZaiURL)
	for _, n := range nicks {
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_"+strings.ToUpper(n), "cred-"+n)
	}
	for k, v := range extra {
		t.Setenv(k, v)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	store := quota.NewStore()
	p := NewPools(reg, store, clock.now, logOut)
	for _, c := range p.byPool {
		c.SetProbeHTTPClient(issue249FailingClient())
	}
	return p, store
}

func issue249SetStale(c *Controller, stale bool) {
	c.pollerStale = func(backend.Backend) bool { return stale }
}

func issue249CredPark(c *Controller, nick string) (credentialParkEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.credentialPark[nick]
	return e, ok
}

func issue249RetryAfter(t *testing.T, resp *http.Response) int {
	t.Helper()
	ra, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil {
		t.Fatalf("Retry-After %q: %v", resp.Header.Get("Retry-After"), err)
	}
	return ra
}

// AC1: Option 5's purpose is met by #316 — a z.ai 429 with a fresh full 5h
// window and a nil reset parks until AsOf + defaultExhaustionWindow even
// while the poller is live.
func TestIssue249_zaiAtCapNilResetParks(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p, store := issue249Pools(t, clock, io.Discard, nil, "a", "b")
	c := p.byPool["auto"]
	issue249SetStale(c, false)
	full, asOf := 1.0, clock.now().Add(-time.Minute)
	store.Put("a", quota.Snapshot{Unified5hUtilization: &full, AsOf: asOf})

	resp := statuslessFailure(t, c, "a", http.StatusTooManyRequests)
	if resp.StatusCode != http.StatusServiceUnavailable || c.Current() != "b" {
		t.Fatalf("response=%d active=%q; want 503 and failover to b", resp.StatusCode, c.Current())
	}
	if got, ok := c.exhaustedUntil("a"); !ok || !got.Equal(asOf.Add(defaultExhaustionWindow)) {
		t.Fatalf("park = %v,%v; want %v,true", got, ok, asOf.Add(defaultExhaustionWindow))
	}
}

// AC2/AC3 (review v1 regression): with the poller stale, a z.ai 429 parks and
// fails over even when the store holds fresh non-full data — including the
// top-level-only unified-status the observer admits from the triggering 429
// itself at AsOf == now.
func TestIssue249_stalePollerParksDespiteFreshNonFullStore(t *testing.T) {
	half := 0.5
	for _, tc := range []struct {
		name string
		snap func(now time.Time) *quota.Snapshot
	}{
		{"no store data", func(time.Time) *quota.Snapshot { return nil }},
		{"top-level status at now", func(now time.Time) *quota.Snapshot {
			return &quota.Snapshot{UnifiedStatus: "rejected", AsOf: now}
		}},
		{"recent sub-cap poll", func(now time.Time) *quota.Snapshot {
			return &quota.Snapshot{Unified5hUtilization: &half, AsOf: now.Add(-time.Minute)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			var logBuf bytes.Buffer
			p, store := issue249Pools(t, clock, &logBuf, nil, "a", "b")
			c := p.byPool["auto"]
			issue249SetStale(c, true)
			if s := tc.snap(clock.now()); s != nil {
				store.Put("a", *s)
			}

			resp := statuslessFailure(t, c, "a", http.StatusTooManyRequests)
			if resp.StatusCode != http.StatusServiceUnavailable || c.Current() != "b" {
				t.Fatalf("response=%d active=%q; want 503 and failover to b", resp.StatusCode, c.Current())
			}
			for i := 0; i < 3; i++ {
				if got, _, exhausted := c.ResolveAuto(); exhausted || got.Nick != "b" {
					t.Fatalf("ResolveAuto = %q exhausted=%v; want b (a must stay parked)", got.Nick, exhausted)
				}
			}
			if got, ok := c.exhaustedUntil("a"); !ok || !got.Equal(clock.now().Add(defaultExhaustionWindow)) {
				t.Fatalf("park = %v,%v; want now+5h", got, ok)
			}
			if e, ok := issue249CredPark(c, "a"); !ok || !e.windowFact || e.authRejected || !e.since.Equal(clock.now()) {
				t.Fatalf("credentialPark[a] = %+v,%v; want windowFact, non-auth, since=now", e, ok)
			}
			if !memberParked(c.poolStatus(store, nil, nil), "a") {
				t.Fatal("poolStatus(a) not parked; the clear button would not be offered")
			}
			log := logBuf.String()
			if !strings.Contains(log, "z.ai 429 with stale poller signal") || strings.Contains(log, "concurrency throttle") {
				t.Fatalf("log = %q; want the stale-poller line, not the throttle line", log)
			}
		})
	}
}

// AC2: a single-member pool reaches #203's dry-pool 503 with the long
// Retry-After when the park-time recovery probe fails or still reads at cap.
// A healthy probe is the recovery path and unparks immediately.
func TestIssue249_stalePollerSingletonDryPool(t *testing.T) {
	for _, tc := range []struct {
		name       string
		client     *http.Client
		wantParked bool
	}{
		{"probe fails", issue249FailingClient(), true},
		{"probe at cap", issue249QuotaClient(100), true},
		{"probe healthy", issue249QuotaClient(50), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			p, store := issue249Pools(t, clock, io.Discard, nil, "a")
			c := p.byPool["auto"]
			c.SetProbeHTTPClient(tc.client)
			issue249SetStale(c, true)
			store.Put("a", quota.Snapshot{UnifiedStatus: "rejected", AsOf: clock.now()})

			resp := statuslessFailure(t, c, "a", http.StatusTooManyRequests)
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", resp.StatusCode)
			}
			ra := issue249RetryAfter(t, resp)
			_, _, exhausted := c.ResolveAuto()
			if tc.wantParked {
				if ra < int(defaultExhaustionWindow/time.Second)-1 {
					t.Fatalf("Retry-After = %d; want the dry-pool ~%ds wait", ra, int(defaultExhaustionWindow/time.Second))
				}
				if !exhausted {
					t.Fatal("ResolveAuto not exhausted; the stale-poller park did not hold")
				}
				return
			}
			if ra > zaiThrottleRetryAfterSeconds || exhausted {
				t.Fatalf("Retry-After=%d exhausted=%v; want a recovered member and a short switch hint", ra, exhausted)
			}
		})
	}
}

// AC3: later evidence retires the park — a store snapshot observed after it,
// the 5h bound, and an explicit clear.
func TestIssue249_stalePollerParkLaterRetirement(t *testing.T) {
	setup := func(t *testing.T) (*fixedClock, *Pools, *quota.Store, *Controller) {
		clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
		p, store := issue249Pools(t, clock, io.Discard, nil, "a", "b")
		c := p.byPool["auto"]
		issue249SetStale(c, true)
		statuslessFailure(t, c, "a", http.StatusTooManyRequests)
		if _, ok := c.exhaustedUntil("a"); !ok {
			t.Fatal("setup: a not parked")
		}
		return clock, p, store, c
	}

	t.Run("later store snapshot", func(t *testing.T) {
		clock, _, store, c := setup(t)
		half := 0.5
		store.Put("a", quota.Snapshot{Unified5hUtilization: &half, AsOf: clock.now()})
		if _, ok := c.exhaustedUntil("a"); !ok {
			t.Fatal("a snapshot stamped at the park instant retired it; only later evidence may")
		}
		clock.advance(time.Minute)
		store.Put("a", quota.Snapshot{Unified5hUtilization: &half, AsOf: clock.now()})
		if got, ok := c.exhaustedUntil("a"); ok {
			t.Fatalf("park still %v after a later fresh sub-cap snapshot", got)
		}
	})

	t.Run("5h bound", func(t *testing.T) {
		clock, _, _, c := setup(t)
		clock.advance(defaultExhaustionWindow - time.Second)
		if _, ok := c.exhaustedUntil("a"); !ok {
			t.Fatal("park expired before its bound")
		}
		clock.advance(time.Second)
		if got, ok := c.exhaustedUntil("a"); ok {
			t.Fatalf("park still %v at its bound", got)
		}
	})

	t.Run("clear", func(t *testing.T) {
		_, p, _, c := setup(t)
		cleared, _, ok := p.ClearExhausted("auto")
		if !ok || len(cleared) != 1 || cleared[0] != "a" {
			t.Fatalf("ClearExhausted = %v,%v; want [a],true", cleared, ok)
		}
		if _, ok := c.exhaustedUntil("a"); ok {
			t.Fatal("a still parked after clear")
		}
		if _, ok := issue249CredPark(c, "a"); ok {
			t.Fatal("credentialPark[a] survived clear")
		}
	})
}

// AC3: the park propagates to sibling pools sharing the nick, carrying since,
// so the shared store's pre-park snapshot cannot neutralize it there either.
func TestIssue249_stalePollerParkPropagates(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	env := propagationEnv()
	for _, pool := range []string{"A", "B", "C"} {
		env[backend.EnvPrefix+pool+"_BASE_URL"] = issue249ZaiURL
	}
	scrubPoolEnv(t)
	for k, v := range env {
		t.Setenv(k, v)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	store := quota.NewStore()
	p := NewPools(reg, store, clock.now, io.Discard)
	for _, c := range p.byPool {
		c.SetProbeHTTPClient(issue249FailingClient())
	}
	issue249SetStale(p.byPool["a"], true)
	store.Put("ccz", quota.Snapshot{UnifiedStatus: "rejected", AsOf: clock.now()})

	b, _ := p.CurrentRegistry().ResolveIn("a", "ccz")
	req, _ := http.NewRequestWithContext(backend.WithBackend(context.Background(), b), http.MethodPost, "/v1/messages", nil)
	resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{}, Request: req, Body: io.NopCloser(strings.NewReader("1302"))}
	if err := p.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	for pool, want := range map[string]string{"a": "a2", "b": "b2", "c": "c2"} {
		if got, _, _, exhausted := p.Route(pool); exhausted || got.Nick != want {
			t.Errorf("pool %s routed %q exhausted=%v; want %s", pool, got.Nick, exhausted, want)
		}
		if e, ok := issue249CredPark(p.byPool[pool], "ccz"); !ok || !e.since.Equal(clock.now()) {
			t.Errorf("pool %s credentialPark[ccz] = %+v,%v; want since=now", pool, e, ok)
		}
	}
}

// AC4: a live poller, or no poller at all, keeps #153's absorb.
func TestIssue249_livePollerOrNoPollerAbsorbs(t *testing.T) {
	for _, tc := range []struct {
		name string
		hook func(*Controller)
	}{
		{"live poller", func(c *Controller) { issue249SetStale(c, false) }},
		{"no poller", func(c *Controller) { c.pollerStale = nil }},
		{"unset poller on Pools", func(*Controller) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			p, _ := issue249Pools(t, clock, io.Discard, nil, "a", "b")
			c := p.byPool["auto"]
			tc.hook(c)
			resp := statuslessFailure(t, c, "a", http.StatusTooManyRequests)
			if resp.StatusCode != http.StatusServiceUnavailable || issue249RetryAfter(t, resp) != zaiThrottleRetryAfterSeconds {
				t.Fatalf("response=%d Retry-After=%q; want the 3s throttle 503", resp.StatusCode, resp.Header.Get("Retry-After"))
			}
			if c.Current() != "a" {
				t.Fatalf("active = %q; want a (absorb must not switch)", c.Current())
			}
			if _, ok := c.exhaustedUntil("a"); ok {
				t.Fatal("a parked on an absorbed throttle")
			}
		})
	}
}

// AC3b: liveness is pool-keyed. A non-current worker-assigned member that
// the poller has never polled is absorbed while the pool's poller is live,
// and parked (worker reassigned) only when the pool's poller is stale.
func TestIssue249_workerMemberUsesPoolLiveness(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run("stale="+strconv.FormatBool(stale), func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			p, _ := issue249Pools(t, clock, io.Discard, map[string]string{
				backend.EnvPrefix + "AUTO_CONCURRENCY": "2",
			}, "a", "b", "c")
			c := p.byPool["auto"]
			var gotBackend backend.Backend
			c.pollerStale = func(b backend.Backend) bool { gotBackend = b; return stale }
			for _, w := range []string{"worker-a", "worker-b"} {
				if _, _, _, exhausted := p.RouteWorker("auto", w); exhausted {
					t.Fatalf("RouteWorker(%s) exhausted", w)
				}
			}
			if got := c.workerAffinity["worker-b"]; got != "b" || c.Current() == "b" {
				t.Fatalf("setup: worker-b on %q, current %q; want worker-b on non-current b", got, c.Current())
			}

			statuslessFailure(t, c, "b", http.StatusTooManyRequests)
			if gotBackend.Nick != "b" {
				t.Fatalf("hook consulted for %q; want b's provider", gotBackend.Nick)
			}
			got, _, _, _ := p.RouteWorker("auto", "worker-b")
			_, parked := c.exhaustedUntil("b")
			if !stale && (parked || got.Nick != "b") {
				t.Fatalf("live pool poller: b parked=%v, worker-b on %q; want absorbed and kept on b", parked, got.Nick)
			}
			if stale && (!parked || got.Nick == "b") {
				t.Fatalf("stale pool poller: b parked=%v, worker-b on %q; want parked and reassigned", parked, got.Nick)
			}
		})
	}
}

// Wiring: Pools.SetPoller feeds the real poller's pool liveness to the gate —
// a tracked pool that has never polled reads stale and parks; after a
// successful poll the same 429 is absorbed.
func TestIssue249_setPollerWiring(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p, store := issue249Pools(t, clock, io.Discard, nil, "a", "b")
	pl := poller.New([]string{"auto"}, p.Current, p.MarkLocalSnapshot, store, issue249QuotaClient(50), time.Minute, clock.now, io.Discard)
	p.SetPoller(pl)
	c := p.byPool["auto"]

	pl.PollAllForTest(context.Background())
	resp := statuslessFailure(t, c, "a", http.StatusTooManyRequests)
	if issue249RetryAfter(t, resp) != zaiThrottleRetryAfterSeconds || c.Current() != "a" {
		t.Fatalf("fresh poller: Retry-After=%q active=%q; want absorb on a", resp.Header.Get("Retry-After"), c.Current())
	}

	clock.advance(poller.StaleAfterIntervals*time.Minute + time.Second)
	statuslessFailure(t, c, "a", http.StatusTooManyRequests)
	if c.Current() != "b" {
		t.Fatalf("stale poller: active=%q; want failover to b", c.Current())
	}
}

// The since stamp survives the state file; a legacy entry without it
// restores to the zero value (existing reconciliation).
func TestIssue249_sincePersists(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p, _ := issue249Pools(t, clock, io.Discard, nil, "a", "b")
	issue249SetStale(p.byPool["auto"], true)
	statuslessFailure(t, p.byPool["auto"], "a", http.StatusTooManyRequests)

	raw, err := json.Marshal(p.PersistState())
	if err != nil {
		t.Fatal(err)
	}
	var states map[string]PoolPersistState
	if err := json.Unmarshal(raw, &states); err != nil {
		t.Fatal(err)
	}
	q, _ := issue249Pools(t, clock, io.Discard, nil, "a", "b")
	q.LoadPersistState(states)
	if e, ok := issue249CredPark(q.byPool["auto"], "a"); !ok || !e.since.Equal(clock.now()) || !e.windowFact {
		t.Fatalf("restored credentialPark[a] = %+v,%v; want since=now, windowFact", e, ok)
	}

	var legacy CredentialParkPersist
	if err := json.Unmarshal([]byte(`{"reset":"2030-01-01T00:00:00Z","window_fact":true}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if !legacy.Since.IsZero() {
		t.Fatalf("legacy Since = %v; want zero", legacy.Since)
	}
	if out, _ := json.Marshal(CredentialParkPersist{Reset: clock.now()}); strings.Contains(string(out), "since") {
		t.Fatalf("zero Since serialized: %s", out)
	}
}
