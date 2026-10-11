package auto

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// reprobePools builds the #254 two-pool shape (a, b sharing nick ccz, each
// with a private fallback ranked below it) wired to store, so store-sourced
// exhaustion is live. baseURL is the pools' default upstream.
func reprobePools(t *testing.T, clock *fixedClock, store *quota.Store, baseURL string) *Pools {
	t.Helper()
	scrubPoolEnv(t)
	for k, v := range map[string]string{
		backend.EnvPrefix + "A_BACKEND_CCZ": "cred-ccz",
		backend.EnvPrefix + "A_BACKEND_A2":  "cred-a2",
		backend.EnvPrefix + "A_PRIORITY":    "ccz",
		backend.EnvPrefix + "B_BACKEND_CCZ": "cred-ccz",
		backend.EnvPrefix + "B_BACKEND_B2":  "cred-b2",
		backend.EnvPrefix + "B_PRIORITY":    "ccz",
	} {
		t.Setenv(k, v)
	}
	reg, err := backend.Load(baseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return NewPools(reg, store, clock.now, io.Discard)
}

// windowSnap is a status-bearing snapshot for one window ("5h" or "7d"), the
// shape quota.Extract produces from Anthropic unified headers.
func windowSnap(window, status string, util float64, reset, asOf time.Time) quota.Snapshot {
	s := quota.Snapshot{UnifiedStatus: status, AsOf: asOf}
	if window == "7d" {
		s.Unified7dStatus, s.Unified7dUtilization, s.Unified7dReset = status, &util, &reset
	} else {
		s.Unified5hStatus, s.Unified5hUtilization, s.Unified5hReset = status, &util, &reset
	}
	return s
}

// staticQuery returns a ReprobeQuery that counts its calls and returns snap.
func staticQuery(calls *atomic.Int32, snap quota.Snapshot, err error) ReprobeQuery {
	return func(context.Context, backend.Backend) (quota.Snapshot, error) {
		calls.Add(1)
		return snap, err
	}
}

func (c *Controller) probeRefs(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.probeInFlight[key]
}

// TestReprobe_storeRejectedHealthyRestores is the #350 field shape: the store
// holds a status-bearing rejected window with a reset days out (5h or 7d),
// the member is exhausted but not parked, and upstream has in fact reset. A
// reprobe merges the fresh allowed headers and the member is selectable again
// with no restart.
func TestReprobe_storeRejectedHealthyRestores(t *testing.T) {
	for _, window := range []string{"5h", "7d"} {
		t.Run(window, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			store := quota.NewStore()
			p := reprobePools(t, clock, store, testDefaultBaseURL)
			store.Put("ccz", windowSnap(window, unifiedStatusRejected, 1, clock.now().Add(72*time.Hour), clock.now()))

			ps, _ := p.PoolStatus("a", store, nil)
			if got := memberStatus_(ps, "ccz"); got != "exhausted" {
				t.Fatalf("before reprobe: ccz status %q, want exhausted", got)
			}
			if memberParked_(ps, "ccz") {
				t.Fatalf("before reprobe: ccz parked, want store-only exhaustion")
			}

			var calls atomic.Int32
			fresh := windowSnap(window, "allowed", 0.01, clock.now().Add(5*time.Hour), clock.now())
			res, status, err := p.Reprobe(context.Background(), "a", "ccz", staticQuery(&calls, fresh, nil))
			if err != nil || status != http.StatusOK {
				t.Fatalf("Reprobe = %d, %v; want 200", status, err)
			}
			if !res.Healthy || res.Pool != "a" || res.Nick != "ccz" {
				t.Errorf("result = %+v, want healthy a/ccz", res)
			}
			if _, ok := p.byPool["a"].exhaustedUntil("ccz"); ok {
				t.Errorf("ccz still exhausted after healthy reprobe")
			}
			ps, _ = p.PoolStatus("a", store, nil)
			if got := memberStatus_(ps, "ccz"); got == "exhausted" {
				t.Errorf("after reprobe: ccz status %q, want selectable", got)
			}
			if b, _, _, exhausted := p.Route("a"); exhausted || b.Nick != "ccz" {
				t.Errorf("Route(a) = %q exhausted=%v, want ccz", b.Nick, exhausted)
			}
		})
	}
}

// TestReprobe_stillRejectedStaysExhausted: an honest upstream rejection is a
// successful reprobe that confirms exhaustion. The fresh reset is merged, the
// member stays exhausted until it, and no park is recorded.
func TestReprobe_stillRejectedStaysExhausted(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	p := reprobePools(t, clock, store, testDefaultBaseURL)
	store.Put("ccz", windowSnap("7d", unifiedStatusRejected, 1, clock.now().Add(72*time.Hour), clock.now()))

	var calls atomic.Int32
	newReset := clock.now().Add(30 * time.Hour)
	res, status, err := p.Reprobe(context.Background(), "a", "ccz",
		staticQuery(&calls, windowSnap("7d", unifiedStatusRejected, 1, newReset, clock.now()), nil))
	if err != nil || status != http.StatusOK {
		t.Fatalf("Reprobe = %d, %v; want 200", status, err)
	}
	if res.Healthy {
		t.Errorf("healthy = true, want false (upstream still rejects)")
	}
	if res.Snapshot.Unified7dReset == nil || !res.Snapshot.Unified7dReset.Equal(newReset) {
		t.Errorf("merged 7d reset = %v, want upstream's %v", res.Snapshot.Unified7dReset, newReset)
	}
	if until, ok := p.byPool["a"].exhaustedUntil("ccz"); !ok || !until.Equal(newReset) {
		t.Errorf("exhaustedUntil = %v/%v, want %v/true", until, ok, newReset)
	}
	c := p.byPool["a"]
	c.mu.Lock()
	_, parked := c.exhausted["ccz"]
	c.mu.Unlock()
	if parked {
		t.Errorf("reprobe recorded a live park; it must only merge the snapshot")
	}
}

// TestReprobe_clearsLiveParkEverywhere: a member parked by a 401 (live park
// here, propagated credential park in the sibling) is released in both pools
// when a reprobe returns healthy (issue #254 propagation).
func TestReprobe_clearsLiveParkEverywhere(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	p := reprobePools(t, clock, store, testDefaultBaseURL)
	bInA, _ := p.CurrentRegistry().ResolveIn("a", "ccz")
	if err := p.ModifyResponse(respAuth(bInA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	for _, pool := range []string{"a", "b"} {
		ps, _ := p.PoolStatus(pool, store, nil)
		if !memberParked_(ps, "ccz") {
			t.Fatalf("pool %s: ccz not parked before reprobe", pool)
		}
	}

	var calls atomic.Int32
	res, status, err := p.Reprobe(context.Background(), "a", "ccz",
		staticQuery(&calls, windowSnap("5h", "allowed", 0.1, clock.now().Add(5*time.Hour), clock.now()), nil))
	if err != nil || status != http.StatusOK || !res.Healthy {
		t.Fatalf("Reprobe = %+v, %d, %v; want healthy 200", res, status, err)
	}
	for _, pool := range []string{"a", "b"} {
		ps, _ := p.PoolStatus(pool, store, nil)
		if memberParked_(ps, "ccz") {
			t.Errorf("pool %s: ccz still parked after healthy reprobe", pool)
		}
		if _, ok := p.byPool[pool].exhaustedUntil("ccz"); ok {
			t.Errorf("pool %s: ccz still exhausted after healthy reprobe", pool)
		}
	}
}

// TestReprobe_validation covers the request-error contract; none of these
// reach the query.
func TestReprobe_validation(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := reprobePools(t, clock, quota.NewStore(), testDefaultBaseURL)
	if _, err := p.SetMemberDisabled("a", "a2", true); err != nil {
		t.Fatalf("SetMemberDisabled: %v", err)
	}
	var calls atomic.Int32
	q := staticQuery(&calls, quota.Snapshot{}, nil)
	for _, tc := range []struct {
		pool, nick string
		want       int
	}{
		{"nope", "ccz", http.StatusNotFound},
		{"a", "ghost", http.StatusBadRequest},
		{"a", "b2", http.StatusBadRequest},
		{"a", "a2", http.StatusConflict},
	} {
		if _, status, err := p.Reprobe(context.Background(), tc.pool, tc.nick, q); status != tc.want || err == nil {
			t.Errorf("Reprobe(%s, %s) = %d, %v; want %d", tc.pool, tc.nick, status, err, tc.want)
		}
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("query called %d times, want 0", n)
	}
}

// TestReprobe_queryErrorLeavesStateUntouched: a failed query proves nothing,
// so the stored snapshot and the live park survive byte-for-byte.
func TestReprobe_queryErrorLeavesStateUntouched(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	p := reprobePools(t, clock, store, testDefaultBaseURL)
	store.Put("ccz", windowSnap("5h", unifiedStatusRejected, 1, clock.now().Add(3*time.Hour), clock.now()))
	parkAt := clock.now().Add(4 * time.Hour)
	p.byPool["a"].record429("ccz", parkAt)
	before := store.Get("ccz")

	var calls atomic.Int32
	boom := errors.New("credential rejected")
	_, status, err := p.Reprobe(context.Background(), "a", "ccz", staticQuery(&calls, quota.Snapshot{}, boom))
	if status != http.StatusBadGateway || !errors.Is(err, ErrReprobeQuery) || !errors.Is(err, boom) {
		t.Fatalf("Reprobe = %d, %v; want 502 wrapping ErrReprobeQuery and the query error", status, err)
	}
	if after := store.Get("ccz"); !reflect.DeepEqual(before, after) {
		t.Errorf("snapshot changed on failed query:\nbefore %+v\nafter  %+v", before, after)
	}
	c := p.byPool["a"]
	c.mu.Lock()
	got, ok := c.exhausted["ccz"]
	c.mu.Unlock()
	if !ok || !got.Equal(parkAt) {
		t.Errorf("park = %v/%v, want %v/true", got, ok, parkAt)
	}
}

// blockingQuery returns a ReprobeQuery that signals entered on its first
// call, then blocks until release closes.
func blockingQuery(calls *atomic.Int32, entered chan<- struct{}, release <-chan struct{}, snap quota.Snapshot) ReprobeQuery {
	var once sync.Once
	return func(context.Context, backend.Backend) (quota.Snapshot, error) {
		calls.Add(1)
		once.Do(func() { close(entered) })
		<-release
		return snap, nil
	}
}

// TestReprobe_coalescesAcrossPoolsWithoutCooldown: concurrent reprobes of one
// QuotaKey — from the same pool and from a sibling pool sharing the nick —
// issue a single upstream query; each pool marks the snapshot local and
// reconciles its own park. An immediate follow-up click queries again
// (probeCooldown does not apply).
func TestReprobe_coalescesAcrossPoolsWithoutCooldown(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	p := reprobePools(t, clock, store, testDefaultBaseURL)
	for _, pool := range []string{"a", "b"} {
		p.byPool[pool].record429("ccz", clock.now().Add(4*time.Hour))
	}

	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	q := blockingQuery(&calls, entered, release, windowSnap("5h", "allowed", 0.1, clock.now().Add(5*time.Hour), clock.now()))

	callers := []string{"a", "a", "b", "b"}
	results := make([]ReprobeResult, len(callers))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		results[0], _, _ = p.Reprobe(context.Background(), callers[0], "ccz", q)
	}()
	<-entered
	for i := 1; i < len(callers); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], _, _ = p.Reprobe(context.Background(), callers[i], "ccz", q)
		}(i)
	}
	// Give the followers time to find the in-flight call before it ends.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Fatalf("query called %d times, want 1 (coalesced across pools)", n)
	}
	for i, r := range results {
		if !r.Healthy || r.Pool != callers[i] {
			t.Errorf("caller %d: result %+v, want healthy for pool %s", i, r, callers[i])
		}
	}
	for _, pool := range []string{"a", "b"} {
		c := p.byPool[pool]
		c.mu.Lock()
		_, local := c.poolLocalSnapshots["ccz"]
		_, parked := c.exhausted["ccz"]
		c.mu.Unlock()
		if !local {
			t.Errorf("pool %s: ccz snapshot not marked local", pool)
		}
		if parked {
			t.Errorf("pool %s: ccz park not reconciled", pool)
		}
		if n := c.probeRefs("ccz"); n != 0 {
			t.Errorf("pool %s: probeInFlight[ccz] = %d after reprobe, want 0", pool, n)
		}
	}

	if _, status, err := p.Reprobe(context.Background(), "a", "ccz", staticQuery(&calls, windowSnap("5h", "allowed", 0.1, clock.now().Add(5*time.Hour), clock.now()), nil)); err != nil || status != http.StatusOK {
		t.Fatalf("follow-up Reprobe = %d, %v", status, err)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("follow-up click: query called %d times total, want 2 (no cooldown)", n)
	}
}

// pollerReprobePools is reprobePools against an httptest quota endpoint
// registered as a poller provider, so the automatic recovery probe treats
// ccz as probe-eligible. hits counts automatic probe requests; each one
// blocks on gate (when non-nil) after signalling entered.
func pollerReprobePools(t *testing.T, clock *fixedClock, hits *atomic.Int32, entered chan<- struct{}, gate <-chan struct{}) *Pools {
	t.Helper()
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if entered != nil {
			once.Do(func() { close(entered) })
		}
		if gate != nil {
			<-gate
		}
		fmt.Fprint(w, `{"data":{"limits":[{"type":"TOKENS_LIMIT","percentage":10,"nextResetTime":4102444800000}]}}`)
	}))
	t.Cleanup(srv.Close)
	poller.WithTestProviderForTest(t, srv.URL, poller.HostURLForTest("/api/monitor/usage/quota/limit"), poller.RawAuthForTest, poller.ParseZhipuForTest)
	p := reprobePools(t, clock, quota.NewStore(), srv.URL)
	for _, pool := range []string{"a", "b"} {
		// Park ccz so it is a non-active, probe-eligible member everywhere.
		p.byPool[pool].record429("ccz", clock.now().Add(4*time.Hour))
	}
	return p
}

// TestReprobe_manualInFlightSuppressesAutomaticProbe: while a manual reprobe
// holds the key, the automatic recovery probe skips it in every pool sharing
// the nick, and the reference is released afterwards.
func TestReprobe_manualInFlightSuppressesAutomaticProbe(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var hits atomic.Int32
	p := pollerReprobePools(t, clock, &hits, nil, nil)

	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	q := blockingQuery(&calls, entered, release, windowSnap("5h", "allowed", 0.1, clock.now().Add(5*time.Hour), clock.now()))
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = p.Reprobe(context.Background(), "a", "ccz", q)
	}()
	<-entered
	for _, pool := range []string{"a", "b"} {
		if got := p.byPool[pool].tryRecoverParkedNonActive(); got != "" {
			t.Errorf("pool %s: automatic recovery recovered %q during manual reprobe, want skip", pool, got)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("automatic probe hit upstream %d times during manual reprobe, want 0", n)
	}
	close(release)
	<-done
	for _, pool := range []string{"a", "b"} {
		if n := p.byPool[pool].probeRefs("ccz"); n != 0 {
			t.Errorf("pool %s: probeInFlight[ccz] = %d after reprobe, want 0", pool, n)
		}
	}
}

// TestReprobe_doesNotClearAutomaticProbeSignal: a manual reprobe that starts
// and finishes while an automatic probe is in flight drops only its own
// reference; the automatic probe's signal survives until it ends.
func TestReprobe_doesNotClearAutomaticProbeSignal(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var hits atomic.Int32
	entered, gate := make(chan struct{}), make(chan struct{})
	p := pollerReprobePools(t, clock, &hits, entered, gate)
	a := p.byPool["a"]

	autoDone := make(chan struct{})
	go func() {
		defer close(autoDone)
		a.tryRecoverParkedNonActive()
	}()
	<-entered

	var calls atomic.Int32
	if _, status, err := p.Reprobe(context.Background(), "a", "ccz",
		staticQuery(&calls, windowSnap("5h", "allowed", 0.1, clock.now().Add(5*time.Hour), clock.now()), nil)); err != nil || status != http.StatusOK {
		t.Fatalf("Reprobe = %d, %v", status, err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("manual query called %d times, want 1 (not suppressed by the automatic probe)", n)
	}
	if n := a.probeRefs("ccz"); n != 1 {
		t.Errorf("probeInFlight[ccz] = %d after manual reprobe, want 1 (automatic probe still running)", n)
	}
	close(gate)
	<-autoDone
	if n := a.probeRefs("ccz"); n != 0 {
		t.Errorf("probeInFlight[ccz] = %d after both probes, want 0", n)
	}
}
