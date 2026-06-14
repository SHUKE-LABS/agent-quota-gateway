package auto

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// putUtil files a snapshot reporting nick fully (or partially) consumed in
// the store, mirroring what the poller writes for a z.ai / MiniMaxi member
// or what the header observer writes for Anthropic.
func putUtil(t *testing.T, store *quota.Store, c *Controller, nick string, util float64, reset time.Time) {
	t.Helper()
	store.Put(c.resolve(t, nick).QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &reset,
		AsOf:                 reset.Add(-time.Hour),
	})
}

// newPriorityControllerWithStore builds a priority-pool controller wired to
// store, so the store-exhaustion signal is live (the shared helpers pass a
// nil store and exercise pure 429-driven failover).
func newPriorityControllerWithStore(t *testing.T, start int, clock *fixedClock, store *quota.Store, priorityCSV string, nicks ...string) *Controller {
	t.Helper()
	for _, n := range nicks {
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_"+strings.ToUpper(n), "cred-"+n)
	}
	t.Setenv(backend.EnvPrefix+"AUTO_PRIORITY", priorityCSV)
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return NewController(reg, "auto", start, store, clock.now, io.Discard)
}

// TestResolveAuto_failsOffStoreExhaustedMember is the core regression: a
// member the store reports at 100% utilization (future reset) must be failed
// off even though no live 429 ever reached ModifyResponse — the situation a
// poller-tracked z.ai member produces.
func TestResolveAuto_failsOffStoreExhaustedMember(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard) // sticky on a

	putUtil(t, store, c, "a", 1.0, clock.now().Add(time.Hour))

	b, retry, exhausted := c.ResolveAuto()
	if exhausted {
		t.Fatalf("ResolveAuto exhausted=true, want false (b is healthy)")
	}
	if retry != 0 {
		t.Errorf("ResolveAuto retry=%v, want 0", retry)
	}
	if b.Nick != "b" {
		t.Errorf("ResolveAuto picked %q, want b (a is store-exhausted)", b.Nick)
	}
}

// TestResolveAuto_storeBelowThresholdStaysSticky proves a busy-but-not-spent
// window does not trigger failover: the sticky-until-exhausted design holds
// for any utilization short of the cap.
func TestResolveAuto_storeBelowThresholdStaysSticky(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	putUtil(t, store, c, "a", 0.99, clock.now().Add(time.Hour))

	if b, _, _ := c.ResolveAuto(); b.Nick != "a" {
		t.Errorf("ResolveAuto picked %q, want a (99%% is not exhausted)", b.Nick)
	}
}

// TestResolveAuto_storePastResetStaysSticky proves a frozen snapshot whose
// reset has already elapsed reads healthy without a re-poll, so the member is
// selectable again (the poller stops tracking a failed-off member, freezing
// its entry at the old reset).
func TestResolveAuto_storePastResetStaysSticky(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	putUtil(t, store, c, "a", 1.0, clock.now().Add(-time.Minute)) // reset already passed

	if b, _, _ := c.ResolveAuto(); b.Nick != "a" {
		t.Errorf("ResolveAuto picked %q, want a (exhaustion window already reset)", b.Nick)
	}
}

// TestResolveAuto_allStoreExhaustedForwardsPreciseWait proves the all-dry
// path uses store resets for the honest 429 Retry-After, picking the soonest
// to free up.
func TestResolveAuto_allStoreExhaustedForwardsPreciseWait(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	putUtil(t, store, c, "a", 1.0, clock.now().Add(2*time.Hour))
	putUtil(t, store, c, "b", 1.0, clock.now().Add(30*time.Minute)) // soonest

	b, retry, exhausted := c.ResolveAuto()
	if !exhausted {
		t.Fatalf("ResolveAuto exhausted=false, want true (both members store-exhausted)")
	}
	if b.Nick != "b" {
		t.Errorf("ResolveAuto pointed at %q, want b (soonest reset)", b.Nick)
	}
	if retry != 30*time.Minute {
		t.Errorf("ResolveAuto retry=%v, want 30m (precise wait to soonest reset)", retry)
	}
}

// TestStoreExhaustion_priorityFailsOffAndPreemptsBack walks the full
// lifecycle for a priority pool whose highest-priority member is a
// poller-tracked backend: it is failed off on the store signal alone, the
// preemptor schedules a wake at its precise reset, and once that reset passes
// the pool is preempted back to it.
func TestStoreExhaustion_priorityFailsOffAndPreemptsBack(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	// Highest-priority zai (z.ai-backed) starts active; m3 is the fallback.
	c := newPriorityControllerWithStore(t, -1, clock, store, "zai,m3", "m3", "zai")
	if got := c.Current(); got != "zai" {
		t.Fatalf("Current()=%q, want zai (highest priority at start)", got)
	}

	reset := clock.now().Add(time.Hour)
	putUtil(t, store, c, "zai", 1.0, reset)

	// Fail off zai to m3 on the store signal — no 429 was ever observed.
	if b, _, _ := c.ResolveAuto(); b.Nick != "m3" {
		t.Fatalf("ResolveAuto picked %q, want m3 (zai store-exhausted)", b.Nick)
	}

	p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)

	// Before the reset: schedule a wake at it, stay on m3.
	if wait := p.tick(); wait != time.Hour {
		t.Fatalf("tick wait=%v, want 1h (zai's precise store reset)", wait)
	}
	if got := c.Current(); got != "m3" {
		t.Fatalf("Current()=%q, want m3 (no preempt before reset)", got)
	}

	// After the reset the frozen entry reads healthy; preempt back to zai.
	clock.advance(time.Hour + time.Second)
	p.tick()
	if got := c.Current(); got != "zai" {
		t.Errorf("Current()=%q, want zai (preempted back after window reset)", got)
	}
}
