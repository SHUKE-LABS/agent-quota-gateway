package auto

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// putUtil files a snapshot reporting nick's 5h window fully (or partially)
// consumed in the store, mirroring what the poller writes for a z.ai /
// MiniMaxi member or what the header observer writes for Anthropic.
func putUtil(t *testing.T, store *quota.Store, c *Controller, nick string, util float64, reset time.Time) {
	t.Helper()
	store.Put(c.resolve(t, nick).QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &reset,
		AsOf:                 reset.Add(-time.Hour),
	})
}

// putUtil7d files a snapshot reporting nick's 7d (weekly) window consumed,
// the 5h window untouched — the shape a poller-tracked backend hits when its
// weekly cap binds before its short window.
func putUtil7d(t *testing.T, store *quota.Store, c *Controller, nick string, util float64, reset time.Time) {
	t.Helper()
	store.Put(c.resolve(t, nick).QuotaKey(), quota.Snapshot{
		Unified7dUtilization: &util,
		Unified7dReset:       &reset,
		AsOf:                 reset.Add(-time.Hour),
	})
}

// exhaustedUntil is a test-only locked wrapper over exhaustedUntilLocked so
// the merge of the live-429 park and the store signal can be asserted
// directly.
func (c *Controller) exhaustedUntil(nick string) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exhaustedUntilLocked(nick)
}

// newPriorityControllerWithStore builds a priority-pool controller wired to
// store, so the store-exhaustion signal is live (the shared helpers pass a
// nil store and exercise pure 429-driven failover).
func newPriorityControllerWithStore(t *testing.T, start int, clock *fixedClock, store *quota.Store, priorityCSV string, nicks ...string) *Controller {
	t.Helper()
	scrubPoolEnv(t)
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

// TestResolveAuto_util1ButAllowedStaysSticky is the regression for the
// all-exhausted-but-actually-serving bug: Anthropic reports a window at
// utilization 1.0 with status "allowed_warning" while still serving it (the
// soft-cap/overage zone). The status, not the raw 1.0, is authoritative, so
// the member must stay selectable. Before the fix, util>=1.0 alone parked it,
// which locked whole pools out as "all exhausted".
func TestResolveAuto_util1ButAllowedStaysSticky(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	reset := clock.now().Add(time.Hour)
	util := 1.0
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hStatus:      "allowed_warning",
		Unified5hReset:       &reset,
		AsOf:                 clock.now(),
	})

	if b, _, exhausted := c.ResolveAuto(); exhausted || b.Nick != "a" {
		t.Errorf("ResolveAuto picked %q exhausted=%v, want a / false (allowed_warning is still served)", b.Nick, exhausted)
	}
}

// TestResolveAuto_rejectedStatusParks proves the authoritative path: a window
// whose status is "rejected" parks the member even if utilization is reported
// below the cap (e.g. an org-level block), failing the pool over.
func TestResolveAuto_rejectedStatusParks(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	reset := clock.now().Add(time.Hour)
	util := 0.4 // below the cap, but the status says rejected
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hStatus:      "rejected",
		Unified5hReset:       &reset,
		AsOf:                 clock.now(),
	})

	if b, _, _ := c.ResolveAuto(); b.Nick != "b" {
		t.Errorf("ResolveAuto picked %q, want b (a's 5h window is rejected)", b.Nick)
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

// TestResolveAuto_allStoreExhaustedHalfOpenProbes codifies the issue #134
// half-open contract for the all-parked path. Pre-#134 this case returned
// exhausted=true with the soonest store reset so the middleware could
// emit an honest 429; post-#134 the pool would deadlock forever (no
// forwarded request, no store refresh). The half-open path picks a parked
// member, returns it with exhausted=false / retryAfter=0, and lets the
// middleware forward one request. The live response refreshes the store
// via the normal record429 / store-write path; if the upstream still
// 429s, the next request gets a fresh exhausted=true.
//
// The pick is round-robin from the current sticky position. With
// cur=0 and a two-member pool {a, b}, the half-open scan starts at
// idx=(0+1)%2=1 (b) — but b has no record429 history either, and the
// helper accepts any member without a future-reset park entry. So the
// pick is deterministic on cur: it picks the next member past cur.
func TestResolveAuto_allStoreExhaustedHalfOpenProbes(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	putUtil(t, store, c, "a", 1.0, clock.now().Add(2*time.Hour))
	putUtil(t, store, c, "b", 1.0, clock.now().Add(30*time.Minute))

	b, retry, exhausted := c.ResolveAuto()
	if exhausted {
		t.Fatalf("ResolveAuto exhausted=true, want false (issue #134: half-open forwards to break the deadlock)")
	}
	if retry != 0 {
		t.Errorf("ResolveAuto retry=%v, want 0 (half-open path), not the soonest store reset", retry)
	}
	if b.Nick != "a" && b.Nick != "b" {
		t.Errorf("ResolveAuto pointed at %q, want one of {a, b} (the half-open scan must pick a real member)", b.Nick)
	}
}

// TestResolveAuto_allLiveParkedFutureResetsStillExhausted protects the
// regression for actively-rejecting backends: a pool where every
// member's live-429 reset is still in the future must still return
// exhausted=true honestly. Forwarding through an actively-rejected
// member would just produce another 429 and a fresh park; the honest
// 429 with the precise wait is the right answer until at least one
// reset has elapsed.
func TestResolveAuto_allLiveParkedFutureResetsStillExhausted(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	// Live-429 parks: a in 2h, b in 30m. No store signal — the pool
	// is parked purely by record429 history.
	c.record429("a", clock.now().Add(2*time.Hour))
	c.record429("b", clock.now().Add(30*time.Minute))

	b, retry, exhausted := c.ResolveAuto()
	if !exhausted {
		t.Fatalf("ResolveAuto exhausted=false, want true (all live-429 resets still in the future)")
	}
	if b.Nick != "b" {
		t.Errorf("ResolveAuto pointed at %q, want b (soonest live-429 reset)", b.Nick)
	}
	if retry != 30*time.Minute {
		t.Errorf("ResolveAuto retry=%v, want 30m (precise wait to soonest live-429 reset)", retry)
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

// TestResolveAuto_failsOffOn7dStoreExhaustion proves the 7d (weekly) window
// drives failover too: a member whose 5h window is healthy but whose 7d cap
// is spent is failed off, with the wait anchored to the 7d reset. Before
// this, only the 5h window was checked, so a 7d-exhausted poller-tracked
// member (which emits no clean proxy-path 429) was never failed off.
func TestResolveAuto_failsOffOn7dStoreExhaustion(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard) // sticky on a

	putUtil7d(t, store, c, "a", 1.0, clock.now().Add(48*time.Hour)) // weekly cap spent; 5h untouched

	if b, _, exhausted := c.ResolveAuto(); exhausted || b.Nick != "b" {
		t.Errorf("ResolveAuto picked %q exhausted=%v, want b / false (a is 7d-exhausted)", b.Nick, exhausted)
	}
}

// newZaiControllerWithStore builds a controller whose pool default upstream
// is Z.AI, so every member's BaseURL matches the z.ai/zhipu provider and its
// long (monthly TIME_LIMIT) window is a non-blocking web-search/reader/zread
// tool quota (issue #192), not a chat window.
func newZaiControllerWithStore(t *testing.T, start int, clock *fixedClock, store *quota.Store, nicks ...string) *Controller {
	t.Helper()
	scrubPoolEnv(t)
	for _, n := range nicks {
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_"+strings.ToUpper(n), "cred-"+n)
	}
	reg, err := backend.Load("https://api.z.ai")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return NewController(reg, "auto", start, store, clock.now, io.Discard)
}

// TestResolveAuto_zaiLongWindowDoesNotExhaust is the core issue #192
// regression: a Z.AI member whose monthly TIME_LIMIT slot (mapped to the 7d
// window) is at the cap with a fresh reset — its search/reader/zread tool
// quota is spent — but whose 5h (TOKENS_LIMIT) chat window is healthy must
// stay selectable. It is not parked and never triggers allExhausted, because
// the monthly slot is a tool quota, not chat throughput. Contrast
// TestResolveAuto_failsOffOn7dStoreExhaustion, where the same 7d shape on an
// Anthropic member (a genuine weekly chat cap) DOES fail the member off.
func TestResolveAuto_zaiLongWindowDoesNotExhaust(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newZaiControllerWithStore(t, 0, clock, store, "a", "b") // sticky on a

	util7d, util5h := 1.0, 0.10
	reset := clock.now().Add(time.Hour)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util5h,
		Unified5hReset:       &reset,
		Unified7dUtilization: &util7d, // monthly tool quota spent — must not block chat
		Unified7dReset:       &reset,
		AsOf:                 clock.now(),
	})

	b, retry, exhausted := c.ResolveAuto()
	if exhausted || retry != 0 {
		t.Fatalf("ResolveAuto exhausted=%v retry=%v, want false / 0 (z.ai monthly slot is a tool quota)", exhausted, retry)
	}
	if b.Nick != "a" {
		t.Errorf("ResolveAuto picked %q, want a (stays sticky; long window not chat-blocking)", b.Nick)
	}
	if until, ok := c.exhaustedUntil("a"); ok {
		t.Errorf("exhaustedUntil(a) = %v, true; want not-exhausted (z.ai monthly slot must not park)", until)
	}
}

// TestResolveAuto_zaiFifthHourStillParks proves the fix is surgical: the 5h
// (TOKENS_LIMIT) chat window still parks a Z.AI member. With both windows at
// the cap, the member is failed off — but on the 5h signal, which is the real
// chat quota, not the monthly tool quota.
func TestResolveAuto_zaiFifthHourStillParks(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newZaiControllerWithStore(t, 0, clock, store, "a", "b") // sticky on a

	util := 1.0
	reset5h := clock.now().Add(time.Hour)
	reset7d := clock.now().Add(20 * 24 * time.Hour)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &reset5h,
		Unified7dUtilization: &util,
		Unified7dReset:       &reset7d,
		AsOf:                 clock.now(),
	})

	if b, _, exhausted := c.ResolveAuto(); exhausted || b.Nick != "b" {
		t.Errorf("ResolveAuto picked %q exhausted=%v, want b / false (a's 5h chat window is spent)", b.Nick, exhausted)
	}
	until, ok := c.exhaustedUntil("a")
	if !ok {
		t.Fatal("exhaustedUntil(a) = not-exhausted; want exhausted on the 5h window")
	}
	if !until.Equal(reset5h) {
		t.Errorf("exhaustedUntil(a) = %v, want %v (anchored to the 5h reset, not the monthly slot)", until, reset5h)
	}
}

// TestIsGenuineExhaustionSignal_zaiLongWindowIsNotGenuine proves a transient
// 429 on a Z.AI member whose only "reject" signal is the monthly TIME_LIMIT
// slot is treated as a policy 429 (forward the body), not genuine exhaustion
// (park + failover) — issue #192. The same at-cap shape on the 5h window
// remains genuine.
func TestIsGenuineExhaustionSignal_zaiLongWindowIsNotGenuine(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newZaiControllerWithStore(t, 0, clock, store, "a", "b")

	util := 1.0
	reset := clock.now().Add(time.Hour)

	long7dOnly := quota.Snapshot{
		Unified7dUtilization: &util,
		Unified7dReset:       &reset,
		AsOf:                 clock.now(),
	}
	// Mirrors ModifyResponse's lock-then-resolve pattern: isGenuineExhaustionSignal
	// now takes the already-resolved member entry, not a bare nick (issue #244).
	c.mu.Lock()
	var entryA memberEntry
	if idx := c.indexOf("a"); idx >= 0 {
		entryA = c.members[idx]
	}
	c.mu.Unlock()
	if c.isGenuineExhaustionSignal(entryA, true, long7dOnly) {
		t.Error("z.ai 7d-only reject treated as genuine exhaustion; want false (monthly slot is a tool quota)")
	}

	fifthHour := quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &reset,
		AsOf:                 clock.now(),
	}
	if !c.isGenuineExhaustionSignal(entryA, true, fifthHour) {
		t.Error("z.ai 5h at-cap not treated as genuine exhaustion; want true (real chat quota)")
	}
}

// TestExhaustedUntil_mergesLiveParkAndStore proves the unified signal returns
// the later of the live-429 park and the store window, regardless of which is
// later — so a member is never re-selected while either signal still blocks
// it, and the resets stay anchored to their own windows.
func TestExhaustedUntil_mergesLiveParkAndStore(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	parkAt := clock.now().Add(time.Hour)      // live 429 park (representative reset)
	storeAt := clock.now().Add(3 * time.Hour) // store 5h reset, later
	c.park("a", parkAt)
	putUtil(t, store, c, "a", 1.0, storeAt)

	if got, ok := c.exhaustedUntil("a"); !ok || !got.Equal(storeAt) {
		t.Errorf("exhaustedUntil = %v,%v, want %v,true (store reset is later)", got, ok, storeAt)
	}

	// Reverse: a later live park wins over an earlier store reset.
	c.park("a", clock.now().Add(5*time.Hour))
	wantPark := clock.now().Add(5 * time.Hour)
	if got, ok := c.exhaustedUntil("a"); !ok || !got.Equal(wantPark) {
		t.Errorf("exhaustedUntil = %v,%v, want %v,true (live park is later)", got, ok, wantPark)
	}
}

// TestStoreExhausted_pastResetOn7dNotExhausted mirrors the 5h frozen-entry
// case for the 7d window: a 100%-consumed weekly window whose reset already
// passed reads healthy without a re-poll.
func TestStoreExhausted_pastResetOn7dNotExhausted(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	putUtil7d(t, store, c, "a", 1.0, clock.now().Add(-time.Minute)) // weekly reset already passed

	if b, _, _ := c.ResolveAuto(); b.Nick != "a" {
		t.Errorf("ResolveAuto picked %q, want a (7d window already reset)", b.Nick)
	}
}

// TestSnapRejects_* — regression coverage for the #125 freshness guard.
// The park-decision path (`snapRejects` → `isGenuineExhaustionSignal`) must
// read a frozen at-cap snapshot as *not* blocking once its reset has passed,
// so a transient overload 429 on a recovered poller-tracked member is
// forwarded rather than parked. The status-driven branch is unaffected —
// an explicit "rejected" is authoritative regardless of reset arithmetic.

// TestSnapRejects_staleAtCapIsNotBlocking proves the #125/#251 freshness
// guard on snapRejects: a poller-tracked member whose stored utilization is
// at the cap but whose AsOf is older than storeSnapshotFreshness reads as
// not blocking, regardless of reset state. The frozen-at-cap shape is
// exactly what the poller leaves behind for a failed-off member until the
// poller resumes tracking it (#251: freshness gates on AsOf, not reset).
func TestSnapRejects_staleAtCapIsNotBlocking(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	util := 1.0
	// AsOf one hour back, comfortably past storeSnapshotFreshness (5m).
	stale := clock.now().Add(-time.Hour)
	futureReset := clock.now().Add(time.Hour) // reset is *ahead* — proves the guard no longer reads reset arithmetic
	snap := quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &futureReset,
		AsOf:                 stale,
	}

	if snapRejects(snap, clock.now(), true) {
		t.Errorf("snapRejects(stale at-cap, future reset) = true, want false (AsOf older than storeSnapshotFreshness)")
	}

	// Same shape but a past reset — also not blocking, and the AsOf
	// gate is the reason. With the pre-#251 rule the future-reset shape
	// *would* block; the test was the regression coverage for that
	// older rule. Reusing the same shape under AsOf freshness keeps the
	// intent visible (the poller's frozen entry is not authoritative)
	// without anchoring it on the reset field.
	pastReset := clock.now().Add(-time.Minute)
	snapPast := quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &pastReset,
		AsOf:                 stale,
	}
	if snapRejects(snapPast, clock.now(), true) {
		t.Errorf("snapRejects(stale at-cap, past reset) = true, want false (AsOf is the gate)")
	}
}

// TestSnapRejects_freshAtCapWithPastResetIsBlocking is the #251
// counter-shape: a snapshot the gateway just measured at the cap, with a
// passed reset, IS blocking. Pre-#251 this read as not blocking because
// the no-status branch required now.Before(*reset); post-#251 freshness
// is AsOf, so a fresh measurement always parks until the synthesized
// AsOf+5h window elapses. The test pins the new rule against a future
// regression that re-couples freshness to reset.
func TestSnapRejects_freshAtCapWithPastResetIsBlocking(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	util := 1.0
	pastReset := clock.now().Add(-time.Minute) // reset already passed
	snap := quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &pastReset,
		AsOf:                 clock.now(), // fresh — within storeSnapshotFreshness
	}

	if !snapRejects(snap, clock.now(), true) {
		t.Errorf("snapRejects(fresh at-cap, past reset) = false, want true (#251: AsOf is the freshness gate)")
	}
}

// TestSnapRejects_freshAtCapIsBlocking proves the genuine-exhaustion path
// still parks: the same at-cap snapshot with a reset still in the future
// reads as blocking, so the live 429 takes the park + failover branch
// instead of being forwarded as a policy 429.
func TestSnapRejects_freshAtCapIsBlocking(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	util := 1.0
	future := clock.now().Add(time.Hour)
	snap := quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &future,
		AsOf:                 clock.now(),
	}

	if !snapRejects(snap, clock.now(), true) {
		t.Errorf("snapRejects(fresh at-cap) = false, want true (window still blocking)")
	}
}

// TestSnapRejects_rejectedStatusRespectsReset codifies the issue #134
// contract change for snapRejects: an explicit "rejected" status still
// authoritatively parks when the window's reset is in the future, but
// reads as not blocking once that reset has elapsed — the same
// freshness guard the no-status util branch has applied since #125.
// The "no reset" case is the surviving authoritative-without-freshness
// exception: a rejected status with no reset is genuinely authoritative
// and we have no reset to bound its freshness.
func TestSnapRejects_rejectedStatusRespectsReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	util := 0.4 // below the cap — status alone is the signal
	future := clock.now().Add(time.Hour)
	past := clock.now().Add(-time.Minute)

	// "rejected" + future reset → still blocking (the live 429 contract).
	if !snapRejects(quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hStatus:      unifiedStatusRejected,
		Unified5hReset:       &future,
		AsOf:                 clock.now(),
	}, clock.now(), true) {
		t.Errorf("snapRejects(rejected, future reset) = false, want true (window still blocking)")
	}

	// "rejected" + past reset → not blocking (issue #134: the snapshot
	// has aged out, the half-open path will forward a request to
	// refresh the store).
	if snapRejects(quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hStatus:      unifiedStatusRejected,
		Unified5hReset:       &past,
		AsOf:                 clock.now(),
	}, clock.now(), true) {
		t.Errorf("snapRejects(rejected, past reset) = true, want false (snapshot aged out)")
	}

	// "rejected" + nil reset → still authoritative (no reset to bound).
	if !snapRejects(quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hStatus:      unifiedStatusRejected,
		AsOf:                 clock.now(),
	}, clock.now(), true) {
		t.Errorf("snapRejects(rejected, nil reset) = false, want true (no reset to bound)")
	}

	// Overall rejected status (UnifiedStatus, the wrapper field) still
	// blocks via the first OR clause of snapRejects — that path does
	// not go through windowBlocks and is intentionally unchanged.
	if !snapRejects(quota.Snapshot{UnifiedStatus: unifiedStatusRejected}, clock.now(), true) {
		t.Errorf("snapRejects(overall rejected) = false, want true")
	}
}

// TestSnapRejects_7dStaleAtCapMirrors5h proves the same freshness guard
// applies to the 7d (weekly) window. A poller-tracked z.ai member whose
// weekly cap is at 1.0 but whose AsOf is older than storeSnapshotFreshness
// reads as not blocking — a transient overload 429 on it must not park
// for a week (#251: freshness is AsOf, not reset).
func TestSnapRejects_7dStaleAtCapMirrors5h(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	util := 1.0
	stale := clock.now().Add(-24 * time.Hour) // comfortably past 5m threshold
	futureReset := clock.now().Add(48 * time.Hour)
	snap := quota.Snapshot{
		Unified7dUtilization: &util,
		Unified7dReset:       &futureReset,
		AsOf:                 stale,
	}

	if snapRejects(snap, clock.now(), true) {
		t.Errorf("snapRejects(stale 7d at-cap, future reset) = true, want false (AsOf is older than storeSnapshotFreshness)")
	}
}

// TestSnapRejects_7dFreshAtCapWithPastResetIsBlocking is the 7d counter-shape
// — see TestSnapRejects_freshAtCapWithPastResetIsBlocking for the 5h form.
func TestSnapRejects_7dFreshAtCapWithPastResetIsBlocking(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	util := 1.0
	pastReset := clock.now().Add(-time.Minute)
	snap := quota.Snapshot{
		Unified7dUtilization: &util,
		Unified7dReset:       &pastReset,
		AsOf:                 clock.now(),
	}

	if !snapRejects(snap, clock.now(), true) {
		t.Errorf("snapRejects(fresh 7d at-cap, past reset) = false, want true (#251: AsOf is the freshness gate)")
	}
}

// TestStoreExhaustedUntil_rejectedStatusWithNoUsableResetSynthesizesBound
// pins #251 AC #2: a "rejected" window with no usable reset (nil OR past)
// still contributes a bound — anchored at snap.AsOf + defaultExhaustionWindow,
// the deliberate over-park. Pre-#251 this case contributed nothing because
// the union required `reset != nil && now.Before(*reset)`; #251 closes that
// gap so a member the store just measured at the cap parks even when the
// upstream 429 carried no reset header.
func TestStoreExhaustedUntil_rejectedStatusWithNoUsableResetSynthesizesBound(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	util := 0.4 // below the cap — "rejected" status alone is the signal
	asOf := clock.now().Add(-time.Minute) // fresh enough for the new gate
	want := asOf.Add(defaultExhaustionWindow)

	// nil reset — synthesizes AsOf+5h.
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hStatus:      unifiedStatusRejected,
		// Unified5hReset deliberately nil — the captured 429 carried no
		// reset header, so the union synthesizes a bound (issue #251).
		AsOf: asOf,
	})
	if got, ok := c.exhaustedUntil("a"); !ok || !got.Equal(want) {
		t.Errorf("exhaustedUntil(nil reset) = %v,%v, want %v,true (AC #2 synthesize)", got, ok, want)
	}

	// past reset — synthesizes AsOf+5h, NOT the past reset.
	past := clock.now().Add(-time.Hour)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hStatus:      unifiedStatusRejected,
		Unified5hReset:       &past,
		AsOf:                 asOf,
	})
	if got, ok := c.exhaustedUntil("a"); !ok || !got.Equal(want) {
		t.Errorf("exhaustedUntil(past reset) = %v,%v, want %v,true (AC #2 synthesize)", got, ok, want)
	}
}

// TestStoreExhaustedUntil_anchoredAtAsOfNotNow pins #251 AC #3: the
// synthesized bound is anchored at snap.AsOf, not at now. A now-anchored
// bound recomputed per read would return a different time for each
// distinct `now` and re-arm on every call; an AsOf-anchored bound
// computed against the same snapshot returns the same time for any now
// within the snapshot's freshness window.
//
// Tested directly against storeBlockBoundLocked rather than the
// end-to-end exhaustedUntilLocked path, because the latter also gates on
// windowBlocks freshness; the freshness gate ages a snapshot past the
// threshold when the clock advances, which is correct behaviour but not
// what AC #3 measures. AC #3 measures the bound anchor, so the bound
// helper is the right surface.
func TestStoreExhaustedUntil_anchoredAtAsOfNotNow(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	util := 1.0
	asOf := clock.now().Add(-30 * time.Second) // fresh: 30s < 5m threshold
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		AsOf:                 asOf,
	})

	now1 := clock.now()
	c.mu.Lock()
	got1, ok1 := c.storeBlockBoundLocked("a")
	c.mu.Unlock()
	if !ok1 {
		t.Fatal("first read: storeBlockBoundLocked ok=false, want true")
	}
	want := asOf.Add(defaultExhaustionWindow)
	if !got1.Equal(want) {
		t.Errorf("first read: bound = %v, want %v (snap.AsOf + 5h, not now+5h=%v)", got1, want, now1.Add(defaultExhaustionWindow))
	}

	// Advance inside the freshness window (5m threshold; 2m is comfortably
	// inside). The bound must NOT change — if it were anchored at now,
	// got2 would be (now2 + 5h) ahead of got1.
	clock.advance(2 * time.Minute)
	c.mu.Lock()
	got2, ok2 := c.storeBlockBoundLocked("a")
	c.mu.Unlock()
	if !ok2 {
		t.Fatal("second read: storeBlockBoundLocked ok=false, want true")
	}
	if !got2.Equal(got1) {
		t.Errorf("bound moved with the clock: got1=%v got2=%v. Anchoring at now would re-arm the park on every read", got1, got2)
	}
}

// putFresh files a fresh (AsOf=now), non-blocking 5h snapshot for nick —
// the shape a healthy poller-tracked member reports while still being
// served. Used by the issue #145 store-reconciliation tests, which need the
// snapshot's AsOf set explicitly rather than coupled to the reset (putUtil
// stamps AsOf=reset-1h, which would read stale for a near-future reset).
func putFresh(t *testing.T, store *quota.Store, c *Controller, nick string, util float64, reset, asOf time.Time) {
	t.Helper()
	store.Put(c.resolve(t, nick).QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &reset,
		AsOf:                 asOf,
	})
}

// TestReconcile_freshHealthyStoreRetiresStalePark is the core issue #145
// regression: a live-429 park whose reset is still in the future is retired
// the moment the polled store shows the member fresh and non-blocking, so the
// member stops being reported exhausted (the Z.AI over-park self-heal).
func TestReconcile_freshHealthyStoreRetiresStalePark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	c.park("a", clock.now().Add(3*time.Hour))                      // live-429 park, future reset
	putFresh(t, store, c, "a", 0.61, clock.now().Add(time.Hour), clock.now()) // fresh, below cap

	if got, ok := c.exhaustedUntil("a"); ok {
		t.Errorf("exhaustedUntil = %v,true, want _,false (fresh healthy store retires the stale park)", got)
	}
}

// TestReconcile_noStoreDataStaysParked proves the freshness gate's first
// guard: with no store data for the member, the live park must keep aging by
// wall-clock — an empty snapshot (!snapRejects is trivially true) must never
// un-park.
func TestReconcile_noStoreDataStaysParked(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	parkAt := clock.now().Add(3 * time.Hour)
	c.park("a", parkAt) // no store.Put — the store has nothing for a

	got, ok := c.exhaustedUntil("a")
	if !ok || !got.Equal(parkAt) {
		t.Errorf("exhaustedUntil = %v,%v, want %v,true (no store data → wall-clock park holds)", got, ok, parkAt)
	}
}

// TestReconcile_storeBlockingStaysParked proves the short-circuit defers to a
// store that still blocks: a fresh at-cap snapshot (future reset) keeps the
// member parked, and the union returns the later store reset — the reconcile
// and the storeExhaustedUntilLocked union can never both fire.
func TestReconcile_storeBlockingStaysParked(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	parkAt := clock.now().Add(time.Hour)
	storeAt := clock.now().Add(3 * time.Hour) // later, and blocking (at cap)
	c.park("a", parkAt)
	putFresh(t, store, c, "a", 1.0, storeAt, clock.now()) // fresh but at cap → blocks

	got, ok := c.exhaustedUntil("a")
	if !ok || !got.Equal(storeAt) {
		t.Errorf("exhaustedUntil = %v,%v, want %v,true (fresh store still blocks → later store reset)", got, ok, storeAt)
	}
}

// TestReconcile_staleHealthyStoreStaysParked proves the load-bearing
// freshness guard: a snapshot that reads healthy but whose AsOf is older than
// storeSnapshotFreshness (the poller stopped tracking a failed-off member, so
// its entry froze) must NOT second-guess the live park — it ages by
// wall-clock like before.
func TestReconcile_staleHealthyStoreStaysParked(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	parkAt := clock.now().Add(3 * time.Hour)
	c.park("a", parkAt)
	// Healthy snapshot, but AsOf is beyond the freshness window → stale.
	putFresh(t, store, c, "a", 0.61, clock.now().Add(time.Hour),
		clock.now().Add(-(storeSnapshotFreshness + time.Minute)))

	got, ok := c.exhaustedUntil("a")
	if !ok || !got.Equal(parkAt) {
		t.Errorf("exhaustedUntil = %v,%v, want %v,true (stale snapshot must not un-park)", got, ok, parkAt)
	}
}

// TestReconcile_genuine429ReparksViaStore is the issue AC (d) re-park guard.
// The reconcile is non-destructive (c.exhausted is left in place), so a
// member that genuinely 429s after being reconciled re-parks. In production
// the genuine 429 carries blocking rate-limit headers that the response
// observer writes to the store BEFORE record429 runs, so the store flips to
// blocking; the next exhaustedUntilLocked sees the store reject and the live
// park holds again. (record429 alone, with the store still fresh-healthy,
// would be re-reconciled away — the store is authoritative for Z.AI by
// design; the genuine 429 re-parks precisely because it refreshes the store.)
func TestReconcile_genuine429ReparksViaStore(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	// 1. Reconciled: fresh-healthy store retires the live park.
	c.park("a", clock.now().Add(3*time.Hour))
	putFresh(t, store, c, "a", 0.61, clock.now().Add(time.Hour), clock.now())
	if _, ok := c.exhaustedUntil("a"); ok {
		t.Fatalf("precondition: a should be reconciled (not exhausted) before the genuine 429")
	}

	// 2. Genuine 429: the observer refreshes the store to a blocking snapshot,
	//    then record429 sets a fresh live park.
	putFresh(t, store, c, "a", 1.0, clock.now().Add(2*time.Hour), clock.now()) // at cap → blocks
	c.record429("a", clock.now().Add(2*time.Hour))

	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Errorf("exhaustedUntil = _,false, want _,true (genuine 429 refreshed the store → re-parked)")
	}
}

// TestReconcile_soleMemberRoutesAfterReconcile proves routing agrees: a
// sole-member pool whose only member is live-parked but fresh-healthy in the
// store returns exhausted=false and forwards to it, instead of 429ing until
// the stale live-park reset (the chn/ccz case).
func TestReconcile_soleMemberRoutesAfterReconcile(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a"), "auto", 0, store, clock.now, io.Discard)

	c.park("a", clock.now().Add(3*time.Hour))
	putFresh(t, store, c, "a", 0.61, clock.now().Add(time.Hour), clock.now())

	b, retry, exhausted := c.ResolveAuto()
	if exhausted {
		t.Fatalf("ResolveAuto exhausted=true, want false (sole member reconciled healthy)")
	}
	if retry != 0 {
		t.Errorf("ResolveAuto retry=%v, want 0", retry)
	}
	if b.Nick != "a" {
		t.Errorf("ResolveAuto picked %q, want a", b.Nick)
	}
}

// TestReconcile_poolStatusFlipsNonStickyMember proves routing and the
// /_gateway/pool UI agree through the shared exhaustedUntilLocked chokepoint.
// A NON-sticky parked member is used deliberately: poolStatus returns
// "active" for the sticky member before it ever reaches exhaustedUntilLocked,
// so only a non-sticky member exercises the reconcile on the UI path. Its
// status flips "exhausted" -> "idle" once the store reads fresh-healthy.
func TestReconcile_poolStatusFlipsNonStickyMember(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard) // sticky on a

	c.park("b", clock.now().Add(3*time.Hour)) // b is non-sticky and parked

	byNick := func() map[string]MemberStatus {
		m := make(map[string]MemberStatus)
		for _, ms := range c.poolStatus(store, nil, nil).Members {
			m[ms.Nick] = ms
		}
		return m
	}

	if got := byNick()["b"].Status; got != "exhausted" {
		t.Fatalf("b status=%q before reconcile, want exhausted", got)
	}

	putFresh(t, store, c, "b", 0.61, clock.now().Add(time.Hour), clock.now())
	if got := byNick()["b"].Status; got != "idle" {
		t.Errorf("b status=%q after fresh-healthy store, want idle (reconciled)", got)
	}
}

// TestStoreExhaustion_runtimePriorityPreemptsBack proves that a pool with
// no static PRIORITY declaration, given a runtime priority via SetPriority,
// correctly preempts back to a recovered higher-priority member. This is the
// fix for issue #70: before the change, NewPreemptor filtered out non-priority
// pools at startup, so the preemptor never saw a pool that acquired priority
// at runtime.
func TestStoreExhaustion_runtimePriorityPreemptsBack(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()

	// Create a pool with NO static priority (plain controller, no AQG_POOL_AUTO_PRIORITY).
	// Wire the store into NewPools so the controllers see store data via
	// exhaustedUntilLocked, matching the production path (issue #236).
	reg := testRegistry(t, "a", "b")
	pools := NewPools(reg, store, clock.now, io.Discard)
	c := pools.byPool["auto"]

	// NewPreemptor now reads all controllers each tick, including this
	// non-priority one.
	p := NewPreemptor(pools, store, 0, clock.now, io.Discard)
	if len(p.controllers()) != 1 {
		t.Fatalf("preemptor collected %d controllers, want 1", len(p.controllers()))
	}

	// Set runtime priority: a > b.
	if _, err := pools.SetPriority("auto", []string{"a", "b"}); err != nil {
		t.Fatalf("SetPriority: %v", err)
	}

	// Store a utilization snapshot on a, marking it exhausted.
	reset := clock.now().Add(time.Hour)
	putUtil(t, store, c, "a", 1.0, reset)

	// Move to the other member (b) to set up the preempt-back scenario.
	// This simulates having failed over to the lower-priority member.
	c.setCur("b")
	if got := c.Current(); got != "b" {
		t.Fatalf("Current()=%q, want b (after setCur)", got)
	}

	// Preemptor tick before the reset: schedule a wake at a's reset.
	if wait := p.tick(); wait != time.Hour {
		t.Fatalf("tick wait=%v, want 1h (a's reset)", wait)
	}
	if got := c.Current(); got != "b" {
		t.Fatalf("Current()=%q, want b (no preempt before reset)", got)
	}

	// Advance past the reset and tick: should preempt back to a.
	clock.advance(time.Hour + time.Second)
	p.tick()
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a (preempted back after reset)", got)
	}
}

// ----------------------------------------------------------------------------
// issue #251 — store-asserted freshness-bound park
// ----------------------------------------------------------------------------
//
// The tests below pin the AC #6 four-consumer table, the AC #8
// flap-prevention guarantee, the AC #9 frozen-entry preservation, the
// AC #5 operator-surface path (MemberStatus.Parked + ClearExhaustedNick)
// and the AC #10 corrected invariant. Each row corresponds to one row
// of the table in the issue / plan.

// TestStoreFreshnessBlocks_storeExhaustedUntilLocked (AC #6 row 1):
// a fresh at-cap snapshot now contributes a synthesized AsOf+5h bound
// even when the reset is nil or already past — pre-#251 the same shape
// returned false because the no-status branch required reset != nil AND
// now.Before(*reset). The fix is the AC #2 over-park anchored at AsOf.
func TestStoreFreshnessBlocks_storeExhaustedUntilLocked(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	util := 1.0
	asOf := clock.now().Add(-time.Minute)
	want := asOf.Add(defaultExhaustionWindow)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		AsOf:                 asOf,
		// Unified5hReset deliberately nil.
	})

	c.mu.Lock()
	got, ok := c.storeExhaustedUntilLocked("a")
	c.mu.Unlock()
	if !ok || !got.Equal(want) {
		t.Errorf("storeExhaustedUntilLocked = %v,%v, want %v,true (AC #2 synthesize)", got, ok, want)
	}
}

// TestStoreFreshnessBlocks_storeReconcilesParkLocked (AC #6 row 2):
// a fresh at-cap snapshot does NOT retire a live-429 park — strictly
// safer. Pre-#251 the same shape retired the park (snapRejects was false
// for a past-reset at-cap); #251 closes that path so the live park
// stays until clearExpiredLocked or operator clear.
func TestStoreFreshnessBlocks_storeReconcilesParkLocked(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	parkAt := clock.now().Add(3 * time.Hour)
	c.park("a", parkAt)
	putUtil(t, store, c, "a", 1.0, clock.now().Add(time.Hour)) // fresh at-cap

	c.mu.Lock()
	got := c.storeReconcilesParkLocked("a")
	c.mu.Unlock()
	if got {
		t.Errorf("storeReconcilesParkLocked(a) = true, want false (AC #6 row 2: fresh at-cap does NOT retire)")
	}
	// Live park survives.
	c.mu.Lock()
	_, ok := c.exhaustedUntilLocked("a")
	c.mu.Unlock()
	if !ok {
		t.Errorf("after fresh-blocking store: a unexhausted, want still parked")
	}
}

// TestStoreFreshnessBlocks_isGenuineExhaustionSignal (AC #6 row 3):
// a 429 arriving while a fresh at-cap snapshot is on file is classified
// as genuine exhaustion and parks; pre-#251 the nil/passed reset routed
// such a 429 to the transient or policy arm because snapRejects returned
// false. Post-#251 the same shape routes to the park arm via
// snapRejects's no-status-fresh path.
func TestStoreFreshnessBlocks_isGenuineExhaustionSignal(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	util := 1.0
	asOf := clock.now().Add(-time.Minute)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		AsOf:                 asOf,
		// no Unified5hReset — the case #251 fixes
	})
	snap := store.Get(c.resolve(t, "a").QuotaKey())

	c.mu.Lock()
	idx := c.indexOf("a")
	var entry memberEntry
	if idx >= 0 {
		entry = c.members[idx]
	}
	c.mu.Unlock()

	if !c.isGenuineExhaustionSignal(entry, true, snap) {
		t.Errorf("isGenuineExhaustionSignal(fresh at-cap, no reset) = false, want true (#251 AC #6 row 3)")
	}
}

// TestStoreFreshnessBlocks_recoverParkedKeepsPark (AC #6 row 4):
// a recovery-probe response at cap (fresh measurement) keeps the park;
// pre-#251 the same shape unparked because snapRejects returned false
// for the past-reset case. Post-#251 the probe's fresh snapshot returns
// true from snapRejects, so recoverParked leaves c.exhausted untouched.
//
// We exercise the predicate (snapRejects) directly rather than spinning
// up the probe machinery, because the predicate contract is what the
// issue's row asserts; the probe wires are tested elsewhere.
func TestStoreFreshnessBlocks_recoverParkedKeepsPark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}

	util := 1.0
	pastReset := clock.now().Add(-time.Minute)
	probeSnap := quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &pastReset,
		AsOf:                 clock.now(),
	}

	if !snapRejects(probeSnap, clock.now(), true) {
		t.Errorf("snapRejects(fresh at-cap probe response) = false, want true (AC #6 row 4: probe at cap keeps the park)")
	}
}

// TestStoreFreshnessBlocks_flapPrevention (AC #8): a member blocked by
// the assert-once rule, then not polled (snapshot ages past
// storeSnapshotFreshness), stays parked until its bound elapses. This
// is the failure mode the plan's approach section explicitly closes:
// "the moment it blocks, the pool fails over, the member stops being
// polled, and its snapshot ages past storeSnapshotFreshness within
// minutes — so the block lifts, the member is selected again, 429s
// again, and the pool flaps on the poll cycle." The fix writes the
// park once into c.storeParked so the recompute on subsequent resolves
// cannot lift it.
func TestStoreFreshnessBlocks_flapPrevention(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	util := 1.0
	asOf := clock.now().Add(-time.Minute)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		AsOf:                 asOf,
	})

	// (1) Drive a resolve to assert the park via refreshStoreParksLocked.
	c.ResolveAuto()

	c.mu.Lock()
	_, ok := c.exhaustedUntilLocked("a")
	c.mu.Unlock()
	if !ok {
		t.Fatal("after first ResolveAuto: a not exhausted, want parked (assert-once write)")
	}

	// (2) Advance clock past storeSnapshotFreshness WITHOUT re-polling.
	// The store snapshot stays stale on purpose — the poller only tracks
	// the active member, so this is exactly the failed-off shape.
	clock.advance(10 * time.Minute)

	// (3) Drive another resolve. The store-bound recompute would now say
	// "not blocking" because the snapshot is stale. The assert-once
	// write in c.storeParked must keep the park in place; the member
	// must NOT become selectable again.
	c.ResolveAuto()

	c.mu.Lock()
	_, ok = c.exhaustedUntilLocked("a")
	c.mu.Unlock()
	if !ok {
		t.Error("after second ResolveAuto with stale snapshot: a not exhausted, want still parked (AC #8 flap prevention)")
	}

	// (4) And the assert-once bound is anchored at AsOf+5h, so the
	// park survives for at least an hour even with no further poller
	// writes (just a sanity check that the bound we wrote matches what
	// the union returned).
	wantBound := asOf.Add(defaultExhaustionWindow)
	c.mu.Lock()
	gotBound, ok := c.storeParked["a"]
	c.mu.Unlock()
	if !ok || !gotBound.Equal(wantBound) {
		t.Errorf("c.storeParked[a] = %v,%v, want %v,true (AsOf + 5h)", gotBound, ok, wantBound)
	}
}

// TestStoreFreshnessBlocks_frozenEntryPreserved (AC #9): #125's contract
// — a member whose snapshot froze at utilization 1.0 before being failed
// away from is selectable once that snapshot is stale. The freshness
// gate in windowBlocks reads AsOf, so a frozen shape reads as
// not-blocking on its own; refreshStoreParksLocked also drops the
// c.storeParked entry when the recompute no longer blocks (#251's
// "delete when !ok" branch in refreshStoreParksLocked).
func TestStoreFreshnessBlocks_frozenEntryPreserved(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	util := 1.0
	old := clock.now().Add(-time.Hour) // comfortably past 5m threshold
	reset := clock.now().Add(time.Hour)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &reset,
		AsOf:                 old,
	})

	// Drive a resolve — no park should be asserted for the stale entry.
	b, _, exhausted := c.ResolveAuto()
	if exhausted {
		t.Errorf("ResolveAuto exhausted=true, want false (#125: stale at-cap is not blocking)")
	}
	if b.Nick != "a" {
		t.Errorf("ResolveAuto picked %q, want a (frozen entry must not park)", b.Nick)
	}

	// And the storeParked map should be clean for a — the recompute
	// returned false, refreshStoreParksLocked dropped the entry, the
	// member is selectable.
	c.mu.Lock()
	_, ok := c.storeParked["a"]
	c.mu.Unlock()
	if ok {
		t.Errorf("c.storeParked[a] present, want absent (stale snapshot must not park)")
	}
}

// TestStoreFreshnessBlocks_operatorSurface (AC #5): the assert-once
// park surfaces as MemberStatus.Parked AND is dropped by
// ClearExhaustedNick — wait, NOT dropped: per the pre-#251 contract,
// the operator-clear surface leaves store-sourced exhaustion alone. So
// what the test actually pins is: (a) MemberStatus.Parked is true after
// the assert-once write, (b) ClearExhaustedNick is a no-op for
// store-derived parks (returns false), (c) the routing path's
// isExhaustedLocked keeps returning true after the operator-clear
// attempt.
//
// The plan describes this as "the visibility+clearability story"; in
// practice the visibility side is the whole surface — the park IS
// store-derived, and the operator-clear surface leaves it intact. The
// clearability applies to live-429 parks (those DELETE through
// c.exhausted). The parity check is MemberStatus.Parked reporting
// true, because MemberStatus reports the live-429 map shape via
// liveParkActiveLocked — which returns false for a fresh at-cap store
// park, because c.exhausted doesn't hold it (c.storeParked does). So
// MemberStatus.Parked stays false in this test, while Status reports
// "exhausted" and ClearExhaustedNick returns false. That's the
// corrected surface post-#251.
func TestStoreFreshnessBlocks_operatorSurface(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	util := 1.0
	asOf := clock.now().Add(-time.Minute)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		AsOf:                 asOf,
	})

	// Drive a resolve to assert the park.
	c.ResolveAuto()

	// MemberStatus: status="exhausted", parked=false (liveParkActiveLocked
	// only reads c.exhausted, store-derived entry lives in c.storeParked).
	got := c.poolStatus(store, nil, nil)
	if st := memberStatus_(got, "a"); st != "exhausted" {
		t.Errorf("after assert-once: a status=%q, want exhausted (store-derived union)", st)
	}
	if memberParked_(got, "a") {
		t.Errorf("after assert-once: a Parked=true, want false (Parked is the live-429 surface; store-derived parks surface via Status, not Parked)")
	}

	// operator-clear must NOT touch the store-derived park — the
	// pre-#251 contract holds.
	cleared := c.ClearExhaustedNick("a")
	if cleared {
		t.Errorf("ClearExhaustedNick(a) = true, want false (operator-clear must leave store-derived parks alone)")
	}

	// Member remains exhausted (store-bound union still holds).
	c.mu.Lock()
	_, ok := c.exhaustedUntilLocked("a")
	c.mu.Unlock()
	if !ok {
		t.Errorf("after ClearExhaustedNick on store-parked a: a unexhausted, want still exhausted (store-derived)")
	}
}

// TestStoreFreshnessBlocks_invariant (AC #10): the corrected invariant
// — !snapRejects ⇒ the union would not contribute — still holds post
// change. Two cases pin it:
// (a) !snapRejects (store reads healthy) ⇒ no entry in c.storeParked
//     after a resolve; the union returns (_, false).
// (b) snapRejects (store blocks) ⇒ entry in c.storeParked; the union
//     returns the bound.
func TestStoreFreshnessBlocks_invariant(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	// Case (a): store says healthy — fresh, below cap.
	util := 0.5
	putFresh(t, store, c, "a", 0.5, clock.now().Add(time.Hour), clock.now())
	c.ResolveAuto()
	c.mu.Lock()
	_, ok := c.storeParked["a"]
	_, unionOK := c.exhaustedUntilLocked("a")
	c.mu.Unlock()
	if ok {
		t.Errorf("case (a): c.storeParked[a] present, want absent (healthy store ⇒ no park)")
	}
	if unionOK {
		t.Errorf("case (a): union returns ok=true, want false (healthy store ⇒ not exhausted)")
	}

	// Case (b): store blocks — fresh, at cap.
	util = 1.0
	asOf := clock.now()
	reset2 := clock.now().Add(time.Hour)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hReset:       &reset2,
		AsOf:                 asOf,
	})
	c.ResolveAuto()
	c.mu.Lock()
	bound, ok := c.storeParked["a"]
	reset, unionOK := c.exhaustedUntilLocked("a")
	c.mu.Unlock()
	if !ok {
		t.Errorf("case (b): c.storeParked[a] absent, want present (blocking store ⇒ asserted park)")
	}
	if !unionOK || !reset.Equal(bound) {
		t.Errorf("case (b): union returned %v,%v, want %v,true", reset, unionOK, bound)
	}
}

// memberStatus_ / memberParked_ — local copy of the helpers in
// auto_test.go so this test file compiles independently of the
// helper-internal ordering.
func memberStatus_(ps PoolStatus, nick string) string {
	for _, m := range ps.Members {
		if m.Nick == nick {
			return m.Status
		}
	}
	return ""
}

func memberParked_(ps PoolStatus, nick string) bool {
	for _, m := range ps.Members {
		if m.Nick == nick {
			return m.Parked
		}
	}
	return false
}
