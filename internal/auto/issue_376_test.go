package auto

import (
	"io"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

func putTwoWindowSnapshot(t *testing.T, store *quota.Store, c *Controller, fiveReset, weeklyReset time.Time, asOf time.Time) {
	t.Helper()
	full := 1.0
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &full,
		Unified5hReset:       &fiveReset,
		Unified7dUtilization: &full,
		Unified7dReset:       &weeklyReset,
		AsOf:                 asOf,
	})
}

func assertCodexFallbackParked(t *testing.T, c *Controller, store *quota.Store) {
	t.Helper()
	if got := c.Current(); got != "b" {
		t.Fatalf("Current()=%q, want b while Codex weekly window remains binding", got)
	}
	if !memberParked(c.poolStatus(store, nil, nil), "a") {
		t.Fatal("Codex member a is not parked while its weekly window remains binding")
	}
	b, _, exhausted := c.ResolveAuto()
	if exhausted || b.Nick != "b" {
		t.Fatalf("ResolveAuto=(%q, exhausted=%v), want fallback b / false", b.Nick, exhausted)
	}
}

func TestPreempt_codexLaterWeeklyResetKeepsFallbackUntilWeeklyReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := issue316Controller(t, "https://chatgpt.com/backend-api/codex", clock, store)
	c.setCur("b")
	fiveReset := clock.now().Add(2 * time.Hour)
	weeklyReset := clock.now().Add(6 * time.Hour)
	putTwoWindowSnapshot(t, store, c, fiveReset, weeklyReset, clock.now())
	c.record429WithSource("a", fiveReset, true, true)

	p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)
	if wait := p.tick(); wait != defaultPreemptInterval {
		t.Fatalf("initial tick wait=%v, want cadence cap while weekly reset is far away", wait)
	}
	assertCodexFallbackParked(t, c, store)

	// Every tick after the 5h reset and before the weekly reset must retain
	// both the live park and the fallback route (Reviewer observation).
	for _, advance := range []time.Duration{2 * time.Hour, time.Hour, 2 * time.Hour} {
		clock.advance(advance)
		if wait := p.tick(); wait != defaultPreemptInterval {
			t.Fatalf("tick at %v wait=%v, want cadence cap until weekly reset", clock.now(), wait)
		}
		assertCodexFallbackParked(t, c, store)
	}

	clock.advance(time.Hour) // exactly the weekly reset
	if wait := p.tick(); wait != defaultPreemptInterval {
		t.Errorf("weekly-reset tick wait=%v, want idle cadence after preempt", wait)
	}
	if got := c.Current(); got != "a" {
		t.Fatalf("Current()=%q at weekly reset, want a after preempt-back", got)
	}
	if memberParked(c.poolStatus(store, nil, nil), "a") {
		t.Fatal("Codex member a remains parked after its weekly reset")
	}
}

func TestPreempt_codexFiveHourOnlyStillPreemptsAtFiveHourReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := issue316Controller(t, "https://chatgpt.com/backend-api/codex", clock, store)
	c.setCur("b")
	fiveReset := clock.now().Add(2 * time.Minute)
	full := 1.0
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &full,
		Unified5hReset:       &fiveReset,
		AsOf:                 clock.now(),
	})
	c.park("a", clock.now().Add(5*time.Hour))

	p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)
	if wait := p.tick(); wait != 2*time.Minute {
		t.Fatalf("initial tick wait=%v, want precise 5h reset", wait)
	}
	clock.advance(2 * time.Minute)
	p.tick()
	if got := c.Current(); got != "a" {
		t.Fatalf("Current()=%q after 5h-only reset, want a", got)
	}
}

func TestPreempt_statuslessLongWindowRequiresFreshLivePark(t *testing.T) {
	t.Run("stale snapshot does not extend live park", func(t *testing.T) {
		clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
		store := quota.NewStore()
		c := issue316Controller(t, "https://chatgpt.com/backend-api/codex", clock, store)
		c.setCur("b")
		fiveReset := clock.now().Add(2 * time.Hour)
		weeklyReset := clock.now().Add(6 * time.Hour)
		putTwoWindowSnapshot(t, store, c, fiveReset, weeklyReset,
			clock.now().Add(-storeSnapshotFreshness-time.Minute))
		c.park("a", fiveReset)

		p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)
		p.tick()
		c.mu.Lock()
		got := c.exhausted["a"]
		c.mu.Unlock()
		if !got.Equal(fiveReset) {
			t.Fatalf("stale snapshot extended park to %v, want original 5h reset %v", got, fiveReset)
		}

		clock.advance(2 * time.Hour)
		p.tick()
		if got := c.Current(); got != "a" {
			t.Fatalf("Current()=%q after stale snapshot's 5h reset, want a", got)
		}
	})

	t.Run("expired park is not revived", func(t *testing.T) {
		clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
		store := quota.NewStore()
		c := issue316Controller(t, "https://chatgpt.com/backend-api/codex", clock, store)
		c.setCur("b")
		weeklyReset := clock.now().Add(time.Hour)
		putTwoWindowSnapshot(t, store, c, clock.now().Add(time.Hour), weeklyReset, clock.now())
		c.park("a", clock.now().Add(-time.Minute))

		p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)
		p.tick()
		if got := c.Current(); got != "a" {
			t.Fatalf("Current()=%q with expired park, want a; stale park must not be revived", got)
		}
		c.mu.Lock()
		_, parked := c.exhausted["a"]
		c.mu.Unlock()
		if parked {
			t.Fatal("expired park revived from statusless weekly snapshot")
		}
	})
}

func TestPreempt_statuslessWeeklySnapshotAloneRemainsSelectable(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := issue316Controller(t, "https://chatgpt.com/backend-api/codex", clock, store)
	c.setCur("b")
	full := 1.0
	weeklyReset := clock.now().Add(time.Hour)
	store.Put(c.resolve(t, "a").QuotaKey(), quota.Snapshot{
		Unified7dUtilization: &full,
		Unified7dReset:       &weeklyReset,
		AsOf:                 clock.now(),
	})

	p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)
	p.tick()
	if got := c.Current(); got != "a" {
		t.Fatalf("Current()=%q, want a (statusless weekly snapshot alone is not a park)", got)
	}
	if memberParked(c.poolStatus(store, nil, nil), "a") {
		t.Fatal("statusless weekly snapshot created a store-only park")
	}
}

func TestPreempt_zaiMonthlyLongWindowDoesNotExtendPark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := issue316Controller(t, "https://api.z.ai/api/anthropic", clock, store)
	c.setCur("b")
	fiveReset := clock.now().Add(2 * time.Minute)
	monthlyReset := clock.now().Add(24 * time.Hour)
	putTwoWindowSnapshot(t, store, c, fiveReset, monthlyReset, clock.now())
	c.park("a", clock.now().Add(5*time.Hour))

	p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)
	if wait := p.tick(); wait != 2*time.Minute {
		t.Fatalf("initial tick wait=%v, want 5h reset; monthly long window must be ignored", wait)
	}
	clock.advance(2 * time.Minute)
	p.tick()
	if got := c.Current(); got != "a" {
		t.Fatalf("Current()=%q after 5h reset, want a (Z.AI monthly window is not chat-blocking)", got)
	}
}

func TestPreempt_codexQuotaParkExtensionPropagatesToSiblingPools(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	scrubPoolEnv(t)
	env := propagationEnv()
	env[backend.EnvPrefix+"A_BASE_URL"] = "https://chatgpt.com/backend-api/codex"
	env[backend.EnvPrefix+"B_BASE_URL"] = "https://chatgpt.com/backend-api/codex"
	for key, value := range env {
		t.Setenv(key, value)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := NewPools(reg, store, clock.now, io.Discard)

	ca, cb := pools.byPool["a"], pools.byPool["b"]
	cb.setCur("b2")
	fiveReset := clock.now().Add(2 * time.Hour)
	weeklyReset := clock.now().Add(6 * time.Hour)
	full := 1.0
	ccz := ca.resolve(t, "ccz")
	store.Put(ccz.QuotaKey(), quota.Snapshot{
		Unified5hUtilization: &full,
		Unified5hReset:       &fiveReset,
		Unified7dUtilization: &full,
		Unified7dReset:       &weeklyReset,
		AsOf:                 clock.now(),
	})
	ca.record429WithSource("ccz", fiveReset, true, true)

	p := newPreemptor([]*Controller{ca, cb}, store, 0, clock.now, io.Discard)
	p.tick()
	for _, c := range []*Controller{ca, cb} {
		c.mu.Lock()
		entry, ok := c.credentialPark["ccz"]
		c.mu.Unlock()
		if !ok || !entry.reset.Equal(weeklyReset) {
			t.Fatalf("pool %s credentialPark[ccz]=%+v,%v, want propagated weekly reset %v", c.name(), entry, ok, weeklyReset)
		}
		if got := c.Current(); got == "ccz" {
			t.Fatalf("pool %s preempted to ccz before weekly reset", c.name())
		}
	}

	// The shared snapshot is now stale, but the already-extended live quota
	// park must remain binding in both pools through the earlier 5h reset.
	clock.advance(2 * time.Hour)
	p.tick()
	for _, c := range []*Controller{ca, cb} {
		if got := c.Current(); got == "ccz" {
			t.Fatalf("pool %s preempted to ccz at the 5h reset", c.name())
		}
		if !memberParked(c.poolStatus(store, nil, nil), "ccz") {
			t.Fatalf("pool %s lost ccz park before weekly reset", c.name())
		}
	}

	clock.advance(4 * time.Hour)
	p.tick()
	for _, c := range []*Controller{ca, cb} {
		if got := c.Current(); got != "ccz" {
			t.Fatalf("pool %s Current()=%q after weekly reset, want ccz", c.name(), got)
		}
	}
}

func TestPreempt_persistedLongWindowBoundSurvivesRestart(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := issue316Controller(t, "https://chatgpt.com/backend-api/codex", clock, store)
	c.setCur("b")
	fiveReset := clock.now().Add(2 * time.Hour)
	weeklyReset := clock.now().Add(6 * time.Hour)
	putTwoWindowSnapshot(t, store, c, fiveReset, weeklyReset, clock.now())
	c.record429WithSource("a", fiveReset, true, true)

	pre := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)
	pre.tick() // folds the fresh weekly reset into the persisted quota park
	saved := c.persistState()
	entry, ok := saved.CredentialPark["a"]
	if !ok || !entry.LongWindowBound || !entry.Reset.Equal(weeklyReset) {
		t.Fatalf("saved Codex park=%+v,%v, want weekly reset with long_window_bound", entry, ok)
	}

	// Rebuild the controller from persisted routing state, then let the
	// shared snapshot age past the freshness window before the 5h reset.
	restarted := issue316Controller(t, "https://chatgpt.com/backend-api/codex", clock, store)
	restarted.loadState(saved.Sticky, saved.Exhausted, saved.LocalSnapshotNicks)
	restarted.loadCredentialPark(saved.CredentialPark)
	clock.advance(2 * time.Hour)
	post := newPreemptor([]*Controller{restarted}, store, 0, clock.now, io.Discard)
	post.tick()
	if got := restarted.Current(); got != "b" {
		t.Fatalf("Current()=%q after restart at 5h reset, want fallback b", got)
	}
	if !memberParked(restarted.poolStatus(store, nil, nil), "a") {
		t.Fatal("restarted Codex member a lost its weekly park at the stale 5h tick")
	}

	clock.advance(4 * time.Hour)
	post.tick()
	if got := restarted.Current(); got != "a" {
		t.Fatalf("Current()=%q at weekly reset after restart, want a", got)
	}
}
