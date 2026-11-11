package auto

import (
	"io"
	"testing"
	"time"

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
	c.park("a", fiveReset)

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
