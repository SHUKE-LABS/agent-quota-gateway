package auto

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// parkLocked is a test helper that marks a controller member exhausted
// until reset, mirroring what a 429 records via record429 but without
// building a response. It is the analogue of clock-driven failover for the
// preemptor's unit tests.
func (c *Controller) park(nick string, reset time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.exhausted[nick] = reset
}

// setCur points the sticky pointer at nick (test-only).
func (c *Controller) setCur(nick string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur = c.indexOf(nick)
}

func TestPreemptTo_switchesToHealthyHigher(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "a,b", "a", "b")
	c.setCur("b") // fell over to the lower-priority member

	if !c.PreemptTo("a") {
		t.Fatal("PreemptTo(a) = false, want true (a is higher priority and healthy)")
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current() = %q, want a after preempt", got)
	}
}

func TestPreemptTo_refusesExhausted(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "a,b", "a", "b")
	c.setCur("b")
	c.park("a", clock.now().Add(time.Hour)) // a still rate-limited

	if c.PreemptTo("a") {
		t.Fatal("PreemptTo(a) = true, want false (a is exhausted)")
	}
	if got := c.Current(); got != "b" {
		t.Errorf("Current() = %q, want b (unchanged)", got)
	}
}

func TestPreemptTo_refusesNonHigher(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "a,b", "a", "b")
	// Starts on a (highest). Preempting to the lower-priority b must be a no-op.
	if c.PreemptTo("b") {
		t.Fatal("PreemptTo(b) = true, want false (b is lower priority than current a)")
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current() = %q, want a (unchanged)", got)
	}
	// An unknown nick is also refused.
	if c.PreemptTo("nope") {
		t.Fatal("PreemptTo(nope) = true, want false (unknown nick)")
	}
}

func TestPreemptTo_nonPriorityPoolNoOp(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b") // no priority declared
	if c.PreemptTo("b") {
		t.Fatal("PreemptTo on a non-priority pool = true, want false")
	}
}

func TestPreempt_tickIdleWhenOnHighest(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "a,b", "a", "b")
	p := newPreemptor([]*Controller{c}, quota.NewStore(), 0, clock.now, io.Discard)

	wait := p.tick()
	if wait != defaultPreemptInterval {
		t.Errorf("tick wait = %v, want idle %v (already on highest)", wait, defaultPreemptInterval)
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current() = %q, want a (no switch)", got)
	}
}

func TestPreempt_switchesNowWhenHigherHealthy(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "a,b", "a", "b")
	c.setCur("b") // on lower-priority b; a is healthy and unused
	p := newPreemptor([]*Controller{c}, quota.NewStore(), 0, clock.now, io.Discard)

	if wait := p.tick(); wait != defaultPreemptInterval {
		t.Errorf("tick wait = %v, want idle after the immediate switch", wait)
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current() = %q, want a (switched back to healthy higher)", got)
	}
}

func TestPreempt_schedulesThenSwitchesViaPreciseQuotaReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "zai,m3", "m3", "zai")
	c.setCur("m3")
	// zai 429'd with no reset header → parked the conservative 5h default,
	// but the poller knows its window actually resets in 1h.
	c.park("zai", clock.now().Add(defaultExhaustionWindow))
	store := quota.NewStore()
	qReset := clock.now().Add(time.Hour)
	store.Put(c.resolve(t, "zai").QuotaKey(), quota.Snapshot{Unified5hReset: &qReset, AsOf: clock.now()})

	p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)

	// Before the precise reset: schedule a wake at it, no switch yet.
	wait := p.tick()
	if wait != time.Hour {
		t.Fatalf("tick wait = %v, want 1h (the precise quota reset, not the 5h park)", wait)
	}
	if got := c.Current(); got != "m3" {
		t.Fatalf("Current() = %q, want m3 (no switch before reset)", got)
	}

	// At the precise reset the conservative park is still in the future, so
	// the preemptor must override it and switch back to zai.
	clock.advance(time.Hour)
	if wait := p.tick(); wait != defaultPreemptInterval {
		t.Errorf("tick wait = %v, want idle after the switch", wait)
	}
	if got := c.Current(); got != "zai" {
		t.Errorf("Current() = %q, want zai (preempted back at the precise reset)", got)
	}
}

func TestPreempt_fallsBackToParkResetWithoutQuota(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "a,b", "a", "b")
	c.setCur("b")
	c.park("a", clock.now().Add(30*time.Minute)) // no quota data for a
	p := newPreemptor([]*Controller{c}, quota.NewStore(), 0, clock.now, io.Discard)

	if wait := p.tick(); wait != 30*time.Minute {
		t.Fatalf("tick wait = %v, want 30m (the controller park reset)", wait)
	}
	if got := c.Current(); got != "b" {
		t.Fatalf("Current() = %q, want b (parked, no switch yet)", got)
	}

	// Past the park reset the mark clears and the member reads healthy.
	clock.advance(31 * time.Minute)
	p.tick()
	if got := c.Current(); got != "a" {
		t.Errorf("Current() = %q, want a (switched after the park reset elapsed)", got)
	}
}

// TestPreempt_noFlapOnStaleQuotaReset proves the dedup: after preempting
// back on a precise reset, a member that is immediately re-limited (its
// quota entry frozen at the same past reset) is not switched to again — the
// pool rides the fallback instead, honouring reactive 429 precedence.
func TestPreempt_noFlapOnStaleQuotaReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "zai,m3", "m3", "zai")
	c.setCur("m3")
	c.park("zai", clock.now().Add(defaultExhaustionWindow))
	store := quota.NewStore()
	qReset := clock.now() // reset is exactly now → due
	store.Put(c.resolve(t, "zai").QuotaKey(), quota.Snapshot{Unified5hReset: &qReset, AsOf: clock.now()})
	p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)

	// First tick switches back to zai on the due reset.
	p.tick()
	if got := c.Current(); got != "zai" {
		t.Fatalf("Current() = %q, want zai (first preempt)", got)
	}

	// zai immediately 429s again: re-parked, failover to m3. The quota entry
	// is frozen at the same stale reset (the poller stopped tracking zai).
	c.setCur("m3")
	c.park("zai", clock.now().Add(defaultExhaustionWindow))

	wait := p.tick()
	if got := c.Current(); got != "m3" {
		t.Errorf("Current() = %q, want m3 (no flap on the stale reset)", got)
	}
	// And it waits on the fresh park, not the stale precise value.
	if wait != defaultExhaustionWindow {
		t.Errorf("tick wait = %v, want the fresh %v park (stale reset ignored)", wait, defaultExhaustionWindow)
	}
}

// TestPreempt_noFlapViaSwitchNowPath proves the switch-now path (a higher
// member whose park already elapsed) also anchors the dedup: a member that
// is re-limited the instant it is switched to, with its quota entry frozen
// at a stale past reset, is not switched to again.
func TestPreempt_noFlapViaSwitchNowPath(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "zai,m3", "m3", "zai")
	c.setCur("m3")
	// zai is healthy (no park) but its store entry is frozen at a past reset.
	store := quota.NewStore()
	stale := clock.now().Add(-time.Hour)
	store.Put(c.resolve(t, "zai").QuotaKey(), quota.Snapshot{Unified5hReset: &stale, AsOf: clock.now()})
	p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)

	p.tick()
	if got := c.Current(); got != "zai" {
		t.Fatalf("Current() = %q, want zai (switched to healthy higher)", got)
	}

	// zai immediately 429s again, re-parked, failover to m3; the stale reset
	// is unchanged. The pool must ride m3, not flap back to zai.
	c.setCur("m3")
	c.park("zai", clock.now().Add(defaultExhaustionWindow))
	wait := p.tick()
	if got := c.Current(); got != "m3" {
		t.Errorf("Current() = %q, want m3 (no flap via the switch-now path)", got)
	}
	if wait != defaultExhaustionWindow {
		t.Errorf("tick wait = %v, want the fresh %v park", wait, defaultExhaustionWindow)
	}
}

// TestPreempt_noFlapViaSwitchNowFutureReset proves the switch-now path
// anchors the dedup even when the frozen store reset is still in the
// future: after switching to a healthy higher member that immediately 429s,
// the (now stale) reset ageing into the past must not re-trigger a switch.
func TestPreempt_noFlapViaSwitchNowFutureReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "zai,m3", "m3", "zai")
	c.setCur("m3")
	store := quota.NewStore()
	// zai is healthy but its store entry is frozen at a reset 2m ahead.
	future := clock.now().Add(2 * time.Minute)
	store.Put(c.resolve(t, "zai").QuotaKey(), quota.Snapshot{Unified5hReset: &future, AsOf: clock.now()})
	p := newPreemptor([]*Controller{c}, store, 0, clock.now, io.Discard)

	p.tick()
	if got := c.Current(); got != "zai" {
		t.Fatalf("Current() = %q, want zai (switched to healthy higher)", got)
	}

	// zai 429s again, re-parked; the frozen future reset stays put and then
	// ages into the past as the idle interval elapses.
	c.setCur("m3")
	c.park("zai", clock.now().Add(defaultExhaustionWindow))
	clock.advance(5 * time.Minute) // now past the frozen 2m reset
	wait := p.tick()
	if got := c.Current(); got != "m3" {
		t.Errorf("Current() = %q, want m3 (no flap on the aged-out frozen reset)", got)
	}
	if wait <= 0 || wait > defaultExhaustionWindow {
		t.Errorf("tick wait = %v, want a positive wait on the fresh park", wait)
	}
}

// TestPreempt_climbsIncrementally proves a three-member pool climbs one
// rank at a time as higher members recover: m3 -> b -> a.
func TestPreempt_climbsIncrementally(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "a,b,m3", "a", "b", "m3")
	c.setCur("m3")
	c.park("a", clock.now().Add(2*time.Hour))
	c.park("b", clock.now().Add(time.Hour))
	p := newPreemptor([]*Controller{c}, quota.NewStore(), 0, clock.now, io.Discard)

	// Earliest reset is b at 1h.
	if wait := p.tick(); wait != time.Hour {
		t.Fatalf("tick wait = %v, want 1h (b is the soonest higher reset)", wait)
	}

	// b recovers first → climb to b (a is still parked).
	clock.advance(time.Hour)
	if wait := p.tick(); wait != time.Hour {
		t.Errorf("tick wait = %v, want 1h remaining until a's reset", wait)
	}
	if got := c.Current(); got != "b" {
		t.Fatalf("Current() = %q, want b (first climb)", got)
	}

	// a recovers → climb to a (the top).
	clock.advance(time.Hour)
	p.tick()
	if got := c.Current(); got != "a" {
		t.Errorf("Current() = %q, want a (top of priority)", got)
	}
}

func TestPreempt_logsSwitchWithoutCredentials(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "a,b", "a", "b")
	c.setCur("b")
	var logBuf bytes.Buffer
	p := newPreemptor([]*Controller{c}, quota.NewStore(), 0, clock.now, &logBuf)

	p.tick()
	log := logBuf.String()
	if !strings.Contains(log, "preempt[auto]: b -> a") {
		t.Errorf("preempt switch not logged as expected; got %q", log)
	}
	if strings.Contains(log, "cred") {
		t.Errorf("preempt log leaked a credential: %q", log)
	}
}

// TestNewPreemptor_skipsNonPriorityPools proves only priority pools are
// collected, so equal-strength pools never preempt and Run is a no-op when
// no pool declared a priority.
func TestNewPreemptor_skipsNonPriorityPools(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// Single non-priority pool → no controllers collected → Run returns.
	reg := testRegistry(t, "a", "b")
	pools := NewPools(reg, nil, clock.now, io.Discard)
	p := NewPreemptor(pools, quota.NewStore(), 0, clock.now, io.Discard)
	if len(p.controllers) != 0 {
		t.Fatalf("collected %d controllers, want 0 (no priority pool)", len(p.controllers))
	}

	done := make(chan struct{})
	go func() { p.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return immediately with no priority pools")
	}
}

func TestPreemptor_RunStopsOnContextCancel(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "a,b", "a", "b")
	// Long interval so the loop parks in the timer after one immediate tick.
	p := newPreemptor([]*Controller{c}, quota.NewStore(), time.Hour, clock.now, io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

// TestPreemptor_disabledMemberPreemptBack proves that when the highest-priority
// member is disabled, preempt-back still reaches the next available preferred
// member (decision 4a). Without this, the disabled member would appear in
// preemptView with exhausted=false, causing tick() to target it and then
// fail in PreemptTo, stopping the scan before reaching the next healthy member.
func TestPreemptor_disabledMemberPreemptBack(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer

	// Priority pool: c > b > a. The pool has fallen all the way down to the
	// lowest member a. The top member c is then operator-disabled while the
	// middle member b is healthy — so preempt-back must skip c (iterated
	// first, highest priority) and climb to the next available preferred
	// member b. The preemptor only ever climbs up, so the current member
	// must start below the target for preempt-back to have anything to do.
	c := newPriorityController(t, -1, clock, &logBuf, "c,b,a", "a", "b", "c")
	c.setCur("a")

	// Disable c (the highest-priority member).
	c.mu.Lock()
	c.setDisabledLocked("c", true)
	c.mu.Unlock()

	// Empty store: b reads healthy (no exhaustion snapshot).
	store := quota.NewStore()
	p := newPreemptor([]*Controller{c}, store, 0, clock.now, &logBuf)

	// Tick should skip the disabled c (which, left in the view, would be
	// targeted as !exhausted and then refused by PreemptTo, stalling the
	// scan) and climb to the healthy b.
	wait := p.tick()

	// We should have switched up to b (not stalled on a, never landing on c).
	if cur := c.Current(); cur != "b" {
		t.Errorf("after tick: current=%q, want b (disabled c skipped, climb to healthy b)", cur)
	}

	// The wait should be the idle interval: the healthy b was switched to
	// immediately, so there is no parked higher member to wait on.
	if wait != 5*time.Minute {
		t.Errorf("tick wait=%v, want 5m (idle interval)", wait)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, "preempt[auto]:") {
		t.Errorf("expected preempt-back log, got: %q", logs)
	}
	if strings.Contains(logs, "cred") {
		t.Errorf("log leaked a credential: %q", logs)
	}
}
