package auto

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// propagationEnv builds a 3-pool env (a, b, c) sharing one nick "ccz" — the
// #254 shared-credential shape (nick ccz live in four pools in the field
// report) — plus a pool-private second member in each so a park on ccz has
// somewhere healthy to fail over to. Each pool's priority pins ccz first so
// NewPools' otherwise-random start is deterministic.
func propagationEnv() map[string]string {
	return map[string]string{
		backend.EnvPrefix + "A_BACKEND_CCZ": "cred-ccz",
		backend.EnvPrefix + "A_BACKEND_A2":  "cred-a2",
		backend.EnvPrefix + "A_PRIORITY":    "ccz",
		backend.EnvPrefix + "B_BACKEND_CCZ": "cred-ccz",
		backend.EnvPrefix + "B_BACKEND_B2":  "cred-b2",
		backend.EnvPrefix + "B_PRIORITY":    "ccz",
		backend.EnvPrefix + "C_BACKEND_CCZ": "cred-ccz",
		backend.EnvPrefix + "C_BACKEND_C2":  "cred-c2",
		backend.EnvPrefix + "C_PRIORITY":    "ccz",
	}
}

// nextParkedButResetPassed is a test-only locked wrapper over
// nextParkedButResetPassedLocked, mirroring the exhaustedUntil wrapper in
// store_exhaustion_test.go, so the half-open picker can be asserted directly.
func (c *Controller) nextParkedButResetPassed() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nextParkedButResetPassedLocked()
}

// TestCredentialPark_401PropagatesToSiblings covers AC1 and AC4: a 401 caught
// by pool a must make ccz unavailable and reported Parked in b and c too,
// without either ever observing its own 401.
func TestCredentialPark_401PropagatesToSiblings(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())

	reg := p.CurrentRegistry()
	bCCZinA, ok := reg.ResolveIn("a", "ccz")
	if !ok {
		t.Fatalf("ResolveIn(a, ccz) not found")
	}

	if err := p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	for _, pool := range []string{"a", "b", "c"} {
		status, ok := p.PoolStatus(pool, quota.NewStore(), nil)
		if !ok {
			t.Fatalf("PoolStatus(%s) not found", pool)
		}
		if !memberParked(status, "ccz") {
			t.Errorf("pool %s: ccz Parked=false, want true (401 on a must propagate)", pool)
		}
	}

	// b and c never saw their own 401, but must route away from ccz on their
	// very next resolve (AC1: "without each pool having to observe its own
	// 401").
	for pool, want := range map[string]string{"b": "b2", "c": "c2"} {
		backendGot, _, _, exhausted := p.Route(pool)
		if exhausted {
			t.Errorf("pool %s: exhausted=true, want a healthy fallback (%s)", pool, want)
		}
		if backendGot.Nick != want {
			t.Errorf("pool %s: routed to %q, want %q (ccz is credential-parked)", pool, backendGot.Nick, want)
		}
	}
}

// TestCredentialPark_headerless429Propagates covers AC2: a 429 whose reset
// falls back to defaultExhaustionWindow (no usable response reset) is the
// AC2 residue and must propagate exactly like a 401/403.
func TestCredentialPark_headerless429Propagates(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())

	reg := p.CurrentRegistry()
	bCCZinA, ok := reg.ResolveIn("a", "ccz")
	if !ok {
		t.Fatalf("ResolveIn(a, ccz) not found")
	}

	// resetIn=0 omits the unified-reset header, so resetFrom falls back to
	// defaultExhaustionWindow — store-unrepresentable.
	if err := p.ModifyResponse(resp429(bCCZinA, clock, 0)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	statusB, _ := p.PoolStatus("b", quota.NewStore(), nil)
	statusC, _ := p.PoolStatus("c", quota.NewStore(), nil)
	if !memberParked(statusB, "ccz") {
		t.Errorf("pool b: ccz Parked=false, want true (headerless 429 on a must propagate)")
	}
	if !memberParked(statusC, "ccz") {
		t.Errorf("pool c: ccz Parked=false, want true (headerless 429 on a must propagate)")
	}
}

// TestCredentialPark_futureReset429NotPropagated covers AC3: a 429 carrying a
// genuine future unified-reset is store-derivable, so it must NOT be copied
// into c.credentialPark anywhere — sibling pools stay unaware and healthy.
func TestCredentialPark_futureReset429NotPropagated(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())

	reg := p.CurrentRegistry()
	bCCZinA, ok := reg.ResolveIn("a", "ccz")
	if !ok {
		t.Fatalf("ResolveIn(a, ccz) not found")
	}

	if err := p.ModifyResponse(resp429(bCCZinA, clock, time.Hour)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	// a itself parks locally (unchanged pre-existing behaviour) ...
	statusA, _ := p.PoolStatus("a", quota.NewStore(), nil)
	if !memberParked(statusA, "ccz") {
		t.Fatalf("pool a: ccz Parked=false, want true (local 429 park)")
	}
	// ... but b/c must never learn about it: no credentialPark entry, no
	// Parked flag, still routing to ccz.
	statusB, _ := p.PoolStatus("b", quota.NewStore(), nil)
	if memberParked(statusB, "ccz") {
		t.Errorf("pool b: ccz Parked=true, want false (store-derivable park must not propagate)")
	}
	cb := p.byPool["b"]
	cb.mu.Lock()
	_, hasCP := cb.credentialPark["ccz"]
	cb.mu.Unlock()
	if hasCP {
		t.Errorf("pool b: credentialPark[ccz] present, want absent (store-derivable park)")
	}
	if backendGot, _, _, exhausted := p.Route("b"); exhausted || backendGot.Nick != "ccz" {
		t.Errorf("pool b: routed to %q exhausted=%v, want ccz still healthy there", backendGot.Nick, exhausted)
	}
}

// TestCredentialPark_clearReleasesEverywhere covers AC5 and AC12: a clear
// issued against ANY pool holding the nick — the origin or a pure sibling —
// releases the propagated park in every other pool, and reports which ones.
func TestCredentialPark_clearReleasesEverywhere(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())
	reg := p.CurrentRegistry()
	bCCZinA, _ := reg.ResolveIn("a", "ccz")

	if err := p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	// Clearing from the ORIGIN pool (a) must release b and c too.
	cleared, releasedElsewhere, ok := p.ClearExhaustedNick("a", "ccz")
	if !ok || !cleared {
		t.Fatalf("ClearExhaustedNick(a, ccz) = cleared=%v ok=%v, want true,true", cleared, ok)
	}
	if len(releasedElsewhere) != 2 || releasedElsewhere[0] != "b" || releasedElsewhere[1] != "c" {
		t.Errorf("releasedElsewhere=%v, want [b c]", releasedElsewhere)
	}
	for _, pool := range []string{"a", "b", "c"} {
		status, _ := p.PoolStatus(pool, quota.NewStore(), nil)
		if memberParked(status, "ccz") {
			t.Errorf("pool %s: ccz still Parked=true after clear on a", pool)
		}
	}

	// Re-assert, then clear from a PURE SIBLING (b, which never itself saw
	// the 401) — must still release a and c (issue #254: "a clear issued
	// against one pool releases a propagated park in every pool holding the
	// nick", not just the origin).
	if err := p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse (re-assert): %v", err)
	}
	cleared, releasedElsewhere, ok = p.ClearExhaustedNick("b", "ccz")
	if !ok || !cleared {
		t.Fatalf("ClearExhaustedNick(b, ccz) = cleared=%v ok=%v, want true,true", cleared, ok)
	}
	if len(releasedElsewhere) != 2 || releasedElsewhere[0] != "a" || releasedElsewhere[1] != "c" {
		t.Errorf("releasedElsewhere=%v, want [a c]", releasedElsewhere)
	}
	for _, pool := range []string{"a", "b", "c"} {
		status, _ := p.PoolStatus(pool, quota.NewStore(), nil)
		if memberParked(status, "ccz") {
			t.Errorf("pool %s: ccz still Parked=true after clear on b", pool)
		}
	}
}

// TestCredentialPark_wholePoolClearReleasesEverywhere is the ClearExhausted
// (whole-pool) counterpart to TestCredentialPark_clearReleasesEverywhere,
// pinning the Pools.ClearExhausted (AC5/AC12) reporting shape.
func TestCredentialPark_wholePoolClearReleasesEverywhere(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())
	reg := p.CurrentRegistry()
	bCCZinA, _ := reg.ResolveIn("a", "ccz")

	if err := p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	cleared, releasedElsewhere, ok := p.ClearExhausted("a")
	if !ok {
		t.Fatalf("ClearExhausted(a) ok=false")
	}
	if len(cleared) != 1 || cleared[0] != "ccz" {
		t.Errorf("cleared=%v, want [ccz]", cleared)
	}
	if got := releasedElsewhere["ccz"]; len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Errorf("releasedElsewhere[ccz]=%v, want [b c]", got)
	}
	for _, pool := range []string{"a", "b", "c"} {
		status, _ := p.PoolStatus(pool, quota.NewStore(), nil)
		if memberParked(status, "ccz") {
			t.Errorf("pool %s: ccz still Parked=true after whole-pool clear on a", pool)
		}
	}
}

// TestCredentialPark_storeSourcedExhaustionSurvivesClear pins the tail of
// AC5: a clear releases only the reactive/credential park, never
// store-sourced exhaustion — a nick also blocked by a polled window stays
// unavailable in that pool after the clear.
func TestCredentialPark_storeSourcedExhaustionSurvivesClear(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	p := loadMovePools(t, clock, propagationEnv())
	p.store = store
	for _, name := range []string{"a", "b", "c"} {
		p.byPool[name].store = store
	}

	reg := p.CurrentRegistry()
	bCCZinA, _ := reg.ResolveIn("a", "ccz")
	if err := p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	// b independently also has a polled at-cap snapshot for ccz — a
	// store-sourced block, wholly unrelated to the propagated credential
	// park.
	reset := clock.now().Add(2 * time.Hour)
	putUtil(t, store, p.byPool["b"], "ccz", 1.0, reset)
	// Force the assert-once write so the block lives in c.exhausted too,
	// matching how a real resolve would observe it.
	if _, _, _, exhausted := p.Route("b"); exhausted {
		t.Fatalf("pool b unexpectedly fully exhausted before clear")
	}

	if _, _, ok := p.ClearExhaustedNick("a", "ccz"); !ok {
		t.Fatalf("ClearExhaustedNick(a, ccz) ok=false")
	}

	if until, ok := p.byPool["b"].exhaustedUntil("ccz"); !ok || until.Before(clock.now()) {
		t.Errorf("pool b: ccz exhaustedUntil=%v ok=%v, want still blocked by the store-sourced window", until, ok)
	}
}

// TestCredentialPark_halfOpenPickerSkipsPropagatedPark covers AC8: the
// half-open picker (nextParkedButResetPassedLocked) must never forward a
// live client request to a nick whose only park is credential-fatal, even
// once its live-429 sibling's reset has elapsed and it would otherwise be
// the round-robin pick.
func TestCredentialPark_halfOpenPickerSkipsPropagatedPark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, nil, "x", "y")

	c.mu.Lock()
	c.curNick = "x"
	c.credentialPark["x"] = credentialParkEntry{reset: clock.now().Add(time.Hour)} // still future — must be skipped
	c.exhausted["y"] = clock.now().Add(-time.Minute)                               // live-429 reset already elapsed
	c.mu.Unlock()

	nick, ok := c.nextParkedButResetPassed()
	if !ok || nick != "y" {
		t.Fatalf("nextParkedButResetPassed()=(%q,%v), want (y,true) — must skip the credential-parked x", nick, ok)
	}
}

// TestCredentialPark_reconcilePreservesAcrossUnrelatedMutation covers AC9: a
// runtime registry mutation on pool b that does not touch ccz's membership
// must not lose b's propagated credential park.
func TestCredentialPark_reconcilePreservesAcrossUnrelatedMutation(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())
	reg := p.CurrentRegistry()
	bCCZinA, _ := reg.ResolveIn("a", "ccz")

	if err := p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	// Unrelated mutation on b: add a brand-new member. Triggers
	// reconcileLocked for b's controller but never touches ccz's membership.
	if status, err := p.AddMember("b", "extra", "cred-extra", "", []string{"ccz", "extra"}); err != nil {
		t.Fatalf("AddMember(b, extra): status=%d err=%v", status, err)
	}

	statusB, _ := p.PoolStatus("b", quota.NewStore(), nil)
	if !memberParked(statusB, "ccz") {
		t.Errorf("pool b: ccz Parked=false after unrelated AddMember, want true (propagated park must survive reconcile)")
	}
}

// TestCredentialPark_removalFromOnePoolDoesNotClearOthers covers AC10: a
// RemoveMember on pool b drops b's own copy of the park (ccz is no longer
// even a member there) but must not clear it in a or c.
func TestCredentialPark_removalFromOnePoolDoesNotClearOthers(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())
	reg := p.CurrentRegistry()
	bCCZinA, _ := reg.ResolveIn("a", "ccz")

	if err := p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	if status, err := p.RemoveMember("b", "ccz"); err != nil {
		t.Fatalf("RemoveMember(b, ccz): status=%d err=%v", status, err)
	}

	if _, ok := p.CurrentRegistry().ResolveIn("b", "ccz"); ok {
		t.Errorf("ccz still resolves in pool b after RemoveMember")
	}

	statusA, _ := p.PoolStatus("a", quota.NewStore(), nil)
	statusC, _ := p.PoolStatus("c", quota.NewStore(), nil)
	if !memberParked(statusA, "ccz") {
		t.Errorf("pool a: ccz Parked=false after RemoveMember(b, ccz), want still true")
	}
	if !memberParked(statusC, "ccz") {
		t.Errorf("pool c: ccz Parked=false after RemoveMember(b, ccz), want still true")
	}
}

// TestCredentialPark_persistRoundTrip covers AC7: a propagated park survives
// a restart in every pool it held, restored via the normal
// PersistState/LoadPersistState path.
func TestCredentialPark_persistRoundTrip(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p1 := loadMovePools(t, clock, propagationEnv())
	reg := p1.CurrentRegistry()
	bCCZinA, _ := reg.ResolveIn("a", "ccz")

	if err := p1.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	saved := p1.PersistState()

	entry, ok := saved["b"].CredentialPark["ccz"]
	if len(saved["b"].CredentialPark) != 1 || !ok || entry.Reset.IsZero() {
		t.Fatalf("PersistState()[b].CredentialPark=%v, want a future ccz entry", saved["b"].CredentialPark)
	}
	if entry.WindowFact {
		t.Errorf("PersistState()[b].CredentialPark[ccz].WindowFact=true, want false (401/403 subclass)")
	}

	p2 := loadMovePools(t, clock, propagationEnv())
	p2.LoadPersistState(saved)

	for _, pool := range []string{"a", "b", "c"} {
		status, _ := p2.PoolStatus(pool, quota.NewStore(), nil)
		if !memberParked(status, "ccz") {
			t.Errorf("pool %s: ccz Parked=false after restart, want true (persisted credential park)", pool)
		}
	}
}

// TestCredentialPark_loadCredentialParkDropsAbsentNick pins the second half
// of AC7: the state-load writer does not resurrect a park in a pool the nick
// has since left.
func TestCredentialPark_loadCredentialParkDropsAbsentNick(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, nil, "x", "y")

	c.loadCredentialPark(map[string]CredentialParkPersist{
		"x":       {Reset: clock.now().Add(time.Hour)},  // present member — kept
		"removed": {Reset: clock.now().Add(time.Hour)},  // not a member — dropped
		"y":       {Reset: clock.now().Add(-time.Hour)}, // present but already expired — dropped
	})

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.credentialPark["x"]; !ok {
		t.Errorf("credentialPark[x] missing, want restored")
	}
	if _, ok := c.credentialPark["removed"]; ok {
		t.Errorf("credentialPark[removed] present, want dropped (not a current member)")
	}
	if _, ok := c.credentialPark["y"]; ok {
		t.Errorf("credentialPark[y] present, want dropped (reset already passed)")
	}
}

// TestParkSameNickTwoPoolsRace covers AC6: two pools concurrently parking the
// same nick (one 401 on a, one on b, hammered from separate goroutines) must
// not deadlock or race under -race, and must converge on a single agreed
// bound — the fixed clock means every record429WithSource call computes the
// identical now+defaultExhaustionWindow, so a and b's credentialPark[ccz]
// must end up exactly equal.
func TestParkSameNickTwoPoolsRace(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())
	reg := p.CurrentRegistry()
	bCCZinA, _ := reg.ResolveIn("a", "ccz")
	bCCZinB, _ := reg.ResolveIn("b", "ccz")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized))
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = p.ModifyResponse(respAuth(bCCZinB, http.StatusForbidden))
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	for _, pool := range []string{"a", "b", "c"} {
		status, _ := p.PoolStatus(pool, quota.NewStore(), nil)
		if !memberParked(status, "ccz") {
			t.Errorf("pool %s: ccz not parked after concurrent 401s", pool)
		}
	}

	ca, cb, cc := p.byPool["a"], p.byPool["b"], p.byPool["c"]
	ca.mu.Lock()
	entryA, okA := ca.credentialPark["ccz"]
	ca.mu.Unlock()
	cb.mu.Lock()
	entryB, okB := cb.credentialPark["ccz"]
	cb.mu.Unlock()
	cc.mu.Lock()
	entryC, okC := cc.credentialPark["ccz"]
	cc.mu.Unlock()
	// Presence, not just equality, is the real assertion here: a fixedClock
	// means every record429WithSource call computes the identical
	// now+defaultExhaustionWindow bound, so comparing only the values would
	// pass just as well if propagation silently wrote nothing at all (three
	// zero-value reads are "equal" too).
	if !okA || !okB || !okC {
		t.Fatalf("credentialPark[ccz] missing somewhere: a=%v b=%v c=%v", okA, okB, okC)
	}
	if entryA.reset.IsZero() || entryB.reset.IsZero() || entryC.reset.IsZero() {
		t.Fatalf("credentialPark[ccz] reset is zero somewhere: a=%v b=%v c=%v", entryA.reset, entryB.reset, entryC.reset)
	}
	if !entryA.reset.Equal(entryB.reset) || !entryA.reset.Equal(entryC.reset) {
		t.Errorf("credentialPark reset diverged across pools: a=%v b=%v c=%v", entryA.reset, entryB.reset, entryC.reset)
	}
}

// TestCredentialPark_propagationRetainsLatestReset covers issue #275: an
// older propagation delayed behind a newer local park must not shorten the
// credential park in any sibling, and must not replace the retained entry's
// windowFact classification.
func TestCredentialPark_propagationRetainsLatestReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())

	olderReset := clock.now().Add(5 * time.Hour)
	newerReset := olderReset.Add(30 * time.Minute)
	ca, cb := p.byPool["a"], p.byPool["b"]

	// Hold pool a's propagation callback so its local write completes before
	// the delayed sibling update is delivered.
	originalAPropagation := ca.propagatePark
	var delayedAPropagation func()
	ca.propagatePark = func(nick string, reset time.Time, windowFact bool) {
		delayedAPropagation = func() {
			originalAPropagation(nick, reset, windowFact)
		}
	}
	ca.record429WithSource("ccz", olderReset, true, true)
	ca.propagatePark = originalAPropagation
	if delayedAPropagation == nil {
		t.Fatal("pool a propagation callback was not captured")
	}

	// Pool b observes a later credential-fatal park. Suppress its callback so
	// the test can deliver the newer propagation and then a's delayed older
	// propagation in a controlled order.
	originalBPropagation := cb.propagatePark
	cb.propagatePark = nil
	cb.record429WithSource("ccz", newerReset, true, false)
	cb.propagatePark = originalBPropagation
	p.propagateCredentialPark("b", "ccz", newerReset, false)
	delayedAPropagation()

	for _, pool := range []string{"a", "b", "c"} {
		c := p.byPool[pool]
		c.mu.Lock()
		entry, ok := c.credentialPark["ccz"]
		c.mu.Unlock()
		if !ok {
			t.Fatalf("pool %s: credentialPark[ccz] missing", pool)
		}
		if !entry.reset.Equal(newerReset) {
			t.Errorf("pool %s: credentialPark[ccz].reset=%v, want newer reset %v", pool, entry.reset, newerReset)
		}
		if entry.windowFact {
			t.Errorf("pool %s: credentialPark[ccz].windowFact=true, want retained credential-fatal classification", pool)
		}
	}
}

// TestCredentialPark_headerlessResidueRetiredByFreshStore is a regression for
// a #254 rescue-review finding: credentialPark's read-path used to exempt
// BOTH subclasses from storeReconcilesParkLocked (#145), but only the 401/403
// subclass deserves that exemption. The header-less-429 residue (windowFact
// true) is a real quota-window fact, so a fresh healthy store snapshot must
// retire it early exactly like an ordinary c.exhausted entry — otherwise it
// over-parks for the full defaultExhaustionWindow even after the account is
// demonstrably fine again.
func TestCredentialPark_headerlessResidueRetiredByFreshStore(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	c.record429WithSource("a", clock.now().Add(5*time.Hour), true, true) // headerless-429 residue

	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Fatalf("a should be parked before the store goes fresh+healthy")
	}

	putUtil(t, store, c, "a", 0.1, clock.now().Add(2*time.Hour))

	if until, ok := c.exhaustedUntil("a"); ok {
		t.Errorf("a still exhaustedUntil=%v after a fresh healthy store snapshot, want retired (#145 applies to the header-less-429 residue)", until)
	}
}

// TestCredentialPark_authFatalNotRetiredByFreshStore is the 401/403
// counterpart: a fresh healthy store snapshot must NOT retire that subclass
// early — it proves the account isn't quota-exhausted, not that a
// since-revoked credential authenticates again.
func TestCredentialPark_authFatalNotRetiredByFreshStore(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	c.record429WithSource("a", clock.now().Add(5*time.Hour), true, false) // 401/403

	putUtil(t, store, c, "a", 0.1, clock.now().Add(2*time.Hour))

	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Errorf("a retired by a fresh healthy store snapshot, want still parked (401/403 is never store-reconciled)")
	}
}

// TestCredentialPark_recoveryProbeClearsCredentialPark is a regression for a
// #254 rescue-review finding: recoverParked's successful-probe branch used to
// delete only c.exhausted, leaving a mirrored credentialPark entry in place —
// so a nick parked via record429WithSource (either store-unrepresentable
// subclass) stayed unavailable (isUnavailableLocked unions both maps) even
// after a live health probe proved it recovered, while the caller still
// reported the nick "recovered" and pointed sticky at it.
func TestCredentialPark_recoveryProbeClearsCredentialPark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000},
			{"type":"TIME_LIMIT","percentage":30,"nextResetTime":4105046400000}
		]}}`)
	})

	c.record429WithSource("a", clock.now().Add(5*time.Hour), true, false) // 401/403-style

	c.mu.Lock()
	_, hadCP := c.credentialPark["a"]
	c.mu.Unlock()
	if !hadCP {
		t.Fatalf("credentialPark[a] missing before probe")
	}

	got := c.tryRecoverParked()
	if got != "a" {
		t.Fatalf("tryRecoverParked = %q, want %q (probe returned healthy)", got, "a")
	}
	if _, ok := c.exhaustedUntil("a"); ok {
		t.Errorf("a still exhaustedUntil after successful recovery probe")
	}

	c.mu.Lock()
	_, stillCP := c.credentialPark["a"]
	c.mu.Unlock()
	if stillCP {
		t.Errorf("credentialPark[a] still present after recovery probe — member remains unavailable despite being reported recovered")
	}
}

// TestNoteRecovered_clearsWindowFactNotAuthFatal is a regression for a #254
// rescue-review finding: the preemptor's noteRecovered used to delete only
// c.exhausted, so a precise store-reset supersession never actually made a
// credentialPark'd member selectable again. It must clear the
// header-less-429 residue (windowFact true — a real quota-window fact the
// precise reset is entitled to supersede) but must never clear a 401/403
// entry (windowFact false — a precise quota reset proves nothing about
// whether a since-revoked credential authenticates again).
func TestNoteRecovered_clearsWindowFactNotAuthFatal(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, nil, "x", "y")

	c.mu.Lock()
	c.exhausted["x"] = clock.now().Add(5 * time.Hour)
	c.credentialPark["x"] = credentialParkEntry{reset: clock.now().Add(5 * time.Hour), windowFact: true}
	c.exhausted["y"] = clock.now().Add(5 * time.Hour)
	c.credentialPark["y"] = credentialParkEntry{reset: clock.now().Add(5 * time.Hour), windowFact: false}
	c.mu.Unlock()

	c.noteRecovered("x")
	c.noteRecovered("y")

	c.mu.Lock()
	_, xCP := c.credentialPark["x"]
	_, yCP := c.credentialPark["y"]
	_, xExh := c.exhausted["x"]
	_, yExh := c.exhausted["y"]
	c.mu.Unlock()

	if xCP {
		t.Errorf("credentialPark[x] present after noteRecovered, want cleared (windowFact residue — precise reset supersedes it)")
	}
	if !yCP {
		t.Errorf("credentialPark[y] cleared after noteRecovered, want retained (401/403 — a quota reset proves nothing about credential liveness)")
	}
	if xExh || yExh {
		t.Errorf("exhausted map not cleared by noteRecovered for x or y: x=%v y=%v", xExh, yExh)
	}
}

// TestCredentialPark_clearDoesNotWipeSiblingsOwnWindowedPark is a regression
// for a #254 rescue-review finding: propagateCredentialParkClear used to drop
// a sibling's c.exhausted entry unconditionally whenever the sibling held a
// credentialPark entry for the same nick, even when that exhausted entry was
// a genuinely unrelated, independently-observed windowed 429 (a different
// reset). Only an exhausted entry that is the exact mirror of the
// credentialPark entry being released (same reset) may be dropped alongside
// it.
func TestCredentialPark_clearDoesNotWipeSiblingsOwnWindowedPark(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())
	reg := p.CurrentRegistry()
	bCCZinA, _ := reg.ResolveIn("a", "ccz")

	if err := p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	// Pool b independently observes its own genuine windowed 429 for ccz —
	// unrelated to the propagated credential park, with a different reset.
	cb := p.byPool["b"]
	genuineReset := clock.now().Add(30 * time.Minute)
	cb.record429WithSource("ccz", genuineReset, false, false)

	if _, _, ok := p.ClearExhaustedNick("a", "ccz"); !ok {
		t.Fatalf("ClearExhaustedNick(a, ccz) ok=false")
	}

	cb.mu.Lock()
	_, hasCP := cb.credentialPark["ccz"]
	exhReset, hasExh := cb.exhausted["ccz"]
	cb.mu.Unlock()
	if hasCP {
		t.Errorf("pool b: credentialPark[ccz] still present after clear on a")
	}
	if !hasExh || !exhReset.Equal(genuineReset) {
		t.Errorf("pool b: exhausted[ccz]=%v ok=%v, want the genuine independently-observed reset %v preserved", exhReset, hasExh, genuineReset)
	}
}

// TestClearAllExhausted_deterministicReport is a regression for a #254
// rescue-review finding: ClearAllExhausted used to report each pool's
// cleared nicks from ClearExhausted's own return value, which depends on
// controllersSnapshot()'s nondeterministic map iteration order — a pool
// visited after its propagator had already had the nick cleared out from
// under it by propagation, and reported nothing for a nick it demonstrably
// held. Repeated runs must report every pool's parked nick consistently
// regardless of visit order.
func TestClearAllExhausted_deterministicReport(t *testing.T) {
	for i := 0; i < 20; i++ {
		clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
		p := loadMovePools(t, clock, propagationEnv())
		reg := p.CurrentRegistry()
		bCCZinA, _ := reg.ResolveIn("a", "ccz")
		if err := p.ModifyResponse(respAuth(bCCZinA, http.StatusUnauthorized)); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}

		out := p.ClearAllExhausted()
		for _, pool := range []string{"a", "b", "c"} {
			nicks := out[pool]
			if len(nicks) != 1 || nicks[0] != "ccz" {
				t.Fatalf("iteration %d: ClearAllExhausted()[%s]=%v, want [ccz] regardless of controller visit order", i, pool, nicks)
			}
		}
	}
}

// TestNoteRecovered_propagatesWindowFactClearToSiblings is a regression for a
// #254 rescue-review finding surfaced on re-verification: noteRecovered only
// dropped its own controller's windowFact credentialPark entry, never
// propagating the clear. The preemptor's trigger (tick, preempt.go) accepts a
// frozen store snapshot as long as its precise reset has passed — looser than
// storeReconcilesParkLocked's freshness gate — so a sibling reading the same
// frozen snapshot cannot always reconcile the entry away on its own next
// read, and would keep reporting the nick Parked after a sibling's
// noteRecovered released it, violating AC4 (no pool may disagree on parked
// for a store-unrepresentable park).
func TestNoteRecovered_propagatesWindowFactClearToSiblings(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := loadMovePools(t, clock, propagationEnv())
	reg := p.CurrentRegistry()
	bCCZinA, _ := reg.ResolveIn("a", "ccz")

	// Headerless 429 residue on a, propagated into b and c.
	if err := p.ModifyResponse(resp429(bCCZinA, clock, 0)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	for _, pool := range []string{"a", "b", "c"} {
		status, _ := p.PoolStatus(pool, quota.NewStore(), nil)
		if !memberParked(status, "ccz") {
			t.Fatalf("pool %s: ccz Parked=false before noteRecovered, want true", pool)
		}
	}

	p.byPool["a"].noteRecovered("ccz")

	for _, pool := range []string{"a", "b", "c"} {
		status, _ := p.PoolStatus(pool, quota.NewStore(), nil)
		if memberParked(status, "ccz") {
			t.Errorf("pool %s: ccz still Parked=true after noteRecovered on a, want released everywhere", pool)
		}
	}
}
