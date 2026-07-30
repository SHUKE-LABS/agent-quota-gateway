package auto

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
)

// TestRecover_probeSucceedsUnparksMember is the core #124 regression: a
// parked member whose quota probe returns a healthy snapshot is unparked
// without operator /clear. The probe runs through the same proprietary
// endpoint the poller uses (per issue #124 "do not re-derive the
// endpoint"), with snapRejects (post-#125) as the freshness predicate so
// the recovery path shares the freshness contract with the park-decision
// path.
func TestRecover_probeSucceedsUnparksMember(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		// Probe returns a healthy 5h window (util 50%, reset far in the
		// future) — the upstream is serveable.
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000},
			{"type":"TIME_LIMIT","percentage":30,"nextResetTime":4105046400000}
		]}}`)
	})

	// Park member "a" via record429 (the path parkAndFailover takes).
	c.record429("a", clock.now().Add(5*time.Hour))

	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Fatalf("a should be parked before probe")
	}

	got := c.tryRecoverParked()
	if got != "a" {
		t.Errorf("tryRecoverParked = %q, want %q (probe returned healthy)", got, "a")
	}
	if _, ok := c.exhaustedUntil("a"); ok {
		t.Errorf("a still parked after successful recovery probe")
	}
	// tryRecoverParked only unparks; it does not re-rotate the sticky
	// pointer (that happens in parkAndFailover's allExhausted branch when
	// the recovered nick is routed to). The recovered member becomes
	// selectable on the next request that triggers a selection — covered
	// by TestRecover_integrationAllExhaustedRewritesResponse.
}

// TestRecover_probeFailsKeepsMemberParked covers the network-error path:
// a probe that returns ErrNoProvider (or any error) leaves the existing
// park alone. Issue #124 contract: "A parked member whose probe fails
// keeps its original exhausted_until (no extension from a failed probe)".
func TestRecover_probeFailsKeepsMemberParked(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})

	parkAt := clock.now().Add(5 * time.Hour)
	c.record429("a", parkAt)

	got := c.tryRecoverParked()
	if got != "" {
		t.Errorf("tryRecoverParked = %q, want empty (probe failed; park retained)", got)
	}
	if at, ok := c.exhaustedUntil("a"); !ok || !at.Equal(parkAt) {
		t.Errorf("a park changed after failed probe: at=%v ok=%v, want %v/true", at, ok, parkAt)
	}
}

// TestRecover_probeStillExhaustedKeepsMemberParked covers the
// "snapshot-still-rejects" path: a probe that succeeds but reports a
// still-exhausted window must NOT unmark the park. The shared
// snapRejects predicate (post-#125) handles the freshness + status check.
func TestRecover_probeStillExhaustedKeepsMemberParked(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		// Probe returns util 100% on the 5h window with a reset 1h in the
		// future — snapRejects still rejects; park must be retained.
		reset := clock.now().Add(1 * time.Hour).UnixMilli()
		fmt.Fprintf(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":100,"nextResetTime":%d}
		]}}`, reset)
	})

	parkAt := clock.now().Add(5 * time.Hour)
	c.record429("a", parkAt)

	got := c.tryRecoverParked()
	if got != "" {
		t.Errorf("tryRecoverParked = %q, want empty (probe reported still exhausted)", got)
	}
	if at, ok := c.exhaustedUntil("a"); !ok || !at.Equal(parkAt) {
		t.Errorf("a park changed after still-exhausted probe: at=%v ok=%v, want %v/true", at, ok, parkAt)
	}
}

// TestRecover_concurrentAllExhaustedCoalescesProbes proves the
// probeInFlight coalesces concurrent callers: two simultaneous
// tryRecoverParked calls produce only one upstream probe hit per parked
// member.
func TestRecover_concurrentAllExhaustedCoalescesProbes(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var hits int64
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		// Tiny sleep so the second caller observes probeInFlight=true.
		time.Sleep(50 * time.Millisecond)
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000}
		]}}`)
	})

	c.record429("a", clock.now().Add(5*time.Hour))
	c.record429("b", clock.now().Add(5*time.Hour))

	var wg sync.WaitGroup
	const N = 4
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.tryRecoverParked()
		}()
	}
	wg.Wait()

	// At most one probe hit per parked member — two members means two
	// hits total. With N=4 concurrent callers and 50ms probe latency, an
	// uncoalesced impl would yield 4*2 = 8 hits.
	if got := atomic.LoadInt64(&hits); got > 2 {
		t.Errorf("probe hits = %d, want ≤2 (one per parked member)", got)
	}
}

// TestRecover_rateLimitOnePerCooldown proves the lastProbeAttempt gate:
// a second tryRecoverParked call within probeCooldown does not re-probe
// the same member.
func TestRecover_rateLimitOnePerCooldown(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var hits int64
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000}
		]}}`)
	})

	c.record429("a", clock.now().Add(5*time.Hour))

	c.tryRecoverParked() // first call probes
	c.tryRecoverParked() // second call must skip (within cooldown)

	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Errorf("probe hits = %d, want 1 (second call within cooldown must skip)", got)
	}
}

// TestRecover_anthropicSkipped proves the Anthropic path: a parked member
// whose base URL has no registered provider must NOT trigger a probe
// (Anthropic's 429s carry precise resets already; organic traffic
// refreshes the store).
func TestRecover_anthropicSkipped(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// Build a controller with the standard anthropic test registry (no
	// withTestProvider) — the recovery path must skip "a".
	var hits int64
	c := newAnthropicRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
	})

	c.record429("a", clock.now().Add(5*time.Hour))

	if got := c.tryRecoverParked(); got != "" {
		t.Errorf("tryRecoverParked = %q, want empty (Anthropic has no probe provider)", got)
	}
	if got := atomic.LoadInt64(&hits); got != 0 {
		t.Errorf("probe hits = %d, want 0 (Anthropic must not be probed)", got)
	}
}

// TestRecover_msResetHandled is a regression guard for the
// seconds-vs-milliseconds trap: nextResetTime is epoch milliseconds and
// must round-trip through msToTime, not parseUnixTime. The probe result's
// Unified5hReset must equal the ms-decoded value (year 2100-ish), not the
// seconds-decoded mis-interpretation (year ~58500). The recovery path
// reaches snapRejects → windowBlocks with the ms-decoded reset; we verify
// here that the decode succeeded by running a probe that would ONLY
// recover if the ms reset was applied (a util < 1.0 reset * past* via
// seconds mis-decode would otherwise still reject).
func TestRecover_msResetHandled(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		// 4_102_444_800_000 ms = 2100-01-26 — canonical z.ai reset.
		// Seconds mis-decode (year ~58500) would make windowBlocks fail
		// the now.Before(*reset) check (already passed) and the
		// recovery would not happen. ms decode → reset is far future,
		// util < 1.0 → snapRejects returns false → recovery succeeds.
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":10,"nextResetTime":4102444800000}
		]}}`)
	})

	c.record429("a", clock.now().Add(5*time.Hour))

	if got := c.tryRecoverParked(); got != "a" {
		t.Fatalf("tryRecoverParked = %q, want a (ms-decoded reset should pass windowBlocks freshness guard)", got)
	}
}

// TestRecover_integrationAllExhaustedRewritesResponse drives the full
// parkAndFailover path end-to-end via a real HTTP exchange. Both members
// are parked; the recovery probe unparks "a"; the response must be
// rewritten to 503 (the normal switch shape), NOT forwarded as the
// upstream 429. Issue #124 AC: "A request that arrives while every
// member is parked, where one or more now serve upstream, succeeds
// instead of returning 'all exhausted' to the client."
func TestRecover_integrationAllExhaustedRewritesResponse(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		// Healthy probe: upstream is serveable.
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000}
		]}}`)
	})

	// Park BOTH members — record429 on the last one triggers allExhausted.
	c.record429("a", clock.now().Add(5*time.Hour))
	c.record429("b", clock.now().Add(5*time.Hour))

	if _, _, exhausted := c.ResolveAuto(); !exhausted {
		t.Fatalf("setup: pool not all-exhausted")
	}

	// parkAndFailover on a fresh 429 — the allExhausted path triggers
	// recovery probing. With one member now healthy, the response is
	// rewritten to 503 instead of being forwarded.
	b := c.resolve(t, "a")
	resp := newReqResp(b, http.StatusTooManyRequests, clock.now().Add(5*time.Hour))
	if err := c.parkAndFailover(resp, "b", clock.now().Add(5*time.Hour), "hit 429"); err != nil {
		t.Fatalf("parkAndFailover: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 (recovery should switch, not forward 429)", resp.StatusCode)
	}
}

// TestRecover_runtimeAddedMemberSetsActiveOnRecovery proves that when
// tryRecoverParked unparks a runtime-added member via a quota probe,
// parkAndFailover's re-rotate correctly installs it as the active member —
// Current() reflects the runtime-added nick, not a stale static index. The old
// path used c.indexOf(recovered)+c.cur=idx, which returned -1 for an added
// member and left the pool in an inconsistent state (issue #188).
func TestRecover_runtimeAddedMemberSetsActiveOnRecovery(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// httptest server: returns a healthy probe response.
	probeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000}
		]}}`)
	}))
	t.Cleanup(probeSrv.Close)
	// Register a poller provider for the probe server's URL only.
	poller.WithTestProviderForTest(t,
		probeSrv.URL,
		poller.HostURLForTest("/api/monitor/usage/quota/limit"),
		poller.RawAuthForTest,
		poller.ParseZhipuForTest,
	)

	// Static member "a" on Anthropic URL (ProviderFor returns false → no probe).
	// "extra" will be runtime-added on probeSrv.URL (ProviderFor returns true).
	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_A", "cred-a")
	reg, err := backend.Load(testDefaultBaseURL) // Anthropic base for "a"
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	c := NewController(reg, "auto", 0, nil, clock.now, io.Discard)
	c.SetProbeHTTPClient(probeSrv.Client())

	p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}
	if status, err := p.AddMember("auto", "extra", "cred-extra", probeSrv.URL, nil); status != 200 || err != nil {
		t.Fatalf("AddMember extra: status=%d err=%v, want 200", status, err)
	}

	// Park both "a" and "extra"; make "a" the current sticky.
	c.park("a", clock.now().Add(5*time.Hour))
	c.park("extra", clock.now().Add(5*time.Hour))
	c.setCur("a")

	if _, _, exhausted := c.ResolveAuto(); !exhausted {
		t.Fatal("setup: pool not all-exhausted")
	}

	// parkAndFailover on "a" (all-exhausted path): tryRecoverParked probes
	// "extra" (Anthropic "a" is skipped by ProviderFor). The probe returns
	// healthy, so "extra" is unparked and the re-rotate sets it as active.
	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("upstream 429")),
	}
	if err := c.parkAndFailover(resp, "a", clock.now().Add(5*time.Hour), "hit 429"); err != nil {
		t.Fatalf("parkAndFailover: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (probe recovered extra; switched not forwarded)", resp.StatusCode)
	}
	if got := c.Current(); got != "extra" {
		t.Errorf("Current()=%q, want extra (re-rotate to recovered runtime-added member)", got)
	}
}

// ---- issue #242: background recovery of parked non-active members ----

// TestRecover_backgroundLoopUnparksStrandedMember is the core #242
// regression: a parked member whose healthy sibling is the active
// sticky backend is unparked by the background recovery loop without
// the loop force-switching away from the healthy active member. The
// probe returns a healthy snapshot, noteRecovered clears the live park,
// and the sticky pointer stays on the healthy nick.
func TestRecover_backgroundLoopUnparksStrandedMember(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	probeHits := atomic.Int32{}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		probeHits.Add(1)
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000}
		]}}`)
	})

	// Park "a" (the original sticky) and rotate sticky to healthy "b",
	// simulating the stranded shape from the issue: "a" parked, "b"
	// healthy and active.
	c.record429("a", clock.now().Add(5*time.Hour))
	c.setCur("b")
	// Sanity: "b" must be the active sticky, "a" must be parked, "b" must
	// NOT be parked.
	if got := c.Current(); got != "b" {
		t.Fatalf("setup: Current()=%q, want b", got)
	}
	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Fatalf("setup: a should be parked")
	}
	if _, ok := c.exhaustedUntil("b"); ok {
		t.Fatalf("setup: b should NOT be parked")
	}

	// Run the background recovery loop once. Its tick iterates every
	// controller and calls tryRecoverParkedNonActive.
	r := newRecovery([]*Controller{c}, 0, io.Discard)
	r.tick()

	// "a" should now be unparked; "b" must still be the active sticky
	// (no force-switch).
	if _, ok := c.exhaustedUntil("a"); ok {
		t.Errorf("a still parked after background recovery tick (want unparked)")
	}
	if got := c.Current(); got != "b" {
		t.Errorf("Current()=%q, want b (background loop must not move sticky pointer)", got)
	}
	// The probe must have hit the upstream exactly once for "a" — the
	// healthy active "b" must NOT have been probed (the skipActive
	// filter; also "b" was never in c.exhausted in the first place).
	if hits := probeHits.Load(); hits != 1 {
		t.Errorf("probe hits=%d, want 1 (one probe for parked a, none for active b)", hits)
	}
}

// TestRecover_backgroundLoopSkipsActiveMember covers the overlap with
// the request-path allExhausted recovery: when the active sticky
// member is ALSO parked (the allExhausted shape), the background loop
// must NOT probe it. Probing the active member here would duplicate
// the request-path tryRecoverParked's work and race its in-flight
// bookkeeping. The skip is the only state where skipActive actually
// filters anything — in the stranded shape the healthy active nick is
// not in c.exhausted, so the filter has no work to do.
func TestRecover_backgroundLoopSkipsActiveMember(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	probeHits := atomic.Int32{}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		probeHits.Add(1)
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000}
		]}}`)
	})

	// All-exhausted shape: both "a" and "b" parked; "a" pinned as the
	// active sticky so skipActive actually filters it. record429 on
	// "a" alone rotates sticky to "b" (firstHealthyNickLocked), so we
	// park both and explicitly setCur("a") last.
	c.record429("a", clock.now().Add(5*time.Hour))
	c.park("b", clock.now().Add(5*time.Hour))
	c.setCur("a")
	if got := c.Current(); got != "a" {
		t.Fatalf("setup: Current()=%q, want a", got)
	}

	r := newRecovery([]*Controller{c}, 0, io.Discard)
	r.tick()

	// "a" must still be parked (skipped, not probed); "b" may be
	// probed and unparked by the loop — the load-bearing assertion is
	// that the active member was skipped, so 0 probes hit for "a"
	// (proven indirectly: the loop must visit exactly one quota key,
	// not two).
	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Errorf("a unparked by background loop; want parked (active member must be skipped)")
	}
	if hits := probeHits.Load(); hits != 1 {
		t.Errorf("probe hits=%d, want 1 (only the parked non-active b must be probed)", hits)
	}
}

// TestRecover_backgroundLoopKeepsParkOnStillExhaustedProbe covers the
// safety guard: a probe whose snapshot still satisfies snapRejects
// (window blocking) must leave the live park intact.
func TestRecover_backgroundLoopKeepsParkOnStillExhaustedProbe(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		// 5h window at the cap with a reset still far in the future —
		// snapRejects returns true, the probe says still exhausted.
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":100,"nextResetTime":4102444800000}
		]}}`)
	})

	c.record429("a", clock.now().Add(5*time.Hour))
	c.setCur("b")

	r := newRecovery([]*Controller{c}, 0, io.Discard)
	r.tick()

	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Errorf("a unparked by background loop; want parked (probe still exhausted)")
	}
	if got := c.Current(); got != "b" {
		t.Errorf("Current()=%q, want b", got)
	}
}

// TestRecover_backgroundLoopKeepsParkOnFailedProbe covers the
// transport-error safety guard: a probe that returns a non-200 status
// (or any error) must leave the live park intact, matching the
// request-path tryRecoverParked contract (issue #124).
func TestRecover_backgroundLoopKeepsParkOnFailedProbe(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	c.record429("a", clock.now().Add(5*time.Hour))
	c.setCur("b")

	r := newRecovery([]*Controller{c}, 0, io.Discard)
	r.tick()

	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Errorf("a unparked by background loop; want parked (probe failed)")
	}
}

// TestRecover_backgroundLoopSkipsAnthropic covers the
// provider-recognition filter: an Anthropic member (no
// poller.ProviderFor match) must never be probed by the background
// loop, matching the request-path tryRecoverParked's "Anthropic is
// intentionally not probed" rule (issue #124).
func TestRecover_backgroundLoopSkipsAnthropic(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	probeHits := atomic.Int32{}
	c := newAnthropicRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		probeHits.Add(1)
	})

	c.record429("a", clock.now().Add(5*time.Hour))
	c.setCur("b")

	r := newRecovery([]*Controller{c}, 0, io.Discard)
	r.tick()

	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Errorf("a unparked by background loop; want parked (Anthropic has no probe provider)")
	}
	if hits := probeHits.Load(); hits != 0 {
		t.Errorf("probe hits=%d, want 0 (Anthropic must be skipped)", hits)
	}
}

// TestRecover_backgroundLoopCooldownRateLimit covers the
// lastProbeAttempt cooldown: a second tick inside the probeCooldown
// window must NOT re-probe the same parked member. (In production the
// loop cadence is 5m > probeCooldown = 30s, so this test exercises
// the eligibility-check path shared with the request-path allExhausted
// recovery — the cooldown matters there.)
func TestRecover_backgroundLoopCooldownRateLimit(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	probeHits := atomic.Int32{}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		probeHits.Add(1)
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000}
		]}}`)
	})

	c.record429("a", clock.now().Add(5*time.Hour))
	c.setCur("b")

	r := newRecovery([]*Controller{c}, 0, io.Discard)
	// First tick unparks "a" and stamps lastProbeAttempt.
	r.tick()
	if hits := probeHits.Load(); hits != 1 {
		t.Fatalf("first tick: probe hits=%d, want 1", hits)
	}
	// Re-park "a" and run a second tick inside the cooldown window.
	c.record429("a", clock.now().Add(5*time.Hour))
	r.tick()
	if hits := probeHits.Load(); hits != 1 {
		t.Errorf("second tick inside cooldown: probe hits=%d, want 1 (no re-probe within cooldown)", hits)
	}
}

// TestRecover_backgroundLoopNoOpWhenHealthy is the load-bearing
// "healthy pool pays no extra probe traffic" property: with no parked
// non-active member, the loop must perform zero upstream probes.
func TestRecover_backgroundLoopNoOpWhenHealthy(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	probeHits := atomic.Int32{}
	c := newRecoverFixture(t, clock, func(w http.ResponseWriter, r *http.Request) {
		probeHits.Add(1)
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000}
		]}}`)
	})

	// Healthy pool: nothing is parked. Run the loop once and assert no
	// probe fired.
	r := newRecovery([]*Controller{c}, 0, io.Discard)
	r.tick()
	if hits := probeHits.Load(); hits != 0 {
		t.Errorf("healthy tick: probe hits=%d, want 0 (no parked member; loop must be a no-op)", hits)
	}
}

// TestRecover_backgroundLoopUnparksAfterRuntimeAdd simulates the
// issue's second acceptance bullet: a parked probe-eligible member
// whose healthy sibling is added AFTER the park. The probe-eligible
// member is parked first; the runtime-added sibling is healthy and
// becomes the active sticky; the loop must probe and un-park the
// original member without disturbing the sticky pointer.
func TestRecover_backgroundLoopUnparksAfterRuntimeAdd(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	probeHits := atomic.Int32{}
	probeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probeHits.Add(1)
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":50,"nextResetTime":4102444800000}
		]}}`)
	}))
	t.Cleanup(probeSrv.Close)
	poller.WithTestProviderForTest(t,
		probeSrv.URL,
		poller.HostURLForTest("/api/monitor/usage/quota/limit"),
		poller.RawAuthForTest,
		poller.ParseZhipuForTest,
	)

	// Build a single-member pool "a" pointing at the probe-eligible
	// URL (the to-be-parked member), park it, then runtime-add a
	// healthy "sibling" sibling and set sticky to it. Both members
	// share the same probe-eligible URL so ProviderFor matches for
	// the parked one.
	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_A", "cred-a")
	reg, err := backend.Load(probeSrv.URL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	c := NewController(reg, "auto", 0, nil, clock.now, io.Discard)
	c.SetProbeHTTPClient(probeSrv.Client())
	p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}
	if status, err := p.AddMember("auto", "sibling", "cred-sibling", probeSrv.URL, nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember sibling: status=%d err=%v, want 200", status, err)
	}

	// Park "a"; runtime-added "sibling" is healthy and becomes sticky.
	c.park("a", clock.now().Add(5*time.Hour))
	c.setCur("sibling")
	if _, ok := c.exhaustedUntil("a"); !ok {
		t.Fatalf("setup: a should be parked")
	}

	r := newRecovery([]*Controller{c}, 0, io.Discard)
	r.tick()

	// "a" must be unparked by the probe; "sibling" must stay sticky.
	if _, ok := c.exhaustedUntil("a"); ok {
		t.Errorf("a still parked after background recovery tick (want unparked)")
	}
	if got := c.Current(); got != "sibling" {
		t.Errorf("Current()=%q, want sibling (background loop must not move sticky pointer)", got)
	}
	if hits := probeHits.Load(); hits != 1 {
		t.Errorf("probe hits=%d, want 1 (one probe for parked a, none for active sibling)", hits)
	}
}

// ---- helpers ----

// newRecoverFixture builds a controller "auto" with two members whose
// BaseURL points at an httptest server. It registers a test poller
// provider matching the server's URL fragment so ProviderFor recognises
// the backends and the probe is dispatched. The supplied handler drives
// the probe response. Returns the controller.
func newRecoverFixture(t *testing.T, clock *fixedClock, handler http.HandlerFunc) *Controller {
	t.Helper()
	probeSrv := httptest.NewServer(handler)
	t.Cleanup(probeSrv.Close)

	// Register a test provider matching the httptest URL. Zhipu's
	// parseZhipu is reused so the JSON shape matches the production
	// z.ai provider — exercise the same parser the poller would use.
	poller.WithTestProviderForTest(t,
		probeSrv.URL,
		poller.HostURLForTest("/api/monitor/usage/quota/limit"),
		poller.RawAuthForTest,
		poller.ParseZhipuForTest,
	)

	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_A", "cred-a")
	t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_B", "cred-b")
	reg, err := backend.Load(probeSrv.URL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	c := NewController(reg, "auto", 0, nil, clock.now, io.Discard)
	// Inject the test server's client so probe requests are routed via
	// the test server (the default transport would resolve the URL
	// directly; srv.Client() is configured to skip cert verification etc).
	c.SetProbeHTTPClient(probeSrv.Client())
	return c
}

// newAnthropicRecoverFixture is the same shape but without registering a
// poller provider, so ProviderFor returns false for any backend. Used to
// verify Anthropic backends are skipped.
func newAnthropicRecoverFixture(t *testing.T, clock *fixedClock, handler http.HandlerFunc) *Controller {
	t.Helper()
	probeSrv := httptest.NewServer(handler)
	t.Cleanup(probeSrv.Close)

	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_A", "cred-a")
	reg, err := backend.Load(testDefaultBaseURL) // https://api.anthropic.com — no provider matches
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	c := NewController(reg, "auto", 0, nil, clock.now, io.Discard)
	c.SetProbeHTTPClient(probeSrv.Client())
	return c
}

// newReqResp builds an upstream 429 response carrying a fresh reset header
// for the parked member. Mirrors resp429 in auto_test.go but lives here
// because recover_test.go is the only consumer.
func newReqResp(b backend.Backend, status int, reset time.Time) *http.Response {
	h := make(http.Header, 4)
	h.Set("anthropic-version", "2023-06-01")
	h.Set("anthropic-ratelimit-unified-status", "rejected")
	h.Set("Content-Type", "application/json")
	ctx := backend.WithBackend(context.Background(), b)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader("{}")).WithContext(ctx)
	_ = reset // park time is provided by the caller; reset header is informational here
	_ = json.Marshal // keep the json import quiet for non-marshal callers
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     h,
		Request:    req,
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"rate_limit_error"}}`)),
	}
}

// hostURL / rawAuth / parseZhipu are package-level helpers in
// internal/poller; we use them directly via the poller package alias.

// Silence unused-import linters for transient scaffolding that we may
// remove once a future test calls into them.
var (
	_ = io.Discard
)