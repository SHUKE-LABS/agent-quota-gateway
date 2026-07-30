// This file adds the background recovery loop for parked non-active members
// (issue #242). The allExhausted recovery probe in tryRecoverParked only runs
// when every member of a pool is parked on the request path, and the priority
// preemptor only visits higher-priority members when a priority pool is
// already serving a fallback — so a plain pool whose parked nick is no longer
// the active sticky backend (because a healthy sibling — possibly added after
// the park — has taken over) had no remaining self-heal path. This loop fills
// that gap by re-checking parked non-active members on a bounded cadence.
//
// The loop is deliberately narrow:
//
//   - It runs out-of-band from the proxy path, so a healthy pool pays no
//     synchronous latency. The controller's own bookkeeping (lastProbeAttempt
//     + probeInFlight, issue #124) coalesces any overlap with a concurrent
//     request-path probe of the same quota key, so the two paths cannot
//     storm the same upstream.
//   - It only clears the live park via noteRecovered; it never moves the
//     sticky pointer (the healthy active member is left alone, matching
//     issue #242's "without force-switching away from the current healthy
//     active nick" requirement). The recovered member rejoins rotation on
//     the next selection event, same shape as the existing allExhausted path.
//   - It does not Merge the recovered snapshot into the store (matching the
//     existing tryRecoverParked contract); a recovered member's pool-view
//     snapshot cell may briefly show the previously-frozen data until
//     organic traffic refreshes the store. The recovery decision and the
//     displayed snapshot are independent signals.
//   - It uses the same probe eligibility contract (probe-eligible provider,
//     not disabled, past probeCooldown, no in-flight probe) as the
//     request-path recovery, so the two paths can never disagree on what
//     counts as "no trustworthy recovery signal".
package auto

import (
	"context"
	"io"
	"os"
	"time"
)

// defaultRecoveryInterval is the idle fallback cadence for the recovery
// loop: when no controller has a parked non-active member to reconsider,
// the loop re-checks at this interval. 5m matches the preemptor's
// defaultPreemptInterval so the two background loops idle at the same
// cadence. A concrete probe cooldown (probeCooldown = 30s) is a tighter
// inner bound — it is only meaningful when a request-path probe runs
// concurrently; the background loop's own 5m cadence always exceeds it.
const defaultRecoveryInterval = 5 * time.Minute

// Recovery runs the bounded-cadence background recovery loop for parked
// non-active members (issue #242). It is a single background goroutine
// fronting every pool. The zero value is not usable; build it with
// NewRecovery. State lives entirely in the controller (lastProbeAttempt
// + probeInFlight), so the Recovery itself needs no mutex.
type Recovery struct {
	// controllers resolves the pools to evaluate fresh on every tick, taken
	// under Pools' read lock, so a pool created at runtime (AddPool) is
	// picked up without a restart (issue #202). tick() itself is a no-op
	// when no parked non-active member exists, so returning every
	// controller is correct and cheap; the empty case only arises with
	// zero pools.
	controllers func() []*Controller
	interval    time.Duration
	logOut      io.Writer
}

// NewRecovery builds a Recovery over the pools in p. It reads p's current
// controller set fresh on every tick (via p.sortedControllers, under
// Pools' lock), so runtime-created pools are picked up automatically.
// interval defaults to 5 minutes, and logOut to os.Stderr when their
// zero value is passed. The loop does not consult a clock for scheduling
// (it always sleeps for `interval` between ticks, since the recovery
// decision is a per-tick re-evaluation, not a wake-on-reset like the
// Preemptor), so no `now` parameter is exposed — the controller's
// own clock drives the per-member cooldown checks via the existing
// recoverParked path.
func NewRecovery(p *Pools, interval time.Duration, logOut io.Writer) *Recovery {
	return newRecoveryFunc(p.sortedControllers, interval, logOut)
}

// newRecovery is a static-source constructor used by tests: it evaluates
// the given controller slice on every tick. Production uses NewRecovery,
// whose source re-reads Pools each tick (issue #202).
func newRecovery(controllers []*Controller, interval time.Duration, logOut io.Writer) *Recovery {
	return newRecoveryFunc(func() []*Controller { return controllers }, interval, logOut)
}

// newRecoveryFunc is the shared constructor that applies the zero-value
// defaults over a dynamic controller source.
func newRecoveryFunc(controllers func() []*Controller, interval time.Duration, logOut io.Writer) *Recovery {
	if interval <= 0 {
		interval = defaultRecoveryInterval
	}
	if logOut == nil {
		logOut = os.Stderr
	}
	return &Recovery{
		controllers: controllers,
		interval:    interval,
		logOut:      logOut,
	}
}

// Run drives the background recovery loop until ctx is cancelled. Each
// pass tries to recover any parked non-active member and returns the
// idle interval when nothing was found; Run then sleeps until then (or
// until ctx is done). It returns immediately only when there are no
// pools at all; a deployment with only healthy pools still idles at
// the fallback interval doing nothing, since tick() is a no-op when no
// parked non-active member exists. Run blocks; callers start it in a
// goroutine.
func (r *Recovery) Run(ctx context.Context) {
	if len(r.controllers()) == 0 {
		return
	}
	for {
		wait := r.tick()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// tick performs one background recovery pass across every controller.
// For each controller it delegates to tryRecoverParkedNonActive, which
// applies the same eligibility contract as the request-path
// tryRecoverParked (probe-eligible provider, not disabled, past
// probeCooldown, no in-flight probe) plus the new "skip the active
// sticky member" filter. No controller is force-rotated by this loop:
// tryRecoverParkedNonActive / noteRecovered only clear the live park
// and leave the sticky pointer alone, matching the issue's "without
// force-switching away from the current healthy active nick"
// requirement. Returns the idle interval when nothing was recovered —
// there is no concrete "earliest reset" to wake on, since the loop
// never holds a healthy member hostage.
func (r *Recovery) tick() time.Duration {
	for _, c := range r.controllers() {
		// tryRecoverParkedNonActive is a no-op when c.exhausted is empty
		// or has no probe-eligible member; the per-pool cost of an idle
		// tick is one mutex acquire + map iteration, so a fleet of
		// healthy pools pays no extra upstream traffic.
		c.tryRecoverParkedNonActive()
	}
	return r.interval
}

// Note: tryRecoverParkedNonActive can return a non-empty recovered nick
// when it succeeds (the same shape as tryRecoverParked). The current
// loop discards the value because the issue's contract is to clear the
// park without moving the sticky pointer; the existing tryRecoverParked
// log line in auto.go already records the un-park on the controller's
// own logOut, so the loop does not need to repeat it.