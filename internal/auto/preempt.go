// This file adds reset-driven preempt-back (issue #31) on top of the
// priority routing introduced in #29. Phase 1 made a priority pool prefer
// its highest-priority member for the *initial pick* and the *failover
// target*, but once a pool fell over to a lower-priority member it rode it
// until that member itself 429'd. A member like z-ai resets its short
// window on a rolling schedule and grants a large budget, so to actually
// drain that budget the pool must return to it promptly each time its
// window resets — not wait for the active fallback to burn out.
//
// The Preemptor is a single background goroutine fronting every priority
// pool. It watches when a higher-priority member than the one currently
// active will recover and, on that reset, switches the pool back to it. It
// is generic and config-driven: only pools that opted into priority via
// AQG_POOL_<POOL>_PRIORITY are touched, so equal-strength pools never
// preempt and their prompt cache is never interrupted. No vendor or model
// name appears here.
package auto

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// defaultPreemptInterval is the preemptor's polling cadence. When no priority
// pool has a parked higher-priority member to wait for, the loop idles at this
// interval rather than spinning. A scheduled reset within the interval still
// takes precedence — the loop wakes at the exact reset — but a reset scheduled
// farther out is capped at this interval (issue #288): sleeping until an
// arbitrarily far reset would blind the loop to mid-sleep parks and recoveries
// on every other pool, e.g. one 7d-window reset on a single pool suppressing
// all preempt-back for the entire window.
const defaultPreemptInterval = 5 * time.Minute

// Preemptor returns a priority pool to a higher-priority member when that
// member's quota window resets. The zero value is not usable; build it
// with NewPreemptor. State (the per-member dedup record) lives only here
// and is touched only from Run's single goroutine, so it needs no mutex;
// every read or write of Controller state goes through the controller's
// own lock.
type Preemptor struct {
	// controllers resolves the pools to evaluate fresh on every tick, taken
	// under Pools' read lock, so a pool created at runtime (AddPool) — or an
	// existing pool later given a priority order — is picked up without a
	// restart (issue #202). tick() skips non-priority pools, so returning all
	// of them is correct and cheap; the empty case only arises with zero pools.
	controllers func() []*Controller
	store       *quota.Store
	interval    time.Duration
	now         func() time.Time
	logOut      io.Writer

	// lastActed records, per member quota key, the precise reset value the
	// preemptor last switched a pool back on. A member's store entry freezes
	// at its exhausted window's reset once the pool fails off it (the poller
	// only tracks the active member), so a member that resets but is then
	// immediately re-limited would otherwise re-trigger every tick on the
	// same stale frozen value. Skipping a reset already acted on bounds
	// preempt-back to one probe attempt per genuine reset; reactive 429
	// failover then keeps the pool on the fallback until the next real reset.
	lastActed map[string]time.Time
}

// NewPreemptor builds a Preemptor over the pools in p. It reads p's current
// controller set fresh on every tick (via p.sortedControllers, under Pools'
// lock), so runtime-created pools are picked up automatically; tick() itself
// skips any pool that has not declared a priority order, so equal-strength
// pools never preempt. store supplies the precise unified_5h_reset; interval
// defaults to 5 minutes, now to time.Now, and logOut to os.Stderr when their
// zero value is passed.
func NewPreemptor(p *Pools, store *quota.Store, interval time.Duration, now func() time.Time, logOut io.Writer) *Preemptor {
	return newPreemptorFunc(p.sortedControllers, store, interval, now, logOut)
}

// newPreemptor is a static-source constructor used by tests: it evaluates the
// given controller slice on every tick. Production uses NewPreemptor, whose
// source re-reads Pools each tick (issue #202).
func newPreemptor(controllers []*Controller, store *quota.Store, interval time.Duration, now func() time.Time, logOut io.Writer) *Preemptor {
	return newPreemptorFunc(func() []*Controller { return controllers }, store, interval, now, logOut)
}

// newPreemptorFunc is the shared constructor that applies the zero-value
// defaults over a dynamic controller source.
func newPreemptorFunc(controllers func() []*Controller, store *quota.Store, interval time.Duration, now func() time.Time, logOut io.Writer) *Preemptor {
	if interval <= 0 {
		interval = defaultPreemptInterval
	}
	if now == nil {
		now = time.Now
	}
	if logOut == nil {
		logOut = os.Stderr
	}
	return &Preemptor{
		controllers: controllers,
		store:       store,
		interval:    interval,
		now:         now,
		logOut:      logOut,
		lastActed:   make(map[string]time.Time),
	}
}

// Run drives the preempt-back loop until ctx is cancelled. Each pass
// performs any due switches and returns the duration until the next
// evaluation; Run then sleeps until then (or until ctx is done). An empty
// pool set is an ordinary idle tick, so a pool added later is picked up by
// the next pass. A deployment with only equal-strength pools also idles at
// the fallback interval doing nothing, since tick() skips every non-priority
// pool. Run blocks; callers start it in a goroutine.
func (p *Preemptor) Run(ctx context.Context) {
	for {
		wait := p.tick()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// tick performs one preempt-back evaluation across every priority pool and
// returns how long to sleep before the next one. For each pool it walks the
// members ranked strictly above the active one, highest priority first, and
// either switches now (the member has recovered) or records when it will,
// scheduling the loop to wake at the soonest such reset. The returned wait is
// capped at the interval: a reset within the interval is slept to exactly,
// while a farther one is re-evaluated at each interval mark — so a member
// that parks or recovers mid-sleep on any pool is seen within one interval,
// not at some far reset another pool scheduled (issue #288). With nothing to
// wait for it returns the idle interval.
func (p *Preemptor) tick() time.Duration {
	now := p.now()

	// earliest tracks the soonest future reset to wake on across all pools;
	// scheduled stays false when nothing is parked above an active member, in
	// which case the loop idles at the fallback interval.
	var earliest time.Time
	scheduled := false
	schedule := func(at time.Time) {
		if d := at.Sub(now); d <= 0 {
			return
		}
		if !scheduled || at.Before(earliest) {
			earliest, scheduled = at, true
		}
	}

	for _, c := range p.controllers() {
		v := c.preemptView()
		if !v.isPriority {
			continue
		}

		var target string
		for _, m := range v.higher { // highest priority first
			// The precise window reset from the quota store (populated for
			// Anthropic via headers, for z-ai/MiniMaxi via the poller) is
			// preferred over the controller's conservative park.
			qReset := p.store.Get(m.quotaKey).Unified5hReset
			// m.exhausted already incorporates store-driven exhaustion via
			// exhaustedUntilLocked → storeExhaustedUntilLocked → windowBlocks,
			// which is the only predicate that correctly handles Anthropic's
			// allowed_warning status (util=1.0, status≠"rejected" → not blocking,
			// issue #236). A raw util≥1.0 gate here would wrongly veto
			// allowed_warning members that windowBlocks correctly passes.
			isAvailable := !m.exhausted

			if isAvailable {
				// A higher-priority member the controller already considers
				// healthy is sitting unused — switch back to it now. Anchor the
				// dedup on its store reset (whether past or still future) so
				// that, should the member be re-limited the instant we switch
				// and its frozen entry later age past that reset, the stale
				// value cannot re-trigger a switch before the poller refreshes
				// it. A genuinely newer reset always differs and still fires.
				if qReset != nil {
					p.lastActed[m.quotaKey] = *qReset
				}
				target = m.nick
				break
			}

			// Member is exhausted (by park, store, or both). Check if it has recovered.
			if qReset != nil && !now.Before(*qReset) {
				// The precise window has reset. Act once per distinct reset so a
				// member that resets but is immediately re-limited does not flap
				// on its stale frozen value.
				if !p.lastActed[m.quotaKey].Equal(*qReset) {
					c.noteRecovered(m.nick)
					p.lastActed[m.quotaKey] = *qReset
					target = m.nick
					break
				}
				// Already handled this reset; ignore the stale value and wait on
				// the controller's fresh park instead.
			}

			// Still exhausted: wake at the soonest of the park reset or the
			// precise store reset (when known and still ahead).
			// When park reset is zero (member not parked), use the store reset.
			next := m.reset
			if next.IsZero() && qReset != nil && qReset.After(now) {
				next = *qReset
			} else if qReset != nil && qReset.After(now) && qReset.Before(next) {
				next = *qReset
			}
			if !next.IsZero() {
				schedule(next)
			}
		}

		if target != "" {
			if c.PreemptTo(target) {
				fmt.Fprintf(p.logOut, "preempt[%s]: %s -> %s (higher-priority member recovered)\n", c.name(), v.current, target)
			}
		}
	}

	if !scheduled {
		return p.interval
	}
	// Cap the wait at the interval (issue #288): an arbitrarily far earliest
	// reset must not blind the loop to mid-sleep parks and recoveries on other
	// pools, so a farther reset only re-schedules at interval marks. A reset
	// within the interval is still slept to exactly, so the precise switch
	// time is preserved.
	if d := earliest.Sub(now); d < p.interval {
		return d
	}
	return p.interval
}

// preemptView is a read-only snapshot of a priority controller's state the
// preemptor needs to schedule and decide a preempt-back. isPriority is
// false (and higher nil) for a non-priority pool, which the preemptor
// skips.
type preemptView struct {
	isPriority bool
	current    string
	// higher lists the members ranked strictly above the active one,
	// highest priority first, with each member's current park state.
	higher []memberState
}

// memberState describes one higher-priority member at snapshot time.
type memberState struct {
	nick      string
	quotaKey  string    // the quota.Store key, for the precise reset lookup
	exhausted bool      // whether the controller currently parks it
	reset     time.Time // park reset; valid only when exhausted
}

// preemptView snapshots the members ranked above the active one. It clears
// expired marks first so a member whose park already elapsed reads as
// healthy. Returns the zero view for a non-priority pool.
func (c *Controller) preemptView() preemptView {
	c.mu.Lock()
	defer c.mu.Unlock()

	pri := c.effectivePriorityLocked()
	if len(pri) == 0 {
		return preemptView{}
	}
	c.clearExpiredLocked()

	cur := c.curNick
	curRank := c.rankLocked(cur)
	v := preemptView{isPriority: true, current: cur}
	for _, nick := range pri { // highest priority first
		if c.rankLocked(nick) >= curRank {
			continue // only members strictly above the active one
		}
		idx := c.indexOf(nick)
		if idx < 0 {
			continue
		}
		// Skip only operator-disabled members: a disabled higher-priority
		// member must not appear in the view, otherwise tick() reads it as
		// healthy (!exhausted), targets it, and breaks before reaching the
		// next available preferred member — while PreemptTo then refuses it.
		// Exhausted members MUST remain in the view: tick() schedules the
		// loop's wake on their reset, which is the whole point of preempt-back.
		if c.disabled[nick] {
			continue
		}
		ms := memberState{nick: nick, quotaKey: c.backendAt(idx).QuotaKey()}
		if r, ok := c.exhaustedUntilLocked(nick); ok {
			ms.exhausted = true
			ms.reset = r
		}
		v.higher = append(v.higher, ms)
	}
	return v
}

// PreemptTo switches the pool's sticky pointer back to nick. It is the
// preempt-back counterpart to the reactive failover in record429: where
// failover steps *down* to a healthy fallback, PreemptTo steps *up* to a
// recovered preferred member. It refuses (returns false, leaving the
// pointer put) for a pool with no declared priority, an unknown nick, a
// nick that is not strictly higher priority than the current member, or a
// nick that is still unavailable (exhausted or disabled) — so a preempt never
// lands on a member known to be rate-limited or operator-disabled, and never
// moves the pool away from its preference. Atomic under c.mu.
func (c *Controller) PreemptTo(nick string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.effectivePriorityLocked()) == 0 {
		return false
	}
	c.clearExpiredLocked()

	if c.indexOf(nick) < 0 {
		return false
	}
	if c.rankLocked(nick) >= c.rankLocked(c.curNick) {
		return false
	}
	if c.isUnavailableLocked(nick) {
		return false
	}
	c.setActiveMemberLocked(nick)
	return true
}

// noteRecovered clears nick's park mark. The preemptor calls it when the
// precise quota reset for nick has arrived but the controller's own
// conservative park (e.g. the default 5h window applied to a 429 that
// carried no reset header) has not yet elapsed: the precise signal
// supersedes the default. Clearing the mark lets the following PreemptTo —
// and any request-path resolve — treat the member as selectable again.
//
// Also drops nick's credentialPark entry (issue #254), but only when
// windowFact is true — the header-less-429 residue this comment describes
// is exactly that subclass, a real quota-window fact the precise store
// reset is entitled to supersede. A 401/403 entry (windowFact false) is left
// untouched: a precise quota-window reset says nothing about whether a
// since-revoked credential authenticates again, so it must never clear that
// subclass.
//
// Propagates the clear to sibling pools when it drops a windowFact entry,
// same as an explicit operator clear (AC4/AC5): the preemptor's trigger
// (tick, above) accepts a frozen store snapshot as long as its reset has
// passed, which is looser than storeReconcilesParkLocked's freshness gate —
// so a sibling reading the same frozen snapshot cannot always reconcile the
// entry away on its own next read, and would otherwise disagree on `parked`
// with the pool that just cleared it. Safe to call with c.mu released: tick's
// caller (p.controllers(), i.e. Pools.sortedControllers) already returns its
// slice after releasing p.mu, so no lock is held here to invert against.
func (c *Controller) noteRecovered(nick string) {
	c.mu.Lock()
	delete(c.exhausted, nick)
	var clearedWindowFact bool
	if entry, ok := c.credentialPark[nick]; ok && entry.windowFact {
		delete(c.credentialPark, nick)
		clearedWindowFact = true
	}
	c.notifyMutate()
	c.mu.Unlock()

	if clearedWindowFact && c.propagateParkClear != nil {
		c.propagateParkClear(nick)
	}
}

// rankLocked returns nick's position in the pool's priority order (lower is
// higher priority). effectiveOrder places every member in the effective
// priority, so a real member always has a rank; an unknown nick sorts last.
// Caller holds c.mu.
func (c *Controller) rankLocked(nick string) int {
	for i, n := range c.effectivePriorityLocked() {
		if n == nick {
			return i
		}
	}
	return len(c.effectivePriorityLocked())
}
