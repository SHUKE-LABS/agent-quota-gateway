package auto

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// ReprobeQuery runs one upstream quota query for b and returns the fresh
// snapshot to merge into the store (issue #350). The class dispatch —
// poller endpoint, Codex hello request, or a minimal Anthropic Messages call —
// lives with the caller, so this package stays free of proxy/codexhello
// imports. A non-nil error means the query proved nothing: the store and every
// park are left untouched.
type ReprobeQuery func(ctx context.Context, b backend.Backend) (quota.Snapshot, error)

// ReprobeResult is the outcome of an operator reprobe for one pool member.
// Snapshot is the store's merged view after the fresh query, which is what
// routing reads next; Healthy reports that it no longer rejects.
type ReprobeResult struct {
	Pool     string
	Nick     string
	Healthy  bool
	Snapshot quota.Snapshot
}

// ErrReprobeQuery wraps a failed upstream query so a caller can tell it from
// the request-validation errors Reprobe also returns.
var ErrReprobeQuery = errors.New("reprobe query failed")

// reprobeCall is one in-flight upstream query shared by every concurrent
// reprobe of the same QuotaKey. snap and err are written by the leader before
// done closes and only read after.
type reprobeCall struct {
	done chan struct{}
	snap quota.Snapshot
	err  error
}

// Reprobe is the operator's "Reset & re-probe" (issue #350): it queries
// upstream once for one member, merges the fresh snapshot into the shared
// store, and reconciles the member's live park in this pool against it.
//
// It exists because a status-bearing rejected store window strands a member
// whose upstream has since reset: sticky follows the pool's other members, so
// the stranded one is never re-forwarded and its snapshot never refreshed, and
// the clear-park path (issue #147) cannot move a store-sourced exhaustion.
//
// Concurrent reprobes of the same QuotaKey — from this pool or any other pool
// sharing the nick — coalesce onto one upstream query. probeCooldown is
// deliberately not consulted: the operator asked now. Unlike the automatic
// recovery path the query is never suppressed by an in-flight automatic probe
// either; both hold their own probeInFlight reference, so neither can clear
// the other's signal.
//
// A healthy result drops this pool's c.exhausted and c.credentialPark entries
// for the nick and propagates the credential-park release to sibling pools
// (issue #254), exactly like recoverParked's healthy branch. A still-rejecting
// result changes no park: the merged store window, carrying the upstream's
// own reset, keeps the member exhausted. Reprobe never records a park and
// never moves the sticky pointer — the member rejoins rotation through the
// ordinary selection path once its snapshot stops rejecting.
//
// The returned status is the HTTP status for the error: 404 unknown pool, 400
// unknown nick, 409 disabled member, 502 for a failed query (wrapping
// ErrReprobeQuery and the query's own error).
func (p *Pools) Reprobe(ctx context.Context, poolName, nick string, query ReprobeQuery) (ReprobeResult, int, error) {
	name := backend.NormalizeName(poolName)
	normalized := backend.NormalizeName(nick)
	c, ok := p.controller(name)
	if !ok {
		return ReprobeResult{}, http.StatusNotFound, fmt.Errorf("pool not found")
	}
	if normalized == "" {
		return ReprobeResult{}, http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}
	c.mu.Lock()
	b, present := c.backendByNickLocked(normalized)
	disabled := c.disabled[normalized]
	c.mu.Unlock()
	if !present {
		return ReprobeResult{}, http.StatusBadRequest, fmt.Errorf("unknown nick: %s", normalized)
	}
	if disabled {
		return ReprobeResult{}, http.StatusConflict, fmt.Errorf("member disabled")
	}

	snap, err := p.reprobeShared(ctx, b, query)
	if err != nil {
		fmt.Fprintf(c.logOut, "auto[%s]: reprobe for %s failed: %v (snapshot and park unchanged)\n", c.name(), normalized, err)
		return ReprobeResult{}, http.StatusBadGateway, fmt.Errorf("%w: %w", ErrReprobeQuery, err)
	}
	healthy, merged := c.reconcileReprobe(normalized, b, snap)
	return ReprobeResult{Pool: name, Nick: normalized, Healthy: healthy, Snapshot: merged}, http.StatusOK, nil
}

// reprobeShared runs query for b's QuotaKey once across all concurrent
// callers and merges a successful result into the store before releasing the
// waiters, so every caller reconciles against the same merged snapshot. It
// returns the fresh (pre-merge) snapshot.
//
// The leader runs the query on a context detached from its own request's
// cancellation: followers from other requests share the result, so one
// client hanging up must not fail them. The query bounds its own duration.
// A follower whose own request is cancelled stops waiting.
func (p *Pools) reprobeShared(ctx context.Context, b backend.Backend, query ReprobeQuery) (quota.Snapshot, error) {
	key := b.QuotaKey()
	p.reprobeMu.Lock()
	if call, ok := p.reprobeCalls[key]; ok {
		p.reprobeMu.Unlock()
		select {
		case <-call.done:
			return call.snap, call.err
		case <-ctx.Done():
			return quota.Snapshot{}, ctx.Err()
		}
	}
	if p.reprobeCalls == nil {
		p.reprobeCalls = make(map[string]*reprobeCall)
	}
	call := &reprobeCall{done: make(chan struct{})}
	p.reprobeCalls[key] = call
	p.reprobeMu.Unlock()

	// Hold an in-flight reference in every pool that carries the nick, so the
	// automatic recovery probe (issue #124/#242) for the same account skips
	// it everywhere while this query runs.
	holders := p.beginProbeEverywhere(b.Nick, key)
	call.snap, call.err = query(context.WithoutCancel(ctx), b)
	if call.err == nil && p.store != nil {
		p.store.Merge(key, call.snap)
	}
	for _, c := range holders {
		c.mu.Lock()
		c.endProbeLocked(key)
		c.mu.Unlock()
	}

	p.reprobeMu.Lock()
	delete(p.reprobeCalls, key)
	p.reprobeMu.Unlock()
	close(call.done)
	return call.snap, call.err
}

// beginProbeEverywhere takes one probeInFlight reference on key in every
// controller whose pool contains nick, returning those controllers so the
// caller releases exactly the references it took. One controller lock at a
// time — never two at once.
func (p *Pools) beginProbeEverywhere(nick, key string) []*Controller {
	var holders []*Controller
	for _, c := range p.controllersSnapshot() {
		c.mu.Lock()
		if c.indexOf(nick) >= 0 {
			c.beginProbeLocked(key)
			holders = append(holders, c)
		}
		c.mu.Unlock()
	}
	return holders
}

// reconcileReprobe marks this pool as having observed nick's snapshot (issue
// #111) and, when the merged store snapshot no longer rejects, drops nick's
// live park here and releases a propagated credential park in sibling pools
// (issue #254). It reports the verdict and the snapshot it was judged on.
// Without a store the fresh snapshot alone is judged.
func (c *Controller) reconcileReprobe(nick string, b backend.Backend, fresh quota.Snapshot) (bool, quota.Snapshot) {
	c.MarkLocalSnapshot(nick)
	merged := fresh
	if c.store != nil {
		merged = c.store.Get(b.QuotaKey())
	}

	c.mu.Lock()
	healthy := !snapRejects(merged, c.now(), poller.LongWindowBlocksExhaustion(b.BaseURL))
	var hadCredPark bool
	if healthy {
		_, hadExhausted := c.exhausted[nick]
		_, hadCredPark = c.credentialPark[nick]
		delete(c.exhausted, nick)
		delete(c.credentialPark, nick)
		if hadExhausted || hadCredPark {
			c.notifyMutate()
		}
		fmt.Fprintf(c.logOut, "auto[%s]: reprobe for %s returned healthy; park (if any) cleared\n", c.name(), nick)
	} else {
		fmt.Fprintf(c.logOut, "auto[%s]: reprobe for %s still rejected; member stays exhausted\n", c.name(), nick)
	}
	c.mu.Unlock()

	if hadCredPark && c.propagateParkClear != nil {
		c.propagateParkClear(nick)
	}
	return healthy, merged
}
