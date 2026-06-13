// Package auto fronts every backend pool with a sticky pointer into the
// pool's members, with reactive 429 failover and zero synthetic probes.
//
// The design (locked in issue #24, generalized to per-pool in #26)
// prioritizes stickiness so Anthropic's per-account prompt cache is
// preserved: ride one member until it actually returns a 429, then
// switch. Nothing is probed — a member's quota is learned only from the
// real responses organic traffic produces, which also keeps each
// account's rolling 5h window anchored to its own first use so resets
// stay naturally staggered.
//
// Each pool has its own Controller. State lives entirely in the
// Controller (in process memory, like the quota store). There is no
// on-disk state and no background goroutine: the sticky pointer only
// moves on a request path (resolution or an upstream 429), all under one
// mutex. A Pools value bundles one Controller per pool and routes a
// request to the right one by the pool the client selected.
package auto

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// defaultExhaustionWindow is the conservative fallback parking time for a
// backend that 429s without a usable reset header. The 5h figure is the
// documented upper bound of the unified short window; a real 429 carries
// an absolute reset we use instead, so this only covers a missing or
// already-past timestamp (where no precise value exists). Parking for a
// bounded window guarantees forward progress — a backend marked exhausted
// is never re-selected until it is known to have recovered.
const defaultExhaustionWindow = 5 * time.Hour

// switchRetryAfterSeconds is the Retry-After the synthetic 503 carries
// when a pool switches members. It is deliberately short: the switch is
// instantaneous server-side, so the client should retry almost
// immediately and rebuild its cache on the new backend.
const switchRetryAfterSeconds = 1

// Pools fronts each configured pool with its own Controller and routes a
// request to the right one. It implements backend.PoolRouter.
type Pools struct {
	byPool map[string]*Controller
}

// NewPools builds one Controller per pool in reg. Each controller starts
// at a random member (start < 0) so no probe traffic is needed to anchor
// it. now defaults to time.Now and logOut to os.Stderr when nil.
func NewPools(reg *backend.Registry, now func() time.Time, logOut io.Writer) *Pools {
	byPool := make(map[string]*Controller)
	for _, name := range reg.PoolNames() {
		byPool[name] = NewController(reg, name, -1, now, logOut)
	}
	return &Pools{byPool: byPool}
}

// Route implements backend.PoolRouter: it resolves the named pool's
// controller and returns its current sticky backend. ok is false for an
// unknown pool.
func (p *Pools) Route(poolName string) (backend.Backend, time.Duration, bool, bool) {
	c, ok := p.byPool[poolName]
	if !ok {
		return backend.Backend{}, 0, false, false
	}
	b, retryAfter, exhausted := c.ResolveAuto()
	return b, retryAfter, true, exhausted
}

// ModifyResponse is the proxy.ResponseModifier hook. It dispatches the
// response to the controller of the pool the request resolved through,
// so a 429 fails over within that pool only.
func (p *Pools) ModifyResponse(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}
	b, ok := backend.FromContext(resp.Request.Context())
	if !ok {
		return nil
	}
	c, ok := p.byPool[b.Pool]
	if !ok {
		return nil
	}
	return c.ModifyResponse(resp)
}

// Current returns the active sticky backend of the named pool, for the
// quota view's active_backend field. ok is false for an unknown pool.
func (p *Pools) Current(poolName string) (backend.Backend, bool) {
	c, ok := p.byPool[poolName]
	if !ok {
		return backend.Backend{}, false
	}
	return c.CurrentBackend(), true
}

// Controller is the sticky selector for one pool. The zero value is not
// usable; call NewController.
type Controller struct {
	mu sync.Mutex

	reg   *backend.Registry
	pool  string
	nicks []string // the pool's members, in stable sorted order; len >= 1

	// cur indexes nicks: nicks[cur] is the backend every request to this
	// pool sticks to until it 429s.
	cur int

	// exhausted maps a nick to the absolute time its blocking window
	// resets. Presence means "exhausted-until-reset"; entries are cleared
	// lazily once now >= reset.
	exhausted map[string]time.Time

	now    func() time.Time
	logOut io.Writer
}

// NewController builds the sticky selector over the members of poolName
// in reg. When start < 0 the initial sticky backend is chosen at random
// (the spec's rotating start index — no probe, so any starting point is
// equally valid); otherwise start selects the index deterministically
// (used by tests). now defaults to time.Now and logOut to os.Stderr when
// nil.
func NewController(reg *backend.Registry, poolName string, start int, now func() time.Time, logOut io.Writer) *Controller {
	if now == nil {
		now = time.Now
	}
	if logOut == nil {
		logOut = os.Stderr
	}
	nicks := reg.PoolNicks(poolName) // sorted; Load guarantees at least one per pool
	c := &Controller{
		reg:       reg,
		pool:      poolName,
		nicks:     nicks,
		exhausted: make(map[string]time.Time),
		now:       now,
		logOut:    logOut,
	}
	n := len(nicks)
	if n == 0 {
		// Defensive: a pool with no members should never reach here, but
		// guard so cur stays in range.
		return c
	}
	if start < 0 {
		start = randIndex(n)
	}
	c.cur = ((start % n) + n) % n
	return c
}

// ResolveAuto returns the backend a request to this pool should use now.
// When the whole pool is exhausted it returns exhausted=true with the
// soonest-resetting member and the wait until that reset; the caller
// emits an honest 429.
func (c *Controller) ResolveAuto() (backend.Backend, time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clearExpiredLocked()

	if !c.isExhaustedLocked(c.nicks[c.cur]) {
		return c.backendAt(c.cur), 0, false
	}
	if idx, ok := c.firstHealthyLocked(); ok {
		c.cur = idx
		return c.backendAt(idx), 0, false
	}
	// All exhausted: point at the soonest to free up so the client's
	// post-wait retry lands on it, and report the precise wait.
	idx, reset := c.soonestLocked()
	c.cur = idx
	return c.backendAt(idx), c.waitUntil(reset), true
}

// Current returns the nick of the active sticky backend.
func (c *Controller) Current() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nicks[c.cur]
}

// CurrentBackend returns the active sticky backend, for the quota view.
func (c *Controller) CurrentBackend() backend.Backend {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.backendAt(c.cur)
}

// ModifyResponse is the per-pool failover hook. It acts on a request that
// drew an upstream 429; everything else passes through untouched. On such
// a 429 it records the backend's reset window, advances the sticky
// pointer, and either rewrites the response to a 503 (a healthy member
// remains — the client retry will succeed there) or forwards an honest
// 429 with a precise Retry-After (the pool is dry).
func (c *Controller) ModifyResponse(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		return nil
	}
	b, ok := backend.FromContext(resp.Request.Context())
	if !ok {
		return nil
	}

	reset := c.resetFrom(resp)
	res := c.record429(b.Nick, reset)

	if res.allExhausted {
		secs := retryAfterSeconds(res.retryAfter)
		setRetryAfter(resp.Header, secs)
		fmt.Fprintf(c.logOut, "auto[%s]: all backends exhausted; forwarding 429 (retry after %ds)\n", c.pool, secs)
		return nil
	}

	if res.switched {
		fmt.Fprintf(c.logOut, "auto[%s]: %s -> %s (%s hit 429)\n", c.pool, b.Nick, res.to, b.Nick)
	}
	rewriteTo503(resp)
	return nil
}

// record429Result reports the outcome of recording an upstream 429.
type record429Result struct {
	to           string        // the sticky nick after the call
	switched     bool          // whether the sticky pointer actually moved
	retryAfter   time.Duration // wait until soonest reset (allExhausted only)
	allExhausted bool          // whether the whole pool is now exhausted
}

// record429 marks nick exhausted until reset and advances the sticky
// pointer if needed. It only rotates when the current sticky backend is
// itself exhausted: under concurrent 429s on the same backend, the first
// call rotates and later calls see an already-healthy sticky pointer and
// leave it put, so stickiness is not eroded by redundant hops. When every
// backend is exhausted it points the sticky pointer at the soonest to
// reset.
func (c *Controller) record429(nick string, reset time.Time) record429Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.exhausted[nick] = reset
	c.clearExpiredLocked() // housekeeping; never clears the future reset just set

	// Another request may have already rotated off the failed backend; if
	// the current sticky is healthy, keep it.
	if !c.isExhaustedLocked(c.nicks[c.cur]) {
		return record429Result{to: c.nicks[c.cur]}
	}
	if idx, ok := c.firstHealthyLocked(); ok {
		from := c.cur
		c.cur = idx
		return record429Result{to: c.nicks[idx], switched: idx != from}
	}
	idx, soonest := c.soonestLocked()
	c.cur = idx
	return record429Result{to: c.nicks[idx], retryAfter: c.waitUntil(soonest), allExhausted: true}
}

// clearExpiredLocked drops exhausted marks whose reset has passed, so a
// recovered backend becomes selectable again. Caller holds c.mu.
func (c *Controller) clearExpiredLocked() {
	now := c.now()
	for nick, reset := range c.exhausted {
		if !now.Before(reset) { // now >= reset
			delete(c.exhausted, nick)
		}
	}
}

// isExhaustedLocked reports whether nick currently has an active (future)
// exhausted mark. Caller holds c.mu.
func (c *Controller) isExhaustedLocked(nick string) bool {
	reset, ok := c.exhausted[nick]
	return ok && c.now().Before(reset)
}

// firstHealthyLocked finds the next non-exhausted backend, scanning
// round-robin from just after cur so switches spread across the pool
// rather than always hopping to the lexically-first nick. Caller holds
// c.mu.
func (c *Controller) firstHealthyLocked() (int, bool) {
	n := len(c.nicks)
	for off := 1; off <= n; off++ {
		idx := (c.cur + off) % n
		if !c.isExhaustedLocked(c.nicks[idx]) {
			return idx, true
		}
	}
	return 0, false
}

// soonestLocked returns the index and reset time of the backend that
// frees up first. It is only meaningful when every backend is exhausted
// (the all-dry case); it falls back to cur if the map is somehow empty.
// Caller holds c.mu.
func (c *Controller) soonestLocked() (int, time.Time) {
	bestIdx, bestSet := c.cur, false
	var bestReset time.Time
	for idx, nick := range c.nicks {
		reset, ok := c.exhausted[nick]
		if !ok {
			continue
		}
		if !bestSet || reset.Before(bestReset) {
			bestIdx, bestReset, bestSet = idx, reset, true
		}
	}
	if !bestSet {
		return c.cur, c.now()
	}
	return bestIdx, bestReset
}

// backendAt resolves the backend at pool index i. The nick comes from the
// registry, so ResolveIn always succeeds.
func (c *Controller) backendAt(i int) backend.Backend {
	b, _ := c.reg.ResolveIn(c.pool, c.nicks[i])
	return b
}

// waitUntil is the non-negative duration from now until reset, floored so
// callers never see a zero/negative wait.
func (c *Controller) waitUntil(reset time.Time) time.Duration {
	d := reset.Sub(c.now())
	if d < 0 {
		d = 0
	}
	return d
}

// resetFrom extracts the binding window's reset from a 429 response. The
// unified-reset header already names the representative window's reset, so
// it is the authoritative value. A missing or already-past timestamp has
// no precise meaning, so we park the backend for the conservative default
// window instead — this keeps failover working against a sparse 429.
func (c *Controller) resetFrom(resp *http.Response) time.Time {
	now := c.now()
	snap := quota.Extract(resp)
	if snap.UnifiedReset != nil && snap.UnifiedReset.After(now) {
		return *snap.UnifiedReset
	}
	return now.Add(defaultExhaustionWindow)
}

// rewriteTo503 turns an upstream 429 into the transient 503 a pool hands
// a client during a switch. The body is replaced with a small JSON
// object, Retry-After invites an almost-immediate retry, and the upstream
// rate-limit headers are stripped so the synthetic response does not
// carry the rejected backend's quota state out the pool channel.
func rewriteTo503(resp *http.Response) {
	body := []byte(`{"error":"backend switching; retry"}`)

	resp.StatusCode = http.StatusServiceUnavailable
	resp.Status = strconv.Itoa(http.StatusServiceUnavailable) + " " + http.StatusText(http.StatusServiceUnavailable)
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))

	h := resp.Header
	for k := range h {
		if strings.HasPrefix(strings.ToLower(k), "anthropic-ratelimit-") {
			h.Del(k)
		}
	}
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h.Del("Content-Encoding")
	h.Set("Retry-After", strconv.Itoa(switchRetryAfterSeconds))
}

// setRetryAfter sets the Retry-After header to whole seconds.
func setRetryAfter(h http.Header, secs int) {
	h.Set("Retry-After", strconv.Itoa(secs))
}

// retryAfterSeconds converts a duration to the whole-second value a
// Retry-After header carries: ceiled (never advertise a shorter wait than
// reality) and floored at 1 (a client must wait at least a tick).
func retryAfterSeconds(d time.Duration) int {
	secs := int(math.Ceil(d.Seconds()))
	if secs < 1 {
		secs = 1
	}
	return secs
}

// randIndex returns a pseudo-random index in [0, n). Go auto-seeds the
// global source, so the start backend differs across process restarts
// without any explicit seeding. n is always >= 1 here.
func randIndex(n int) int {
	if n <= 1 {
		return 0
	}
	return rand.Intn(n)
}
