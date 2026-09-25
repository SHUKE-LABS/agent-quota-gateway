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
// Each pool has its own Controller. Its global sticky pointer and optional
// per-worker affinity map are protected by one mutex; runtime observations are
// snapshotted to the configured state file. Global stickiness moves on normal
// resolution or an upstream failure, while worker assignments move only when
// their member becomes unavailable. A Pools value routes by the selected pool.
package auto

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// defaultExhaustionWindow is the conservative fallback bound when a failed
// upstream response confirms a full statusless window but no usable reset is
// available. The five-hour bound is anchored to the quota snapshot's AsOf.
const defaultExhaustionWindow = 5 * time.Hour

// exhaustionUtilizationThreshold is the full-window utilization value used
// with a failed upstream response to identify statusless quota exhaustion.
const exhaustionUtilizationThreshold = 1.0

// unifiedStatusRejected is the per-window unified-status value Anthropic
// reports when a window is actually blocking requests. The other values
// ("allowed", "allowed_warning") are still served — a window can sit at
// utilization 1.0 with status "allowed_warning" in the soft-cap/overage
// zone — so when a snapshot carries a status it, not the utilization, is the
// authoritative exhaustion signal. See windowBlocks.
const unifiedStatusRejected = "rejected"

// storeSnapshotFreshness bounds how recent a polled quota snapshot must be
// for it to retire a stale live-429 park (issue #145). The poller refreshes
// the active member every defaultInterval (2m) and organic proxy traffic
// refreshes it continuously, so a member still being served reads fresh well
// within this 5m bound. A failed-off member the poller no longer tracks (it
// polls only the active member) freezes its snapshot and crosses this bound
// within a few minutes, falling back to wall-clock park aging — the
// load-bearing safety guard that keeps stale data from second-guessing a
// live park. See storeReconcilesParkLocked.
const storeSnapshotFreshness = 5 * time.Minute

// switchRetryAfterSeconds is the Retry-After the synthetic 503 carries
// when a pool switches members. It is deliberately short: the switch is
// instantaneous server-side, so the client should retry almost
// immediately and rebuild its cache on the new backend.
const switchRetryAfterSeconds = 1

// anthropicOverloadStatusCode is the non-standard status native Anthropic
// uses for a capacity overload. It is handled only for the native Anthropic
// host; compatible vendors remain pass-through for arbitrary 529 responses.
const anthropicOverloadStatusCode = 529

// anthropicOverloadRetryAfterSeconds gives native Anthropic capacity wobbles
// a longer same-member retry window than the short 429 throttle back-off.
const anthropicOverloadRetryAfterSeconds = 60

const nativeAnthropicHost = "api.anthropic.com"

// zaiThrottleRetryAfterSeconds is the Retry-After a z.ai/Zhipu concurrency
// throttle (the 1302 "Rate limit reached for requests" 429) is absorbed
// into. It is longer than switchRetryAfterSeconds so a single-member z.ai
// pool's retry gives the GLM concurrency window (often capped at 1) time
// to free up instead of immediately re-hitting the throttle, while still
// being short enough for a transparent client retry (issue #153).
const zaiThrottleRetryAfterSeconds = 3

// rateLimitBackoffMinSeconds / rateLimitBackoffMaxSeconds bound the Retry-After
// the gateway advertises when it absorbs a transient Anthropic per-minute
// rate-limit 429 (RPM/ITPM/OTPM) as a 503 back-off. The band is deliberately
// short: the per-minute limit clears in seconds and the SAME member serves
// again, so the client should retry almost immediately — but not so fast it
// re-hits the throttle on the next tick. The upstream retry-after is honoured
// when present, clamped into this band; a rate-limit 429 with no usable
// retry-after defaults to the top of the band (issue #191).
const (
	rateLimitBackoffMinSeconds = 1
	rateLimitBackoffMaxSeconds = 3
)

// Pools fronts each configured pool with its own Controller and routes a
// request to the right one. It implements backend.PoolRouter.
//
// byPool is built once at startup from the env-defined pools, but is also
// mutated at runtime by AddPool (the POST /_gateway/pool API) and at startup
// by LoadAddedPools. Because the proxy hot path reads it concurrently with
// those writes, every access goes through mu. The retained reg/store/now/
// logOut/onMutate fields are the ingredients NewController needs, kept so a
// runtime-created pool can be constructed identically to a startup one.
type Pools struct {
	mu     sync.RWMutex
	byPool map[string]*Controller

	// reg is the current authoritative registry — operator intent for every
	// pool (issue #198: config is the single source of truth). Runtime
	// mutations replace it wholesale with a fresh copy-on-write registry
	// (backend.Registry.With*) under p.mu, then reconcile the affected
	// controllers and trigger a config-file write. Read under p.mu.
	reg      *backend.Registry
	store    *quota.Store
	now      func() time.Time
	logOut   io.Writer
	onMutate func()
	// onConfigChange, if non-nil, is called (non-blocking) after any operator
	// mutation that changes operator intent, so the configfile writer flushes
	// the new config to disk. Distinct from onMutate, which persists runtime
	// observation (sticky/exhausted/worker affinity) to the state file.
	onConfigChange func()

	// defaultBaseURL, when non-nil, resolves the gateway default upstream at
	// call time for the add-member fallback (issue #302). main wires an
	// env-reading resolver only in env-only mode; in file mode it stays nil so
	// the registry's build-time default is used — env is never consulted again
	// there (issue #198). Read under p.mu.
	defaultBaseURL func() string
}

// NewPools builds one Controller per pool in reg. Each controller starts
// at a random member (start < 0) so no probe traffic is needed to anchor
// it. store is the shared quota store the controllers consult to fail off a
// member reported fully consumed (poller- or header-sourced) even without a
// live 429; a nil store disables that signal and keeps pure 429-driven
// failover. now defaults to time.Now and logOut to os.Stderr when nil.
// logOut is non-nil after construction (issue #252): callers may write to
// it without a nil guard.
func NewPools(reg *backend.Registry, store *quota.Store, now func() time.Time, logOut io.Writer) *Pools {
	if logOut == nil {
		logOut = os.Stderr
	}
	byPool := make(map[string]*Controller)
	for _, name := range reg.PoolNames() {
		byPool[name] = NewController(reg, name, -1, store, now, logOut)
	}
	p := &Pools{
		byPool: byPool,
		reg:    reg,
		store:  store,
		now:    now,
		logOut: logOut,
	}
	for _, c := range byPool {
		p.wireCredentialParkPropagation(c)
	}
	return p
}

// controller resolves a pool's controller under the read lock. The returned
// *Controller carries its own mutex, so callers operate on it after the map
// lookup without holding p.mu.
func (p *Pools) controller(name string) (*Controller, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, ok := p.byPool[name]
	return c, ok
}

// controllersSnapshot returns a name->controller copy taken under the read
// lock, so a ranging caller iterates a stable set without holding p.mu while
// it touches each controller.
func (p *Pools) controllersSnapshot() map[string]*Controller {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]*Controller, len(p.byPool))
	for name, c := range p.byPool {
		out[name] = c
	}
	return out
}

// name returns the controller's current byPool key. It is the lock-free
// accessor for the atomic field; callers that need a stable name across
// multiple operations must take c.mu and snapshot the result. A controller
// always has a non-nil pool name — NewController initialises it before any
// other path can observe the controller, and the rename writer stores a fresh
// pointer rather than clearing it — so the dereference here is safe.
func (c *Controller) name() string {
	return *c.poolName.Load()
}

// PoolNames returns the current pool names in sorted order, taken under the
// read lock so it reflects runtime-created pools (AddPool). The poller calls
// it fresh each tick so a pool created after startup is polled without a
// restart (issue #202).
func (p *Pools) PoolNames() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.byPool))
	for name := range p.byPool {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// sortedControllers returns every controller in sorted-name order, taken under
// the read lock so the set reflects runtime-created pools (AddPool). The
// preemptor calls it fresh each tick, so a pool created — or given a priority
// order — after startup is subject to preempt-back (issue #202). Sorted so the
// preemptor evaluates pools deterministically.
func (p *Pools) sortedControllers() []*Controller {
	p.mu.RLock()
	defer p.mu.RUnlock()
	names := make([]string, 0, len(p.byPool))
	for name := range p.byPool {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*Controller, 0, len(names))
	for _, name := range names {
		out = append(out, p.byPool[name])
	}
	return out
}

// Route implements backend.PoolRouter: it resolves the named pool's
// controller and returns its current sticky backend. ok is false for an
// unknown pool.
func (p *Pools) Route(poolName string) (backend.Backend, time.Duration, bool, bool) {
	c, ok := p.controller(poolName)
	if !ok {
		return backend.Backend{}, 0, false, false
	}
	b, retryAfter, exhausted := c.ResolveAuto()
	return b, retryAfter, true, exhausted
}

// RouteWorker resolves a request to the member assigned to workerNickname.
// The worker identity is routing context only; pool selection remains the
// caller's existing selector.
func (p *Pools) RouteWorker(poolName, workerNickname string) (backend.Backend, time.Duration, bool, bool) {
	c, ok := p.controller(poolName)
	if !ok || !backend.IsValidWorkerNickname(workerNickname) {
		return backend.Backend{}, 0, false, false
	}
	b, retryAfter, exhausted := c.ResolveWorker(workerNickname)
	return b, retryAfter, true, exhausted
}

// ModifyResponse is the proxy.ResponseModifier hook. It dispatches the
// response to the controller of the pool the request resolved through,
// so a 429 fails over within that pool only and a native Anthropic 529
// overload is absorbed without changing pool state.
func (p *Pools) ModifyResponse(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}
	b, ok := backend.FromContext(resp.Request.Context())
	if !ok {
		return nil
	}
	c, ok := p.controller(b.Pool)
	if !ok {
		return nil
	}
	return c.ModifyResponse(resp)
}

// MarkLocalSnapshot records that the controller for poolName has itself
// observed a quota snapshot for nick. Called by the upstream-response
// observer and the quota poller to gate poolStatus' snapshot attach so a
// runtime-added nick does not briefly render another pool's data. Unknown
// poolName or an empty nick are no-ops.
func (p *Pools) MarkLocalSnapshot(poolName, nick string) {
	if poolName == "" || nick == "" {
		return
	}
	c, ok := p.controller(poolName)
	if !ok {
		return
	}
	c.MarkLocalSnapshot(nick)
}

// Current returns the active sticky backend of the named pool, for the
// quota view's active_backend field. ok is false for an unknown pool.
func (p *Pools) Current(poolName string) (backend.Backend, bool) {
	c, ok := p.controller(poolName)
	if !ok {
		return backend.Backend{}, false
	}
	return c.CurrentBackend(), true
}

// ClearExhausted drops the named pool's recorded parks (see
// Controller.ClearExhausted). ok is false for an unknown pool.
// releasedElsewhere names, per nick, the sibling pools a propagated
// credential park was also released in (issue #254 AC5/AC12).
func (p *Pools) ClearExhausted(poolName string) (cleared []string, releasedElsewhere map[string][]string, ok bool) {
	c, ok := p.controller(poolName)
	if !ok {
		return nil, nil, false
	}
	cleared, releasedElsewhere = c.ClearExhausted()
	return cleared, releasedElsewhere, true
}

// ClearExhaustedNick drops one member's recorded park in the named pool (see
// Controller.ClearExhaustedNick). ok is false for an unknown pool; cleared
// reports whether a recorded park was actually present for the nick.
// releasedElsewhere names the sibling pools a propagated credential park was
// also released in (issue #254 AC5/AC12).
func (p *Pools) ClearExhaustedNick(poolName, nick string) (cleared bool, releasedElsewhere []string, ok bool) {
	c, ok := p.controller(poolName)
	if !ok {
		return false, nil, false
	}
	cleared, releasedElsewhere = c.ClearExhaustedNick(nick)
	return cleared, releasedElsewhere, true
}

// ClearAllExhausted drops recorded parks across every pool, returning a
// map of pool name to the nicks cleared (pools with nothing parked are
// omitted). Clearing every pool already releases every propagated credential
// park by construction, so there is no separate cross-pool surface to report
// here (contrast ClearExhausted / ClearExhaustedNick, issue #254 AC12).
//
// The reported set is taken from a snapshot of each pool's parked nicks
// BEFORE any pool's clear runs, not from ClearExhausted's own return value.
// p.controllersSnapshot() is a map, so visit order is nondeterministic;
// ClearExhausted's cross-pool propagation deletes a nick from a sibling's
// maps as a side effect, so a pool visited after its propagator would
// otherwise report nothing for a nick it demonstrably had — the observed
// result would depend on iteration order rather than on what was actually
// parked. Snapshotting first removes that dependency.
func (p *Pools) ClearAllExhausted() map[string][]string {
	controllers := p.controllersSnapshot()
	out := make(map[string][]string, len(controllers))
	for name, c := range controllers {
		if nicks := c.parkedNicksSnapshot(); len(nicks) > 0 {
			out[name] = nicks
		}
	}
	for _, c := range controllers {
		c.ClearExhausted()
	}
	return out
}

// MemberStatus describes one pool member's current state for /_gateway/pool.
type MemberStatus struct {
	Nick           string          `json:"nick"`
	Status         string          `json:"status"`          // "active", "exhausted", "idle", "disabled"
	ExhaustedUntil *time.Time      `json:"exhausted_until"` // RFC 3339 or null
	Snapshot       *quota.Snapshot `json:"snapshot"`        // null when no snapshot recorded

	// Disabled mirrors c.disabled[nick] — the same source the "disabled"
	// status string derives from, so the two can never disagree. The UI's
	// per-member toggle reads this on every /_gateway/pool poll to stay in
	// sync with an out-of-band disable (second tab, API, another operator);
	// without it the live overlay had no data to sync the switch from and it
	// froze at the last full config render while the badge moved (issue #159).
	Disabled bool `json:"disabled"`

	// Parked reports whether an explicit failed-response park is currently holding this member
	// out of rotation — present, reset still in the future, and not reconciled
	// away by a fresh healthy store snapshot (issue #145). It is the gate for
	// the per-nick "clear park" affordance (issue #147): exactly the set of
	// parks ClearExhaustedNick can usefully drop. Distinct from Status:
	// a status-bearing rejected store window can report "exhausted" without
	// being Parked, since clearing the recorded park cannot move it.
	Parked bool `json:"parked"`
}

// PoolStatus is the /_gateway/pool response for one pool.
type PoolStatus struct {
	Pool        string         `json:"pool"`
	Active      string         `json:"active"`
	Concurrency int            `json:"concurrency"`
	Members     []MemberStatus `json:"members"`

	// Poller carries the per-pool liveness observation (issue #247):
	// last successful poll, last error and its time, consecutive-
	// failure count, and a derived staleness verdict. It is omitted
	// entirely (the JSON key is absent, not "poller":null) for pools
	// the poller does not track — Anthropic and any other untracked
	// backend see no delta, so a caller can tell "this pool has no
	// poller signal at all" from "this pool's poller signal is stale
	// or failing". The poller package is the source of truth for
	// tracking and staleness math (it owns the StaleAfterIntervals
	// constant); auto only wires the result through.
	Poller *poller.PoolStatus `json:"poller,omitempty"`
}

// PoolConfigView is the /_gateway/config response for one pool.
// It carries the effective configuration (static + runtime overlay) with
// all credentials redacted.
type PoolConfigView struct {
	Pool        string                 `json:"pool"`
	Concurrency int                    `json:"concurrency"`
	Priority    []string               `json:"priority,omitempty"`
	Members     []PoolMemberConfigView `json:"members"`
}

// PoolMemberConfigView describes one pool member in the config view.
type PoolMemberConfigView struct {
	Nick     string `json:"nick"`
	BaseURL  string `json:"base_url"`
	Disabled bool   `json:"disabled"`
	Status   string `json:"status"` // "active", "idle", "exhausted", "disabled"
}

// PoolStatus returns the current status of the named pool, or ok=false for an unknown pool.
func (p *Pools) PoolStatus(poolName string, store *quota.Store, pl *poller.Poller) (PoolStatus, bool) {
	c, ok := p.controller(poolName)
	if !ok {
		return PoolStatus{}, false
	}
	// nil map — the single-pool path goes through LookupStatus's
	// fallback which calls pl.Status() under the poller's mutex once.
	return c.poolStatus(store, pl, nil), true
}

// AllPoolStatuses returns status for every pool in sorted order.
func (p *Pools) AllPoolStatuses(store *quota.Store, pl *poller.Poller) []PoolStatus {
	snapshot := p.controllersSnapshot()
	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	sort.Strings(names)
	// Pre-build the per-pool poller map once (review of #267): each
	// Controller.poolStatus would otherwise rebuild it under the
	// poller's mutex N times per call, which compounds under UI polling.
	// The map is empty for a nil poller, in which case every pool
	// view simply carries no `poller` key (preserving AC2).
	var pollerMap map[string]poller.PoolStatus
	if pl != nil {
		pollerMap = pl.Status()
	}
	out := make([]PoolStatus, 0, len(snapshot))
	for _, name := range names {
		out = append(out, snapshot[name].poolStatus(store, pl, pollerMap))
	}
	return out
}

// CredentialParkPersist is the persisted shape of one credentialPark entry
// (issue #254 AC7). WindowFact mirrors credentialParkEntry.windowFact so a
// reload keeps applying store-reconciliation (#145) and the preemptor's
// precise-reset supersession to the header-less-429 residue, without ever
// treating a reloaded 401/403 entry as reconcilable by either.
type CredentialParkPersist struct {
	Reset      time.Time `json:"reset"`
	WindowFact bool      `json:"window_fact,omitempty"`
}

// PoolPersistState is the serializable routing state for one pool.
// It is exported so the persist package can embed it in GatewayState.
type PoolPersistState struct {
	Sticky    string               `json:"sticky"`
	Exhausted map[string]time.Time `json:"exhausted"`
	// WorkerAffinity maps validated worker identities to their per-pool AQG
	// member nick. WorkerCursor is the next member nick considered for a new
	// assignment; both are runtime routing observations, never configuration.
	WorkerAffinity map[string]string `json:"worker_affinity,omitempty"`
	WorkerCursor   string            `json:"worker_cursor,omitempty"`
	// CredentialPark persists a store-unrepresentable park (401/403, or a
	// header-less 429 fallback) — local or propagated from a sibling pool —
	// so it survives a restart in every pool it held (issue #254 AC7).
	// Absent/empty in older state files; a missing key loads as no
	// propagated park, which is safe (the parking pool's own restart
	// re-asserts and re-propagates on its next failure).
	CredentialPark map[string]CredentialParkPersist `json:"credential_park,omitempty"`
	// LocalSnapshotNicks lists members for which this controller has
	// observed a snapshot since the last restart. Persisted unconditionally
	// so a pool does not lose the
	// "this pool has seen traffic" signal across a restart. Empty/absent
	// in older state files; treated as "no observed snapshots" on load,
	// which is the same as the pre-fix behaviour for the first observation.
	LocalSnapshotNicks []string `json:"local_snapshot_nicks,omitempty"`
}

// memberEntry is one member in the Controller's ordered member collection,
// re-derived from the config registry on every reconcile (issue #198).
type memberEntry struct {
	Nick       string
	Credential string
	BaseURL    string
}

// LoadPersistState applies previously persisted routing state to each pool's
// controller. Called once at startup, before the server begins serving.
func (p *Pools) LoadPersistState(states map[string]PoolPersistState) {
	for name, s := range states {
		if c, ok := p.controller(name); ok {
			c.loadState(s.Sticky, s.Exhausted, s.LocalSnapshotNicks)
			c.loadCredentialPark(s.CredentialPark)
			c.loadWorkerAffinity(s.WorkerAffinity, s.WorkerCursor)
		}
	}
}

// SetPriority sets the runtime priority override for the named pool.
// The order list is validated (all nicks must exist in the pool, no duplicates,
// no empty strings) and then expanded via effectiveOrder() to a total order.
// Returns (httpStatus, error) with error containing a credential-free message.
func (p *Pools) SetPriority(poolName string, order []string) (int, error) {
	name := backend.NormalizeName(poolName)
	p.mu.Lock()
	defer p.mu.Unlock()

	c, ok := p.byPool[name]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}

	c.mu.Lock()
	// Normalize and validate the input order against the current membership.
	seen := make(map[string]bool)
	validOrder := make([]string, 0, len(order))
	for _, raw := range order {
		nick := backend.NormalizeName(raw)
		if nick == "" {
			c.mu.Unlock()
			return http.StatusBadRequest, fmt.Errorf("priority list contains empty nick")
		}
		if seen[nick] {
			c.mu.Unlock()
			return http.StatusBadRequest, fmt.Errorf("priority list contains duplicate nick: %s", nick)
		}
		seen[nick] = true
		if c.indexOf(nick) < 0 {
			c.mu.Unlock()
			return http.StatusBadRequest, fmt.Errorf("unknown nick: %s", nick)
		}
		validOrder = append(validOrder, nick)
	}
	c.mu.Unlock()

	// Write the priority through to the config registry (single source of
	// truth). effectiveOrder expansion (unlisted members rank last) happens on
	// reconcile, matching NewController.
	next, err := p.reg.WithPriority(name, validOrder)
	if err != nil {
		return http.StatusBadRequest, err
	}
	p.applyRegistryLocked(next, name)
	p.markConfigDirtyLocked()
	return http.StatusOK, nil
}

// SetMemberDisabled sets or clears the disabled flag for a member in a pool.
// The target nick must be a present (non-removed) member — static or
// runtime-added — mirroring RemoveMember's semantics. Re-enabling a member that
// was operator-removed is rejected, matching the rest of the runtime-member
// surface. The post-mutation log line names the resulting state (issue #252)
// rather than the verb, so an operator reading the journal can tell whether a
// member was disabled or re-enabled without knowing which endpoint was
// called. Returns (httpStatus, error) with error containing a
// credential-free message.
func (p *Pools) SetMemberDisabled(poolName, nick string, off bool) (int, error) {
	name := backend.NormalizeName(poolName)
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byPool[name]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}

	c.mu.Lock()
	present := c.indexOf(normalized) >= 0
	c.mu.Unlock()
	if !present {
		return http.StatusBadRequest, fmt.Errorf("unknown nick: %s", normalized)
	}

	next, err := p.reg.WithMemberDisabled(name, normalized, off)
	if err != nil {
		return http.StatusBadRequest, err
	}
	p.applyRegistryLocked(next, name)
	p.markConfigDirtyLocked()
	state := "enabled"
	if off {
		state = "disabled"
	}
	fmt.Fprintf(c.logOut, "auto[%s]: %s %s\n", name, normalized, state)
	return http.StatusOK, nil
}

// AddMember adds a runtime member to a pool. Credential and baseURL are optional
// for a *known* subscription: when omitted, they are resolved by scanning the
// other pools for the same nick (credential and base_url resolve independently).
// A priority target requires an explicit placement (must include nick), reusing
// the move path's validation; other targets must carry none. A base_url
// that stays unresolved after the cross-pool scan and the unanimous in-pool
// borrow (empty pool, or members disagreeing, issue #248) falls back to the
// gateway default upstream (issue #302). The resolved concrete base_url is
// persisted — never an empty string. Returns (httpStatus, error) with a
// credential-free message.
func (p *Pools) AddMember(poolName, nick, credential, baseURL string, placement []string) (int, error) {
	name := backend.NormalizeName(poolName)
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}
	if baseURL != "" {
		if _, err := backend.ValidateBaseURL(baseURL); err != nil {
			return http.StatusBadRequest, fmt.Errorf("invalid base_url: %w", err)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byPool[name]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}

	// Resolve omitted credential/base_url from the other pools of the current
	// registry (lock-free; reg is immutable). Credential and base_url resolve
	// independently.
	resolvedCred := credential
	resolvedURL := baseURL
	if credential == "" || baseURL == "" {
		creds, baseURLs := crossPoolResolve(p.reg, name, normalized)
		if credential == "" {
			switch len(creds) {
			case 1:
				resolvedCred = creds[0]
			case 0:
				// (0 credentials, 0 URLs): the registry holds no declaration
				// for nick at all. Distinct from the ambiguity errors below,
				// which are real conflicts that need an explicit operator
				// choice. Build validation rejects empty credentials today, so
				// len(creds)==0 implies len(baseURLs)==0 — the AND-ed gate
				// documents that contract and protects the no-declaration
				// helper from a future (0 creds, 1+ URLs) shape.
				if len(baseURLs) == 0 {
					return http.StatusBadRequest, nickNotResolvableError(normalized, p.reg)
				}
				return http.StatusBadRequest, fmt.Errorf("credential is required: nick %s is not a known subscription in any other pool", normalized)
			default:
				return http.StatusBadRequest, fmt.Errorf("credential for nick %s is ambiguous across pools; specify it explicitly", normalized)
			}
		}
		if baseURL == "" && len(baseURLs) > 1 {
			return http.StatusBadRequest, fmt.Errorf("base_url for nick %s is ambiguous across pools; specify it explicitly", normalized)
		}
		if baseURL == "" && len(baseURLs) == 1 {
			resolvedURL = baseURLs[0]
		}
		// len(baseURLs)==0 leaves resolvedURL empty → pool default below.
	}

	c.mu.Lock()
	// Duplicate check: already a member.
	if c.indexOf(normalized) >= 0 {
		c.mu.Unlock()
		return http.StatusConflict, fmt.Errorf("nick %s already exists as a member", normalized)
	}
	// Resolve base_url to a concrete value so the config record is
	// self-describing. An unresolved (new-nick) base_url borrows the pool's
	// members' URL only when every member already agrees on one effective
	// upstream — a mixed-provider pool cannot lend its first member's URL
	// because that member's upstream is alphabetical, not authoritative
	// (issue #248). An empty pool, or a pool whose members disagree, has no
	// borrowable URL; both fall back to the gateway default upstream instead
	// of erroring (issue #302): the operator's mental model is that a fresh
	// credential in a Claude pool defaults to the configured gateway upstream
	// (ANTHROPIC_BASE_URL), so demanding an explicit base_url was friction
	// with no value. The fallback is persisted as the member's resolved URL,
	// so aqg.json stays self-describing and the next mutation sees a stable
	// value.
	if resolvedURL == "" {
		borrowed := ""
		if len(c.members) > 0 {
			borrowed = c.members[0].BaseURL
			for i := 1; i < len(c.members); i++ {
				if c.members[i].BaseURL != borrowed {
					borrowed = ""
					break
				}
			}
		}
		if borrowed != "" {
			resolvedURL = borrowed
		} else {
			resolvedURL = p.gatewayDefaultBaseURLLocked()
		}
	}
	// Placement: a priority target needs an explicit order including nick; a
	// target without a priority order must not carry one.
	isPriorityTarget := len(c.effectivePriorityLocked()) > 0
	var normPlacement []string
	if isPriorityTarget {
		var status int
		var err error
		normPlacement, status, err = c.validatePlacementLocked(normalized, placement)
		if err != nil {
			c.mu.Unlock()
			return status, err
		}
	} else if len(placement) > 0 {
		c.mu.Unlock()
		return http.StatusBadRequest, fmt.Errorf("placement is only applicable to a priority target pool")
	}
	c.mu.Unlock()

	// Commit through the config registry. An empty resolvedURL means the member
	// inherits the pool default (a fresh pool with a member's own base_url).
	next, err := p.reg.WithMemberSet(name, normalized, resolvedCred, resolvedURL, false)
	if err != nil {
		return http.StatusBadRequest, err
	}
	if isPriorityTarget {
		next, err = next.WithPriority(name, normPlacement)
		if err != nil {
			return http.StatusBadRequest, err
		}
	}
	p.applyRegistryLocked(next, name)
	p.markConfigDirtyLocked()
	fmt.Fprintf(c.logOut, "auto[%s]: added member %s\n", name, normalized)
	return http.StatusOK, nil
}

// RemoveMember removes a member from a pool. Removal is permanent: the member
// is deleted from the config registry (issue #198), pruned from the pool's
// priority order, and if it was the active sticky member the pointer
// force-switches on reconcile. Returns (httpStatus, error) with a
// credential-free message.
func (p *Pools) RemoveMember(poolName, nick string) (int, error) {
	name := backend.NormalizeName(poolName)
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byPool[name]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}

	c.mu.Lock()
	present := c.indexOf(normalized) >= 0
	c.mu.Unlock()
	if !present {
		return http.StatusBadRequest, fmt.Errorf("nick %s not found in pool", normalized)
	}

	next, err := p.reg.WithMemberRemoved(name, normalized)
	if err != nil {
		return http.StatusBadRequest, err
	}
	p.applyRegistryLocked(next, name)
	p.markConfigDirtyLocked()
	fmt.Fprintf(c.logOut, "auto[%s]: removed member %s\n", name, normalized)
	return http.StatusOK, nil
}

// MoveMember relocates a subscription (nick) from one pool to another. It is
// implemented as the bridge model: persistent remove from the source pool plus
// an add to the target pool carrying the source member's credential and
// resolved base URL. Returns (httpStatus, error) with a credential-free message.
//
// Placement: moving into a priority pool that has no existing slot for nick
// requires an explicit placement order (which must include nick) — there is no
// implicit insertion. Moving into a target without a priority order, or onto an existing
// same-nick slot, needs no placement.
//
// Conflict: an existing same-nick member in the target whose credential and
// resolved base URL match is a silent no-op (slot preserved); a differing one
// returns 409 unless force is set.
//
// No surprise re-anchor: the target's healthy active member is never force-
// switched by the move; the new order applies on the next selection event.
func (p *Pools) MoveMember(fromPool, nick, toPool string, placement []string, force bool) (int, error) {
	from := backend.NormalizeName(fromPool)
	to := backend.NormalizeName(toPool)
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}
	if from == to {
		return http.StatusBadRequest, fmt.Errorf("source and target pools are the same")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	src, ok := p.byPool[from]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("source pool not found")
	}
	dst, ok := p.byPool[to]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("target pool not found")
	}

	// Read the source member's resolved credential + base URL.
	src.mu.Lock()
	srcBackend, srcPresent := src.backendByNickLocked(normalized)
	src.mu.Unlock()
	if !srcPresent {
		return http.StatusBadRequest, fmt.Errorf("nick %s not found in source pool", normalized)
	}

	// Validate the target-side conflict/placement rules.
	dst.mu.Lock()
	var normPlacement []string
	if dstBackend, exists := dst.backendByNickLocked(normalized); exists {
		if dstBackend.Credential != srcBackend.Credential || dstBackend.BaseURL != srcBackend.BaseURL {
			if !force {
				dst.mu.Unlock()
				return http.StatusConflict, fmt.Errorf("target nick %s exists with a different credential or base_url; confirm to overwrite", normalized)
			}
		}
		// Existing slot: no placement needed (identical → effective no-op on dst).
	} else {
		isPriorityTarget := len(dst.effectivePriorityLocked()) > 0
		if isPriorityTarget {
			var status int
			var err error
			normPlacement, status, err = dst.validatePlacementLocked(normalized, placement)
			if err != nil {
				dst.mu.Unlock()
				return status, err
			}
		} else if len(placement) > 0 {
			dst.mu.Unlock()
			return http.StatusBadRequest, fmt.Errorf("placement is only applicable to a priority target pool")
		}
	}
	dst.mu.Unlock()

	// Commit through the config registry: add/overwrite on the target, then
	// remove from the source. A single new registry keeps both halves atomic.
	next, err := p.reg.WithMemberSet(to, normalized, srcBackend.Credential, srcBackend.BaseURL, false)
	if err != nil {
		return http.StatusBadRequest, err
	}
	if len(normPlacement) > 0 {
		next, err = next.WithPriority(to, normPlacement)
		if err != nil {
			return http.StatusBadRequest, err
		}
	}
	next, err = next.WithMemberRemoved(from, normalized)
	if err != nil {
		return http.StatusBadRequest, err
	}
	p.applyRegistryLocked(next, from, to)
	p.markConfigDirtyLocked()
	fmt.Fprintf(src.logOut, "auto: moved member %s from %s to %s\n", normalized, from, to)
	return http.StatusOK, nil
}

// validatePlacementLocked checks an explicit placement order for a priority
// target into which nick is being added: every entry must be a current target
// member (or nick itself), with no empties or duplicates, and the order must
// include nick (no implicit insertion). It returns the normalized placement on
// success. Caller holds c.mu.
func (c *Controller) validatePlacementLocked(nick string, placement []string) ([]string, int, error) {
	if len(placement) == 0 {
		return nil, http.StatusBadRequest, fmt.Errorf("explicit placement is required to move into priority pool %s", c.name())
	}
	prospective := make(map[string]bool)
	for _, m := range c.allMemberNicksLocked() {
		prospective[m] = true
	}
	prospective[nick] = true

	seen := make(map[string]bool, len(placement))
	norm := make([]string, 0, len(placement))
	hasNick := false
	for _, raw := range placement {
		pn := backend.NormalizeName(raw)
		if pn == "" {
			return nil, http.StatusBadRequest, fmt.Errorf("placement contains an empty nick")
		}
		if seen[pn] {
			return nil, http.StatusBadRequest, fmt.Errorf("placement contains duplicate nick: %s", pn)
		}
		seen[pn] = true
		if !prospective[pn] {
			return nil, http.StatusBadRequest, fmt.Errorf("placement contains unknown nick: %s", pn)
		}
		if pn == nick {
			hasNick = true
		}
		norm = append(norm, pn)
	}
	if !hasNick {
		return nil, http.StatusBadRequest, fmt.Errorf("placement must include the moved nick %s", nick)
	}
	return norm, http.StatusOK, nil
}

// EffectiveConfig returns the effective configuration for all pools,
// with credentials fully redacted. Each pool's view includes its effective
// priority (runtime override when set, else env priority),
// and per-member status including the disabled flag.
func (p *Pools) EffectiveConfig() []PoolConfigView {
	snapshot := p.controllersSnapshot()
	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]PoolConfigView, 0, len(names))
	for _, name := range names {
		c := snapshot[name]
		c.mu.Lock()
		view := PoolConfigView{Pool: name, Concurrency: c.workerConcurrency}

		// Effective priority.
		pri := c.effectivePriorityLocked()
		if len(pri) > 0 {
			view.Priority = make([]string, len(pri))
			copy(view.Priority, pri)
		}

		// Members: the effective set (static + runtime-added − removed), sorted.
		// Removal is permanent deletion, so removed members are omitted entirely
		// — consistent with poolStatus and the selection path.
		allMembers := c.allMemberNicksLocked()

		view.Members = make([]PoolMemberConfigView, 0, len(allMembers))

		for _, nick := range allMembers {
			member := PoolMemberConfigView{
				Nick:     nick,
				Disabled: c.disabled[nick],
			}
			if b, ok := c.backendByNickLocked(nick); ok {
				member.BaseURL = b.BaseURL
			}
			// Determine status. Removed members are already excluded from
			// allMembers above, so only the disabled flag maps to "disabled".
			// exhausted is checked before the sticky (curNick) arm so that
			// "active" means the sticky member that is ALSO available: a sticky
			// member that is parked reports exhausted, matching poolStatus and
			// the selection path (isUnavailableLocked), which all use the
			// controller clock and treat a parked member as unavailable.
			if c.disabled[nick] {
				member.Status = "disabled"
			} else if _, ok := c.exhaustedUntilLocked(nick); ok {
				// exhaustedUntilLocked returns ok=false once the park elapses by
				// c.now(), so it is the single source of truth for the exhausted
				// status across this view, poolStatus, and the selection path.
				member.Status = "exhausted"
			} else if nick == c.curNick {
				member.Status = "active"
			} else {
				member.Status = "idle"
			}
			view.Members = append(view.Members, member)
		}
		c.mu.Unlock()
		out = append(out, view)
	}
	return out
}

// PersistState snapshots the current routing state for all pools.
func (p *Pools) PersistState() map[string]PoolPersistState {
	snapshot := p.controllersSnapshot()
	out := make(map[string]PoolPersistState, len(snapshot))
	for name, c := range snapshot {
		out[name] = c.persistState()
	}
	return out
}

// CreatePoolWithMember atomically creates a plain pool and optionally its first
// member. All member validation runs before the registry is swapped (issue #240).
func (p *Pools) CreatePoolWithMember(name, mode, nick, credential, baseURL string, placement []string) (int, error) {
	normalized := backend.NormalizeName(name)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("pool name is empty after normalization")
	}
	if mode == "" {
		mode = "plain"
	}
	if mode != "plain" {
		return http.StatusBadRequest, fmt.Errorf("unsupported mode %q: only \"plain\" is supported", mode)
	}
	member := backend.NormalizeName(nick)
	if member == "" && len(placement) > 0 {
		return http.StatusBadRequest, fmt.Errorf("nick is required when placement is supplied")
	}
	if baseURL != "" {
		if _, err := backend.ValidateBaseURL(baseURL); err != nil {
			return http.StatusBadRequest, fmt.Errorf("invalid base_url: %w", err)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.byPool[normalized]; exists || p.reg.HasPool(normalized) {
		return http.StatusConflict, fmt.Errorf("pool %s already exists", normalized)
	}
	next, err := p.reg.WithPoolCreated(normalized)
	if err != nil {
		return http.StatusConflict, err
	}
	if member != "" {
		resolvedCred, resolvedURL := credential, baseURL
		if credential == "" || baseURL == "" {
			creds, baseURLs := crossPoolResolve(p.reg, normalized, member)
			if credential == "" {
				switch len(creds) {
				case 1:
					resolvedCred = creds[0]
				case 0:
					if len(baseURLs) == 0 {
						return http.StatusBadRequest, nickNotResolvableError(member, p.reg)
					}
					return http.StatusBadRequest, fmt.Errorf("credential is required: nick %s is not a known subscription in any other pool", member)
				default:
					return http.StatusBadRequest, fmt.Errorf("credential for nick %s is ambiguous across pools; specify it explicitly", member)
				}
			}
			if baseURL == "" && len(baseURLs) > 1 {
				return http.StatusBadRequest, fmt.Errorf("base_url for nick %s is ambiguous across pools; specify it explicitly", member)
			}
			if baseURL == "" && len(baseURLs) == 1 {
				resolvedURL = baseURLs[0]
			}
		}
		// A newly created pool has no members to borrow a URL from; the
		// gateway default upstream is the fallback (issue #302), symmetric
		// with AddMember's empty-pool path.
		if resolvedURL == "" {
			resolvedURL = p.gatewayDefaultBaseURLLocked()
		}
		next, err = next.WithMemberSet(normalized, member, resolvedCred, resolvedURL, false)
		if err != nil {
			return http.StatusBadRequest, err
		}
		if len(placement) > 0 {
			return http.StatusBadRequest, fmt.Errorf("placement is only applicable to a priority target pool")
		}
	}
	p.reg = next
	c := NewController(next, normalized, -1, p.store, p.now, p.logOut)
	c.onMutate = p.onMutate
	p.wireCredentialParkPropagation(c)
	p.byPool[normalized] = c
	p.markConfigDirtyLocked()
	return http.StatusCreated, nil
}

// registry (issue #198: runtime pools are config, not a separate state-file
// overlay) and inserting a controller so the proxy can route to it
// immediately. name is normalized; mode defaults to "plain" and only "plain"
// is supported. The pool starts empty (no members, inherits the gateway
// default upstream); members are added afterward via AddMember. Returns
// (httpStatus, error) with a credential-free message; (StatusCreated, nil) on
// success.
func (p *Pools) AddPool(name, mode string) (int, error) {
	normalized := backend.NormalizeName(name)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("pool name is empty after normalization")
	}
	if mode == "" {
		mode = "plain"
	}
	if mode != "plain" {
		return http.StatusBadRequest, fmt.Errorf("unsupported mode %q: only \"plain\" is supported", mode)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.byPool[normalized]; exists || p.reg.HasPool(normalized) {
		return http.StatusConflict, fmt.Errorf("pool %s already exists", normalized)
	}

	next, err := p.reg.WithPoolCreated(normalized)
	if err != nil {
		return http.StatusConflict, err
	}
	p.reg = next
	c := NewController(next, normalized, -1, p.store, p.now, p.logOut)
	c.onMutate = p.onMutate
	p.wireCredentialParkPropagation(c)
	p.byPool[normalized] = c
	p.markConfigDirtyLocked()
	fmt.Fprintf(p.logOut, "auto: created runtime pool %s\n", normalized)
	return http.StatusCreated, nil
}

// RemovePool deletes an empty pool at runtime, the inverse of AddPool (issue
// #232). It requires the pool to be drained first: a pool that still has one or
// more members returns 409 so no persisted credential is silently discarded —
// the operator removes members via DELETE .../member/{nick} first. Deleting the
// last/only pool is permitted; routing afterward fails closed (403 unknown
// selector) since the controller leaves byPool. Dropping the controller
// releases all its runtime observation (sticky/exhausted/local-snapshot),
// and PersistState rebuilds from byPool so the next flush omits it. Returns
// (httpStatus, error) with a credential-free message; (StatusOK, nil) on
// success.
func (p *Pools) RemovePool(name string) (int, error) {
	normalized := backend.NormalizeName(name)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("pool name is empty after normalization")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.byPool[normalized]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}

	c.mu.Lock()
	memberCount := len(c.members)
	c.mu.Unlock()
	if memberCount > 0 {
		return http.StatusConflict, fmt.Errorf("pool %s still has %d member(s); remove them first", normalized, memberCount)
	}

	next, err := p.reg.WithPoolRemoved(normalized)
	if err != nil {
		return http.StatusNotFound, err
	}
	p.reg = next
	delete(p.byPool, normalized)
	p.markConfigDirtyLocked()
	fmt.Fprintf(p.logOut, "auto: removed runtime pool %s\n", normalized)
	return http.StatusOK, nil
}

// RenamePool renames a pool in place from oldName to newName (issue #238),
// preserving the controller's runtime observation — sticky pointer, exhausted
// marks and local-snapshot set are all keyed by member
// nick (not pool name) and so move with the rename for free. The credential-
// free status mapping mirrors AddPool's conflict pattern: unknown old → 404,
// empty / identical-after-normalize new → 400, new name collides with a
// different existing pool → 409. The state-file persister is fired alongside
// the config one so the next flush rewrites the pool under its new key —
// otherwise LoadPersistState would silently drop observation on the next
// restart (the file still carried the old key, with no controller by that
// name to consume it).
//
// Atomicity: the c.poolName atomic.Pointer[string] is swapped under c.mu,
// and the byPool map key moves under p.mu. A request already past Route but
// not yet at ModifyResponse holds a b.Pool string captured at Route time;
// that lookup misses the new key and ModifyResponse returns nil (no 429 park
// for the in-flight request). The next request resolves cleanly. Same class
// of window RemovePool already has, and the AC the issue names — "after
// success only the new pool name remains, and routing by the old selector
// fails closed" — is satisfied; the in-flight edge case is documented so a
// later reader does not file it as a regression.
func (p *Pools) RenamePool(oldName, newName string) (int, error) {
	oldNorm := backend.NormalizeName(oldName)
	newNorm := backend.NormalizeName(newName)
	if oldNorm == "" {
		return http.StatusBadRequest, fmt.Errorf("pool name is empty after normalization")
	}
	if newNorm == "" {
		return http.StatusBadRequest, fmt.Errorf("new pool name is empty after normalization")
	}
	if oldNorm == newNorm {
		return http.StatusBadRequest, fmt.Errorf("rename source and target normalize to the same name %q", newNorm)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	c, ok := p.byPool[oldNorm]
	if !ok || !p.reg.HasPool(oldNorm) {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}
	// Mirror AddPool's conflict check (both byPool and reg) so the two views
	// cannot disagree. newNorm != oldNorm is guaranteed by the early return
	// above, so neither check needs to special-case the same-name case.
	if other, exists := p.byPool[newNorm]; exists && other != c {
		return http.StatusConflict, fmt.Errorf("pool %s already exists", newNorm)
	}
	if p.reg.HasPool(newNorm) {
		return http.StatusConflict, fmt.Errorf("pool %s already exists", newNorm)
	}

	next, err := p.reg.WithPoolRenamed(oldNorm, newNorm)
	if err != nil {
		// Defensive: the pre-checks above caught every conflict the registry
		// could surface (unknown source, identical target, name collision),
		// so this branch is unreachable today. Kept so a future loosening of
		// the pre-checks still maps a registry-side refusal to 400 rather
		// than leaking a 500.
		return http.StatusBadRequest, err
	}

	// Rewire the controller to the new name. The membership, disabled set,
	// priority are unchanged by the rename — only the
	// byPool key and the atomic poolName field need to move. reconcileLocked
	// is intentionally NOT called here: it would re-read from reg.PoolNicks
	// with the now-stale c.poolName if the swap ordering slipped, and
	// nothing the rename changed requires it.
	c.mu.Lock()
	c.reg = next
	newNameCopy := newNorm
	c.poolName.Store(&newNameCopy)
	c.mu.Unlock()

	delete(p.byPool, oldNorm)
	p.byPool[newNorm] = c
	p.reg = next
	p.markConfigDirtyLocked()
	// Fire the state-dirty callback so the next flush rewrites the persisted
	// sticky/exhausted/local-snapshot map under the new key. The callback is
	// a non-blocking channel send (matching markConfigDirtyLocked's pattern),
	// so calling it under p.mu is safe.
	if p.onMutate != nil {
		p.onMutate()
	}
	fmt.Fprintf(p.logOut, "auto: renamed pool %s -> %s\n", oldNorm, newNorm)
	return http.StatusOK, nil
}

// SetOnMutate installs a callback that every controller calls (non-blocking)
// after any mutation to its sticky pointer, exhausted map, or worker
// affinities. The persister uses it to coalesce writes without importing this
// package. The callback is retained on Pools so a runtime-created controller
// (AddPool) is wired to the same persister.
func (p *Pools) SetOnMutate(fn func()) {
	p.mu.Lock()
	p.onMutate = fn
	controllers := make([]*Controller, 0, len(p.byPool))
	for _, c := range p.byPool {
		controllers = append(controllers, c)
	}
	p.mu.Unlock()
	for _, c := range controllers {
		c.onMutate = fn
	}
}

// SetOnConfigChange installs the callback fired (non-blocking) after any
// operator mutation, so the configfile writer flushes the new config to disk
// (issue #198). Retained on Pools so it survives across runtime pool creation.
func (p *Pools) SetOnConfigChange(fn func()) {
	p.mu.Lock()
	p.onConfigChange = fn
	p.mu.Unlock()
}

// SetDefaultBaseURL installs a call-time resolver for the gateway default
// upstream used by the add-member base_url fallback (issue #302). main wires
// it only in env-only mode, where ANTHROPIC_BASE_URL is the live config
// source; a nil (or empty-returning) resolver falls back to the registry's
// build-time default, which in file mode is the authoritative value (env is
// never consulted again there, issue #198).
func (p *Pools) SetDefaultBaseURL(fn func() string) {
	p.mu.Lock()
	p.defaultBaseURL = fn
	p.mu.Unlock()
}

// gatewayDefaultBaseURLLocked resolves the gateway default upstream for the
// add-member fallback: the installed resolver's value when it yields one,
// else the registry's build-time default. Caller holds p.mu.
func (p *Pools) gatewayDefaultBaseURLLocked() string {
	if p.defaultBaseURL != nil {
		if v := p.defaultBaseURL(); v != "" {
			return v
		}
	}
	return p.reg.DefaultBaseURL()
}

// CurrentRegistry returns the current authoritative registry — the operator
// intent the configfile writer serializes to aqg.json. Safe for concurrent
// use with runtime mutations.
func (p *Pools) CurrentRegistry() *backend.Registry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.reg
}

// markConfigDirtyLocked triggers a config-file flush. Caller holds p.mu; the
// callback is non-blocking (a channel signal), so calling it under the lock is
// safe — the actual serialization runs later in the writer goroutine.
func (p *Pools) markConfigDirtyLocked() {
	if p.onConfigChange != nil {
		p.onConfigChange()
	}
}

// applyRegistryLocked installs next as the authoritative registry and
// reconciles the named pools' controllers from it, preserving each
// controller's runtime observation (sticky/exhausted/local-snapshot).
// Caller holds p.mu. Only the named pools are reconciled — every other pool's
// membership is byte-identical in next, so re-deriving it would be a no-op.
func (p *Pools) applyRegistryLocked(next *backend.Registry, pools ...string) {
	p.reg = next
	for _, name := range pools {
		if c, ok := p.byPool[name]; ok {
			c.mu.Lock()
			c.reconcileLocked(next)
			c.mu.Unlock()
		}
	}
}

// crossPoolResolve scans every pool other than skipPool in reg for a member
// with the given (normalized) nick, collecting the distinct credentials and
// distinct resolved base URLs. Used to fill an omitted credential / base_url
// when re-adding a known subscription by name. Lock-free: reg is immutable.
//
// Reachability: every nick that is actually in reg surfaces here — except
// one carrying the same name as skipPool, which the destination pool already
// owns (so adding it there is a 409, not a re-declaration). The registry is
// the single source of operator intent (issue #198): env declarations flow
// in via backend.Load, runtime mutations land via WithMemberSet/With* under
// p.mu, and aqg.json reloads via BuildFromSpec. Build-time validation
// rejects empty credentials (internal/backend/backend.go) and the
// nick↔credential bijection means every registered member of nick has
// exactly one credential — there is no registry path where an in-registry
// nick is invisible to this scan.
func crossPoolResolve(reg *backend.Registry, skipPool, nick string) (creds, baseURLs []string) {
	credSeen := make(map[string]bool)
	urlSeen := make(map[string]bool)
	for _, name := range reg.PoolNames() {
		if name == skipPool {
			continue
		}
		b, ok := reg.ResolveIn(name, nick)
		if !ok {
			continue
		}
		if b.Credential != "" && !credSeen[b.Credential] {
			credSeen[b.Credential] = true
			creds = append(creds, b.Credential)
		}
		if b.BaseURL != "" && !urlSeen[b.BaseURL] {
			urlSeen[b.BaseURL] = true
			baseURLs = append(baseURLs, b.BaseURL)
		}
	}
	return creds, baseURLs
}

// nickNotResolvableError builds the credential-required 400 body for the
// (0 credentials, 0 URLs) result of crossPoolResolve. The message names the
// pools the current registry actually holds (so the operator can see whether
// the missing declaration is one they expected to be there, or simply not
// configured) and closes with the one-line recipe. Only registry *names* are
// echoed — credentials and base URLs are intentionally omitted.
//
// Exclusivity: callers must only invoke this when both returned slices are
// empty. A result like (0 credentials, ≥1 URL) — impossible today because
// build validation rejects empty credentials, but a future schema could
// permit it — must keep its own credential-required failure rather than emit
// this "no declaration anywhere" message, since the nick *is* visible to the
// registry, just without a credential to copy.
func nickNotResolvableError(nick string, reg *backend.Registry) error {
	names := reg.PoolNames() // already sorted
	return fmt.Errorf(
		"credential is required: nick %s is not a known subscription in any pool of the current registry (pools: %s); supply credential + base_url, or restore the missing pool from env / aqg.json",
		nick,
		strings.Join(names, ", "),
	)
}

// propagateCredentialPark mirrors a store-unrepresentable park (401/403, or
// a header-less 429 fallback) into every sibling pool holding nick, so a
// park caught by one pool is immediately visible everywhere the credential
// is shared (issue #254 AC1/AC2/AC4). originPool is excluded — its own
// controller already wrote the entry itself.
//
// It never holds two Controller.mu locks at once: each sibling is locked,
// written, and released before the next is touched, and reg is read
// lock-free (immutable-after-build). So two pools parking the same nick
// concurrently cannot deadlock regardless of pool-name ordering — each
// caller just serializes briefly, one sibling at a time, on whichever
// controller it happens to be writing (issue #254 AC6). An existing later
// reset wins, because propagation can be delayed behind a sibling's own
// observation and overwriting it would shorten the active park. Equal resets
// retain the existing entry so its windowFact classification is preserved
// (issue #275).
func (p *Pools) propagateCredentialPark(originPool, nick string, reset time.Time, windowFact bool) {
	reg := p.CurrentRegistry()
	for _, name := range reg.PoolNames() {
		if name == originPool {
			continue
		}
		if _, ok := reg.ResolveIn(name, nick); !ok {
			continue
		}
		c, ok := p.controller(name)
		if !ok {
			continue
		}
		c.mu.Lock()
		changed := false
		if existing, ok := c.credentialPark[nick]; !ok || reset.After(existing.reset) {
			c.credentialPark[nick] = credentialParkEntry{reset: reset, windowFact: windowFact}
			changed = true
		}
		if changed {
			c.notifyMutate()
		}
		c.mu.Unlock()
	}
}

// propagateCredentialParkClear releases nick's propagated credential park in
// every sibling pool holding it (issue #254 AC5/AC12), returning the sibling
// pool names actually released. Same one-lock-at-a-time discipline as
// propagateCredentialPark.
func (p *Pools) propagateCredentialParkClear(originPool, nick string) []string {
	reg := p.CurrentRegistry()
	var released []string
	for _, name := range reg.PoolNames() {
		if name == originPool {
			continue
		}
		if _, ok := reg.ResolveIn(name, nick); !ok {
			continue
		}
		c, ok := p.controller(name)
		if !ok {
			continue
		}
		c.mu.Lock()
		if entry, had := c.credentialPark[nick]; had {
			delete(c.credentialPark, nick)
			// Also drop this sibling's own c.exhausted entry, but only when
			// it is the exact mirror record429WithSource wrote alongside
			// this credentialPark entry (same reset) — i.e. the sibling is
			// itself an origin that independently observed this same
			// credential-fatal park. A sibling can also hold a genuinely
			// unrelated, independently-observed windowed 429 in c.exhausted
			// for the same nick (different reset); deleting unconditionally
			// would wipe that real park too. The equality guard tells the
			// two cases apart without a provenance tag.
			if ev, hasE := c.exhausted[nick]; hasE && ev.Equal(entry.reset) {
				delete(c.exhausted, nick)
			}
			c.notifyMutate()
			released = append(released, name)
		}
		c.mu.Unlock()
	}
	return released
}

// wireCredentialParkPropagation installs c's cross-pool propagation
// callbacks (issue #254). It closes over c itself rather than a captured
// pool-name string, so a later rename (RenamePool) is picked up via c.name()
// at call time instead of propagating under a stale name.
func (p *Pools) wireCredentialParkPropagation(c *Controller) {
	c.propagatePark = func(nick string, reset time.Time, windowFact bool) {
		p.propagateCredentialPark(c.name(), nick, reset, windowFact)
	}
	c.propagateParkClear = func(nick string) []string {
		return p.propagateCredentialParkClear(c.name(), nick)
	}
}

// credentialParkEntry is one Controller.credentialPark value (issue #254):
// reset is the wall-clock time the park ages out. windowFact distinguishes
// the two store-unrepresentable subclasses the map holds, because they
// disagree on which retirement paths besides wall-clock aging, an explicit
// clear, and a successful recovery probe may drop the entry early:
//
//   - windowFact == false: a 401/403 credential rejection. This is a fact
//     about the credential itself, not a quota window — a fresh, healthy
//     store snapshot proves nothing about whether a since-revoked credential
//     still authenticates (the snapshot could easily predate the
//     revocation), so storeReconcilesParkLocked (#145) and the preemptor's
//     precise-reset supersession (noteRecovered) must never touch it.
//   - windowFact == true: a 429 whose resetFrom fell back to
//     defaultExhaustionWindow (the AC2 residue) — a real quota-window fact
//     the gateway simply lacks a precise bound for. The store or a later
//     precise reset CAN retire this early, exactly as it would for an
//     ordinary c.exhausted entry, so both mechanisms apply.
type credentialParkEntry struct {
	reset      time.Time
	windowFact bool
}

// Controller is the sticky selector for one pool. The zero value is not
// usable; call NewController.
type Controller struct {
	mu sync.Mutex

	reg *backend.Registry
	// poolName is the current byPool key this controller is registered under.
	// It is set at construction (NewController) and updated atomically by
	// (*Pools).RenamePool — the field is *atomic* so the ~20 lock-free readers
	// (logging, PoolStatus, the proxy response modifier) keep their shape
	// while the rename path can swap the value without coordinating with
	// every concurrent 429, preempt, or recovery-probe call site. Reads via
	// (*Controller).name(); the only writer outside construction is the
	// rename path, which holds c.mu and uses Store. Constructed non-nil so a
	// reader racing a Store never dereferences a nil pointer.
	poolName atomic.Pointer[string]

	// members is the ordered member collection, re-derived from the registry
	// on every reconcile (issue #198). A removed member is simply absent from
	// the registry, so it is absent here — there are no tombstones. Accessed
	// only under c.mu.
	members []memberEntry

	// store is the shared quota store. A member whose snapshot reports its
	// unified window fully consumed (with a reset still ahead) is treated as
	// exhausted even when no live 429 was seen on the proxy path — the only
	// exhaustion signal for poller-tracked backends (z.ai / MiniMaxi). nil
	// disables the signal, leaving pure 429-driven failover.
	store *quota.Store

	// priority is the effective preference order (highest first): the
	// operator-declared nicks first, then any unlisted members in sorted
	// order. It is re-derived from the registry on every reconcile (issue
	// #198 collapses the old static/override split — the registry priority is
	// the single source). nil for a pool with no declared priority, which
	// keeps the default random-start, round-robin-failover behaviour.
	priority []string

	// disabled maps member nicks to a disabled flag: a disabled member is
	// unselectable regardless of exhaustion state until re-enabled. It is
	// re-derived from each backend's Disabled flag on reconcile — operator
	// intent lives in the config file now, not a state-file overlay (issue
	// #198). Distinct from the exhausted map (which ages out on reset).
	// Accessed only under c.mu.
	disabled map[string]bool

	// curNick is the nick of the currently active member (the sticky pointer).
	// Replaces the old cur int + curAddedNick string pair. Accessed only
	// under c.mu.
	curNick string

	// exhausted maps a nick to the absolute time its blocking window resets.
	// Entries come from failed upstream responses or explicit live parks and
	// clear lazily once now >= reset. Status-bearing store snapshots can add a
	// separate bound at read time; statusless snapshots alone do not park.
	exhausted map[string]time.Time

	// lastProbeAttempt records the most recent recovery-probe attempt time
	// per quota key, used to rate-limit recovery probes to ≤1 per parked
	// member per cooldown window (issue #124). The preemptor uses the same
	// "act once per distinct reset" pattern via its own lastActed map; this
	// field is a per-Controller variant keyed by quota key. Accessed only
	// under c.mu.
	lastProbeAttempt map[string]time.Time

	// probeInFlight marks a quota key whose recovery probe is currently
	// running on another goroutine; concurrent "all exhausted" requests
	// see the flag and skip the probe for that member, coalescing to one
	// probe per parked member. Accessed only under c.mu.
	probeInFlight map[string]bool

	// probeHTTPClient is the HTTP client used for recovery probes. Defaults
	// to http.DefaultClient; tests inject a client backed by httptest to
	// control probe responses. Accessed without a lock — set once at
	// construction (or via SetProbeHTTPClient in tests) and never modified
	// after.
	probeHTTPClient *http.Client

	now    func() time.Time
	logOut io.Writer

	// onMutate, if non-nil, is called (non-blocking) after any mutation to
	// cur or exhausted. Set by Pools.SetOnMutate to notify the persister.
	onMutate func()

	// poolLocalSnapshots records the nicks for which this controller has
	// itself observed a quota snapshot (header observer or poller tick) since
	// the controller was created. A member is only attached a snapshot in
	// poolStatus when its nick is in this set; otherwise the cell renders
	// "-" to avoid a brief cross-pool flash when a nick already used by
	// another pool is runtime-added here (issue #111). Static members are
	// seeded at construction; runtime-added members are NOT seeded and must
	// wait for the first observation before their cell shows data. Accessed
	// only under c.mu.
	poolLocalSnapshots map[string]struct{}

	// topStatusLogged records nicks we've already announced for the
	// #253 latched top-level UnifiedStatus: when the store carries a
	// top-level "rejected" and no per-window status, log one line per
	// nick — the field is non-routing post-fix (snapRejects no longer
	// reads it), so this log is the only trace that a poller-tracked
	// provider ever set it. Pruned on reconcileLocked so a re-added
	// member can re-trigger; accessed only under c.mu.
	topStatusLogged map[string]struct{}

	// credentialPark maps a nick to a store-unrepresentable park — a 401/403
	// credential rejection, or a 429 whose resetFrom fell back to
	// defaultExhaustionWindow because the response carried no usable reset —
	// keeping it unselectable in THIS pool, whether the park was asserted
	// here or propagated in from a sibling pool that shares the nick (issue
	// #254). Unlike c.exhausted, no store snapshot will ever teach a sibling
	// pool about this bound, so it cannot be re-derived on read; it must be
	// copied. Populated in two ways: locally, by record429WithSource under
	// c.mu when storeUnrepresentable is true; and remotely, by
	// (*Pools).propagateCredentialPark writing directly into this map under
	// c.mu from another controller's assertion. Read alongside c.exhausted by
	// exhaustedUntilLocked (routing), liveParkActiveLocked (MemberStatus.Parked
	// / the clear button), and nextParkedButResetPassedLocked (the half-open
	// probe, which must never forward live traffic to a credential-dead
	// member). recoverParked's candidate scan does not range this map
	// directly — it ranges c.exhausted, which record429WithSource always
	// populates alongside credentialPark for a local assertion — but on a
	// successful probe it now clears both maps together (a live health-check
	// response is decisive evidence for either subclass) and propagates the
	// clear. Accessed only under c.mu.
	credentialPark map[string]credentialParkEntry

	// workerAffinity stores the last member assigned to each worker. Above
	// concurrency 1, a mapped worker bypasses global sticky and preempt while
	// its member remains in the current availability window.
	// Stale targets are rechecked on that worker's next request. Accessed only
	// under c.mu.
	workerAffinity map[string]string
	// workerCursor names the next member in the current window for a new
	// assignment. It is persisted by member nick so order changes can
	// reconcile it safely.
	workerCursor string
	// workerConcurrency is the configured number of available members allowed
	// to serve namespaced workers. One delegates those requests to ResolveAuto.
	workerConcurrency int

	// propagatePark, when set, mirrors a just-asserted store-unrepresentable
	// park into every sibling pool holding the same nick (issue #254 AC1/AC2).
	// Wired once by Pools at controller construction (NewPools/AddPool/
	// CreatePoolWithMember); nil for a bare Controller built directly by a
	// test via NewController, where there is no sibling to reach. Always
	// called with c.mu NOT held — see record429WithSource's doc for why
	// holding it here would risk a lock-order inversion.
	propagatePark func(nick string, reset time.Time, windowFact bool)

	// propagateParkClear, when set, releases nick's propagated credential
	// park in every sibling pool holding it (issue #254 AC5/AC12), returning
	// the sibling pool names actually released so the operator-facing
	// response can name them. Wired alongside propagatePark; nil for a bare
	// Controller. Always called with c.mu NOT held.
	propagateParkClear func(nick string) []string
}

// NewController builds the sticky selector over the members of poolName
// in reg. When start < 0 the initial sticky backend is chosen at random
// (the spec's rotating start index — no probe, so any starting point is
// equally valid); otherwise start selects the index deterministically
// (used by tests). now defaults to time.Now and logOut to os.Stderr when
// nil.
func NewController(reg *backend.Registry, poolName string, start int, store *quota.Store, now func() time.Time, logOut io.Writer) *Controller {
	if now == nil {
		now = time.Now
	}
	if logOut == nil {
		logOut = os.Stderr
	}
	configNicks := reg.PoolNicks(poolName) // sorted; Load guarantees at least one per pool

	// Seed the unified member collection from config. Each nick is resolved
	// once at boot via reg.ResolveIn; the same insertion path is used for
	// runtime-added members so there is no separate static slice (issue #185).
	members := make([]memberEntry, 0, len(configNicks))
	for _, nick := range configNicks {
		b, ok := reg.ResolveIn(poolName, nick)
		if !ok {
			continue // registry guarantees resolution; guard for safety
		}
		members = append(members, memberEntry{
			Nick:       nick,
			Credential: b.Credential,
			BaseURL:    b.BaseURL,
		})
	}

	// Seed poolLocalSnapshots with the config nicks so a pool that was live
	// across a code upgrade (or that has always had a member) does not flash
	// "-" for nicks that already have a snapshot in the shared store from
	// the matching pool's traffic. Runtime-added members are NOT seeded
	// here; they are added in loadRuntimeConfig, and any persisted
	// LocalSnapshotNicks entries that name runtime-added members land via
	// applyPendingLocalSnapshotsLocked at the end of LoadRuntimeConfig
	// (issue #111).
	local := make(map[string]struct{}, len(configNicks))
	for _, n := range configNicks {
		local[n] = struct{}{}
	}

	nicks := configNicks // used only for effectiveOrder below; not stored
	c := &Controller{
		reg:                reg,
		members:            members,
		priority:           effectiveOrder(reg.PoolPriority(poolName), nicks),
		workerConcurrency:  reg.PoolConcurrency(poolName),
		store:              store,
		exhausted:          make(map[string]time.Time),
		credentialPark:     make(map[string]credentialParkEntry),
		workerAffinity:     make(map[string]string),
		lastProbeAttempt:   make(map[string]time.Time),
		probeInFlight:      make(map[string]bool),
		probeHTTPClient:    http.DefaultClient,
		now:                now,
		logOut:             logOut,
		disabled:           make(map[string]bool),
		poolLocalSnapshots: local,
		topStatusLogged:    make(map[string]struct{}),
	}
	// Initialise the atomic pool-name pointer before any other path can observe
	// the controller. NewController hands the *Controller back to the caller
	// (Pools, tests) which may immediately read c.name() under no lock; a
	// nil pointer would crash there.
	c.poolName.Store(&poolName)
	// Seed the disabled set from the registry (config is the single source of
	// truth for the disabled flag now — issue #198).
	for _, m := range members {
		if b, ok := reg.ResolveIn(poolName, m.Nick); ok && b.Disabled {
			c.disabled[m.Nick] = true
		}
	}
	n := len(members)
	if n == 0 {
		// Defensive: a pool with no members should never reach here, but
		// guard so curNick stays empty safely.
		return c
	}
	if start < 0 {
		// A priority pool anchors on its highest-priority member (nothing is
		// exhausted at construction, so that is priority[0]); a plain pool
		// starts at a random member as before.
		if len(c.priority) > 0 {
			if idx := c.indexOf(c.priority[0]); idx >= 0 {
				start = idx
			} else {
				start = 0
			}
		} else {
			start = randIndex(n)
		}
	}
	start = ((start % n) + n) % n
	c.curNick = c.members[start].Nick
	return c
}

// reconcileLocked re-derives this controller's membership, disabled set,
// effective priority and worker concurrency from reg
// (the new authoritative registry after a copy-on-write mutation, issue #198).
// It preserves runtime observations; stale worker targets, including removed
// members, are rechecked on the worker's next request. Other per-member
// observations for removed members are pruned. If the active sticky member
// left the pool, the pointer force-switches to the next healthy member (as on
// a removal/429); a pool that just gained its first member anchors its pointer.
// Caller holds c.mu.
func (c *Controller) reconcileLocked(reg *backend.Registry) {
	c.reg = reg
	nicks := reg.PoolNicks(c.name()) // sorted
	present := make(map[string]bool, len(nicks))
	members := make([]memberEntry, 0, len(nicks))
	disabled := make(map[string]bool)
	for _, nick := range nicks {
		b, ok := reg.ResolveIn(c.name(), nick)
		if !ok {
			continue
		}
		present[nick] = true
		members = append(members, memberEntry{Nick: nick, Credential: b.Credential, BaseURL: b.BaseURL})
		if b.Disabled {
			disabled[nick] = true
		}
	}
	c.members = members
	c.disabled = disabled
	c.priority = effectiveOrder(reg.PoolPriority(c.name()), nicks)
	c.workerConcurrency = reg.PoolConcurrency(c.name())

	// Prune runtime observation for members that left the pool.
	for nick := range c.exhausted {
		if !present[nick] {
			delete(c.exhausted, nick)
		}
	}
	for nick := range c.credentialPark {
		if !present[nick] {
			delete(c.credentialPark, nick)
		}
	}
	for nick := range c.poolLocalSnapshots {
		if !present[nick] {
			delete(c.poolLocalSnapshots, nick)
		}
	}
	for nick := range c.topStatusLogged {
		if !present[nick] {
			delete(c.topStatusLogged, nick)
		}
	}
	c.reconcileWorkerAffinityLocked()

	switch {
	case c.curNick != "" && !present[c.curNick]:
		gone := c.curNick
		c.curNick = ""
		if next, ok := c.firstHealthyNickLocked(); ok {
			c.setActiveMemberLocked(next)
			fmt.Fprintf(c.logOut, "auto[%s]: switched %s -> %s (member %s left the pool)\n", c.name(), gone, next, gone)
		}
	case c.curNick == "" && len(members) > 0:
		if next, ok := c.firstHealthyNickLocked(); ok {
			c.setActiveMemberLocked(next)
		}
	}
}

// effectiveOrder expands a declared priority subset into a total order
// over the pool's members: the declared nicks first (highest priority
// first), then any members not named in the declaration, in their stable
// sorted order. It returns nil when no priority was declared, which is the
// signal to keep the default random/round-robin behaviour.
func effectiveOrder(declared, nicks []string) []string {
	if len(declared) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(declared))
	out := make([]string, 0, len(nicks))
	for _, nick := range declared {
		if !seen[nick] {
			seen[nick] = true
			out = append(out, nick)
		}
	}
	for _, nick := range nicks {
		if !seen[nick] {
			out = append(out, nick)
		}
	}
	return out
}

// workerOrderLocked is the stable first-use cycle for worker assignments:
// effective priority order when configured, otherwise the sorted member order.
// Caller holds c.mu.
func (c *Controller) workerOrderLocked() []string {
	if len(c.priority) > 0 {
		return c.priority
	}
	return c.allMemberNicksLocked()
}

// workerWindowLocked returns the first workerConcurrency available members
// in effective order. A value above the member count naturally includes all
// available members. Caller holds c.mu.
func (c *Controller) workerWindowLocked() []string {
	if c.workerConcurrency <= 0 {
		return nil
	}
	order := c.workerOrderLocked()
	limit := c.workerConcurrency
	if limit > len(order) {
		limit = len(order)
	}
	window := make([]string, 0, limit)
	for _, nick := range order {
		if c.isUnavailableLocked(nick) {
			continue
		}
		window = append(window, nick)
		if len(window) == limit {
			break
		}
	}
	return window
}

// normalizeWorkerCursorLocked moves a cursor outside the current worker
// window to its first member. It returns whether the persisted cursor changed.
// Caller holds c.mu.
func (c *Controller) normalizeWorkerCursorLocked() bool {
	window := c.workerWindowLocked()
	for _, nick := range window {
		if nick == c.workerCursor && c.workerCursor != "" {
			return false
		}
	}
	old := c.workerCursor
	c.workerCursor = ""
	if len(window) > 0 {
		c.workerCursor = window[0]
	}
	return old != c.workerCursor
}

// nextWorkerMemberLocked returns the next nick from the current worker window
// and the following window member as cursor. Caller holds c.mu.
func (c *Controller) nextWorkerMemberLocked() (nick, nextCursor string, ok bool) {
	window := c.workerWindowLocked()
	if len(window) == 0 {
		return "", "", false
	}
	start := 0
	for i, candidate := range window {
		if candidate == c.workerCursor {
			start = i
			break
		}
	}
	return window[start], window[(start+1)%len(window)], true
}

// nextWorkerReassignmentLocked chooses the least-used member in the current
// window, breaking ties from the round-robin cursor. That keeps workers on
// unaffected members in place while filling a newly opened fallback slot.
// Caller holds c.mu.
func (c *Controller) nextWorkerReassignmentLocked() (nick, nextCursor string, ok bool) {
	window := c.workerWindowLocked()
	if len(window) == 0 {
		return "", "", false
	}
	counts := make(map[string]int, len(window))
	for _, candidate := range window {
		counts[candidate] = 0
	}
	for _, assigned := range c.workerAffinity {
		if _, inWindow := counts[assigned]; inWindow {
			counts[assigned]++
		}
	}
	min := int(^uint(0) >> 1)
	for _, candidate := range window {
		if counts[candidate] < min {
			min = counts[candidate]
		}
	}
	start := 0
	for i, candidate := range window {
		if candidate == c.workerCursor {
			start = i
			break
		}
	}
	for offset := 0; offset < len(window); offset++ {
		i := (start + offset) % len(window)
		if counts[window[i]] == min {
			return window[i], window[(i+1)%len(window)], true
		}
	}
	return "", "", false
}

// reconcileWorkerAffinityLocked clears all assignments at concurrency 1.
// Above 1, it leaves stale member references for ResolveWorker to reassign on
// that worker's next request, and repairs the round-robin cursor. A worker that
// never returns can therefore leave a stale runtime entry in the state file.
// Caller holds c.mu.
func (c *Controller) reconcileWorkerAffinityLocked() bool {
	changed := false
	if c.workerConcurrency <= 1 {
		if len(c.workerAffinity) > 0 || c.workerCursor != "" {
			clear(c.workerAffinity)
			c.workerCursor = ""
			changed = true
		}
		if changed {
			c.notifyMutate()
		}
		return changed
	}
	for worker := range c.workerAffinity {
		if !backend.IsValidWorkerNickname(worker) {
			delete(c.workerAffinity, worker)
			changed = true
		}
	}
	if c.normalizeWorkerCursorLocked() {
		changed = true
	}
	if changed {
		c.notifyMutate()
	}
	return changed
}

// indexOf returns the index of nick in c.members, or -1 if absent. Pools are
// small, so a linear scan is cheaper than maintaining a map.
func (c *Controller) indexOf(nick string) int {
	for i, m := range c.members {
		if m.Nick == nick {
			return i
		}
	}
	return -1
}

// allMemberNicksLocked returns all member nicks, sorted. Removal is now
// absence from the registry (issue #198), so there is no tombstone filter.
// Caller holds c.mu.
func (c *Controller) allMemberNicksLocked() []string {
	out := make([]string, 0, len(c.members))
	for _, m := range c.members {
		out = append(out, m.Nick)
	}
	sort.Strings(out)
	return out
}

// effectivePriorityLocked returns the effective priority order for this pool,
// re-derived from the registry on reconcile (issue #198 collapsed the old
// static/override split into a single config-sourced order). Returns nil for a
// non-priority pool. Caller holds c.mu.
func (c *Controller) effectivePriorityLocked() []string {
	return c.priority
}

// isUnavailableLocked reports whether nick is currently unavailable for
// selection, by either signal: exhausted (live 429 or store-driven) or
// operator-disabled. A removed member is simply absent from c.members (issue
// #198), so it never reaches this check. The disabled flag is never
// auto-cleared, unlike exhausted marks which age out on reset. Caller holds c.mu.
func (c *Controller) isUnavailableLocked(nick string) bool {
	if c.disabled[nick] {
		return true
	}
	_, ok := c.exhaustedUntilLocked(nick)
	return ok
}

// ResolveAuto returns the backend a request to this pool should use now.
// When the whole pool is exhausted it returns exhausted=true with the
// soonest-resetting member and the wait until that reset; the caller
// emits an honest 429.
func (c *Controller) ResolveAuto() (backend.Backend, time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clearExpiredLocked()

	// A member-less pool (freshly created runtime pool with no members yet)
	// has an empty curNick; skip the healthy-current branch and fall through
	// to the exhausted return so the caller receives an honest 429.
	if c.curNick != "" && !c.isUnavailableLocked(c.curNick) {
		if b, ok := c.backendByNickLocked(c.curNick); ok {
			return b, 0, false
		}
	}

	// Current is unavailable; find a healthy replacement.
	if nick, ok := c.firstHealthyNickLocked(); ok {
		c.setActiveMemberLocked(nick)
		if b, ok := c.backendByNickLocked(nick); ok {
			return b, 0, false
		}
	}

	// All-parked half-open (issue #134): when firstHealthyNickLocked
	// returned false, no member is strictly healthy. But the pool can
	// still be in a recoverable state — every member's live-429 reset
	// may have elapsed while the quota store still reports "rejected"
	// with a future reset. Forwarding one request through to such a
	// member refreshes the store via the normal record429 / store-write
	// path, breaking the deadlock where a pool of all-parked members
	// never sees a forwarded request and never self-heals. We pick
	// round-robin from the current position and return it with
	// exhausted=false so the middleware forwards.
	if nick, ok := c.nextParkedButResetPassedLocked(); ok {
		c.setActiveMemberLocked(nick)
		if b, ok := c.backendByNickLocked(nick); ok {
			return b, 0, false
		}
	}

	// All exhausted: point at the soonest to free up.
	nick, reset := c.soonestNickLocked()
	c.setActiveMemberLocked(nick)
	if b, ok := c.backendByNickLocked(nick); ok {
		return b, c.waitUntil(reset), true
	}
	// Should never reach here, but return zero values for safety.
	return backend.Backend{}, 0, true
}

// ResolveWorker routes worker through global sticky behavior when concurrency
// is 1. Above 1, workers keep hard affinity within the first N available
// members in effective order. When no member is available, it reports the
// same pool-dry wait without assigning an unavailable nick.
func (c *Controller) ResolveWorker(worker string) (backend.Backend, time.Duration, bool) {
	if !backend.IsValidWorkerNickname(worker) {
		return backend.Backend{}, 0, true
	}
	c.mu.Lock()
	if c.workerConcurrency <= 1 {
		c.mu.Unlock()
		return c.ResolveAuto()
	}
	defer c.mu.Unlock()

	c.clearExpiredLocked()
	mutated := c.normalizeWorkerCursorLocked()
	window := c.workerWindowLocked()
	inWindow := make(map[string]bool, len(window))
	for _, nick := range window {
		inWindow[nick] = true
	}
	reassignment := false
	if nick, exists := c.workerAffinity[worker]; exists {
		if inWindow[nick] {
			if b, ok := c.backendByNickLocked(nick); ok {
				if mutated {
					c.notifyMutate()
				}
				return b, 0, false
			}
		}
		delete(c.workerAffinity, worker)
		mutated = true
		reassignment = true
	}

	var nick, nextCursor string
	var ok bool
	if reassignment {
		nick, nextCursor, ok = c.nextWorkerReassignmentLocked()
	} else {
		nick, nextCursor, ok = c.nextWorkerMemberLocked()
	}
	if ok {
		c.workerAffinity[worker] = nick
		c.workerCursor = nextCursor
		if b, ok := c.backendByNickLocked(nick); ok {
			c.notifyMutate()
			return b, 0, false
		}
		delete(c.workerAffinity, worker)
		mutated = true
	}

	if mutated {
		c.notifyMutate()
	}
	// The worker has no assignment while every member is unavailable. The
	// returned backend is only used for the existing pool-dry response path.
	nick, reset := c.soonestNickLocked()
	if b, ok := c.backendByNickLocked(nick); ok {
		return b, c.waitUntil(reset), true
	}
	return backend.Backend{}, 0, true
}

// parkedNicksSnapshot returns the sorted, deduplicated union of nicks
// currently held in c.exhausted and c.credentialPark, without clearing
// anything — the non-destructive read ClearAllExhausted uses to compute a
// deterministic per-pool report before any pool's clear can run (issue #254).
func (c *Controller) parkedNicksSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.exhausted) == 0 && len(c.credentialPark) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(c.exhausted)+len(c.credentialPark))
	var nicks []string
	for nick := range c.exhausted {
		seen[nick] = true
		nicks = append(nicks, nick)
	}
	for nick := range c.credentialPark {
		if !seen[nick] {
			seen[nick] = true
			nicks = append(nicks, nick)
		}
	}
	sort.Strings(nicks)
	return nicks
}

// ClearExhausted drops every recorded park for this pool, including any
// store-unrepresentable credential park — local or propagated from a sibling
// pool (issue #254) — making each member immediately selectable again unless
// a status-bearing rejected window still blocks it. A statusless full
// snapshot alone does not recreate the park. It exists to
// undo parks written by a transient or erroneous upstream 429 — e.g. an
// account that got 429'd by a misconfigured request but in fact still has
// quota or whose full statusless window did not prove exhaustion. It does
// NOT clear status-bearing rejected windows, which remain store-derived and
// clear on reset. Returns the nicks whose park was
// cleared, sorted, plus — for any of those nicks that carried a propagated
// credential park — the sibling pools the clear also released it in (issue
// #254 AC5/AC12); nil when nothing here was ever propagated.
func (c *Controller) ClearExhausted() (cleared []string, releasedElsewhere map[string][]string) {
	c.mu.Lock()
	if len(c.exhausted) == 0 && len(c.credentialPark) == 0 {
		c.mu.Unlock()
		return nil, nil
	}
	seen := make(map[string]bool, len(c.exhausted)+len(c.credentialPark))
	for nick := range c.exhausted {
		seen[nick] = true
		cleared = append(cleared, nick)
	}
	var propagate []string
	for nick := range c.credentialPark {
		if !seen[nick] {
			seen[nick] = true
			cleared = append(cleared, nick)
		}
		propagate = append(propagate, nick)
	}
	sort.Strings(cleared)
	sort.Strings(propagate)
	c.exhausted = make(map[string]time.Time)
	c.credentialPark = make(map[string]credentialParkEntry)
	c.lastProbeAttempt = make(map[string]time.Time)
	c.probeInFlight = make(map[string]bool)
	c.notifyMutate()
	c.mu.Unlock()

	if c.propagateParkClear != nil {
		for _, nick := range propagate {
			if released := c.propagateParkClear(nick); len(released) > 0 {
				if releasedElsewhere == nil {
					releasedElsewhere = make(map[string][]string)
				}
				releasedElsewhere[nick] = released
			}
		}
	}
	return cleared, releasedElsewhere
}

// ClearExhaustedNick drops a single member's recorded park (issue #147),
// including any store-unrepresentable credential park (issue #254) — the
// per-nick counterpart to ClearExhausted: an operator escape hatch to
// un-stick one over-parked member without clearing the whole pool. Same
// recorded-park-only contract — status-bearing rejected windows remain
// untouched. A statusless full snapshot can re-park only after a failed
// upstream response. Returns whether a park was actually present (false is a
// harmless no-op for an unknown or un-parked nick), plus the sibling pools
// the clear also released the propagated park in (issue #254 AC5/AC12).
// notifyMutate fires only when something changed.
func (c *Controller) ClearExhaustedNick(nick string) (cleared bool, releasedElsewhere []string) {
	c.mu.Lock()
	_, inExhausted := c.exhausted[nick]
	_, inCredentialPark := c.credentialPark[nick]
	if !inExhausted && !inCredentialPark {
		c.mu.Unlock()
		return false, nil
	}
	delete(c.exhausted, nick)
	delete(c.credentialPark, nick)
	delete(c.lastProbeAttempt, nick)
	delete(c.probeInFlight, nick)
	c.notifyMutate()
	c.mu.Unlock()

	if inCredentialPark && c.propagateParkClear != nil {
		releasedElsewhere = c.propagateParkClear(nick)
	}
	return true, releasedElsewhere
}

// Current returns the nick of the active sticky backend, or "" for a
// member-less pool (a freshly created runtime pool with nothing to route to).
func (c *Controller) Current() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curNick
}

// CurrentBackend returns the active sticky backend, for the quota view. A
// member-less pool returns the zero Backend.
func (c *Controller) CurrentBackend() backend.Backend {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.backendByNickLocked(c.curNick); ok {
		return b
	}
	return backend.Backend{}
}

// notifyMutate calls c.onMutate if set. It is safe to call while holding
// c.mu because onMutate is a non-blocking channel send in the persister.
func (c *Controller) notifyMutate() {
	if c.onMutate != nil {
		c.onMutate()
	}
}

// MarkLocalSnapshot records that this controller has itself observed a
// quota snapshot for nick — either from a real upstream response the
// observer captured, or from a poller tick for a tracked backend. The
// first observation flips the nick into poolLocalSnapshots, after which
// poolStatus attaches snapshots for it; without an observation, the
// cell renders "-" so a runtime-added nick does not flash another pool's
// data (issue #111). No-op for nicks that are not a member of this pool.
// The controller's own persister is not poked: the new state is durable
// via the next persistState call and a "-"-only state has no user-visible
// cost if it does not survive a crash.
func (c *Controller) MarkLocalSnapshot(nick string) {
	if nick == "" {
		return
	}
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.backendByNickLocked(normalized); !ok {
		return
	}
	if _, ok := c.poolLocalSnapshots[normalized]; ok {
		return
	}
	c.poolLocalSnapshots[normalized] = struct{}{}
}

// seedLocalSnapshotLocked adds nick to poolLocalSnapshots without going
// through the public MarkLocalSnapshot gating. Used at restore time by
// loadRuntimeConfig / loadState so a runtime-added member that already
// saw traffic before a restart does not re-flash "-" after recovery.
// Caller holds c.mu.
func (c *Controller) seedLocalSnapshotLocked(nick string) {
	if nick == "" {
		return
	}
	if c.poolLocalSnapshots == nil {
		c.poolLocalSnapshots = make(map[string]struct{})
	}
	c.poolLocalSnapshots[nick] = struct{}{}
}

// setActiveMemberLocked moves the sticky pointer to nick and notifies the
// persister. Replaces the old cur/curAddedNick dual-pointer update.
// Caller holds c.mu.
func (c *Controller) setActiveMemberLocked(nick string) {
	c.curNick = nick
	c.notifyMutate()
}

// poolStatus builds the /_gateway/pool response for this controller. store
// is consulted for each member's latest snapshot; a member with no recorded
// snapshot gets snapshot:null. pl is consulted for the per-pool poller
// liveness observation (issue #247); pl may be nil (handlers in some
// tests construct without one), in which case the poller field is left
// nil and omitempty drops it from the wire. pollerMap is a pre-built
// snapshot of pl.Status() to avoid repeating the lock acquisition per
// pool in the AllPoolStatuses path (review of #267). Caller must not
// hold c.mu.
func (c *Controller) poolStatus(store *quota.Store, pl *poller.Poller, pollerMap map[string]poller.PoolStatus) PoolStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearExpiredLocked()

	// Build from the effective member set (all members − removed), matching
	// EffectiveConfig and the selection path, so removed members never surface
	// here. The unified collection covers all members regardless of origin.
	effective := c.allMemberNicksLocked()
	members := make([]MemberStatus, 0, len(effective))
	for _, nick := range effective {
		ms := MemberStatus{Nick: nick, Disabled: c.disabled[nick]}
		// exhausted is checked before the sticky (curNick) arm: "active" must
		// mean the sticky member that is ALSO currently available. A sticky
		// member that is parked is treated as unavailable by the routing path
		// (isUnavailableLocked → exhaustedUntilLocked), which 429s it, so it
		// must report exhausted here too — not a green "active" badge.
		if c.disabled[nick] {
			ms.Status = "disabled"
		} else if reset, ok := c.exhaustedUntilLocked(nick); ok {
			ms.Status = "exhausted"
			r := reset.UTC()
			ms.ExhaustedUntil = &r
		} else if nick == c.curNick {
			ms.Status = "active"
		} else {
			ms.Status = "idle"
		}
		ms.Parked = c.liveParkActiveLocked(nick)
		if b, ok := c.backendByNickLocked(nick); ok {
			// Only attach a snapshot when this controller has itself observed
			// traffic (or polled) for this nick. PR #113 makes the store key
			// the same across pools, so a nick already used in another pool
			// would otherwise show that pool's data the moment the runtime
			// add-member UI re-renders. Suppress until the first local
			// observation so the cell renders "-" instead of a stale 100%.
			if _, local := c.poolLocalSnapshots[nick]; local {
				snap := store.Get(b.QuotaKey())
				if snap.HasData() {
					snapCopy := snap
					ms.Snapshot = &snapCopy
				}
			}
		}
		members = append(members, ms)
	}
	out := PoolStatus{Pool: c.name(), Active: c.curNick, Concurrency: c.workerConcurrency, Members: members}
	// Per-pool poller liveness (issue #247, review of #267): the gate
	// is config-based (the active backend's BaseURL matches a
	// registered proprietary provider), not the poller's state map.
	// A tracked pool that has never been polled surfaces the never-
	// polled shape (last_success:null, stale:true) on this endpoint
	// so it agrees with the quota view; an untracked pool (Anthropic,
	// any unrecognised upstream) carries no `poller` key at all.
	// LookupStatus consults the prebuilt map when present and falls
	// back to a fresh Status() call for the single-pool path.
	if pl != nil {
		if baseURL, ok := c.activeBaseURLLocked(); ok {
			if ps, ok := pl.LookupStatus(pollerMap, c.name(), baseURL); ok {
				out.Poller = &ps
			}
		}
	}
	return out
}

// activeBaseURLLocked returns the current active backend's base URL,
// or ok=false when the controller has no resolvable active backend.
// Caller must hold c.mu.
func (c *Controller) activeBaseURLLocked() (string, bool) {
	if c.curNick == "" {
		return "", false
	}
	b, ok := c.backendByNickLocked(c.curNick)
	if !ok {
		return "", false
	}
	return b.BaseURL, true
}

// loadState applies persisted routing state. Exhausted entries whose reset
// has already passed are silently dropped. Persisted nicks absent from the
// current pool membership are logged and skipped. Called once at startup
// before the server begins serving; does not call onMutate. With config as
// the single source of truth (issue #198), every member — including
// previously runtime-added ones — is already present from NewController, so
// sticky and local-snapshot references resolve immediately (no deferral).
func (c *Controller) loadState(sticky string, exhausted map[string]time.Time, localSnapshots []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.indexOf(sticky) >= 0 {
		c.curNick = sticky
	} else if sticky != "" {
		reason := "random"
		if len(c.priority) > 0 {
			reason = "priority"
		}
		fmt.Fprintf(c.logOut, "loadState[%s]: persisted sticky=%s not in current pool members; keeping %s (%s)\n",
			c.name(), sticky, c.curNick, reason)
	}
	now := c.now()
	for nick, reset := range exhausted {
		if !reset.After(now) {
			continue
		}
		if c.indexOf(nick) < 0 {
			fmt.Fprintf(c.logOut, "loadState[%s]: dropping persisted exhausted entry %s (not in current pool members)\n",
				c.name(), nick)
			continue
		}
		c.exhausted[nick] = reset
	}
	// Restore the per-pool "we have seen traffic for this nick" set, dropping
	// entries that no longer name a current member.
	for _, nick := range localSnapshots {
		if nick == "" {
			continue
		}
		if _, ok := c.backendByNickLocked(nick); ok {
			c.seedLocalSnapshotLocked(nick)
		}
	}
	// The persisted sticky may now name a disabled/exhausted member; re-anchor
	// before serving so Current()/pool.active never point at an unavailable one.
	c.reanchorLocked()
}

// loadCredentialPark restores a persisted propagated/local credential park
// (issue #254 AC7), dropping any entry whose reset has already passed or
// that no longer names a current member — the same pruning loadState applies
// to c.exhausted, kept as a separate method so loadState's signature (and
// its existing call sites) stay untouched.
func (c *Controller) loadCredentialPark(credentialPark map[string]CredentialParkPersist) {
	if len(credentialPark) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for nick, persisted := range credentialPark {
		if !persisted.Reset.After(now) {
			continue
		}
		if c.indexOf(nick) < 0 {
			continue
		}
		c.credentialPark[nick] = credentialParkEntry{reset: persisted.Reset, windowFact: persisted.WindowFact}
	}
}

// loadWorkerAffinity restores worker identities after routing and credential
// parks have been loaded. Assignments are rechecked against the current window
// on first use, so stale targets remain available as reassign markers.
func (c *Controller) loadWorkerAffinity(affinity map[string]string, cursor string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerAffinity = make(map[string]string, len(affinity))
	if c.workerConcurrency <= 1 {
		c.workerCursor = ""
		return
	}
	for worker, nick := range affinity {
		if !backend.IsValidWorkerNickname(worker) || nick == "" {
			continue
		}
		c.workerAffinity[worker] = nick
	}
	c.workerCursor = cursor
	c.normalizeWorkerCursorLocked()
}

// persistState snapshots the controller's routing state for serialisation.
func (c *Controller) persistState() PoolPersistState {
	c.mu.Lock()
	defer c.mu.Unlock()
	ex := make(map[string]time.Time, len(c.exhausted))
	for k, v := range c.exhausted {
		ex[k] = v
	}
	// Persist the active member nick so it can be restored on next start.
	sticky := c.curNick
	ps := PoolPersistState{
		Sticky:    sticky,
		Exhausted: ex,
	}
	if len(c.workerAffinity) > 0 {
		affinity := make(map[string]string, len(c.workerAffinity))
		for worker, nick := range c.workerAffinity {
			if backend.IsValidWorkerNickname(worker) && nick != "" {
				affinity[worker] = nick
			}
		}
		if len(affinity) > 0 {
			ps.WorkerAffinity = affinity
		}
	}
	if c.workerCursor != "" && c.indexOf(c.workerCursor) >= 0 && !c.isUnavailableLocked(c.workerCursor) {
		ps.WorkerCursor = c.workerCursor
	}
	if len(c.credentialPark) > 0 {
		cp := make(map[string]CredentialParkPersist, len(c.credentialPark))
		for k, v := range c.credentialPark {
			cp[k] = CredentialParkPersist{Reset: v.reset, WindowFact: v.windowFact}
		}
		ps.CredentialPark = cp
	}
	if len(c.poolLocalSnapshots) > 0 {
		nicks := make([]string, 0, len(c.poolLocalSnapshots))
		for nick := range c.poolLocalSnapshots {
			// Only persist entries that still name a current member of
			// the pool. Stale entries (e.g. a member that was removed
			// between snapshots) cannot be resolved on load and would
			// just be dropped there; filtering at write time keeps the
			// on-disk set bounded.
			if _, ok := c.backendByNickLocked(nick); ok {
				nicks = append(nicks, nick)
			}
		}
		sort.Strings(nicks)
		if len(nicks) > 0 {
			ps.LocalSnapshotNicks = nicks
		}
	}
	return ps
}

// reanchorLocked moves the active pointer off a current member that is now
// unavailable (removed, disabled, or exhausted), switching to the first healthy
// member when one exists and otherwise to the soonest-resetting non-removed
// member. It is a no-op when the current member is healthy. Used at startup
// after runtime config (including removed-member tombstones) is restored on top
// of the persisted sticky pointer. Caller holds c.mu.
func (c *Controller) reanchorLocked() {
	if len(c.members) == 0 {
		return
	}
	if c.curNick != "" && !c.isUnavailableLocked(c.curNick) {
		return
	}
	if nick, ok := c.firstHealthyNickLocked(); ok {
		c.setActiveMemberLocked(nick)
		return
	}
	// Whole pool is unavailable: anchor on the soonest non-removed member so the
	// active pointer is never a removed one.
	if nick, _ := c.soonestNickLocked(); nick != "" {
		c.setActiveMemberLocked(nick)
	}
}

// ModifyResponse is the per-pool response hook. It acts on three classes of
// upstream response; everything else passes through untouched.
//
//   - Native Anthropic 529 overload: it is a transient capacity wobble, so the
//     response becomes a synthetic same-member 503 with Retry-After: 60. The
//     member is not parked and the pool does not fail over.
//   - 429 Too Many Requests: it first classifies whether the 429 signals
//     genuine quota exhaustion (a "rejected" rate-limit status) or is a
//     policy/punishment 429 (no rate-limit headers). Policy 429s are not
//     parked — the backend stays in rotation and the client receives a 503
//     carrying the upstream error body. Only genuine exhaustion 429s park the
//     backend and advance the sticky pointer. A ChatGPT-Codex member
//     (chatgpt.com) has its own exhaustion signature — the x-codex-* metered
//     family — and parks only on a window at the cap, until the latest
//     contributing reset with a conservative fallback for unusable ones;
//     a usage_limit_reached 429 with sub-cap windows is a same-member
//     transient throttle, and one with no metered signature at all stays on
//     the policy path (issues #304, #314); see codexExhaustion429 for the
//     bound and retirement rules.
//   - 401 Unauthorized / 403 Forbidden: the backend's own credential was
//     rejected — revoked, expired, or the account pulled. The gateway stamps
//     the credential itself (the client never supplies one), so the rejection
//     is always about the backend. The member is parked and the pool fails
//     over, rather than sticking to a dead account and returning the auth
//     error to every client. A pulled account never emits a 429, so without
//     this the pool would never migrate off it (the reported bug).
func (c *Controller) ModifyResponse(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}
	b, ok := backend.FromContext(resp.Request.Context())
	if !ok {
		return nil
	}
	if resp.StatusCode == http.StatusTooManyRequests && isCodexBackend(b) {
		// Preserve Codex's response-meter parsing and reset calculation for
		// a capped window reported on this very 429.
		if reset, precise, full := codexExhaustion429(resp, c.now()); full {
			return c.parkAndFailoverWithSource(resp, b.Nick, reset, "hit 429", true, precise)
		}
	}
	// Classify the original upstream status before any branch rewrites it to a
	// gateway 503. A statusless full window is only exhaustion evidence when
	// this same request failed upstream. Credential rejection and native
	// Anthropic overload have their own handling below.
	if (resp.StatusCode < 200 || resp.StatusCode >= 300) &&
		!isCredentialRejected(resp.StatusCode) &&
		!(resp.StatusCode == anthropicOverloadStatusCode && isNativeAnthropicBackend(b)) {
		if reset, precise, full := c.statuslessFailureBound(resp, b); full {
			return c.parkAndFailoverWithSource(resp, b.Nick, reset,
				fmt.Sprintf("returned %d with a full quota window", resp.StatusCode), true, precise)
		}
	}

	switch {
	case resp.StatusCode == anthropicOverloadStatusCode && isNativeAnthropicBackend(b):
		fmt.Fprintf(c.logOut, "auto[%s]: %s Anthropic 529 overload — absorbing as transient, not parking\n", c.name(), b.Nick)
		rewriteTo503AnthropicOverload(resp)
		return nil
	case resp.StatusCode == http.StatusTooManyRequests:
		// A z.ai/Zhipu proxy-path 429 is always a transient concurrency
		// throttle (the 1302 "Rate limit reached for requests"), never a
		// genuine-exhaustion signal — for z.ai that is detected out-of-band
		// by the poller, never by a proxy 429. Absorb it as the gateway's
		// clean transient 503 + a short backoff so the client retries
		// transparently; never park the member, and never let the upstream
		// 1302 body reach the agent (issue #153). Keyed before the
		// exhaustion classifier so a momentarily at-cap store snapshot can't
		// misclassify the 1302 as genuine exhaustion and over-park.
		if isZaiBackend(b) {
			fmt.Fprintf(c.logOut, "auto[%s]: %s z.ai 429 concurrency throttle — absorbing as transient, not parking\n", c.name(), b.Nick)
			rewriteTo503Throttle(resp, zaiThrottleRetryAfterSeconds)
			return nil
		}
		// Codex 429s with a capped window were handled above. A sub-cap
		// reached-type marker remains a same-member throttle.
		if isCodexBackend(b) {
			// usage_limit_reached with no capped window: the marker alone is
			// not depletion evidence (issue #314 — parking here stranded the
			// last enabled seat until a manual /_gateway/clear), but it does
			// say the plan is being throttled, so absorb as a same-member
			// transient 503 with the short back-off band. Deliberately NOT
			// absorbNonExhaustion429: its policy-body leg would forward
			// "You've hit your usage limit" on the 503 and end the Codex
			// turn instead of letting it retry.
			if strings.EqualFold(quota.CodexReachedType(resp), quota.CodexReachedTypeUsageLimit) {
				fmt.Fprintf(c.logOut, "auto[%s]: %s codex reached-type 429 (sub-cap windows) — throttling same member, not parking\n", c.name(), b.Nick)
				rewriteTo503Throttle(resp, codexThrottleRetryAfter(resp))
				return nil
			}
			// No metered signature → today's transient/policy split, shared
			// with the generic path below.
			return c.absorbNonExhaustion429(resp, b)
		}
		respSnap := quota.Extract(resp)
		// Resolve the member entry under c.mu so a concurrent
		// reconcileLocked (AddMember/RemoveMember/SetMemberDisabled/SetPriority/
		// CreatePool) cannot tear c.members's header out from under the
		// exhaustion classifier (issue #244). isGenuineExhaustionSignal used
		// to call indexOf and backendAt without c.mu, which races the whole-
		// header reassignment in reconcileLocked and either panics inside the
		// proxy's ResponseWriter (no gateway log line, http.Server recovers)
		// or — when the racing read yields idx<0 — silently classifies a
		// genuine 429 as not-genuine and leaves the member un-parked.
		c.mu.Lock()
		var entry memberEntry
		var entryOK bool
		if idx := c.indexOf(b.Nick); idx >= 0 {
			entry = c.members[idx]
			entryOK = true
		}
		c.mu.Unlock()
		if !c.isGenuineExhaustionSignal(entry, entryOK, respSnap) {
			return c.absorbNonExhaustion429(resp, b)
		}
		// A genuine 429 carries a precise window reset; park until then.
		// reset is store-unrepresentable when resetFrom fell back to
		// defaultExhaustionWindow (no usable response reset) — that bound
		// exists only here, so sibling pools sharing the nick need it
		// propagated too (issue #254 AC2). This subclass is a real
		// quota-window fact (windowFact=true), unlike a 401/403 below, so
		// storeReconcilesParkLocked and the preemptor may still retire it early.
		reset, storeUnrepresentable := c.resetFrom(resp)
		return c.parkAndFailoverWithSource(resp, b.Nick, reset, "hit 429", storeUnrepresentable, storeUnrepresentable)
	case isCredentialRejected(resp.StatusCode):
		// An auth rejection has no reset — the credential is simply dead — so
		// park for the conservative default window: long enough to keep the
		// pool off the dead account, short enough that a restored account is
		// retried without an operator restart (or an immediate /_gateway/clear).
		// Always store-unrepresentable: no quota-window field means "this
		// credential is dead", so no sibling pool can ever rederive this park
		// from the shared store — it must be propagated (issue #254 AC1).
		// windowFact=false: no quota window is involved at all, so neither
		// the store's freshness reconciliation nor the preemptor's precise
		// reset may ever retire this park early.
		return c.parkAndFailoverWithSource(resp, b.Nick, c.now().Add(defaultExhaustionWindow), fmt.Sprintf("returned %d", resp.StatusCode), true, false)
	default:
		return nil
	}
}

// statuslessFailureBound combines the latest measured windows with fields on
// this upstream response. The observer normally merges the response before
// ModifyResponse runs; reading its headers here also covers direct callers
// and keeps classification tied to the original upstream response. Only a
// fresh, eligible, statusless window at the cap contributes a bound.
func (c *Controller) statuslessFailureBound(resp *http.Response, b backend.Backend) (time.Time, bool, bool) {
	if isNativeAnthropicBackend(b) && resp.Header.Get(quota.HeaderUnifiedStatus) != "" {
		return time.Time{}, false, false
	}
	now := c.now()
	var snap quota.Snapshot
	if c.store != nil {
		snap = c.store.Get(b.QuotaKey())
	}
	observed := quota.Extract(resp)
	responseWindow := observed.Unified5hUtilization != nil || observed.Unified5hReset != nil || observed.Unified5hStatus != "" ||
		observed.Unified7dUtilization != nil || observed.Unified7dReset != nil || observed.Unified7dStatus != ""
	if isCodexBackend(b) {
		codex := quota.ExtractCodex(resp.Header, now)
		if codex.HasData() {
			observed.OverlayCodex(snap, codex)
			responseWindow = true
		}
	}
	if responseWindow {
		if observed.Unified5hUtilization != nil {
			snap.Unified5hUtilization = observed.Unified5hUtilization
		}
		if observed.Unified5hReset != nil {
			snap.Unified5hReset = observed.Unified5hReset
		}
		if observed.Unified5hStatus != "" {
			snap.Unified5hStatus = observed.Unified5hStatus
		}
		if observed.Unified7dUtilization != nil {
			snap.Unified7dUtilization = observed.Unified7dUtilization
		}
		if observed.Unified7dReset != nil {
			snap.Unified7dReset = observed.Unified7dReset
		}
		if observed.Unified7dStatus != "" {
			snap.Unified7dStatus = observed.Unified7dStatus
		}
		snap.AsOf = now
	}
	if snap.AsOf.IsZero() || now.Sub(snap.AsOf) > storeSnapshotFreshness {
		return time.Time{}, false, false
	}
	var bound time.Time
	full, precise := false, true
	for _, w := range [...]struct {
		util   *float64
		status string
		reset  *time.Time
		use    bool
	}{
		{snap.Unified5hUtilization, snap.Unified5hStatus, snap.Unified5hReset, true},
		{snap.Unified7dUtilization, snap.Unified7dStatus, snap.Unified7dReset, poller.LongWindowBlocksExhaustion(b.BaseURL)},
	} {
		if !w.use || w.status != "" || w.util == nil || *w.util < exhaustionUtilizationThreshold {
			continue
		}
		candidate := snap.AsOf.Add(defaultExhaustionWindow)
		if w.reset != nil && w.reset.After(now) {
			candidate = *w.reset
		} else {
			precise = false
		}
		if !full || candidate.After(bound) {
			bound = candidate
		}
		full = true
	}
	return bound, precise, full && bound.After(now)
}

// isCredentialRejected reports whether code means the backend's credential
// was refused upstream (401/403) — a backend-fatal signal distinct from a
// 429's recoverable quota exhaustion.
func isCredentialRejected(code int) bool {
	return code == http.StatusUnauthorized || code == http.StatusForbidden
}

// absorbNonExhaustion429 handles a 429 already classified as NOT genuine
// quota exhaustion, splitting the two remaining cases by the rate-limit
// signature. A transient per-minute throttle (RPM/ITPM/OTPM) carries an
// upstream retry-after and/or the legacy anthropic-ratelimit-requests/tokens
// headers and clears in seconds — absorb it as a short 503 back-off on the
// SAME member, never parking or switching (issue #191). Everything else is a
// policy/punishment 429 (e.g. "unsupported third-party client", or a
// ChatGPT-Codex 429 with no x-codex-* exhaustion signature — issue #304):
// forward the body on a 503, also without parking.
//
// Shared by the generic classifier fall-through and the codex branch, which
// reaches it directly so a headerless codex 429 can never be parked by the
// generic path's store lookup (AC3).
func (c *Controller) absorbNonExhaustion429(resp *http.Response, b backend.Backend) error {
	if secs, ok := transientRateLimit429(resp); ok {
		fmt.Fprintf(c.logOut, "auto[%s]: %s rate-limit 429 (transient throttle) — backing off %ds, not parking\n", c.name(), b.Nick, secs)
		rewriteTo503Throttle(resp, secs)
		return nil
	}
	fmt.Fprintf(c.logOut, "auto[%s]: %s policy 429 (no exhaustion signal) — not parking\n", c.name(), b.Nick)
	rewriteTo503WithBody(resp)
	return nil
}

// isNativeAnthropicBackend reports whether b points at Anthropic's native
// API host. The host identity is the classifier for 529 overloads: response
// bodies are deliberately not read on the streaming proxy path, and arbitrary
// Anthropic-compatible vendors must keep their non-standard statuses intact.
func isNativeAnthropicBackend(b backend.Backend) bool {
	u, err := url.Parse(b.BaseURL)
	return err == nil && strings.EqualFold(u.Hostname(), nativeAnthropicHost)
}

// isZaiBackend reports whether b is a z.ai/Zhipu backend. For these
// backends a proxy-path 429 is always a transient concurrency/QPS throttle
// (the 1302 "Rate limit reached for requests"): genuine quota exhaustion is
// detected out-of-band by the poller (5h / monthly windows via the quota
// endpoint), never by a proxy 429. So a z.ai proxy 429 is never a
// park-worthy exhaustion signal (issue #153). Detection reuses the same
// URL-keyed provider registry; ProviderFor is a pure match with no network
// call.
func isZaiBackend(b backend.Backend) bool {
	prov, ok := poller.ProviderFor(b.BaseURL)
	return ok && prov.Name() == "z.ai/zhipu"
}

// transientRateLimit429 reports whether a non-genuine, non-z.ai 429 is a
// transient Anthropic per-minute rate-limit throttle (RPM/ITPM/OTPM) rather
// than a policy/punishment 429, and returns the Retry-After (seconds) the
// gateway should advertise for it.
//
// The discriminator is the rate-limit signature: a per-minute rate_limit_error
// 429 carries an upstream retry-after header and/or the legacy
// anthropic-ratelimit-{requests,tokens,input-tokens,output-tokens}-* headers.
// A policy 429 ("unsupported third-party client") carries none of these, so it
// stays on the policy path; genuine quota exhaustion (unified-status rejected)
// is classified earlier and never reaches here.
//
// secs honours the upstream retry-after clamped to [rateLimitBackoffMinSeconds,
// rateLimitBackoffMaxSeconds]; when the header is absent or unparseable it
// defaults to the top of the band (the per-minute window is seconds-scale, so a
// short wait is safe). The legacy headers are read only to identify the 429 —
// never stored: they are a throughput rate, not the subscription budget
// (issue #191 non-goal; internal/quota deliberately ignores them).
func transientRateLimit429(resp *http.Response) (secs int, ok bool) {
	raw := resp.Header.Get("Retry-After")
	if raw == "" && !hasLegacyRateLimitHeader(resp.Header) {
		return 0, false
	}
	secs = rateLimitBackoffMaxSeconds
	if raw != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
			secs = clampRetryAfter(n)
		}
	}
	return secs, true
}

// clampRetryAfter clamps n into the transient rate-limit back-off band.
func clampRetryAfter(n int) int {
	if n < rateLimitBackoffMinSeconds {
		return rateLimitBackoffMinSeconds
	}
	if n > rateLimitBackoffMaxSeconds {
		return rateLimitBackoffMaxSeconds
	}
	return n
}

// hasLegacyRateLimitHeader reports whether h carries any legacy per-minute
// anthropic-ratelimit-* header (requests/tokens/input-tokens/output-tokens),
// excluding the unified-window headers that track the subscription budget. It
// mirrors rewriteTo503's lower-cased prefix scan so a raw-mapped or upstream
// header key matches regardless of canonicalisation.
func hasLegacyRateLimitHeader(h http.Header) bool {
	for k := range h {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "anthropic-ratelimit-") &&
			!strings.HasPrefix(lk, "anthropic-ratelimit-unified-") {
			return true
		}
	}
	return false
}

// parkAndFailover parks nick until reset, advances the sticky pointer, and
// rewrites resp: a 503 "backend switching" (short Retry-After) when a healthy
// member remains, or a 503 with the precise long Retry-After when the pool is
// dry. reason is the log phrase describing why the backend was parked.
//
// When every member is parked (res.allExhausted), a recovery probe is fired
// against each poller-recognised member before the response is finalised:
// if a probe returns a snapshot that no longer satisfies the
// freshness/exhaustion predicate (windowBlocks — the post-#125 rule), the
// member's park mark is cleared and the pool retries selection. If any
// member is now selectable, the response is rewritten to 503 (the normal
// switch shape) and the request is effectively re-routed to the recovered
// member. If the probe does not produce a healthy member, a genuine-quota 429
// is rewritten to a 503 carrying the precise long Retry-After (the exact wait
// until the soonest member resets) rather than forwarded as a 429: a 429 ends
// the Claude Code turn, a 503 is retried, so the agent auto-resumes once the
// window resets (issue #203). A credential-rejection (401/403) all-dead case
// is instead forwarded honestly with the same Retry-After — the client must
// see the real auth status, not a transient 503 (the dry-pool failover itself
// is #124).
func (c *Controller) parkAndFailover(resp *http.Response, nick string, reset time.Time, reason string) error {
	return c.parkAndFailoverWithSource(resp, nick, reset, reason, false, false)
}

// parkAndFailoverWithSource is parkAndFailover plus the storeUnrepresentable
// and windowFact flags threaded through to record429WithSource (issue #254)
// — see that method's doc for what the flags mean and how propagation is
// kept deadlock-free.
func (c *Controller) parkAndFailoverWithSource(resp *http.Response, nick string, reset time.Time, reason string, storeUnrepresentable, windowFact bool) error {
	res := c.record429WithSource(nick, reset, storeUnrepresentable, windowFact)

	if res.allExhausted {
		if recovered := c.tryRecoverParked(); recovered != "" {
			// Re-rotate sticky to the recovered member (it was moved off
			// during record429, which pointed curNick at the soonest-reset
			// member). Take the standard "switched" log + 503 rewrite.
			from := res.to
			c.mu.Lock()
			c.setActiveMemberLocked(recovered)
			c.mu.Unlock()
			fmt.Fprintf(c.logOut, "auto[%s]: %s -> %s (recovered %s via quota probe; upstream reset would have over-parked)\n",
				c.name(), from, recovered, recovered)
			rewriteTo503(resp)
			return nil
		}
		secs := retryAfterSeconds(res.retryAfter)
		if resp.StatusCode == http.StatusTooManyRequests {
			// Genuine quota exhaustion: rewrite the 429 to a 503 carrying the
			// precise long Retry-After so Claude Code retries and auto-resumes
			// once the window resets, rather than ending the turn on a hard 429
			// (issue #203). A credential-rejection (401/403) all-dead case is
			// NOT a 429 and is forwarded honestly below — the client must see
			// the real auth status, not a transient 503.
			rewriteTo503DryPool(resp, secs)
			fmt.Fprintf(c.logOut, "auto[%s]: all backends exhausted; returning 503 (retry after %ds)\n", c.name(), secs)
			return nil
		}
		setRetryAfter(resp.Header, secs)
		fmt.Fprintf(c.logOut, "auto[%s]: all backends exhausted; forwarding upstream %d (retry after %ds)\n", c.name(), resp.StatusCode, secs)
		return nil
	}

	if res.switched {
		fmt.Fprintf(c.logOut, "auto[%s]: %s -> %s (%s %s)\n", c.name(), nick, res.to, nick, reason)
	}
	rewriteTo503(resp)
	return nil
}

// isGenuineExhaustionSignal reports whether a 429 response for a member
// represents real quota exhaustion (park it) versus a policy/punishment 429
// such as an "unsupported third-party client" rejection (leave it in rotation,
// forward the body).
//
// entry is the already-resolved controller member for the request (carrying
// Nick/Credential/BaseURL). The caller resolves entry under c.mu in
// ModifyResponse so this function never reads c.members itself — that is the
// shape #244 requires: any reader of c.members racing reconcileLocked's
// whole-header reassignment can tear the slice header and either panic
// inside the proxy or — when the racing read yields idx<0 — silently drop the
// store-snapshot check, misclassifying a genuine 429 as not-genuine and
// leaving the member un-parked. ok=false means the nick is no longer a member
// of the pool (runtime-removed between request and response): the function
// still honours the response itself but cannot consult the per-member store
// snapshot, since there is no member entry to key it by.
//
// The discriminator is the rate-limit *status*, not utilization. Utilization
// is an unreliable proxy in both directions — Anthropic has rejected at 0.99
// and still served at 1.0 (the soft-cap/overage zone) — so a 1.0 threshold
// both misses genuine 429s (the member then loops: not parked → retried →
// 429 again) and parks members that are fine. A genuine rate-limit 429 self-
// reports a "rejected" unified status (overall or per-window); a policy 429
// carries no unified rate-limit headers at all. Utilization at the cap is
// kept only as a secondary positive signal, and is the sole signal for
// poller-tracked backends (z.ai / MiniMaxi / Ark) that report no status.
// It checks the 429 response first, then the most recent store snapshot.
//
// Snapshot freshness matters for both paths: a poller-tracked member whose
// window reset has already passed but whose stored utilization is still
// frozen at 1.0 (the poller only tracks the active member, so a failed-off
// member's entry freezes at its last good reset) must read *not* blocking —
// otherwise a transient overload 429 on a recovered member is falsely
// parked. The reset-freshness guard lives in windowBlocks for the no-status
// branch, mirroring storeExhaustedUntilLocked's behaviour on the recovery
// side (#125).
func (c *Controller) isGenuineExhaustionSignal(entry memberEntry, ok bool, respSnap quota.Snapshot) bool {
	now := c.now()
	// Fail closed (longBlocks=true) when the nick can't be resolved — a
	// runtime-removed member should still honour a genuine 7d rejection
	// rather than have it silently dropped (issue #192). baseURL on the
	// already-resolved entry is the only state needed for the long-window
	// predicate; the response path's store-snapshot branch is gated on ok
	// so a runtime-removed nick never mis-keys the shared quota store.
	longBlocks := true
	if ok {
		longBlocks = poller.LongWindowBlocksExhaustion(entry.BaseURL)
	}
	if snapRejects(respSnap, now, longBlocks) {
		return true
	}
	if c.store != nil && ok {
		// The quota store is keyed by nick alone (issue #115 — one
		// account, one exhaustion record across pools), so entry.Nick is
		// the correct key and avoids re-resolving via c.backendAt.
		if snapRejects(c.store.Get(entry.Nick), now, longBlocks) {
			return true
		}
	}
	return false
}

// snapRejects reports whether snap shows the backend actually rate-limited:
// either unified window blocking (see windowBlocks — a per-window
// "rejected", or, absent a status, a utilization at the cap with a reset
// still in the future).
//
// now is the controller's clock reading; it gates the no-status util-only
// branch so a frozen at-cap snapshot whose reset has already passed reads
// not blocking. The status branch is bounded by the window's reset field —
// an explicit "rejected" reads as not blocking once its reset has passed,
// so a frozen post-#134 snapshot can't keep a recovered backend parked
// forever.
//
// longBlocks reports whether the backend's long (7d/monthly) window is a
// genuine chat-blocking signal (poller.LongWindowBlocksExhaustion). When
// false — Z.AI/Zhipu, whose monthly slot is a web-search/reader/zread tool
// quota, not chat throughput (issue #192) — the 7d term is dropped so a
// filled tool quota can't park a chat-healthy member. Callers fail closed
// (pass true) when the backend can't be resolved.
//
// Boundedness invariant (issue #253): no unbounded snapshot field
// participates in this predicate. The top-level UnifiedStatus is
// deliberately not consulted — a poller-tracked provider that emits the
// header only when rejecting (z.ai's observed behaviour on the `ccz`
// instance, 2026-07-29) plants the field with no per-window reset to age
// it out, and mergeSnapshot's carry-forward in quota.go:220-222 keeps it
// pinned forever. Every remaining input is gated by its window's reset
// or by AsOf freshness in windowBlocks, so a recovered backend cannot be
// held hostage by stale data.
func snapRejects(snap quota.Snapshot, now time.Time, longBlocks bool) bool {
	return windowBlocks(snap.Unified5hUtilization, snap.Unified5hStatus, snap.Unified5hReset, snap.AsOf, now) ||
		(longBlocks && windowBlocks(snap.Unified7dUtilization, snap.Unified7dStatus, snap.Unified7dReset, snap.AsOf, now))
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
	return c.record429WithSource(nick, reset, false, false)
}

// record429WithSource is record429 plus explicit flags for whether the bound
// is representable in the shared quota store (issue #254). When
// storeUnrepresentable is true — the 401/403 credential-fatal park, or a 429
// whose resetFrom fell back to defaultExhaustionWindow — the park is also
// written into c.credentialPark and propagated to every sibling pool holding
// the nick, since no store-derived signal will ever teach them about it.
// windowFact (meaningful only when storeUnrepresentable is true) distinguishes
// the header-less-429 subclass (true — a real quota-window fact eligible for
// storeReconcilesParkLocked and the preemptor's precise-reset supersession)
// from the 401/403 subclass (false — a credential fact neither may retire
// early); see credentialParkEntry's doc for the full reasoning.
//
// Propagation happens AFTER c.mu is released. c.propagatePark, when set,
// reaches into sibling controllers' own mu one at a time; taking a second
// controller's lock while still holding this one would risk a lock-order
// inversion the moment two pools park the same nick concurrently (this
// method never holds Pools.mu, so the package's usual Pools.mu ->
// Controller.mu order does not apply here — the invariant this method
// upholds instead is "never hold two Controller.mu locks at once", which
// (*Pools).propagateCredentialPark also honours by locking one sibling at a
// time). See that function's doc for the full argument.
func (c *Controller) record429WithSource(nick string, reset time.Time, storeUnrepresentable, windowFact bool) record429Result {
	c.mu.Lock()

	c.exhausted[nick] = reset
	if storeUnrepresentable {
		c.credentialPark[nick] = credentialParkEntry{reset: reset, windowFact: windowFact}
	}
	c.clearExpiredLocked() // housekeeping; never clears the future reset just set

	// Another request may have already rotated off the failed backend; if
	// the current sticky is healthy, keep it.
	prev := c.curNick
	var result record429Result
	if !c.isUnavailableLocked(prev) {
		c.notifyMutate()
		result = record429Result{to: prev}
	} else if next, ok := c.firstHealthyNickLocked(); ok {
		c.setActiveMemberLocked(next)
		result = record429Result{to: next, switched: next != prev}
	} else {
		next, soonest := c.soonestNickLocked()
		c.setActiveMemberLocked(next)
		result = record429Result{to: next, retryAfter: c.waitUntil(soonest), allExhausted: true}
	}
	c.mu.Unlock()

	if storeUnrepresentable && c.propagatePark != nil {
		c.propagatePark(nick, reset, windowFact)
	}
	return result
}

// probeCooldown bounds how often the same parked member's quota endpoint
// may be probed for recovery. 30s is the chosen default: probes are cheap
// (z.ai / MiniMaxi / Ark return synchronously from their proprietary
// quota endpoints, all non-billable), and this cadence is fast enough to
// pick up a recovered member within one slot of operator-visible latency
// while bounding worst-case probe storms on a flapping member. See
// issue #124 for the design rationale.
const probeCooldown = 30 * time.Second

// probeTimeout caps the per-probe network call so a stalled upstream does
// not block the all-exhausted response path indefinitely. The chosen 2s is
// comfortably above the typical proprietary quota-endpoint latency
// (sub-second) and well below the 5s default upstream fallback used by
// resetFrom (issue #124).
const probeTimeout = 2 * time.Second

// SetProbeHTTPClient overrides the HTTP client used by tryRecoverParked.
// Tests inject a client backed by httptest to control probe responses;
// production never calls this. The client is replaced atomically — the
// caller must not invoke tryRecoverParked concurrently with this setter.
func (c *Controller) SetProbeHTTPClient(client *http.Client) {
	c.probeHTTPClient = client
}

// tryRecoverParked fires one quota probe per parked, probe-eligible member
// and unparks any whose upstream now serves. It returns the nick of the
// first member recovered (or "" when no member recovered).
//
// "Probe-eligible" means: the member is parked, has a poller-recognised
// base URL (Anthropic / unknown providers are skipped via
// poller.ErrNoProvider), has not been probed within the last
// probeCooldown, and has no in-flight probe. Concurrent all-exhausted
// requests coalesce via c.probeInFlight.
//
// The recovery decision uses snapRejects (post-#125) so the freshness
// predicate is shared between the park-decision path and the recovery
// path — exactly what issue #124 asks for to avoid divergence.
//
// Caller does NOT hold c.mu. Internal locking is acquired and released
// around each step (snapshot under lock → probe unlocked → result under
// lock); the probe itself runs without c.mu so a stalled upstream does
// not block other pool operations.
func (c *Controller) tryRecoverParked() string {
	return c.recoverParked(false)
}

// tryRecoverParkedNonActive mirrors tryRecoverParked but skips the
// currently active sticky member, so the recovery loop can probe parked
// members without ever probing the backend the pool is already serving
// (issue #242). The skip is a no-op in the stranded shape — the healthy
// active nick is not in c.exhausted — so its only real effect is
// avoiding a redundant probe when the background loop's tick overlaps
// with a request-driven allExhausted recovery. The eligibility,
// snapshot-decision, and in-flight bookkeeping are otherwise identical
// to tryRecoverParked, so the request-path and background-path probes
// share one cooldown / coalescing window.
//
// Caller does NOT hold c.mu.
func (c *Controller) tryRecoverParkedNonActive() string {
	return c.recoverParked(true)
}

// recoverParked is the shared body behind tryRecoverParked (skipActive
// false, the request-path allExhausted recovery) and
// tryRecoverParkedNonActive (skipActive true, the background recovery of
// parked non-active members from issue #242). Both call paths share one
// in-flight / cooldown bookkeeping so concurrent request-path and
// background probes coalesce on the same quota key.
//
// When no probe-eligible member exists, the returned nick is "" (a
// no-op) — the same contract tryRecoverParked always had.
//
// Caller does NOT hold c.mu.
func (c *Controller) recoverParked(skipActive bool) string {
	type probeTarget struct {
		nick     string
		quotaKey string
	}
	var targets []probeTarget
	now := c.now()

	c.mu.Lock()
	if len(c.exhausted) == 0 {
		c.mu.Unlock()
		return ""
	}
	for nick := range c.exhausted {
		// Skip the active sticky member when requested by the background
		// loop — see tryRecoverParkedNonActive (issue #242). The healthy
		// active nick is not in c.exhausted in the stranded shape, so this
		// filter only matters when the loop's tick overlaps with an
		// allExhausted request that just recorded a 429 against the sticky
		// member; avoiding the redundant probe is the only effect.
		if skipActive && nick == c.curNick {
			continue
		}
		// Skip disabled / removed members — they are unreachable regardless
		// of upstream state.
		if c.disabled[nick] {
			continue
		}
		// Resolve the backend so we can detect the poller-recognised
		// providers (z.ai / MiniMaxi / Ark) and probe them. Anthropic is
		// intentionally skipped — its 429s already carry precise resets
		// and organic traffic refreshes the store.
		b, ok := c.backendByNickLocked(nick)
		if !ok {
			continue
		}
		if _, has := poller.ProviderFor(b.BaseURL); !has {
			continue
		}
		quotaKey := b.QuotaKey()
		if c.probeInFlight[quotaKey] {
			continue
		}
		if last, ok := c.lastProbeAttempt[quotaKey]; ok && now.Sub(last) < probeCooldown {
			continue
		}
		// Mark in-flight under the lock so concurrent all-exhausted paths
		// skip this member until the probe returns.
		c.probeInFlight[quotaKey] = true
		c.lastProbeAttempt[quotaKey] = now
		targets = append(targets, probeTarget{nick: nick, quotaKey: quotaKey})
	}
	c.mu.Unlock()

	if len(targets) == 0 {
		return ""
	}

	var recovered string
	for _, t := range targets {
		c.mu.Lock()
		b, ok := c.backendByNickLocked(t.nick)
		c.mu.Unlock()
		if !ok {
			c.clearProbeInFlight(t.quotaKey)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		client := c.probeHTTPClient
		if client == nil {
			client = http.DefaultClient
		}
		snap, err := poller.Probe(ctx, b, client, c.now)
		cancel()
		if err != nil {
			// Includes ErrNoProvider (defensive — we already filtered by
			// ProviderFor above, but a future provider change might slip
			// through). Just clear the in-flight flag; do NOT extend the
			// park — issue #124 contract: failed probe leaves the park alone.
			c.clearProbeInFlight(t.quotaKey)
			fmt.Fprintf(c.logOut, "auto[%s]: recovery probe for %s failed: %v (park retained)\n", c.name(), t.nick, err)
			continue
		}
		// snapRejects (post-#125) shares the freshness predicate with the
		// park-decision path. If the snapshot no longer rejects, the member
		// is healthy — unmark and update lastActed to suppress re-probe
		// thrash until the cooldown.
		c.mu.Lock()
		recoveredNow := c.now()
		var hadCredPark bool
		if !snapRejects(snap, recoveredNow, poller.LongWindowBlocksExhaustion(b.BaseURL)) {
			delete(c.exhausted, t.nick)
			// A live, successful health-check response is decisive evidence
			// the member works — for either credentialPark subclass, not
			// just the header-less-429 residue: it proves the credential
			// itself authenticates, which is exactly what a 401/403 entry
			// doubts. Clear it too, and propagate the release the same way
			// an explicit operator clear would (issue #254) — otherwise a
			// sibling pool's mirrored copy would outlive the very probe that
			// disproved it, and this pool would report "recovered" while
			// still refusing the nick via credentialPark (isUnavailableLocked
			// unions both maps).
			_, hadCredPark = c.credentialPark[t.nick]
			delete(c.credentialPark, t.nick)
			c.notifyMutate()
			if recovered == "" {
				recovered = t.nick
			}
			fmt.Fprintf(c.logOut, "auto[%s]: recovery probe for %s returned healthy; unparked\n", c.name(), t.nick)
		} else {
			fmt.Fprintf(c.logOut, "auto[%s]: recovery probe for %s still exhausted; park retained\n", c.name(), t.nick)
		}
		c.probeInFlight[t.quotaKey] = false
		c.mu.Unlock()

		if hadCredPark && c.propagateParkClear != nil {
			c.propagateParkClear(t.nick)
		}
	}
	return recovered
}

// clearProbeInFlight clears the in-flight flag for quotaKey under c.mu.
// Used by the error path of recoverParked (shared by tryRecoverParked
// and tryRecoverParkedNonActive).
func (c *Controller) clearProbeInFlight(quotaKey string) {
	c.mu.Lock()
	c.probeInFlight[quotaKey] = false
	c.mu.Unlock()
}

// clearExpiredLocked drops parks whose reset has passed, so a recovered
// backend becomes selectable again. The map holds parks recorded from failed
// upstream responses and explicit live-429 signals. Caller holds c.mu.
func (c *Controller) clearExpiredLocked() {
	now := c.now()
	for nick, reset := range c.exhausted {
		if !now.Before(reset) { // now >= reset
			delete(c.exhausted, nick)
		}
	}
	for nick, entry := range c.credentialPark {
		if !now.Before(entry.reset) {
			delete(c.credentialPark, nick)
		}
	}
}

// isExhaustedLocked reports whether nick is currently unselectable, by
// either a recorded failed-response park or a status-bearing rejected
// quota window. Caller holds c.mu.
func (c *Controller) isExhaustedLocked(nick string) bool {
	_, ok := c.exhaustedUntilLocked(nick)
	return ok
}

// liveParkActiveLocked reports whether a live-429 park is currently holding
// nick out of rotation: an entry in c.exhausted whose reset has not elapsed and
// which the fresh store has not reconciled away (issue #145), OR a still-future
// entry in c.credentialPark — local or propagated from a sibling pool (issue
// #254). It is the gate for MemberStatus.Parked / the per-nick clear button
// (issue #147) — the exact condition under which ClearExhaustedNick has a park
// to drop AND that park is what is keeping the member parked. Status-bearing
// store exhaustion is deliberately excluded: clearing the recorded park
// cannot move it.
// A credentialPark entry with windowFact true (the header-less-429 residue) is
// still subject to storeReconcilesParkLocked, same as c.exhausted — it is a
// quota-window fact the store can retire early. windowFact false (401/403) is
// never reconciled by the store: it only ages out by wall-clock or an explicit
// clear. Caller holds c.mu.
func (c *Controller) liveParkActiveLocked(nick string) bool {
	if entry, ok := c.credentialPark[nick]; ok && c.now().Before(entry.reset) {
		if !entry.windowFact || !c.storeReconcilesParkLocked(nick) {
			return true
		}
	}
	reset, ok := c.exhausted[nick]
	if !ok || !c.now().Before(reset) {
		return false
	}
	return !c.storeReconcilesParkLocked(nick)
}

// exhaustedUntilLocked returns the time nick stays unselectable and whether
// it is exhausted at all, unifying three exhaustion signals: a recorded park
// from a failed response, a store-unrepresentable credential park local to
// or propagated into this pool (c.credentialPark, issue #254), and an
// explicit rejected status from the quota store (computed by
// storeExhaustedUntilLocked). When more
// than one applies the later reset wins, so a member is never re-selected
// while any signal still blocks it.
//
// Caller holds c.mu.
func (c *Controller) exhaustedUntilLocked(nick string) (time.Time, bool) {
	now := c.now()
	reset, ok := c.exhausted[nick]
	if ok && !now.Before(reset) {
		ok = false // park already elapsed
	}
	// Store-driven reconciliation of a stale live park (issue #145): when the
	// polled store holds FRESH, non-blocking data for the member, the live 429
	// park is stale — its 429-sourced reset overshot the real quota window
	// (Z.AI's unified-reset runs ~2h52m past the dashboard 5h reset). Retire
	// the live park so the member becomes selectable now, rather than holding
	// the pool in 429 until the 429's reset. Non-destructive: c.exhausted is
	// left in place, so a later stale snapshot or a fresh 429 (via record429)
	// re-asserts the park. The freshness gate is the safety guard — an empty
	// or frozen snapshot (the poller tracks only the active member) keeps the
	// park aging by wall-clock. See storeReconcilesParkLocked.
	if ok && c.storeReconcilesParkLocked(nick) {
		ok = false
		reset = time.Time{}
	}
	// A credential park (401/403, or a header-less 429 fallback) contributes
	// its bound to the union unless it is the header-less-429 subclass
	// (windowFact) AND the store has reconciled it away (#145) — that
	// subclass is a real quota-window fact, so it is retirable exactly like
	// c.exhausted. A 401/403 (windowFact false) is never reconciled by the
	// store — it is not a fact about a quota window at all — so it only ages
	// out by wall-clock or an explicit clear (issue #254).
	if cp, cpOK := c.credentialPark[nick]; cpOK && now.Before(cp.reset) {
		retired := cp.windowFact && c.storeReconcilesParkLocked(nick)
		if !retired && (!ok || cp.reset.After(reset)) {
			reset, ok = cp.reset, true
		}
	}
	if sReset, sOK := c.storeExhaustedUntilLocked(nick); sOK {
		if !ok || sReset.After(reset) {
			reset, ok = sReset, true
		}
	}
	return reset, ok
}

// storeReconcilesParkLocked reports whether a fresh healthy store snapshot
// can retire a previously recorded quota-window park. A statusless full
// snapshot keeps an existing park in place, but cannot create one by itself.
// Empty or stale snapshots never clear a park. Caller holds c.mu; the store
// has its own lock and never calls back into the controller.
func (c *Controller) storeReconcilesParkLocked(nick string) bool {
	if c.store == nil {
		return false
	}
	idx := c.indexOf(nick)
	if idx < 0 {
		return false
	}
	b := c.backendAt(idx)
	snap := c.store.Get(b.QuotaKey())
	if !snap.HasData() {
		return false
	}
	now := c.now()
	if now.Sub(snap.AsOf) > storeSnapshotFreshness {
		return false // frozen / stale snapshot — do not second-guess the park
	}
	return !snapRejects(snap, now, poller.LongWindowBlocksExhaustion(b.BaseURL))
}

// storeBlockBoundLocked returns the bound for a status-bearing rejected
// store window. Statusless full windows are deliberately omitted here: they
// become exhaustion evidence only when combined with a failed upstream
// response in statuslessFailureBound. Provider window eligibility remains
// enforced by LongWindowBlocksExhaustion. Caller holds c.mu.
func (c *Controller) storeBlockBoundLocked(nick string) (time.Time, bool) {
	if c.store == nil {
		return time.Time{}, false
	}
	idx := c.indexOf(nick)
	if idx < 0 {
		return time.Time{}, false
	}
	b := c.backendAt(idx)
	snap := c.store.Get(b.QuotaKey())
	now := c.now()
	longBlocks := poller.LongWindowBlocksExhaustion(b.BaseURL)
	// #253 diagnostic: a poller-tracked provider that emits the top-level
	// anthropic-ratelimit-unified-status header only when rejecting
	// (observed on z.ai for `ccz`, 2026-07-29) plants a top-level
	// "rejected" with no per-window status to age it out — the field is
	// non-routing post-fix, so this log is the only trace that any
	// poller-tracked provider ever set it. Log once per nick; cleared on
	// reconcileLocked so a re-added member re-triggers. Skipped when the
	// per-window branches carry status (a real Anthropic response), which
	// is the supported path and would log spam.
	if snap.UnifiedStatus == unifiedStatusRejected &&
		snap.Unified5hStatus == "" && snap.Unified7dStatus == "" {
		if _, dup := c.topStatusLogged[nick]; !dup {
			c.topStatusLogged[nick] = struct{}{}
			fmt.Fprintf(c.logOut, "auto[%s]: %s stored snapshot carries unified_status=rejected with no per-window status — post-#253 non-routing; field kept operator-visible\n", c.name(), nick)
		}
	}
	windows := [...]struct {
		util   *float64
		status string
		reset  *time.Time
		use    bool
	}{
		{snap.Unified5hUtilization, snap.Unified5hStatus, snap.Unified5hReset, true},
		{snap.Unified7dUtilization, snap.Unified7dStatus, snap.Unified7dReset, longBlocks},
	}
	var bound time.Time
	have := false
	for _, w := range windows {
		// Statusless utilization is evidence only alongside a failed
		// upstream response, which is recorded in c.exhausted. A full
		// store snapshot alone cannot make a member unavailable.
		if !w.use || w.status == "" {
			continue
		}
		// A window contributes exactly when windowBlocks says it is still
		// blocking. For a "rejected" status that means honouring the
		// per-window reset: a future (or nil) reset blocks, an elapsed
		// reset does not (issue #286). Gating rejected on windowBlocks
		// here — rather than treating the verdict as authoritative
		// regardless of reset — is what lets a member leave `exhausted`
		// once its 5h window resets, instead of being re-parked to
		// AsOf + 5h on every read.
		if !windowBlocks(w.util, w.status, w.reset, snap.AsOf, now) {
			continue
		}
		// Window contributes. Pick the bound: the snapshot's reset when
		// it's still in the future, otherwise AsOf + 5h as the
		// conservative over-park. A rejected window reaches the fallback
		// only when its reset is nil (an elapsed reset failed the gate
		// above); the past-bound guard below then keeps a long-stale
		// fallback from re-parking on every read.
		var candidate time.Time
		if w.reset != nil && now.Before(*w.reset) {
			candidate = *w.reset
		} else if !snap.AsOf.IsZero() {
			candidate = snap.AsOf.Add(defaultExhaustionWindow)
		} else {
			// No observable age, no usable reset — happens when a window
			// with no recorded snapshot carries a status. Bound at
			// now+5h; this branch is the only one not AsOf-anchored
			// because there is no AsOf to anchor against. The
			// now-anchored answer would re-arm per read, but
			// monotonically-growing times do eventually pass, so the
			// park still expires on its own.
			candidate = now.Add(defaultExhaustionWindow)
		}
		if !have || candidate.After(bound) {
			bound, have = candidate, true
		}
	}
	// A store-derived bound contributes to exhaustion only while it is still
	// future (issue #286). The nil-reset AsOf + 5h fallback for a rejected
	// snapshot can land in the past once the snapshot ages beyond 5h; without
	// this guard exhaustedUntilLocked would merge that past bound and every
	// status/routing read would report `exhausted` with an already-elapsed
	// exhausted_until. Reset-derived and now+5h bounds are future by
	// construction, so this only filters the stale nil-reset fallback.
	if have && !bound.After(now) {
		return time.Time{}, false
	}
	return bound, have
}

// storeExhaustedUntilLocked reports the bound a blocking store snapshot
// implies for nick (see storeBlockBoundLocked). It considers the 5h window
// always, and the 7d/long window only when that window is a genuine
// chat-blocking signal (poller.LongWindowBlocksExhaustion). When both
// qualify, the later bound wins.
//
// ok is false when no window qualifies — no store, unknown nick, no
// blocking window (a rejected window whose reset has elapsed no longer
// blocks, issue #286). A blocking window with a usable future reset
// returns that reset; a blocking window with no reset returns
// snap.AsOf + defaultExhaustionWindow (the over-park, AC #2). That
// fallback is anchored at snap.AsOf, not now, so a frozen entry's bound is
// deterministic across reads (AC #3), and once even the fallback has
// elapsed the bound stops contributing rather than being recreated in the
// past (issue #286).
//
// Checking 7d matters for poller-tracked backends (MiniMaxi / Ark),
// which report a weekly cap through the dashboard API and emit no clean
// proxy-path 429 to catch a 7d-exhausted-but-5h-healthy member the
// reactive way. Z.AI/Zhipu is the exception: its monthly slot is a
// web-search/reader/zread tool quota, not chat throughput, so its long
// window is skipped here (issue #192).
//
// Caller holds c.mu; the store has its own lock and never calls back into
// the controller.
func (c *Controller) storeExhaustedUntilLocked(nick string) (time.Time, bool) {
	return c.storeBlockBoundLocked(nick)
}

// windowBlocks reports whether a quota window supplies exhaustion evidence.
// An explicit status is authoritative: only rejected blocks, and an elapsed
// rejected reset no longer blocks. Without a status, a fresh full window is a
// positive signal used with an original failed upstream response; it does
// not independently make a store-backed member unavailable. The five-minute
// AsOf freshness bound prevents a frozen poller snapshot from representing
// current quota state. Long-window eligibility is applied by each caller.
func windowBlocks(util *float64, status string, reset *time.Time, asOf time.Time, now time.Time) bool {
	if status != "" {
		if status != unifiedStatusRejected {
			return false
		}
		// "rejected" with no reset is authoritative (snapshot has no
		// freshness bound). "rejected" with a reset respects it — once the
		// reset has passed the snapshot is stale and reads as not blocking
		// (issue #134). The reset gate stays on the status branch only;
		// the no-status branch is decoupled from the reset field and reads
		// asOf against storeSnapshotFreshness instead.
		return reset == nil || now.Before(*reset)
	}
	// No-status branch. An unrecorded key returns a zero AsOf — treat that
	// as stale rather than "eternally fresh": quota.Store.Get on a missing
	// key returns a stamped-but-empty snapshot whose !HasData() must read
	// not blocking (storeReconcilesParkLocked rejects !HasData() for the
	// same reason; empty != healthy).
	if util == nil || *util < exhaustionUtilizationThreshold {
		return false
	}
	if asOf.IsZero() {
		return false
	}
	return now.Sub(asOf) <= storeSnapshotFreshness
}

// soonestNickLocked returns the nick and reset time of the member that
// frees up first. Only meaningful when every member is exhausted (the
// all-dry case); falls back to curNick if the map is somehow empty.
// Caller holds c.mu.
func (c *Controller) soonestNickLocked() (string, time.Time) {
	best, bestSet := c.curNick, false
	var bestReset time.Time
	for _, m := range c.members {
		if c.disabled[m.Nick] {
			continue
		}
		reset, ok := c.exhaustedUntilLocked(m.Nick)
		if !ok {
			continue
		}
		if !bestSet || reset.Before(bestReset) {
			best, bestReset, bestSet = m.Nick, reset, true
		}
	}
	if !bestSet {
		return c.curNick, c.now()
	}
	return best, bestReset
}

// backendAt resolves the backend at unified member index i.
func (c *Controller) backendAt(i int) backend.Backend {
	m := c.members[i]
	return backend.Backend{
		Pool:       c.name(),
		Nick:       m.Nick,
		Credential: m.Credential,
		BaseURL:    m.BaseURL,
	}
}

// backendByNickLocked resolves a backend by nick from the unified member
// collection. Returns (Backend, false) when nick is not a member.
// Caller holds c.mu.
func (c *Controller) backendByNickLocked(nick string) (backend.Backend, bool) {
	if idx := c.indexOf(nick); idx >= 0 {
		return c.backendAt(idx), true
	}
	return backend.Backend{}, false
}

// firstHealthyNickLocked finds the nick to fail over to. For a priority pool
// it returns the highest-priority available member; for a plain pool it scans
// round-robin from just after the current sticky position so switches spread
// across the pool. Covers all members in the unified collection.
// Returns ("", false) when all are unavailable. Caller holds c.mu.
func (c *Controller) firstHealthyNickLocked() (string, bool) {
	if pri := c.effectivePriorityLocked(); len(pri) > 0 {
		for _, nick := range pri {
			if !c.isUnavailableLocked(nick) {
				return nick, true
			}
		}
		return "", false
	}

	// Plain pool: scan round-robin from just after the current position.
	effectiveNicks := c.allMemberNicksLocked()
	n := len(effectiveNicks)
	if n == 0 {
		return "", false
	}
	startIdx := 0
	for i, nick := range effectiveNicks {
		if nick == c.curNick {
			startIdx = i
			break
		}
	}
	for off := 1; off <= n; off++ {
		idx := (startIdx + off) % n
		if !c.isUnavailableLocked(effectiveNicks[idx]) {
			return effectiveNicks[idx], true
		}
	}
	return "", false
}

// nextParkedButResetPassedLocked returns the nick of a parked member whose
// live-429 reset has already elapsed — i.e. a member that the exhaustion
// map no longer actively blocks but which the quota store may still be
// flagging. Forwarding one request through to such a member lets the live
// response refresh the store via the normal record429 / store-write path
// and break the issue #134 deadlock where a pool of all-parked members
// never sees a forwarded request.
//
// The pick is round-robin from the current sticky position, matching
// firstHealthyNickLocked's scan order, so a flapping pool doesn't
// repeatedly hammer the same member.
//
// "Reset has elapsed" here means: the live-429 map entry (c.exhausted)
// either is absent or carries a past reset. Disabled / removed members
// are skipped — they are unreachable regardless of upstream state.
//
// Caller holds c.mu.
func (c *Controller) nextParkedButResetPassedLocked() (string, bool) {
	effectiveNicks := c.allMemberNicksLocked()
	if len(effectiveNicks) == 0 {
		return "", false
	}

	startIdx := 0
	for i, nick := range effectiveNicks {
		if nick == c.curNick {
			startIdx = i
			break
		}
	}

	now := c.now()
	n := len(effectiveNicks)
	for off := 1; off <= n; off++ {
		idx := (startIdx + off) % n
		nick := effectiveNicks[idx]
		if c.disabled[nick] {
			continue
		}
		// Look only at the live-429 park (c.exhausted). If it has a
		// future reset, the upstream is still actively rejecting — the
		// half-open probe is for a member whose live signal has
		// actually cleared. The store side is intentionally ignored
		// here: that's the signal the probe is meant to refresh.
		if reset, ok := c.exhausted[nick]; ok && now.Before(reset) {
			continue
		}
		// A credential park (401/403, or a header-less 429 fallback) — local
		// or propagated from a sibling pool — means the credential is dead,
		// not merely rate-limited. Forwarding a real client request to it
		// would not refresh anything the store can use (issue #254 AC8): it
		// would just hand the client another auth failure. Skip it exactly
		// like a live c.exhausted park.
		if entry, ok := c.credentialPark[nick]; ok && now.Before(entry.reset) {
			continue
		}
		return nick, true
	}
	return "", false
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
//
// storeUnrepresentable reports whether the returned reset came from that
// fallback rather than a real response reset (issue #254 AC2): a 429 with a
// genuine window reset is store-derivable — sibling pools rederive the same
// block from the shared quota store — but the fallback bound exists only in
// this call, so the caller must propagate it the same way a 401/403 park is
// propagated.
func (c *Controller) resetFrom(resp *http.Response) (reset time.Time, storeUnrepresentable bool) {
	now := c.now()
	snap := quota.Extract(resp)
	if snap.UnifiedReset != nil && snap.UnifiedReset.After(now) {
		return *snap.UnifiedReset, false
	}
	return now.Add(defaultExhaustionWindow), true
}

// rewriteTo503Response applies the shared synthetic-503 response shape. When
// replaceBody is false, the upstream body, content type, content length, and
// content encoding remain untouched for policy responses that expose the
// original error.
//
// Content-Encoding is deleted inside the replaceBody branch, not beside the
// anthropic-ratelimit-* strip, because the two headers answer different
// questions. The rate-limit strip is pool-boundary hygiene — a synthetic
// response must never carry the rejected member's quota state out the pool
// channel — and so is unconditional. Content-Encoding describes the *bytes of
// the body*, so it is only stale once those bytes have been swapped for the
// plain-JSON substitute; deleting it on the pass-through path mislabels a
// still-compressed upstream body as uncompressed JSON (issue #291).
func rewriteTo503Response(resp *http.Response, body []byte, replaceBody bool, retryAfter int) {
	resp.StatusCode = http.StatusServiceUnavailable
	resp.Status = strconv.Itoa(http.StatusServiceUnavailable) + " " + http.StatusText(http.StatusServiceUnavailable)

	h := resp.Header
	for k := range h {
		lk := strings.ToLower(k)
		// anthropic-ratelimit-* and x-codex-* are both the rejected member's
		// metered quota state — pool-boundary hygiene says a synthetic
		// response must never carry either out the pool channel (the codex
		// family joined the strip with issue #304).
		if strings.HasPrefix(lk, "anthropic-ratelimit-") || strings.HasPrefix(lk, "x-codex-") {
			h.Del(k)
		}
	}
	if replaceBody {
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength = int64(len(body))
		h.Set("Content-Type", "application/json")
		h.Set("Content-Length", strconv.Itoa(len(body)))
		h.Del("Content-Encoding")
	}
	setRetryAfter(h, retryAfter)
}

// rewriteTo503 turns an upstream 429 into the transient 503 a pool hands
// a client during a switch. The body is replaced with a small JSON
// object, Retry-After invites an almost-immediate retry, and the upstream
// rate-limit headers are stripped so the synthetic response does not
// carry the rejected backend's quota state out the pool channel.
func rewriteTo503(resp *http.Response) {
	rewriteTo503Response(resp, []byte(`{"error":"backend switching; retry"}`), true, switchRetryAfterSeconds)
}

// rewriteTo503AnthropicOverload turns native Anthropic's 529 capacity wobble
// into the same-member transient response shape with its fixed 60-second
// back-off. It delegates to the non-switching helper so the synthetic body
// and header hygiene stay aligned with the other transient throttle paths.
func rewriteTo503AnthropicOverload(resp *http.Response) {
	rewriteTo503Throttle(resp, anthropicOverloadRetryAfterSeconds)
}

// rewriteTo503Throttle turns an upstream transient throttle or overload the
// gateway has decided NOT to switch on into the transient 503 a client should
// retry against the SAME member — the z.ai/Zhipu concurrency-throttle absorb
// (issue #153), the Anthropic per-minute rate-limit back-off (issue #191), and
// native Anthropic's 529 overload path. Distinct from rewriteTo503 in two
// ways:
//
//   - body: "backend throttled; same member" — declares the action the code
//     actually took (back off the active member, no failover), so a sustained
//     stream of these responses cannot be misread as a stuck failover (issue
//     #245). A future refactor that points one of these call sites at
//     rewriteTo503 would be a behaviour change, not a cosmetic one.
//   - Retry-After is the caller-supplied secs — the absorb/backoff band is
//     deliberately longer than the 1 s switch hint so a retry lets the
//     throttle window clear before re-hitting it.
func rewriteTo503Throttle(resp *http.Response, secs int) {
	rewriteTo503Response(resp, []byte(`{"error":"backend throttled; same member"}`), true, secs)
}

// rewriteTo503DryPool turns the upstream 429 that exhausted the last member
// into the client-facing 503 emitted when the whole pool is dry. Unlike
// rewriteTo503 (the switch shape, Retry-After≈1s) it carries the precise
// long Retry-After (secs = the exact wait until the soonest member resets),
// so a client that honours it retries once the window actually recovers.
// It is a 503 rather than a forwarded 429 on purpose: a 429 ends the Claude
// Code turn as a hard rate-limit error, a 503 is a transient signal it
// retries, so the agent auto-resumes (issue #203). The upstream body is
// replaced with a generic object matching backend.writeRateLimited so both
// pool-dry emission points read consistently, and the upstream
// anthropic-ratelimit-* headers are stripped — pool-boundary hygiene: the
// synthetic response must not carry the last-parked member's quota state out
// the pool channel.
func rewriteTo503DryPool(resp *http.Response, secs int) {
	rewriteTo503Response(resp, []byte(`{"error":"all backends rate-limited"}`), true, secs)
}

// rewriteTo503WithBody turns an upstream policy/punishment 429 into a 503
// while keeping the upstream body intact, so the client can read the actual
// error message (e.g. a threatening client-identity warning from Anthropic).
// The upstream rate-limit headers are stripped (they carry no useful quota
// state for a policy 429), but Content-Type is preserved from the upstream.
//
// Content-Encoding is preserved for the same reason, and the reason is load
// bearing: the forwarded bytes are the upstream's own and the gateway never
// decodes them. internal/proxy forwards the client's Accept-Encoding verbatim
// and sets DisableCompression, so http.Transport never adds its own
// Accept-Encoding: gzip and therefore never transparently gunzips — a client
// that asked for gzip gets a compressed body here. Dropping the header that
// describes it handed that client gzip labelled application/json, corrupting
// the one path whose entire purpose is to surface the real upstream error
// (issue #291). Decompressing instead was rejected: it would break the
// bodies-stay-opaque invariant and the FlushInterval = -1 streaming contract.
func rewriteTo503WithBody(resp *http.Response) {
	rewriteTo503Response(resp, nil, false, switchRetryAfterSeconds)
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
