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
	"sort"
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

// exhaustionUtilizationThreshold is the unified-window utilization at or
// above which a member is treated as exhausted from the quota store alone,
// without waiting for a live HTTP 429 on the proxy path. 1.0 means "the
// window reports fully consumed" — the value z.ai / MiniMaxi report at cap
// (via the poller) and that Anthropic reports in its rate-limit headers.
// Keeping it at the cap preserves the sticky-until-exhausted design: a
// member is failed off proactively only once its window is genuinely spent,
// never merely busy.
const exhaustionUtilizationThreshold = 1.0

// unifiedStatusRejected is the per-window unified-status value Anthropic
// reports when a window is actually blocking requests. The other values
// ("allowed", "allowed_warning") are still served — a window can sit at
// utilization 1.0 with status "allowed_warning" in the soft-cap/overage
// zone — so when a snapshot carries a status it, not the utilization, is the
// authoritative exhaustion signal. See windowBlocks.
const unifiedStatusRejected = "rejected"

// switchRetryAfterSeconds is the Retry-After the synthetic 503 carries
// when a pool switches members. It is deliberately short: the switch is
// instantaneous server-side, so the client should retry almost
// immediately and rebuild its cache on the new backend.
const switchRetryAfterSeconds = 1

// window5h and window7d are the lengths of the Anthropic unified
// rate-limit windows. They are used by the lead calculation:
//
//	elapsed_fraction = 1 - (time_until_reset / window_length)
//	lead = utilization - elapsed_fraction
//
// A positive lead means the member is consuming faster than its window
// is depleting and should be cooled down; near-zero is on pace; negative
// is under pace.
const (
	window5h = 5 * time.Hour
	window7d = 7 * 24 * time.Hour
)

// Pools fronts each configured pool with its own Controller and routes a
// request to the right one. It implements backend.PoolRouter.
type Pools struct {
	byPool map[string]*Controller
}

// NewPools builds one Controller per pool in reg. Each controller starts
// at a random member (start < 0) so no probe traffic is needed to anchor
// it. store is the shared quota store the controllers consult to fail off a
// member reported fully consumed (poller- or header-sourced) even without a
// live 429; a nil store disables that signal and keeps pure 429-driven
// failover. now defaults to time.Now and logOut to os.Stderr when nil.
func NewPools(reg *backend.Registry, store *quota.Store, now func() time.Time, logOut io.Writer) *Pools {
	byPool := make(map[string]*Controller)
	for _, name := range reg.PoolNames() {
		byPool[name] = NewController(reg, name, -1, store, now, logOut)
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

// ClearExhausted drops the named pool's live-429 parks (see
// Controller.ClearExhausted). ok is false for an unknown pool.
func (p *Pools) ClearExhausted(poolName string) (cleared []string, ok bool) {
	c, ok := p.byPool[poolName]
	if !ok {
		return nil, false
	}
	return c.ClearExhausted(), true
}

// ClearAllExhausted drops live-429 parks across every pool, returning a
// map of pool name to the nicks cleared (pools with nothing parked are
// omitted).
func (p *Pools) ClearAllExhausted() map[string][]string {
	out := make(map[string][]string)
	for name, c := range p.byPool {
		if cleared := c.ClearExhausted(); len(cleared) > 0 {
			out[name] = cleared
		}
	}
	return out
}

// MemberStatus describes one pool member's current state for /_gateway/pool.
type MemberStatus struct {
	Nick           string          `json:"nick"`
	Status         string          `json:"status"`          // "active", "exhausted", "idle"
	ExhaustedUntil *time.Time      `json:"exhausted_until"` // RFC 3339 or null
	Snapshot       *quota.Snapshot `json:"snapshot"`        // null when no snapshot recorded

	// Lead fields are populated only for pools in balanced mode.
	// Lead is max(Lead5h, Lead7d) over known windows; null when no data.
	// Lead5h and Lead7d are null when the corresponding window has no data.
	// A positive lead means the member is consuming ahead of schedule.
	Lead   *float64 `json:"lead,omitempty"`
	Lead5h *float64 `json:"lead_5h,omitempty"`
	Lead7d *float64 `json:"lead_7d,omitempty"`
}

// PoolStatus is the /_gateway/pool response for one pool.
type PoolStatus struct {
	Pool    string         `json:"pool"`
	Active  string         `json:"active"`
	Members []MemberStatus `json:"members"`
}

// PoolConfigView is the /_gateway/config response for one pool.
// It carries the effective configuration (static + runtime overlay) with
// all credentials redacted.
type PoolConfigView struct {
	Pool         string                 `json:"pool"`
	BalanceMode  string                 `json:"balance_mode,omitempty"`
	BalanceGap   float64                `json:"balance_gap,omitempty"`
	BalanceDwell string                 `json:"balance_dwell,omitempty"`
	Priority     []string               `json:"priority,omitempty"`
	Members      []PoolMemberConfigView `json:"members"`
}

// PoolMemberConfigView describes one pool member in the config view.
type PoolMemberConfigView struct {
	Nick     string `json:"nick"`
	BaseURL  string `json:"base_url"`
	Disabled bool   `json:"disabled"`
	Status   string `json:"status"` // "active", "idle", "exhausted", "disabled"
}

// PoolStatus returns the current status of the named pool, or ok=false for an unknown pool.
func (p *Pools) PoolStatus(poolName string, store *quota.Store) (PoolStatus, bool) {
	c, ok := p.byPool[poolName]
	if !ok {
		return PoolStatus{}, false
	}
	return c.poolStatus(store), true
}

// AllPoolStatuses returns status for every pool in sorted order.
func (p *Pools) AllPoolStatuses(store *quota.Store) []PoolStatus {
	names := make([]string, 0, len(p.byPool))
	for name := range p.byPool {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]PoolStatus, 0, len(names))
	for _, name := range names {
		out = append(out, p.byPool[name].poolStatus(store))
	}
	return out
}

// PoolPersistState is the serializable routing state for one pool.
// It is exported so the persist package can embed it in GatewayState.
type PoolPersistState struct {
	Sticky            string               `json:"sticky"`
	Exhausted         map[string]time.Time `json:"exhausted"`
	LastBalanceSwitch time.Time            `json:"last_balance_switch,omitempty"`
	// BalanceSeq and LastSelectedSeq persist the selection-recency tiebreaker
	// state for balanced pools. Absent in older state files; treated as zero /
	// never-selected on load (backward-compatible).
	BalanceSeq      uint64            `json:"balance_seq,omitempty"`
	LastSelectedSeq map[string]uint64 `json:"last_selected_seq,omitempty"`
}

// PoolRuntimeConfig is the serializable runtime configuration for one pool.
// It carries operator mutations that overlay the immutable static config:
// a priority order override, a per-member disabled flag, and runtime-added
// members with their credentials.
// It is exported so the persist package can embed it in GatewayState.
type PoolRuntimeConfig struct {
	// PriorityOverride is the expanded total order (highest first) when the
	// operator has set a runtime priority order. A partial list (e.g. ["b"])
	// is expanded via effectiveOrder to include all unlisted members in sorted
	// order, so the stored form is always a complete total order. nil means
	// no override is in effect.
	PriorityOverride []string `json:"priority_override,omitempty"`
	// Disabled is the list of member nicks that are operator-disabled.
	// Each nick appears at most once. Empty means no members are disabled.
	Disabled []string `json:"disabled,omitempty"`
	// AddedMembers is the set of runtime-added pool members with their credentials.
	// Keys are normalized nicks; values include credential and optional base URL.
	// The state file may contain credentials after this change, so it must be
	// protected at 0600 (see persist package).
	AddedMembers map[string]AddedMember `json:"added_members,omitempty"`
}

// AddedMember is a runtime-added pool member with credential.
type AddedMember struct {
	Credential string `json:"credential"`           // stored, never returned in config views
	BaseURL    string `json:"base_url,omitempty"` // optional; pool default when empty
}

// LoadPersistState applies previously persisted routing state to each pool's
// controller. Called once at startup, before the server begins serving.
func (p *Pools) LoadPersistState(states map[string]PoolPersistState) {
	for name, s := range states {
		if c, ok := p.byPool[name]; ok {
			c.loadState(s.Sticky, s.Exhausted, s.LastBalanceSwitch, s.BalanceSeq, s.LastSelectedSeq)
		}
	}
}

// SetPriority sets the runtime priority override for the named pool.
// The order list is validated (all nicks must exist in the pool, no duplicates,
// no empty strings) and then expanded via effectiveOrder() to a total order.
// Returns (httpStatus, error) with error containing a credential-free message.
func (p *Pools) SetPriority(poolName string, order []string) (int, error) {
	c, ok := p.byPool[poolName]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}

	// Normalize and validate the input order.
	seen := make(map[string]bool)
	validOrder := make([]string, 0, len(order))
	for _, raw := range order {
		nick := backend.NormalizeName(raw)
		if nick == "" {
			return http.StatusBadRequest, fmt.Errorf("priority list contains empty nick")
		}
		if seen[nick] {
			return http.StatusBadRequest, fmt.Errorf("priority list contains duplicate nick: %s", nick)
		}
		seen[nick] = true
		if c.indexOf(nick) < 0 {
			return http.StatusBadRequest, fmt.Errorf("unknown nick: %s", nick)
		}
		validOrder = append(validOrder, nick)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Reject priority override on a balanced pool (mutually exclusive modes).
	if c.balanceGap > 0 {
		return http.StatusConflict, fmt.Errorf("balanced pools do not support priority override")
	}

	c.setPriorityOverrideLocked(validOrder)
	return http.StatusOK, nil
}

// SetMemberDisabled sets or clears the disabled flag for a member in a pool.
// Returns (httpStatus, error) with error containing a credential-free message.
func (p *Pools) SetMemberDisabled(poolName, nick string, off bool) (int, error) {
	c, ok := p.byPool[poolName]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}
	if c.indexOf(normalized) < 0 {
		return http.StatusBadRequest, fmt.Errorf("unknown nick: %s", normalized)
	}

	c.mu.Lock()
	c.setDisabledLocked(normalized, off)
	c.mu.Unlock()
	return http.StatusOK, nil
}

// AddMember adds a runtime member to a pool with a credential. Returns
// (httpStatus, error) with error containing a credential-free message.
func (p *Pools) AddMember(poolName, nick, credential, baseURL string) (int, error) {
	c, ok := p.byPool[poolName]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}
	if credential == "" {
		return http.StatusBadRequest, fmt.Errorf("credential is required")
	}
	// Validate baseURL if provided.
	if baseURL != "" {
		if _, err := backend.ValidateBaseURL(baseURL); err != nil {
			return http.StatusBadRequest, fmt.Errorf("invalid base_url: %w", err)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate baseURL is provided when pool has no static members to fall back to.
	if baseURL == "" && len(c.nicks) == 0 {
		return http.StatusBadRequest, fmt.Errorf("base_url is required when pool has no static members")
	}

	// Check for duplicate: already exists as static or runtime-added.
	if c.indexOf(normalized) >= 0 {
		return http.StatusConflict, fmt.Errorf("nick %s already exists as a static member", normalized)
	}
	if _, exists := c.addedMembers[normalized]; exists {
		return http.StatusConflict, fmt.Errorf("nick %s already exists as a runtime-added member", normalized)
	}

	// If previously removed, clear the removed flag so it becomes selectable again.
	delete(c.removedMembers, normalized)

	// Store the added member.
	c.addedMembers[normalized] = AddedMember{
		Credential: credential,
		BaseURL:    baseURL,
	}
	c.notifyMutate()
	fmt.Fprintf(c.logOut, "auto[%s]: added runtime member %s\n", c.pool, normalized)
	return http.StatusOK, nil
}

// RemoveMember removes a member (static or runtime-added) from pool selection.
// Returns (httpStatus, error) with error containing a credential-free message.
func (p *Pools) RemoveMember(poolName, nick string) (int, error) {
	c, ok := p.byPool[poolName]
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if it's a static member or a runtime-added member.
	isStatic := c.indexOf(normalized) >= 0
	_, isAdded := c.addedMembers[normalized]

	if !isStatic && !isAdded {
		return http.StatusBadRequest, fmt.Errorf("nick %s not found in pool", normalized)
	}

	// If it's a runtime-added member, remove it entirely.
	if isAdded {
		delete(c.addedMembers, normalized)
		fmt.Fprintf(c.logOut, "auto[%s]: removed runtime-added member %s\n", c.pool, normalized)
	}

	// Mark as removed so it's hidden from selection.
	// For static members, this is the only effect (they stay in static config).
	// For added members, we already deleted them, but set removed flag anyway
	// for consistency and in case of races.
	c.removedMembers[normalized] = true

	// If the removed member was the active sticky pointer, force-switch to
	// the next healthy member. This is similar to what happens on a 429.
	isActive := (c.curAddedNick == normalized) || (c.curAddedNick == "" && normalized == c.nicks[c.cur])
	if isActive {
		if nick, ok := c.firstHealthyNickLocked(); ok {
			// Determine "from" directly without calling Current() (we already hold the lock)
			from := c.curAddedNick
			if from == "" && len(c.nicks) > c.cur {
				from = c.nicks[c.cur]
			}
			c.setActiveMemberLocked(nick)
			fmt.Fprintf(c.logOut, "auto[%s]: switched %s -> %s (removed member %s)\n", c.pool, from, nick, normalized)
		}
	}

	c.notifyMutate()
	return http.StatusOK, nil
}

// EffectiveConfig returns the effective configuration for all pools,
// with credentials fully redacted. Each pool's view includes its balance
// settings, effective priority (runtime override when set, else env priority),
// and per-member status including the disabled flag.
func (p *Pools) EffectiveConfig() []PoolConfigView {
	names := make([]string, 0, len(p.byPool))
	for name := range p.byPool {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]PoolConfigView, 0, len(names))
	for _, name := range names {
		c := p.byPool[name]
		c.mu.Lock()
		view := PoolConfigView{Pool: name}

		// Balance settings.
		if c.balanceGap > 0 {
			view.BalanceMode = "lead"
			view.BalanceGap = c.balanceGap
			view.BalanceDwell = c.balanceDwell.String()
		}

		// Effective priority.
		pri := c.effectivePriorityLocked()
		if len(pri) > 0 {
			view.Priority = make([]string, len(pri))
			copy(view.Priority, pri)
		}

		// Members.
		// Include both static and runtime-added members.
		// Static members always appear (even if removed), runtime-added
		// members disappear when removed.
		allMembers := make([]string, 0, len(c.nicks)+len(c.addedMembers))
		for _, nick := range c.nicks {
			allMembers = append(allMembers, nick)
		}
		for nick := range c.addedMembers {
			if !c.removedMembers[nick] {
				allMembers = append(allMembers, nick)
			}
		}
		sort.Strings(allMembers)

		view.Members = make([]PoolMemberConfigView, 0, len(allMembers))
		curNick := c.curAddedNick
		if curNick == "" && len(c.nicks) > 0 {
			curNick = c.nicks[c.cur]
		}

		for _, nick := range allMembers {
			member := PoolMemberConfigView{
				Nick:     nick,
				Disabled: c.disabled[nick],
			}
			// Get BaseURL: runtime-added members carry their own (or inherit
			// the pool default); static members read it from the backend.
			if c.isAddedMemberLocked(nick) {
				if am, ok := c.addedMembers[nick]; ok {
					if am.BaseURL != "" {
						member.BaseURL = am.BaseURL
					} else if len(c.nicks) > 0 {
						member.BaseURL = c.backendAt(0).BaseURL
					}
				}
			} else {
				member.BaseURL = c.backendAt(c.indexOf(nick)).BaseURL
			}
			// Determine status.
			if c.disabled[nick] || c.removedMembers[nick] {
				member.Status = "disabled"
				// Mark removed members as disabled too
				if c.removedMembers[nick] {
					member.Disabled = true
				}
			} else if nick == curNick {
				member.Status = "active"
			} else if _, ok := c.exhaustedUntilLocked(nick); ok {
				// exhaustedUntilLocked already returns ok=false once the park
				// elapses by c.now(), so it is the single source of truth for
				// the exhausted status — consistent with poolStatus and the
				// selection path, which all use the controller clock.
				member.Status = "exhausted"
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

// PersistRuntimeConfig snapshots the runtime configuration for all pools.
func (p *Pools) PersistRuntimeConfig() map[string]PoolRuntimeConfig {
	out := make(map[string]PoolRuntimeConfig, len(p.byPool))
	for name, c := range p.byPool {
		out[name] = c.runtimeConfig()
	}
	return out
}

// LoadRuntimeConfig restores runtime configuration from persisted state.
func (p *Pools) LoadRuntimeConfig(cfg map[string]PoolRuntimeConfig) {
	for name, poolCfg := range cfg {
		if c, ok := p.byPool[name]; ok {
			c.loadRuntimeConfig(poolCfg)
		}
	}
}

// PersistState snapshots the current routing state for all pools.
func (p *Pools) PersistState() map[string]PoolPersistState {
	out := make(map[string]PoolPersistState, len(p.byPool))
	for name, c := range p.byPool {
		out[name] = c.persistState()
	}
	return out
}

// SetOnMutate installs a callback that every controller calls (non-blocking)
// after any mutation to its sticky pointer or exhausted map. Used by the
// persister to coalesce writes without importing this package.
func (p *Pools) SetOnMutate(fn func()) {
	for _, c := range p.byPool {
		c.onMutate = fn
	}
}

// Controller is the sticky selector for one pool. The zero value is not
// usable; call NewController.
type Controller struct {
	mu sync.Mutex

	reg   *backend.Registry
	pool  string
	nicks []string // the pool's members, in stable sorted order; len >= 1

	// store is the shared quota store. A member whose snapshot reports its
	// unified window fully consumed (with a reset still ahead) is treated as
	// exhausted even when no live 429 was seen on the proxy path — the only
	// exhaustion signal for poller-tracked backends (z.ai / MiniMaxi). nil
	// disables the signal, leaving pure 429-driven failover.
	store *quota.Store

	// priority is the full preference order (highest first) when the pool
	// opted into priority routing via AQG_POOL_<POOL>_PRIORITY: the
	// declared nicks first, then any unlisted members in sorted order. It
	// is nil for a pool with no declared priority, which keeps the default
	// random-start, round-robin-failover behaviour.
	priority []string

	// priorityOverride is the runtime-configurable priority order that
	// overrides the static priority. When set, effectivePriorityLocked()
	// returns this instead of c.priority. nil means no override is in effect.
	priorityOverride []string

	// disabled maps member nicks to a disabled flag: a member in this map
	// is unselectable regardless of its exhaustion state, until explicitly
	// re-enabled via SetMemberDisabled. This is operator-set, never
	// auto-cleared, and distinct from the exhausted map (which ages out
	// on reset). Accessed only under c.mu.
	disabled map[string]bool

	// addedMembers holds runtime-added pool members with their credentials.
	// Keys are normalized nicks. Accessed only under c.mu.
	addedMembers map[string]AddedMember
	// removedMembers marks members (static or runtime-added) as operator-removed.
	// A removed member is hidden from selection even if it exists in the static
	// base or was previously added. Accessed only under c.mu.
	removedMembers map[string]bool

	// curAddedNick is the nick of the currently active runtime-added member.
	// When empty, the active member is a static member at index c.cur.
	// Accessed only under c.mu.
	curAddedNick string

	// cur indexes nicks: nicks[cur] is the backend every request to this
	// pool sticks to until it 429s.
	cur int

	// exhausted maps a nick to the absolute time its blocking window
	// resets. Presence means "exhausted-until-reset"; entries are cleared
	// lazily once now >= reset.
	exhausted map[string]time.Time

	now    func() time.Time
	logOut io.Writer

	// onMutate, if non-nil, is called (non-blocking) after any mutation to
	// cur or exhausted. Set by Pools.SetOnMutate to notify the persister.
	onMutate func()

	// balanceGap is the minimum lead difference (active minus candidate)
	// that triggers a balance switch. 0 means balance mode is off for this
	// pool; populated from AQG_POOL_<POOL>_BALANCE_GAP (default 0.15).
	balanceGap float64
	// balanceDwell is the minimum time between balance switches. Populated
	// from AQG_POOL_<POOL>_BALANCE_DWELL (default 5m).
	balanceDwell time.Duration
	// lastBalanceSwitch records the most recent balance switch time for
	// dwell enforcement. Zero when no balance switch has occurred.
	lastBalanceSwitch time.Time

	// balanceSeq is a pool-level monotonic counter incremented each time the
	// sticky pointer moves to a different member in a balanced pool. Together
	// with lastSelectedSeq it implements the equal-lead tiebreaker: among
	// eligible candidates with the same best lead, the one with the smallest
	// lastSelectedSeq (least recently selected) wins.
	balanceSeq uint64
	// lastSelectedSeq maps a nick to the sequence number at which it last
	// became the active member in a balanced pool. 0 (absent) means the
	// member has never been selected.
	lastSelectedSeq map[string]uint64
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
	nicks := reg.PoolNicks(poolName) // sorted; Load guarantees at least one per pool
	c := &Controller{
		reg:             reg,
		pool:            poolName,
		nicks:           nicks,
		priority:        effectiveOrder(reg.PoolPriority(poolName), nicks),
		store:           store,
		exhausted:       make(map[string]time.Time),
		now:             now,
		logOut:          logOut,
		balanceGap:      reg.PoolBalanceGap(poolName),
		balanceDwell:    reg.PoolBalanceDwell(poolName),
		lastSelectedSeq: make(map[string]uint64),
		disabled:        make(map[string]bool),
		addedMembers:    make(map[string]AddedMember),
		removedMembers:  make(map[string]bool),
	}
	n := len(nicks)
	if n == 0 {
		// Defensive: a pool with no members should never reach here, but
		// guard so cur stays in range.
		return c
	}
	if start < 0 {
		// A priority pool anchors on its highest-priority member (nothing is
		// exhausted at construction, so that is priority[0]); a plain pool
		// starts at a random member as before.
		if len(c.priority) > 0 {
			start = c.indexOf(c.priority[0])
		} else {
			start = randIndex(n)
		}
	}
	c.cur = ((start % n) + n) % n
	// Stamp the initial pick so it is distinguishable from members that have
	// never been active. loadState may overwrite this with persisted values.
	c.stampSelectionLocked(c.nicks[c.cur])
	return c
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

// indexOf returns the index of nick in c.nicks, or -1 if absent. Pools are
// small, so a linear scan is cheaper than maintaining a map.
func (c *Controller) indexOf(nick string) int {
	for i, n := range c.nicks {
		if n == nick {
			return i
		}
	}
	return -1
}

// isAddedMemberLocked reports whether nick is a runtime-added member.
// Caller holds c.mu.
func (c *Controller) isAddedMemberLocked(nick string) bool {
	_, ok := c.addedMembers[nick]
	return ok
}

// isRemovedLocked reports whether nick has been operator-removed (hidden
// from selection). This applies to both static members and runtime-added members.
// Caller holds c.mu.
func (c *Controller) isRemovedLocked(nick string) bool {
	return c.removedMembers[nick]
}

// addedMembersLocked returns the union of static member nicks and runtime-added
// member nicks, sorted. Caller holds c.mu.
func (c *Controller) addedMembersLocked() []string {
	// Start with static nicks (already sorted)
	out := make([]string, 0, len(c.nicks)+len(c.addedMembers))
	seen := make(map[string]bool, len(c.nicks)+len(c.addedMembers))

	for _, nick := range c.nicks {
		if !c.removedMembers[nick] {
			out = append(out, nick)
			seen[nick] = true
		}
	}
	for nick := range c.addedMembers {
		if !seen[nick] && !c.removedMembers[nick] {
			out = append(out, nick)
		}
	}
	sort.Strings(out)
	return out
}

// effectivePriorityLocked returns the effective priority order for this pool:
// c.priorityOverride when set, otherwise c.priority. The override is the
// runtime-configurable order; the base priority is the env-declared order.
// Returns nil for a non-priority pool. Caller holds c.mu.
func (c *Controller) effectivePriorityLocked() []string {
	if c.priorityOverride != nil {
		return c.priorityOverride
	}
	return c.priority
}

// isUnavailableLocked reports whether nick is currently unavailable for
// selection, by either signal: exhausted (live 429 or store-driven),
// operator-disabled, or operator-removed. This unifies the blocking signals
// so the selection path can ask one question. The disabled and removed flags
// are never auto-cleared, unlike exhausted marks which age out on reset.
// Caller holds c.mu.
func (c *Controller) isUnavailableLocked(nick string) bool {
	if c.disabled[nick] || c.removedMembers[nick] {
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

	// Determine the current member's nick.
	var curNick string
	if c.curAddedNick != "" {
		curNick = c.curAddedNick
	} else {
		curNick = c.nicks[c.cur]
	}

	// If current is healthy, return it (with balance check for static).
	if !c.isUnavailableLocked(curNick) {
		// For balanced mode with static members, check for a balance switch.
		if c.balanceGap > 0 && c.curAddedNick == "" {
			if idx, ok := c.balanceSwitchLocked(); ok {
				from := c.nicks[c.cur]
				c.cur = idx
				c.lastBalanceSwitch = c.now()
				c.stampSelectionLocked(c.nicks[idx])
				c.notifyMutate()
				fmt.Fprintf(c.logOut, "auto[%s]: balance %s -> %s (lead gap)\n", c.pool, from, c.nicks[idx])
				return c.backendAt(idx), 0, false
			}
		}
		// Current added member: return it directly.
		if c.curAddedNick != "" {
			if b, ok := c.backendByNickLocked(c.curAddedNick); ok {
				return b, 0, false
			}
			// Fallback if something went wrong: clear and continue.
			c.curAddedNick = ""
		} else {
			return c.backendAt(c.cur), 0, false
		}
	}

	// Current is unavailable; find a healthy replacement.
	if nick, ok := c.firstHealthyNickLocked(); ok {
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

// ClearExhausted drops every live-429 park for this pool, making each
// member immediately selectable again (still subject to the quota store's
// own fully-consumed window check). It exists to undo parks written by a
// transient or erroneous upstream 429 — e.g. an account that got 429'd by
// a misconfigured request but in fact still has quota. It does NOT touch
// store-sourced exhaustion, which reflects polled reality and clears on its
// own reset. Returns the nicks whose park was cleared, sorted.
func (c *Controller) ClearExhausted() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.exhausted) == 0 {
		return nil
	}
	cleared := make([]string, 0, len(c.exhausted))
	for nick := range c.exhausted {
		cleared = append(cleared, nick)
	}
	sort.Strings(cleared)
	c.exhausted = make(map[string]time.Time)
	c.notifyMutate()
	return cleared
}

// Current returns the nick of the active sticky backend.
func (c *Controller) Current() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.curAddedNick != "" {
		return c.curAddedNick
	}
	return c.nicks[c.cur]
}

// CurrentBackend returns the active sticky backend, for the quota view.
func (c *Controller) CurrentBackend() backend.Backend {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.curAddedNick != "" {
		if b, ok := c.backendByNickLocked(c.curAddedNick); ok {
			return b
		}
		// Fallback if something went wrong.
		return c.backendAt(c.cur)
	}
	return c.backendAt(c.cur)
}

// notifyMutate calls c.onMutate if set. It is safe to call while holding
// c.mu because onMutate is a non-blocking channel send in the persister.
func (c *Controller) notifyMutate() {
	if c.onMutate != nil {
		c.onMutate()
	}
}

// stampSelectionLocked records that nick just became the active member in a
// balanced pool. It increments the pool-level sequence counter and stores the
// new value for nick. No-op for non-balanced pools. Caller holds c.mu.
func (c *Controller) stampSelectionLocked(nick string) {
	if c.balanceGap == 0 {
		return
	}
	c.balanceSeq++
	c.lastSelectedSeq[nick] = c.balanceSeq
}

// poolStatus builds the /_gateway/pool response for this controller. store
// is consulted for each member's latest snapshot; a member with no recorded
// snapshot gets snapshot:null. Caller must not hold c.mu.
func (c *Controller) poolStatus(store *quota.Store) PoolStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearExpiredLocked()

	curNick := c.nicks[c.cur]
	members := make([]MemberStatus, 0, len(c.nicks))
	for _, nick := range c.nicks {
		ms := MemberStatus{Nick: nick}
		if c.disabled[nick] {
			ms.Status = "disabled"
		} else if nick == curNick {
			ms.Status = "active"
		} else if reset, ok := c.exhaustedUntilLocked(nick); ok {
			ms.Status = "exhausted"
			r := reset.UTC()
			ms.ExhaustedUntil = &r
		} else {
			ms.Status = "idle"
		}
		if idx := c.indexOf(nick); idx >= 0 {
			snap := store.Get(c.backendAt(idx).QuotaKey())
			if snap.HasData() {
				snapCopy := snap
				ms.Snapshot = &snapCopy
			}
		}
		if c.balanceGap > 0 {
			overall, l5h, l7d, has5h, has7d := c.memberLeadsLocked(nick)
			if has5h || has7d {
				ov := overall
				ms.Lead = &ov
			}
			if has5h {
				v := l5h
				ms.Lead5h = &v
			}
			if has7d {
				v := l7d
				ms.Lead7d = &v
			}
		}
		members = append(members, ms)
	}
	return PoolStatus{Pool: c.pool, Active: curNick, Members: members}
}

// loadState applies persisted routing state. Exhausted entries whose reset
// has already passed are silently dropped. Persisted nicks absent from the
// current pool membership are logged and skipped. Called once at startup
// before the server begins serving; does not call onMutate.
func (c *Controller) loadState(sticky string, exhausted map[string]time.Time, lastBalanceSwitch time.Time, balanceSeq uint64, lastSelectedSeq map[string]uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx := c.indexOf(sticky); idx >= 0 {
		c.cur = idx
	} else if sticky != "" {
		reason := "random"
		if len(c.priority) > 0 {
			reason = "priority"
		}
		fmt.Fprintf(c.logOut, "loadState[%s]: persisted sticky=%s not in current pool members; falling back to %s (%s)\n",
			c.pool, sticky, c.nicks[c.cur], reason)
	}
	now := c.now()
	for nick, reset := range exhausted {
		if !reset.After(now) {
			continue
		}
		if c.indexOf(nick) < 0 {
			fmt.Fprintf(c.logOut, "loadState[%s]: dropping persisted exhausted entry %s (not in current pool members)\n",
				c.pool, nick)
			continue
		}
		c.exhausted[nick] = reset
	}
	if c.balanceDwell > 0 && !lastBalanceSwitch.IsZero() {
		c.lastBalanceSwitch = lastBalanceSwitch
	}
	if c.balanceGap > 0 {
		// Load persisted selection-recency state, skipping nicks no longer in the pool.
		if balanceSeq > c.balanceSeq {
			c.balanceSeq = balanceSeq
		}
		for nick, seq := range lastSelectedSeq {
			if c.indexOf(nick) >= 0 {
				c.lastSelectedSeq[nick] = seq
			}
		}
		// Seed the sticky member if no persisted seq exists (fresh install or
		// upgrade from a state file that predates this feature). This ensures
		// the currently active member is never treated as "never selected",
		// which would let it win all future equal-lead tiebreaks indefinitely.
		if _, stamped := c.lastSelectedSeq[c.nicks[c.cur]]; !stamped {
			c.stampSelectionLocked(c.nicks[c.cur])
		}
	}
}

// persistState snapshots the controller's routing state for serialisation.
func (c *Controller) persistState() PoolPersistState {
	c.mu.Lock()
	defer c.mu.Unlock()
	ex := make(map[string]time.Time, len(c.exhausted))
	for k, v := range c.exhausted {
		ex[k] = v
	}
	ps := PoolPersistState{
		Sticky:            c.nicks[c.cur],
		Exhausted:         ex,
		LastBalanceSwitch: c.lastBalanceSwitch,
	}
	if c.balanceGap > 0 && c.balanceSeq > 0 {
		ps.BalanceSeq = c.balanceSeq
		seqs := make(map[string]uint64, len(c.lastSelectedSeq))
		for k, v := range c.lastSelectedSeq {
			seqs[k] = v
		}
		ps.LastSelectedSeq = seqs
	}
	return ps
}

// setPriorityOverrideLocked sets the runtime priority override for this pool.
// The input order is expanded via effectiveOrder() to produce a total order,
// matching the form env priority is stored in. This means a partial override
// (e.g. ["b"] on a 3-member pool) yields the same total order live and after
// restart. The override does NOT force-switch the active sticky member.
// Caller holds c.mu.
func (c *Controller) setPriorityOverrideLocked(order []string) {
	if len(order) == 0 {
		c.priorityOverride = nil
	} else {
		c.priorityOverride = effectiveOrder(order, c.nicks)
	}
	c.notifyMutate()
}

// setDisabledLocked sets the disabled flag for a member. When off is true,
// the member is marked disabled and becomes unselectable. When off is false,
// the member is re-enabled. The operation does NOT force-switch the active
// sticky member. Caller holds c.mu.
func (c *Controller) setDisabledLocked(nick string, off bool) {
	if off {
		c.disabled[nick] = true
	} else {
		delete(c.disabled, nick)
	}
	c.notifyMutate()
}

// runtimeConfig snapshots the runtime configuration for this pool:
// the current priority override (if any) and the list of disabled members.
// Caller must not hold c.mu.
func (c *Controller) runtimeConfig() PoolRuntimeConfig {
	c.mu.Lock()
	defer c.mu.Unlock()
	var priOverride []string
	if c.priorityOverride != nil {
		priOverride = make([]string, len(c.priorityOverride))
		copy(priOverride, c.priorityOverride)
	}
	disabled := make([]string, 0, len(c.disabled))
	for nick := range c.disabled {
		disabled = append(disabled, nick)
	}
	sort.Strings(disabled)

	// Include added members with credentials (state file may contain credentials).
	addedMembers := make(map[string]AddedMember, len(c.addedMembers))
	for nick, am := range c.addedMembers {
		addedMembers[nick] = AddedMember{
			Credential: am.Credential, // stored, never returned in config views
			BaseURL:    am.BaseURL,
		}
	}

	return PoolRuntimeConfig{
		PriorityOverride: priOverride,
		Disabled:         disabled,
		AddedMembers:     addedMembers,
	}
}

// loadRuntimeConfig restores runtime configuration from persisted state.
// Unknown pool/member references are dropped with a logged warning, never a
// startup failure. The input priority override is expanded via effectiveOrder.
// Caller must not hold c.mu.
func (c *Controller) loadRuntimeConfig(cfg PoolRuntimeConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Restore priority override.
	if len(cfg.PriorityOverride) > 0 {
		// Validate that all nicks in the override are current members.
		validOverride := make([]string, 0, len(cfg.PriorityOverride))
		for _, nick := range cfg.PriorityOverride {
			if c.indexOf(nick) >= 0 {
				validOverride = append(validOverride, nick)
			} else {
				fmt.Fprintf(c.logOut, "loadRuntimeConfig[%s]: dropping unknown nick %q from priority override\n", c.pool, nick)
			}
		}
		if len(validOverride) > 0 {
			c.priorityOverride = effectiveOrder(validOverride, c.nicks)
		} else {
			c.priorityOverride = nil
		}
	} else {
		c.priorityOverride = nil
	}

	// Restore disabled set.
	c.disabled = make(map[string]bool)
	for _, nick := range cfg.Disabled {
		if c.indexOf(nick) >= 0 {
			c.disabled[nick] = true
		} else {
			fmt.Fprintf(c.logOut, "loadRuntimeConfig[%s]: dropping unknown nick %q from disabled list\n", c.pool, nick)
		}
	}

	// Restore added members (including their credentials).
	c.addedMembers = make(map[string]AddedMember)
	for nick, am := range cfg.AddedMembers {
		// Validate that the nick doesn't collide with a static member.
		if c.indexOf(nick) >= 0 {
			fmt.Fprintf(c.logOut, "loadRuntimeConfig[%s]: dropping added member %q (collides with static member)\n", c.pool, nick)
			continue
		}
		// Restore with credential intact.
		c.addedMembers[nick] = AddedMember{
			Credential: am.Credential,
			BaseURL:    am.BaseURL,
		}
	}
	// Note: removedMembers is intentionally not persisted.
	// A removed static member is re-selected on restart unless removed again.
	// A removed added member is simply absent from addedMembers on reload.
}

// ModifyResponse is the per-pool failover hook. It acts on two classes of
// upstream response; everything else passes through untouched.
//
//   - 429 Too Many Requests: it first classifies whether the 429 signals
//     genuine quota exhaustion (a "rejected" rate-limit status) or is a
//     policy/punishment 429 (no rate-limit headers). Policy 429s are not
//     parked — the backend stays in rotation and the client receives a 503
//     carrying the upstream error body. Only genuine exhaustion 429s park the
//     backend and advance the sticky pointer.
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

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		respSnap := quota.Extract(resp)
		if !c.isGenuineExhaustionSignal(b.Nick, respSnap) {
			fmt.Fprintf(c.logOut, "auto[%s]: %s policy 429 (no exhaustion signal) — not parking\n", c.pool, b.Nick)
			rewriteTo503WithBody(resp)
			return nil
		}
		// A genuine 429 carries a precise window reset; park until then.
		return c.parkAndFailover(resp, b.Nick, c.resetFrom(resp), "hit 429")
	case isCredentialRejected(resp.StatusCode):
		// An auth rejection has no reset — the credential is simply dead — so
		// park for the conservative default window: long enough to keep the
		// pool off the dead account, short enough that a restored account is
		// retried without an operator restart (or an immediate /_gateway/clear).
		return c.parkAndFailover(resp, b.Nick, c.now().Add(defaultExhaustionWindow), fmt.Sprintf("returned %d", resp.StatusCode))
	default:
		return nil
	}
}

// isCredentialRejected reports whether code means the backend's credential
// was refused upstream (401/403) — a backend-fatal signal distinct from a
// 429's recoverable quota exhaustion.
func isCredentialRejected(code int) bool {
	return code == http.StatusUnauthorized || code == http.StatusForbidden
}

// parkAndFailover parks nick until reset, advances the sticky pointer, and
// rewrites resp: a 503 "backend switching" when a healthy member remains, or
// the honest upstream status with a precise Retry-After when the pool is dry.
// reason is the log phrase describing why the backend was parked.
func (c *Controller) parkAndFailover(resp *http.Response, nick string, reset time.Time, reason string) error {
	res := c.record429(nick, reset)

	if res.allExhausted {
		secs := retryAfterSeconds(res.retryAfter)
		setRetryAfter(resp.Header, secs)
		fmt.Fprintf(c.logOut, "auto[%s]: all backends exhausted; forwarding upstream %d (retry after %ds)\n", c.pool, resp.StatusCode, secs)
		return nil
	}

	if res.switched {
		fmt.Fprintf(c.logOut, "auto[%s]: %s -> %s (%s %s)\n", c.pool, nick, res.to, nick, reason)
	}
	rewriteTo503(resp)
	return nil
}

// isGenuineExhaustionSignal reports whether a 429 response for nick represents
// real quota exhaustion (park it) versus a policy/punishment 429 such as an
// "unsupported third-party client" rejection (leave it in rotation, forward
// the body).
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
func (c *Controller) isGenuineExhaustionSignal(nick string, respSnap quota.Snapshot) bool {
	if snapRejects(respSnap) {
		return true
	}
	if c.store != nil {
		if idx := c.indexOf(nick); idx >= 0 {
			if snapRejects(c.store.Get(c.backendAt(idx).QuotaKey())) {
				return true
			}
		}
	}
	return false
}

// snapRejects reports whether snap shows the backend actually rate-limited:
// an overall "rejected" unified status, or either unified window blocking
// (see windowBlocks — a per-window "rejected", or, absent a status, a
// utilization at the cap).
func snapRejects(snap quota.Snapshot) bool {
	return snap.UnifiedStatus == unifiedStatusRejected ||
		windowBlocks(snap.Unified5hUtilization, snap.Unified5hStatus) ||
		windowBlocks(snap.Unified7dUtilization, snap.Unified7dStatus)
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
	if !c.isUnavailableLocked(c.nicks[c.cur]) {
		c.notifyMutate()
		return record429Result{to: c.nicks[c.cur]}
	}
	if idx, ok := c.firstHealthyLocked(); ok {
		from := c.cur
		c.cur = idx
		if idx != from {
			c.stampSelectionLocked(c.nicks[idx])
		}
		c.notifyMutate()
		return record429Result{to: c.nicks[idx], switched: idx != from}
	}
	idx, soonest := c.soonestLocked()
	c.cur = idx
	c.notifyMutate()
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

// isExhaustedLocked reports whether nick is currently unselectable, by
// either signal: the live-429 park or the quota store's fully-consumed
// window. Caller holds c.mu.
func (c *Controller) isExhaustedLocked(nick string) bool {
	_, ok := c.exhaustedUntilLocked(nick)
	return ok
}

// exhaustedUntilLocked returns the time nick stays unselectable and whether
// it is exhausted at all, unifying the two exhaustion signals: the explicit
// park set by a live 429 (record429) and the quota store's fully-consumed
// window (poller- or header-sourced). When both apply the later reset wins,
// so a member is never re-selected while either signal still blocks it.
// Caller holds c.mu.
func (c *Controller) exhaustedUntilLocked(nick string) (time.Time, bool) {
	reset, ok := c.exhausted[nick]
	if ok && !c.now().Before(reset) {
		ok = false // park already elapsed
	}
	if sReset, sOK := c.storeExhaustedUntilLocked(nick); sOK {
		if !ok || sReset.After(reset) {
			reset, ok = sReset, true
		}
	}
	return reset, ok
}

// storeExhaustedUntilLocked reports nick's window reset when the quota store
// shows a unified window actually blocking (see windowBlocks: a "rejected"
// status, or — absent a status — utilization at the cap) with a reset still
// in the future. It considers BOTH the 5h and 7d windows: each contributes
// only when its own window blocks and its own reset is ahead, and when both
// qualify the later reset wins, so the returned time is always anchored to
// the window that actually flagged the member — never the 7d reset for a
// 5h-only exhaustion or vice versa.
// Checking 7d matters for poller-tracked backends (z.ai / MiniMaxi), which
// report a weekly cap through the dashboard API and emit no clean
// proxy-path 429 to catch a 7d-exhausted-but-5h-healthy member the reactive
// way.
//
// ok is false when no window qualifies — no store, no snapshot, every
// utilization below threshold, or a missing/past reset. Requiring a future
// reset also makes a stale frozen entry (the poller only tracks the active
// member, so a failed-off member's snapshot freezes at its reset) read
// healthy once that reset passes, without a re-poll. Caller holds c.mu; the
// store has its own lock and never calls back into the controller.
func (c *Controller) storeExhaustedUntilLocked(nick string) (time.Time, bool) {
	if c.store == nil {
		return time.Time{}, false
	}
	idx := c.indexOf(nick)
	if idx < 0 {
		return time.Time{}, false
	}
	snap := c.store.Get(c.backendAt(idx).QuotaKey())
	reset, ok := time.Time{}, false
	for _, w := range [...]struct {
		util   *float64
		status string
		reset  *time.Time
	}{
		{snap.Unified5hUtilization, snap.Unified5hStatus, snap.Unified5hReset},
		{snap.Unified7dUtilization, snap.Unified7dStatus, snap.Unified7dReset},
	} {
		if !windowBlocks(w.util, w.status) {
			continue
		}
		if w.reset == nil || !c.now().Before(*w.reset) {
			continue
		}
		if !ok || w.reset.After(reset) {
			reset, ok = *w.reset, true
		}
	}
	return reset, ok
}

// windowBlocks reports whether a unified rate-limit window is actually
// rejecting requests, deciding by whichever signal the snapshot carries:
//
//   - When the window has a status (Anthropic header path), the status is
//     authoritative. Only "rejected" blocks — Anthropic reports a window at
//     utilization 1.0 with status "allowed"/"allowed_warning" while still
//     serving it (the soft-cap / overage / fallback zone). Treating 1.0 as
//     exhausted there wrongly parks a member Anthropic would happily serve,
//     which can lock an entire pool out as "all exhausted".
//   - When the window has no status (poller-tracked z.ai / MiniMaxi / Ark,
//     which report only a utilization fraction), fall back to the cap.
func windowBlocks(util *float64, status string) bool {
	if status != "" {
		return status == unifiedStatusRejected
	}
	return util != nil && *util >= exhaustionUtilizationThreshold
}

// memberLeadsLocked computes the routing pressure for nick from the quota
// store. It returns per-window leads (utilization minus elapsed window
// fraction, clamped elapsed to [0,1]) and the overall max lead. has5h and
// has7d are true when the corresponding window had enough data (non-nil
// utilization, non-nil reset, reset still in the future). When neither
// window has data all returned floats are 0 and both has flags are false.
// Caller holds c.mu; the store has its own lock.
func (c *Controller) memberLeadsLocked(nick string) (overall, lead5h, lead7d float64, has5h, has7d bool) {
	if c.store == nil {
		return 0, 0, 0, false, false
	}
	idx := c.indexOf(nick)
	if idx < 0 {
		return 0, 0, 0, false, false
	}
	snap := c.store.Get(c.backendAt(idx).QuotaKey())
	now := c.now()

	computeLead := func(util *float64, reset *time.Time, windowLen time.Duration) (float64, bool) {
		if util == nil || reset == nil || !reset.After(now) {
			return 0, false
		}
		elapsed := 1.0 - float64(reset.Sub(now))/float64(windowLen)
		if elapsed < 0 {
			elapsed = 0
		} else if elapsed > 1 {
			elapsed = 1
		}
		return *util - elapsed, true
	}

	lead5h, has5h = computeLead(snap.Unified5hUtilization, snap.Unified5hReset, window5h)
	lead7d, has7d = computeLead(snap.Unified7dUtilization, snap.Unified7dReset, window7d)

	switch {
	case has5h && has7d:
		if lead5h >= lead7d {
			overall = lead5h
		} else {
			overall = lead7d
		}
	case has5h:
		overall = lead5h
	case has7d:
		overall = lead7d
	}
	return overall, lead5h, lead7d, has5h, has7d
}

// balanceSwitchLocked returns the index of the member to switch to when
// the active member's overall lead exceeds the best candidate's lead by
// at least balanceGap and the dwell timer has elapsed since the last
// switch. Returns (0, false) when no switch is warranted. Caller holds
// c.mu.
//
// Among eligible candidates with the same best lead (including the common
// all-zero / no-snapshot case), the one with the smallest lastSelectedSeq
// wins: the member that was least recently active is preferred, spreading
// 5-hour cycles across pool members rather than repeatedly re-selecting
// the lexically-first nick.
func (c *Controller) balanceSwitchLocked() (int, bool) {
	if !c.lastBalanceSwitch.IsZero() && c.now().Sub(c.lastBalanceSwitch) < c.balanceDwell {
		return 0, false
	}
	curOverall, _, _, _, _ := c.memberLeadsLocked(c.nicks[c.cur])

	bestIdx := -1
	bestLead := curOverall
	var bestSeq uint64
	for i, nick := range c.nicks {
		if i == c.cur || c.isUnavailableLocked(nick) {
			continue
		}
		candOverall, _, _, _, _ := c.memberLeadsLocked(nick)
		if curOverall-candOverall < c.balanceGap {
			continue
		}
		seq := c.lastSelectedSeq[nick]
		if bestIdx < 0 || candOverall < bestLead || (candOverall == bestLead && seq < bestSeq) {
			bestLead = candOverall
			bestIdx = i
			bestSeq = seq
		}
	}
	if bestIdx < 0 {
		return 0, false
	}
	return bestIdx, true
}

// firstHealthyLocked finds the backend to fail over to. For a priority
// pool it returns the highest-priority available member (not exhausted,
// not disabled), so failover always climbs toward the preferred backend.
// For a plain pool it scans round-robin from just after cur so switches
// spread across the pool rather than always hopping to the lexically-first
// nick. Caller holds c.mu.
func (c *Controller) firstHealthyLocked() (int, bool) {
	if pri := c.effectivePriorityLocked(); len(pri) > 0 {
		for _, nick := range pri {
			if c.isUnavailableLocked(nick) {
				continue
			}
			if idx := c.indexOf(nick); idx >= 0 {
				return idx, true
			}
		}
		return 0, false
	}
	n := len(c.nicks)
	for off := 1; off <= n; off++ {
		idx := (c.cur + off) % n
		if !c.isUnavailableLocked(c.nicks[idx]) {
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
		if c.disabled[nick] {
			continue
		}
		reset, ok := c.exhaustedUntilLocked(nick)
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

// backendByNickLocked resolves a backend by nick, handling both static and
// runtime-added members. For static members it uses the registry; for added
// members it builds a Backend from the stored credential and base URL.
// Caller holds c.mu.
func (c *Controller) backendByNickLocked(nick string) (backend.Backend, bool) {
	// Check if it's a runtime-added member first.
	if am, ok := c.addedMembers[nick]; ok {
		baseURL := am.BaseURL
		if baseURL == "" && len(c.nicks) > 0 {
			// Fall back to pool's default base URL (from the first static member).
			baseURL = c.backendAt(0).BaseURL
		}
		return backend.Backend{
			Pool:       c.pool,
			Nick:       nick,
			Credential: am.Credential,
			BaseURL:    baseURL,
		}, true
	}
	// Static member: use the registry.
	if idx := c.indexOf(nick); idx >= 0 {
		return c.backendAt(idx), true
	}
	return backend.Backend{}, false
}

// firstHealthyNickLocked finds the nick of a healthy member, considering both
// runtime-added and static members. For a priority pool it returns the
// highest-priority available member; for a plain pool it scans round-robin.
// Returns (nick, true) when found, ("", false) when all are unavailable.
// Caller holds c.mu.
func (c *Controller) firstHealthyNickLocked() (string, bool) {
	// Build the effective member list (static + added, excluding removed).
	effectiveNicks := c.addedMembersLocked()

	if pri := c.effectivePriorityLocked(); len(pri) > 0 {
		// Priority pool: check in priority order.
		for _, nick := range pri {
			if !c.isUnavailableLocked(nick) {
				return nick, true
			}
		}
		return "", false
	}

	// Plain pool: scan round-robin from current position.
	// Find current position in effective list.
	curNick := c.curAddedNick
	if curNick == "" && len(c.nicks) > 0 {
		curNick = c.nicks[c.cur]
	}
	startIdx := 0
	for i, nick := range effectiveNicks {
		if nick == curNick {
			startIdx = i
			break
		}
	}

	// Scan from startIdx+1 to end, then from 0 to startIdx.
	n := len(effectiveNicks)
	if n == 0 {
		return "", false
	}
	for off := 1; off <= n; off++ {
		idx := (startIdx + off) % n
		if !c.isUnavailableLocked(effectiveNicks[idx]) {
			return effectiveNicks[idx], true
		}
	}
	return "", false
}

// soonestNickLocked returns the nick and reset time of the member that frees up
// soonest. It considers both runtime-added and static members.
// Caller holds c.mu.
func (c *Controller) soonestNickLocked() (string, time.Time) {
	effectiveNicks := c.addedMembersLocked()
	if len(effectiveNicks) == 0 {
		// Fallback to static only.
		if len(c.nicks) == 0 {
			return "", c.now()
		}
		idx, reset := c.soonestLocked()
		return c.nicks[idx], reset
	}

	var bestNick string
	var bestReset time.Time
	bestSet := false

	for _, nick := range effectiveNicks {
		if c.disabled[nick] {
			continue
		}
		reset, ok := c.exhaustedUntilLocked(nick)
		if !ok {
			continue
		}
		if !bestSet || reset.Before(bestReset) {
			bestNick, bestReset, bestSet = nick, reset, true
		}
	}

	if !bestSet {
		// No exhausted members: return current.
		if len(effectiveNicks) > 0 {
			return effectiveNicks[0], c.now()
		}
		return "", c.now()
	}
	return bestNick, bestReset
}

// setActiveMemberLocked updates the controller's active member state to nick.
// If nick is a runtime-added member, curAddedNick is set and cur is unchanged.
// If nick is a static member, curAddedNick is cleared and cur is set to the index.
// Caller holds c.mu.
func (c *Controller) setActiveMemberLocked(nick string) {
	if c.isAddedMemberLocked(nick) {
		c.curAddedNick = nick
		// Keep cur at a valid static index for balance mode.
	} else {
		if idx := c.indexOf(nick); idx >= 0 {
			c.cur = idx
			c.curAddedNick = ""
			c.stampSelectionLocked(nick)
		}
	}
	c.notifyMutate()
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

// rewriteTo503WithBody turns an upstream policy/punishment 429 into a 503
// while keeping the upstream body intact, so the client can read the actual
// error message (e.g. a threatening client-identity warning from Anthropic).
// The upstream rate-limit headers are stripped (they carry no useful quota
// state for a policy 429), but Content-Type is preserved from the upstream.
func rewriteTo503WithBody(resp *http.Response) {
	resp.StatusCode = http.StatusServiceUnavailable
	resp.Status = strconv.Itoa(http.StatusServiceUnavailable) + " " + http.StatusText(http.StatusServiceUnavailable)

	h := resp.Header
	for k := range h {
		if strings.HasPrefix(strings.ToLower(k), "anthropic-ratelimit-") {
			h.Del(k)
		}
	}
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
