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
	"context"
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
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
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

// window5h is the length of the Anthropic unified short window, used by
// the lead calculation:
//
//	elapsed_fraction = 1 - (time_until_reset / window_length)
//	lead = utilization - elapsed_fraction
//
// A positive lead means the member is consuming faster than its window
// is depleting and should be cooled down; near-zero is on pace; negative
// is under pace.
//
// The long-window length is provider-aware and resolved per member via
// poller.LongWindowFor (7-day default, ~30-day monthly for Z.AI/Zhipu;
// issue #140), so there is no fixed long-window constant here. Whether the
// long window feeds the lead at all is also provider-aware: for Z.AI/Zhipu
// it is dropped, because its monthly slot is a web-search/reader/zread tool
// quota, not chat throughput (poller.LongWindowBlocksExhaustion; issue #192).
const window5h = 5 * time.Hour

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

	reg      *backend.Registry
	store    *quota.Store
	now      func() time.Time
	logOut   io.Writer
	onMutate func()
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
	return &Pools{
		byPool: byPool,
		reg:    reg,
		store:  store,
		now:    now,
		logOut: logOut,
	}
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

// ClearExhausted drops the named pool's live-429 parks (see
// Controller.ClearExhausted). ok is false for an unknown pool.
func (p *Pools) ClearExhausted(poolName string) (cleared []string, ok bool) {
	c, ok := p.controller(poolName)
	if !ok {
		return nil, false
	}
	return c.ClearExhausted(), true
}

// ClearExhaustedNick drops one member's live-429 park in the named pool (see
// Controller.ClearExhaustedNick). ok is false for an unknown pool; cleared
// reports whether a live park was actually present for the nick.
func (p *Pools) ClearExhaustedNick(poolName, nick string) (cleared bool, ok bool) {
	c, ok := p.controller(poolName)
	if !ok {
		return false, false
	}
	return c.ClearExhaustedNick(nick), true
}

// ClearAllExhausted drops live-429 parks across every pool, returning a
// map of pool name to the nicks cleared (pools with nothing parked are
// omitted).
func (p *Pools) ClearAllExhausted() map[string][]string {
	out := make(map[string][]string)
	for name, c := range p.controllersSnapshot() {
		if cleared := c.ClearExhausted(); len(cleared) > 0 {
			out[name] = cleared
		}
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

	// Parked reports whether a live-429 park is currently holding this member
	// out of rotation — present, reset still in the future, and not reconciled
	// away by a fresh healthy store snapshot (issue #145). It is the gate for
	// the per-nick "clear park" affordance (issue #147): exactly the set of
	// parks ClearExhaustedNick can usefully drop. Distinct from Status:
	// store-sourced exhaustion also reads "exhausted" but is not Parked, since
	// clearing the live park cannot move it.
	Parked bool `json:"parked"`

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
	// WindowLabels is the per-pool column-header hint the UI consumes to
	// render the second rolling-window cell. Anthropic and MiniMaxi label
	// it "7d"; Z.AI labels it "monthly" because its long window is monthly
	// (issue #138). The label is derived from the first member's BaseURL
	// because every member in a pool shares the same upstream provider.
	WindowLabels *PoolWindowLabels `json:"window_labels,omitempty"`
}

// PoolWindowLabels is the per-pool rolling-window label hint the UI
// consumes to render the long-window column. The field names are
// `short` / `long` to match the JSON the UI reads (it is the same hint
// /_gateway/config surfaces in the `window_labels` object). The
// mapping itself lives in poller.WindowLabelsFor; this local type
// exists so the auto package can keep the JSON shape independent of
// any future field additions in poller.WindowLabels.
type PoolWindowLabels struct {
	Short string `json:"short"` // e.g. "5h"
	Long  string `json:"long"`  // e.g. "7d" or "monthly"
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
	c, ok := p.controller(poolName)
	if !ok {
		return PoolStatus{}, false
	}
	return c.poolStatus(store), true
}

// AllPoolStatuses returns status for every pool in sorted order.
func (p *Pools) AllPoolStatuses(store *quota.Store) []PoolStatus {
	snapshot := p.controllersSnapshot()
	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]PoolStatus, 0, len(names))
	for _, name := range names {
		out = append(out, snapshot[name].poolStatus(store))
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
	// LocalSnapshotNicks lists members for which this controller has
	// observed a snapshot since the last restart. Persisted unconditionally
	// (not gated on balanceGap) so a non-balanced pool does not lose the
	// "this pool has seen traffic" signal across a restart. Empty/absent
	// in older state files; treated as "no observed snapshots" on load,
	// which is the same as the pre-fix behaviour for the first observation.
	LocalSnapshotNicks []string `json:"local_snapshot_nicks,omitempty"`
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
	// RemovedMembers is the list of member nicks that have been operator-removed.
	// Persisting these tombstones makes removal permanent and uniform: a removed
	// static member stays removed across restart instead of resurfacing, matching
	// the always-permanent behaviour of a removed runtime-added member. Each nick
	// appears at most once. Empty means no members are removed.
	RemovedMembers []string `json:"removed_members,omitempty"`
}

// AddedMember is a pool member entry in the persisted runtime config.
// Kept as the on-disk format for PoolRuntimeConfig.AddedMembers.
type AddedMember struct {
	Credential string `json:"credential"`         // stored, never returned in config views
	BaseURL    string `json:"base_url,omitempty"` // optional for a known nick (cross-pool resolved); always non-empty once persisted
}

// memberEntry is one member in the Controller's unified ordered member collection.
// It holds no origin tag — config-seeded and runtime-added members are
// indistinguishable once inside the Controller (issue #185).
type memberEntry struct {
	Nick       string
	Credential string
	BaseURL    string
}

// AddedPoolSpec is the persisted marker that a pool name was created at runtime
// (POST /_gateway/pool) and so must be re-instantiated on restart. A runtime
// pool owns no properties beyond its name — members and routing state are
// persisted separately (config / pools), so a re-instantiated pool is a clean
// slate. It carries no fields today but is retained as the persisted value type
// so a pre-change state file whose entry still holds a "base_url" field decodes
// cleanly (Go's decoder ignores the unknown field). Exported so the persist
// package can embed it in GatewayState.
type AddedPoolSpec struct{}

// LoadPersistState applies previously persisted routing state to each pool's
// controller. Called once at startup, before the server begins serving.
func (p *Pools) LoadPersistState(states map[string]PoolPersistState) {
	for name, s := range states {
		if c, ok := p.controller(name); ok {
			c.loadState(s.Sticky, s.Exhausted, s.LastBalanceSwitch, s.BalanceSeq, s.LastSelectedSeq, s.LocalSnapshotNicks)
		}
	}
}

// SetPriority sets the runtime priority override for the named pool.
// The order list is validated (all nicks must exist in the pool, no duplicates,
// no empty strings) and then expanded via effectiveOrder() to a total order.
// Returns (httpStatus, error) with error containing a credential-free message.
func (p *Pools) SetPriority(poolName string, order []string) (int, error) {
	c, ok := p.controller(poolName)
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Reject priority override on a balanced pool (mutually exclusive modes).
	if c.balanceGap > 0 {
		return http.StatusConflict, fmt.Errorf("balanced pools do not support priority override")
	}

	// Normalize and validate the input order against the unified member collection.
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

	// Expand via the effective set (static ∪ added − removed) so unlisted
	// runtime-added members rank last, matching the documented behavior for
	// unlisted static members. setPriorityOverrideEffectiveLocked is already
	// used by MoveMember and loadRuntimeConfig for the same reason.
	c.setPriorityOverrideEffectiveLocked(validOrder)
	return http.StatusOK, nil
}

// SetMemberDisabled sets or clears the disabled flag for a member in a pool.
// The target nick must be a present (non-removed) member — static or
// runtime-added — mirroring RemoveMember's semantics. Re-enabling a member that
// was operator-removed is rejected, matching the rest of the runtime-member
// surface. Returns (httpStatus, error) with error containing a
// credential-free message.
func (p *Pools) SetMemberDisabled(poolName, nick string, off bool) (int, error) {
	c, ok := p.controller(poolName)
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}

	c.mu.Lock()
	present := c.indexOf(normalized) >= 0 && !c.isRemovedLocked(normalized)
	if !present {
		c.mu.Unlock()
		return http.StatusBadRequest, fmt.Errorf("unknown nick: %s", normalized)
	}
	c.setDisabledLocked(normalized, off)
	c.mu.Unlock()
	return http.StatusOK, nil
}

// resolveAcrossPools scans every pool other than skipPool for a present
// (non-removed) member with the given normalized nick, collecting the distinct
// credentials and distinct *resolved* base URLs found. It is used to fill an
// omitted credential and/or base_url when re-adding a known subscription by
// name. Each pool lock is taken and released independently — no other lock is
// held by the caller — mirroring MoveMember's phase separation.
func (p *Pools) resolveAcrossPools(skipPool, nick string) (creds, baseURLs []string) {
	credSeen := make(map[string]bool)
	urlSeen := make(map[string]bool)
	for name, c := range p.controllersSnapshot() {
		if name == skipPool {
			continue
		}
		c.mu.Lock()
		present := c.indexOf(nick) >= 0 && !c.isRemovedLocked(nick)
		if present {
			if b, ok := c.backendByNickLocked(nick); ok {
				if b.Credential != "" && !credSeen[b.Credential] {
					credSeen[b.Credential] = true
					creds = append(creds, b.Credential)
				}
				if b.BaseURL != "" && !urlSeen[b.BaseURL] {
					urlSeen[b.BaseURL] = true
					baseURLs = append(baseURLs, b.BaseURL)
				}
			}
		}
		c.mu.Unlock()
	}
	return creds, baseURLs
}

// AddMember adds a runtime member to a pool. Credential and baseURL are optional
// for a *known* subscription: when omitted, they are resolved by scanning the
// other pools for the same nick (credential and base_url resolve independently).
// A priority target requires an explicit placement (must include nick), reusing
// the move path's validation; plain/balanced targets must carry none. The
// resolved concrete base_url is persisted — never an empty string when one is
// resolvable. Returns (httpStatus, error) with a credential-free message.
func (p *Pools) AddMember(poolName, nick, credential, baseURL string, placement []string) (int, error) {
	c, ok := p.controller(poolName)
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}
	// Validate baseURL if explicitly provided.
	if baseURL != "" {
		if _, err := backend.ValidateBaseURL(baseURL); err != nil {
			return http.StatusBadRequest, fmt.Errorf("invalid base_url: %w", err)
		}
	}

	// Phase 1: resolve omitted credential/base_url from other pools (no target
	// lock held). Credential and base_url are resolved independently.
	resolvedCred := credential
	resolvedURL := baseURL
	if credential == "" || baseURL == "" {
		creds, baseURLs := p.resolveAcrossPools(poolName, normalized)
		if credential == "" {
			switch len(creds) {
			case 1:
				resolvedCred = creds[0]
			case 0:
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

	// Phase 2: validate + commit on the target pool under its lock.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Duplicate check: already a live (non-removed) member.
	if c.indexOf(normalized) >= 0 && !c.isRemovedLocked(normalized) {
		return http.StatusConflict, fmt.Errorf("nick %s already exists as a member", normalized)
	}

	// Resolve base_url to a concrete value so the persisted record is
	// self-describing. An unresolved (new-nick) base_url falls back to the
	// first member's URL; a pool with no members has no default to borrow, so
	// a genuinely new nick must supply base_url explicitly.
	if resolvedURL == "" {
		if len(c.members) > 0 {
			resolvedURL = c.members[0].BaseURL
		} else {
			return http.StatusBadRequest, fmt.Errorf("base_url is required when pool has no members")
		}
	}

	// Placement: a priority target needs an explicit order including nick; a
	// plain/balanced target must not carry one. Same rules as the move path.
	isPriorityTarget := c.balanceGap == 0 && len(c.effectivePriorityLocked()) > 0
	var normPlacement []string
	if isPriorityTarget {
		var status int
		var err error
		normPlacement, status, err = c.validatePlacementLocked(normalized, placement)
		if err != nil {
			return status, err
		}
	} else if len(placement) > 0 {
		return http.StatusBadRequest, fmt.Errorf("placement is only applicable to a priority target pool")
	}

	// Clear any tombstone so the nick becomes selectable again (issue #185:
	// re-adding a config-derived nick after removal now always succeeds).
	delete(c.removedMembers, normalized)

	// Upsert into the unified member collection: update if present (e.g. a
	// config nick that was removed and is being re-added), insert if new.
	if idx := c.indexOf(normalized); idx >= 0 {
		c.members[idx].Credential = resolvedCred
		c.members[idx].BaseURL = resolvedURL
	} else {
		c.members = append(c.members, memberEntry{
			Nick:       normalized,
			Credential: resolvedCred,
			BaseURL:    resolvedURL,
		})
	}
	if isPriorityTarget {
		c.setPriorityOverrideEffectiveLocked(normPlacement)
	}
	c.notifyMutate()
	fmt.Fprintf(c.logOut, "auto[%s]: added member %s\n", c.pool, normalized)
	return http.StatusOK, nil
}

// RemoveMember removes a member (static or runtime-added) from pool selection.
// Returns (httpStatus, error) with error containing a credential-free message.
func (p *Pools) RemoveMember(poolName, nick string) (int, error) {
	c, ok := p.controller(poolName)
	if !ok {
		return http.StatusNotFound, fmt.Errorf("pool not found")
	}
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.indexOf(normalized) < 0 {
		return http.StatusBadRequest, fmt.Errorf("nick %s not found in pool", normalized)
	}

	c.removeMemberLocked(normalized)
	return http.StatusOK, nil
}

// removeMemberLocked tombstones nick so it is hidden from selection and listing.
// Members stay in c.members; isRemovedLocked gates them out at routing time,
// matching the reviewer note (issue #185). If the removed member was the active
// sticky pointer, the pointer force-switches to the next healthy member (as on
// a 429). If a runtime priority override is in effect, it is rebuilt over the
// post-mutation effective member set so the override no longer references the
// removed nick — making live behaviour match post-restart behaviour (issue #120).
// The caller is responsible for validating that the member exists. Caller holds c.mu.
func (c *Controller) removeMemberLocked(nick string) {
	// Tombstone the nick so it is filtered from routing and listing.
	c.removedMembers[nick] = true
	fmt.Fprintf(c.logOut, "auto[%s]: removed member %s\n", c.pool, nick)

	// If the pool currently has a runtime priority override, prune it of any
	// nick no longer in the effective member set (the removed nick plus any
	// other stale nick). allMemberNicksLocked already filters out nicks present
	// in c.removedMembers, so the just-removed nick falls out automatically.
	// An empty filtered list drops the override entirely (the pool becomes a
	// plain pool; the UI hides the priority column). Otherwise re-expand via
	// effectiveOrder so a partial override still yields a total order over the
	// current effective set — symmetric with setPriorityOverrideEffective /
	// loadRuntimeConfig (issue #120).
	if c.priorityOverride != nil {
		effective := c.allMemberNicksLocked()
		keep := make(map[string]bool, len(effective))
		for _, m := range effective {
			keep[m] = true
		}
		filtered := make([]string, 0, len(c.priorityOverride))
		for _, n := range c.priorityOverride {
			if keep[n] {
				filtered = append(filtered, n)
			}
		}
		if len(filtered) == 0 {
			c.priorityOverride = nil
		} else {
			c.priorityOverride = effectiveOrder(filtered, effective)
		}
	}

	// If the removed member was the active sticky pointer, force-switch to
	// the next healthy member. This is similar to what happens on a 429.
	if c.curNick == nick {
		if next, ok := c.firstHealthyNickLocked(); ok {
			c.setActiveMemberLocked(next)
			fmt.Fprintf(c.logOut, "auto[%s]: switched %s -> %s (removed member %s)\n", c.pool, nick, next, nick)
		}
	}

	c.notifyMutate()
}

// MoveMember relocates a subscription (nick) from one pool to another. It is
// implemented as the bridge model: persistent remove from the source pool plus
// an add to the target pool carrying the source member's credential and
// resolved base URL. Returns (httpStatus, error) with a credential-free message.
//
// Placement: moving into a priority pool that has no existing slot for nick
// requires an explicit placement order (which must include nick) — there is no
// implicit insertion. Moving into a plain/balanced pool, or onto an existing
// same-nick slot, needs no placement.
//
// Conflict: an existing same-nick member in the target whose credential and
// resolved base URL match is silently overwritten in place (slot preserved); a
// differing runtime-added member returns 409 unless force is set; a static
// target member can never be overwritten by a move (it is immutable here).
//
// No surprise re-anchor: the target's healthy active member is never force-
// switched by the move; the new order applies on the next selection event.
func (p *Pools) MoveMember(fromPool, nick, toPool string, placement []string, force bool) (int, error) {
	src, ok := p.controller(fromPool)
	if !ok {
		return http.StatusNotFound, fmt.Errorf("source pool not found")
	}
	dst, ok := p.controller(toPool)
	if !ok {
		return http.StatusNotFound, fmt.Errorf("target pool not found")
	}
	normalized := backend.NormalizeName(nick)
	if normalized == "" {
		return http.StatusBadRequest, fmt.Errorf("nick is empty after normalization")
	}
	if fromPool == toPool {
		return http.StatusBadRequest, fmt.Errorf("source and target pools are the same")
	}

	// Phase 1: read the source member's resolved credential + base URL.
	src.mu.Lock()
	srcPresent := src.indexOf(normalized) >= 0 && !src.isRemovedLocked(normalized)
	if !srcPresent {
		src.mu.Unlock()
		return http.StatusBadRequest, fmt.Errorf("nick %s not found in source pool", normalized)
	}
	srcBackend, _ := src.backendByNickLocked(normalized)
	src.mu.Unlock()

	// Phase 2: validate + commit on the target (single lock, no source held).
	dst.mu.Lock()
	status, err := dst.placeMovedMemberLocked(normalized, srcBackend.Credential, srcBackend.BaseURL, placement, force)
	dst.mu.Unlock()
	if err != nil {
		return status, err
	}

	// Phase 3: persistent remove from the source. Committing the target first
	// means the worst-case failure is "briefly present in both", never "lost
	// from both".
	src.mu.Lock()
	src.removeMemberLocked(normalized)
	src.mu.Unlock()

	fmt.Fprintf(src.logOut, "auto: moved member %s from %s to %s\n", normalized, fromPool, toPool)
	return http.StatusOK, nil
}

// placeMovedMemberLocked applies the target-side half of a move: it resolves
// the same-nick conflict / placement rules and commits the add or in-place
// overwrite. In the unified collection there is no static/added distinction —
// any existing live member with matching cred+baseURL is a silent no-op or
// tombstone-clear; differing cred/baseURL requires force (issue #185).
// Returns (httpStatus, error). Caller holds c.mu (c is the target).
func (c *Controller) placeMovedMemberLocked(nick, cred, baseURL string, placement []string, force bool) (int, error) {
	// Existing live member with this nick.
	if idx := c.indexOf(nick); idx >= 0 && !c.isRemovedLocked(nick) {
		tb := c.backendAt(idx)
		if tb.Credential == cred && tb.BaseURL == baseURL {
			return http.StatusOK, nil // identical: no-op
		}
		if !force {
			return http.StatusConflict, fmt.Errorf("target nick %s exists with a different credential or base_url; confirm to overwrite", nick)
		}
		c.members[idx].Credential = cred
		c.members[idx].BaseURL = baseURL
		c.notifyMutate()
		return http.StatusOK, nil
	}

	// Tombstoned member or brand-new nick: add/restore. A priority target needs
	// explicit placement; a plain/balanced target must not carry one.
	isPriorityTarget := c.balanceGap == 0 && len(c.effectivePriorityLocked()) > 0
	var normPlacement []string
	if isPriorityTarget {
		var status int
		var err error
		normPlacement, status, err = c.validatePlacementLocked(nick, placement)
		if err != nil {
			return status, err
		}
	} else if len(placement) > 0 {
		return http.StatusBadRequest, fmt.Errorf("placement is only applicable to a priority target pool")
	}

	delete(c.removedMembers, nick) // clear any stale tombstone
	if idx := c.indexOf(nick); idx >= 0 {
		// Was tombstoned; restore with updated credential/baseURL.
		c.members[idx].Credential = cred
		c.members[idx].BaseURL = baseURL
	} else {
		c.members = append(c.members, memberEntry{Nick: nick, Credential: cred, BaseURL: baseURL})
	}
	if isPriorityTarget {
		c.setPriorityOverrideEffectiveLocked(normPlacement)
	}
	c.notifyMutate()
	return http.StatusOK, nil
}

// validatePlacementLocked checks an explicit placement order for a priority
// target into which nick is being added: every entry must be a current target
// member (or nick itself), with no empties or duplicates, and the order must
// include nick (no implicit insertion). It returns the normalized placement on
// success. Caller holds c.mu.
func (c *Controller) validatePlacementLocked(nick string, placement []string) ([]string, int, error) {
	if len(placement) == 0 {
		return nil, http.StatusBadRequest, fmt.Errorf("explicit placement is required to move into priority pool %s", c.pool)
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
// with credentials fully redacted. Each pool's view includes its balance
// settings, effective priority (runtime override when set, else env priority),
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

		// Members: the effective set (static + runtime-added − removed), sorted.
		// Removal is permanent deletion, so removed members are omitted entirely
		// — consistent with poolStatus and the selection path.
		allMembers := c.allMemberNicksLocked()

		view.Members = make([]PoolMemberConfigView, 0, len(allMembers))

		// Per-pool window-label hint (issue #138). Every member in a pool
		// shares the same upstream provider, so the first member's
		// BaseURL is the right input. An empty pool leaves the field
		// nil, in which case the UI falls back to "5h"/"7d".
		if len(allMembers) > 0 {
			if b, ok := c.backendByNickLocked(allMembers[0]); ok {
				labels := poolWindowLabelsFor(b.BaseURL)
				view.WindowLabels = &labels
			}
		}

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

// PersistRuntimeConfig snapshots the runtime configuration for all pools.
func (p *Pools) PersistRuntimeConfig() map[string]PoolRuntimeConfig {
	snapshot := p.controllersSnapshot()
	out := make(map[string]PoolRuntimeConfig, len(snapshot))
	for name, c := range snapshot {
		out[name] = c.runtimeConfig()
	}
	return out
}

// LoadRuntimeConfig restores runtime configuration from persisted state.
func (p *Pools) LoadRuntimeConfig(cfg map[string]PoolRuntimeConfig) {
	for name, poolCfg := range cfg {
		if c, ok := p.controller(name); ok {
			c.loadRuntimeConfig(poolCfg)
		}
	}
	// After every controller has its runtime members back, seed the
	// local-snapshot set for the persisted entries loadState had to defer
	// (runtime-added members are not visible to backendByNickLocked until
	// now). Apply per controller under its own lock.
	for _, c := range p.controllersSnapshot() {
		c.mu.Lock()
		c.applyPendingLocalSnapshotsLocked()
		c.mu.Unlock()
	}
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

// AddPool creates a new plain pool at runtime and inserts it so the proxy can
// route to it immediately. name is normalized; mode defaults to "plain" and
// only "plain" is supported. A runtime pool owns no base_url — it is a pure
// named container, and each member resolves its own base_url via AddMember's
// fallback chain. The pool starts empty (no members, no routing state) —
// members are added afterward via AddMember. Returns (httpStatus, error) with
// a credential-free message; (http.StatusCreated, nil) on success.
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

	// An env-defined pool name is authoritative and can never be shadowed by a
	// runtime pool.
	if p.reg.HasPool(normalized) {
		return http.StatusConflict, fmt.Errorf("pool %s already exists (env-defined)", normalized)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.byPool[normalized]; exists {
		return http.StatusConflict, fmt.Errorf("pool %s already exists", normalized)
	}

	c := NewController(p.reg, normalized, -1, p.store, p.now, p.logOut)
	c.onMutate = p.onMutate
	p.byPool[normalized] = c
	c.notifyMutate() // persist the new pool (added_pools) promptly
	if p.logOut != nil {
		fmt.Fprintf(p.logOut, "auto: created runtime pool %s\n", normalized)
	}
	return http.StatusCreated, nil
}

// PersistAddedPools snapshots the runtime-created pools for serialisation.
// Env-defined pools are excluded — they are reconstructed from the environment,
// not the state file — so only pools that exist solely at runtime are recorded.
func (p *Pools) PersistAddedPools() map[string]AddedPoolSpec {
	snapshot := p.controllersSnapshot()
	out := make(map[string]AddedPoolSpec, len(snapshot))
	for name := range snapshot {
		if p.reg.HasPool(name) {
			continue
		}
		out[name] = AddedPoolSpec{}
	}
	return out
}

// LoadAddedPools re-instantiates runtime-created pools from persisted state.
// Called once at startup, after NewPools and before LoadPersistState /
// LoadRuntimeConfig so those can find the pool by name. A spec whose name
// collides with an env-defined pool is dropped with a warning (env wins); the
// pool is created as a clean slate (no members, no routing state).
func (p *Pools) LoadAddedPools(specs map[string]AddedPoolSpec) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for rawName := range specs {
		name := backend.NormalizeName(rawName)
		if name == "" {
			continue
		}
		if p.reg.HasPool(name) {
			if p.logOut != nil {
				fmt.Fprintf(p.logOut, "auto: dropping runtime pool %s; name reappeared as env-defined (env wins)\n", name)
			}
			continue
		}
		if _, exists := p.byPool[name]; exists {
			continue
		}
		c := NewController(p.reg, name, -1, p.store, p.now, p.logOut)
		// onMutate is wired later by SetOnMutate, which fans out over byPool.
		p.byPool[name] = c
	}
}

// SetOnMutate installs a callback that every controller calls (non-blocking)
// after any mutation to its sticky pointer or exhausted map. Used by the
// persister to coalesce writes without importing this package. The callback
// is retained on Pools so a runtime-created controller (AddPool) is wired to
// the same persister.
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

// Controller is the sticky selector for one pool. The zero value is not
// usable; call NewController.
type Controller struct {
	mu sync.Mutex

	reg  *backend.Registry
	pool string

	// members is the unified ordered member collection. Config-seeded and
	// runtime-added members are indistinguishable here (issue #185). Members
	// are never evicted from this slice; removedMembers tombstones gate them
	// out of routing and listing, identical to how static members were gated
	// before the collapse. Accessed only under c.mu.
	members []memberEntry

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

	// removedMembers marks members as operator-removed. A removed member is
	// hidden from selection and listing until explicitly re-added. Accessed
	// only under c.mu.
	removedMembers map[string]bool

	// curNick is the nick of the currently active member (the sticky pointer).
	// Replaces the old cur int + curAddedNick string pair. Accessed only
	// under c.mu.
	curNick string

	// pendingSticky carries a persisted sticky nick deferred until
	// loadRuntimeConfig has restored all members so a truly-gone nick can be
	// distinguished from a not-yet-restored one. Always "" outside the load
	// sequence. Accessed only under c.mu.
	pendingSticky string

	// pendingLocalSnapshots carries LocalSnapshotNicks that loadState could
	// not apply because they name a member not yet present in c.members
	// (LoadRuntimeConfig runs after LoadPersistState in main.go). Applied by
	// applyPendingLocalSnapshotsLocked once all members are in place; nicks
	// that still do not resolve are dropped. Always nil outside the load
	// sequence. Accessed only under c.mu.
	pendingLocalSnapshots []string

	// exhausted maps a nick to the absolute time its blocking window
	// resets. Presence means "exhausted-until-reset"; entries are cleared
	// lazily once now >= reset.
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
		pool:               poolName,
		members:            members,
		priority:           effectiveOrder(reg.PoolPriority(poolName), nicks),
		store:              store,
		exhausted:          make(map[string]time.Time),
		lastProbeAttempt:   make(map[string]time.Time),
		probeInFlight:      make(map[string]bool),
		probeHTTPClient:    http.DefaultClient,
		now:                now,
		logOut:             logOut,
		balanceGap:         reg.PoolBalanceGap(poolName),
		balanceDwell:       reg.PoolBalanceDwell(poolName),
		lastSelectedSeq:    make(map[string]uint64),
		disabled:           make(map[string]bool),
		removedMembers:     make(map[string]bool),
		poolLocalSnapshots: local,
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
	// Stamp the initial pick so it is distinguishable from members that have
	// never been active. loadState may overwrite this with persisted values.
	c.stampSelectionLocked(c.curNick)
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

// isRemovedLocked reports whether nick has been operator-removed (hidden
// from selection). Caller holds c.mu.
func (c *Controller) isRemovedLocked(nick string) bool {
	return c.removedMembers[nick]
}

// allMemberNicksLocked returns all non-removed member nicks, sorted. It
// replaces the old addedMembersLocked (which merged two separate stores).
// Caller holds c.mu.
func (c *Controller) allMemberNicksLocked() []string {
	out := make([]string, 0, len(c.members))
	for _, m := range c.members {
		if !c.removedMembers[m.Nick] {
			out = append(out, m.Nick)
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

	// A member-less pool (freshly created runtime pool with no members yet)
	// has an empty curNick; skip the healthy-current branch and fall through
	// to the exhausted return so the caller receives an honest 429.
	if c.curNick != "" && !c.isUnavailableLocked(c.curNick) {
		// Balance mode: check for a switch to a lower-utilization member.
		// Applies to all members in the unified collection (issue #185).
		if c.balanceGap > 0 {
			if next, ok := c.balanceSwitchLocked(); ok {
				from := c.curNick
				c.lastBalanceSwitch = c.now()
				c.setActiveMemberLocked(next)
				fmt.Fprintf(c.logOut, "auto[%s]: balance %s -> %s (lead gap)\n", c.pool, from, next)
				if b, ok := c.backendByNickLocked(next); ok {
					return b, 0, false
				}
			}
		}
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
	c.lastProbeAttempt = make(map[string]time.Time)
	c.probeInFlight = make(map[string]bool)
	c.notifyMutate()
	return cleared
}

// ClearExhaustedNick drops a single member's live-429 park (issue #147), the
// per-nick counterpart to ClearExhausted: an operator escape hatch to un-stick
// one over-parked member without clearing the whole pool. Same "live-park only,
// never store" contract — store-sourced exhaustion is left untouched and a
// genuinely-exhausted member simply re-parks via record429 on its next 429.
// Returns whether a live park was actually present (false is a harmless no-op
// for an unknown or un-parked nick). notifyMutate fires only when something
// changed.
func (c *Controller) ClearExhaustedNick(nick string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.exhausted[nick]; !ok {
		return false
	}
	delete(c.exhausted, nick)
	delete(c.lastProbeAttempt, nick)
	delete(c.probeInFlight, nick)
	c.notifyMutate()
	return true
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

// setActiveMemberLocked moves the sticky pointer to nick and notifies the
// persister. Replaces the old cur/curAddedNick dual-pointer update.
// Caller holds c.mu.
func (c *Controller) setActiveMemberLocked(nick string) {
	c.curNick = nick
	c.stampSelectionLocked(nick)
	c.notifyMutate()
}

// poolStatus builds the /_gateway/pool response for this controller. store
// is consulted for each member's latest snapshot; a member with no recorded
// snapshot gets snapshot:null. Caller must not hold c.mu.
func (c *Controller) poolStatus(store *quota.Store) PoolStatus {
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
	return PoolStatus{Pool: c.pool, Active: c.curNick, Members: members}
}

// loadState applies persisted routing state. Exhausted entries whose reset
// has already passed are silently dropped. Persisted nicks absent from the
// current pool membership are logged and skipped. Called once at startup
// before the server begins serving; does not call onMutate.
func (c *Controller) loadState(sticky string, exhausted map[string]time.Time, lastBalanceSwitch time.Time, balanceSeq uint64, lastSelectedSeq map[string]uint64, localSnapshots []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.indexOf(sticky) >= 0 {
		c.curNick = sticky
	} else if sticky != "" {
		// Not yet a known member. It may name a member that loadRuntimeConfig
		// restores moments later (runtime-added member not yet in c.members), so
		// defer the truly-gone judgment and fall-back logging until then.
		c.pendingSticky = sticky
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
		if _, stamped := c.lastSelectedSeq[c.curNick]; !stamped {
			c.stampSelectionLocked(c.curNick)
		}
	}
	// Restore the per-pool "we have seen traffic for this nick" set, dropping
	// entries that no longer name a current member (mirroring the
	// sticky-pointer drop above). Unconditional — applies to balanced and
	// non-balanced pools alike.
	//
	// For static members this is a direct seed. For runtime-added members
	// addedMembers has not been restored yet (LoadRuntimeConfig runs after
	// LoadPersistState), so we defer them on pendingLocalSnapshots and
	// apply them in applyPendingLocalSnapshotsLocked, invoked at the end
	// of LoadRuntimeConfig.
	for _, nick := range localSnapshots {
		if nick == "" {
			continue
		}
		if _, ok := c.backendByNickLocked(nick); ok {
			c.seedLocalSnapshotLocked(nick)
			continue
		}
		c.pendingLocalSnapshots = append(c.pendingLocalSnapshots, nick)
	}
}

// applyPendingLocalSnapshotsLocked seeds the local-snapshot set for every
// persisted entry that loadState could not resolve at the time because
// runtime-added members had not yet been restored. Entries that still
// name a non-member are dropped (the runtime member was removed between
// runs and there is nothing to attach a snapshot to). Caller holds c.mu.
func (c *Controller) applyPendingLocalSnapshotsLocked() {
	if len(c.pendingLocalSnapshots) == 0 {
		return
	}
	for _, nick := range c.pendingLocalSnapshots {
		if _, ok := c.backendByNickLocked(nick); ok {
			c.seedLocalSnapshotLocked(nick)
		}
	}
	c.pendingLocalSnapshots = nil
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
		Sticky:            sticky,
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

// setPriorityOverrideEffectiveLocked sets the runtime priority override,
// expanding the order over the full effective member set (unified collection
// minus removed). Replaces the old setPriorityOverrideLocked which expanded
// over c.nicks only (issue #185). Caller holds c.mu.
func (c *Controller) setPriorityOverrideEffectiveLocked(order []string) {
	if len(order) == 0 {
		c.priorityOverride = nil
	} else {
		c.priorityOverride = effectiveOrder(order, c.allMemberNicksLocked())
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

	// Persist all members' credentials — the unified collection no longer
	// distinguishes config-derived from runtime-added (issue #185). On load,
	// the second-pass upsert in loadRuntimeConfig overwrites config-derived
	// nicks with fresh env values, so rotating a credential by editing env
	// and restarting is always unconditionally correct.
	addedMembers := make(map[string]AddedMember, len(c.members))
	for _, m := range c.members {
		addedMembers[m.Nick] = AddedMember{
			Credential: m.Credential, // stored, never returned in config views
			BaseURL:    m.BaseURL,
		}
	}

	// Snapshot the removed-member tombstones (sorted) so removal survives restart.
	removed := make([]string, 0, len(c.removedMembers))
	for nick := range c.removedMembers {
		removed = append(removed, nick)
	}
	sort.Strings(removed)

	return PoolRuntimeConfig{
		PriorityOverride: priOverride,
		Disabled:         disabled,
		AddedMembers:     addedMembers,
		RemovedMembers:   removed,
	}
}

// loadRuntimeConfig restores runtime configuration from persisted state.
// Unknown pool/member references are dropped with a logged warning, never a
// startup failure. The input priority override is expanded via effectiveOrder.
// Caller must not hold c.mu.
func (c *Controller) loadRuntimeConfig(cfg PoolRuntimeConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Restore runtime-added members BEFORE the priority override, so a
	// runtime-added member placed into a priority order is recognised as a
	// current member when the override is validated.
	// Config-seeded nicks are already in c.members from NewController; only
	// nicks absent from c.members need to be appended. Tombstoned nicks stay
	// in c.members (gated by isRemovedLocked at routing/listing) — matching
	// the reviewer's note: eviction from the slice is not the right gate here.
	for nick, am := range cfg.AddedMembers {
		if c.indexOf(nick) >= 0 {
			// Already present (config-seeded or previously restored); credentials
			// will be refreshed by the second-pass upsert below for config nicks.
			continue
		}
		// A runtime-added member's base_url is always baked non-empty by
		// AddMember; an empty value here means the state file was hand-edited
		// or corrupted. Refuse to restore it rather than later routing a live
		// request against an empty upstream URL (issue #172). The member is
		// dropped loudly; any priority/sticky references to it fall through
		// their own drop-with-warning validation below.
		if am.BaseURL == "" {
			fmt.Fprintf(c.logOut, "loadRuntimeConfig[%s]: refusing to restore added member %q: persisted base_url is empty (state file corrupted); dropping\n", c.pool, nick)
			continue
		}
		c.members = append(c.members, memberEntry{
			Nick:       nick,
			Credential: am.Credential,
			BaseURL:    am.BaseURL,
		})
	}

	// Second-pass upsert: overwrite credential/BaseURL for every config-declared
	// nick with fresh env-derived values. This makes credential rotation
	// unconditionally correct even when a state file from a prior run already
	// holds a persisted entry for that nick (issue #185). It does NOT touch
	// disabled/priority-override state or tombstones.
	for _, nick := range c.reg.PoolNicks(c.pool) {
		b, ok := c.reg.ResolveIn(c.pool, nick)
		if !ok {
			continue
		}
		if idx := c.indexOf(nick); idx >= 0 {
			c.members[idx].Credential = b.Credential
			c.members[idx].BaseURL = b.BaseURL
		}
	}

	// Restore removed-member tombstones. These make removal permanent across
	// restart. Tombstones for nicks not in c.members are harmless and kept.
	c.removedMembers = make(map[string]bool)
	for _, nick := range cfg.RemovedMembers {
		c.removedMembers[nick] = true
	}

	// Restore priority override. A nick is valid if it is in c.members.
	if len(cfg.PriorityOverride) > 0 {
		validOverride := make([]string, 0, len(cfg.PriorityOverride))
		for _, nick := range cfg.PriorityOverride {
			if c.indexOf(nick) >= 0 {
				validOverride = append(validOverride, nick)
			} else {
				fmt.Fprintf(c.logOut, "loadRuntimeConfig[%s]: dropping unknown nick %q from priority override\n", c.pool, nick)
			}
		}
		if len(validOverride) > 0 {
			c.priorityOverride = effectiveOrder(validOverride, c.allMemberNicksLocked())
		} else {
			c.priorityOverride = nil
		}
	} else {
		c.priorityOverride = nil
	}

	// Restore disabled set. Accept any nick present in c.members.
	c.disabled = make(map[string]bool)
	for _, nick := range cfg.Disabled {
		if c.indexOf(nick) >= 0 {
			c.disabled[nick] = true
		} else {
			fmt.Fprintf(c.logOut, "loadRuntimeConfig[%s]: dropping unknown nick %q from disabled list\n", c.pool, nick)
		}
	}

	// Resolve a sticky that loadState deferred (it could not yet distinguish a
	// member not yet restored from a truly-removed one). For a member-less runtime
	// pool the persisted sticky IS the active added member; anchor on it — when
	// healthy and nothing else is active yet — so the member that was active
	// before restart stays active instead of being replaced by reanchorLocked's
	// first-healthy pick.
	sticky := c.pendingSticky
	c.pendingSticky = ""
	// Apply unconditionally once the member is present: the curNick == ""
	// guard from before the unification was a no-op sentinel (loadState never
	// set curAddedNick for config members) but it silently dropped the sticky
	// for mixed pools (config + added) where curNick is already set by
	// NewController. Drop the guard so the member that was active before
	// restart stays active regardless of pool origin mix (issue #185 fixup).
	if sticky != "" && c.indexOf(sticky) >= 0 && !c.isUnavailableLocked(sticky) {
		c.setActiveMemberLocked(sticky)
		sticky = ""
	}

	// loadState restored the sticky pointer before this runs, so the current
	// member may now be removed (or otherwise unavailable). Re-anchor before
	// serving traffic so Current() / pool.active never point at a removed member.
	c.reanchorLocked()

	// A deferred sticky that is still not a known member is truly gone; report
	// the fall-back here rather than misleadingly at loadState time.
	if sticky != "" && c.indexOf(sticky) < 0 {
		reason := "random"
		if len(c.priority) > 0 {
			reason = "priority"
		}
		fmt.Fprintf(c.logOut, "loadRuntimeConfig[%s]: persisted sticky=%s not in current pool members; falling back to %s (%s)\n",
			c.pool, sticky, c.curNick, reason)
	}
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
			fmt.Fprintf(c.logOut, "auto[%s]: %s z.ai 429 concurrency throttle — absorbing as transient, not parking\n", c.pool, b.Nick)
			rewriteTo503(resp)
			setRetryAfter(resp.Header, zaiThrottleRetryAfterSeconds)
			return nil
		}
		respSnap := quota.Extract(resp)
		if !c.isGenuineExhaustionSignal(b.Nick, respSnap) {
			// Not genuine exhaustion. Split the two remaining cases by the
			// rate-limit signature: a transient per-minute throttle
			// (RPM/ITPM/OTPM) carries an upstream retry-after and/or the legacy
			// anthropic-ratelimit-requests/tokens headers and clears in seconds
			// — absorb it as a short 503 back-off on the SAME member, never
			// parking or switching (issue #191). Everything else is a
			// policy/punishment 429 (e.g. "unsupported third-party client",
			// which carries no rate-limit headers): forward the body on a 503,
			// also without parking.
			if secs, ok := transientRateLimit429(resp); ok {
				fmt.Fprintf(c.logOut, "auto[%s]: %s rate-limit 429 (transient throttle) — backing off %ds, not parking\n", c.pool, b.Nick, secs)
				rewriteTo503(resp)
				setRetryAfter(resp.Header, secs)
				return nil
			}
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

// isZaiBackend reports whether b is a z.ai/Zhipu backend. For these
// backends a proxy-path 429 is always a transient concurrency/QPS throttle
// (the 1302 "Rate limit reached for requests"): genuine quota exhaustion is
// detected out-of-band by the poller (5h / monthly windows via the quota
// endpoint), never by a proxy 429. So a z.ai proxy 429 is never a
// park-worthy exhaustion signal (issue #153). Detection reuses the same
// URL-keyed provider registry as poolWindowLabelsFor; ProviderFor is a pure
// match with no network call.
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
// rewrites resp: a 503 "backend switching" when a healthy member remains, or
// the honest upstream status with a precise Retry-After when the pool is dry.
// reason is the log phrase describing why the backend was parked.
//
// When every member is parked (res.allExhausted), a recovery probe is fired
// against each poller-recognised member before the upstream 429 is
// forwarded: if a probe returns a snapshot that no longer satisfies the
// freshness/exhaustion predicate (windowBlocks — the post-#125 rule), the
// member's park mark is cleared and the pool retries selection. If any
// member is now selectable, the response is rewritten to 503 (the normal
// switch shape) and the request is effectively re-routed to the recovered
// member. If the probe does not produce a healthy member, the existing
// forward-upstream-429 path runs (issue #124).
func (c *Controller) parkAndFailover(resp *http.Response, nick string, reset time.Time, reason string) error {
	res := c.record429(nick, reset)

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
				c.pool, from, recovered, recovered)
			rewriteTo503(resp)
			return nil
		}
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
//
// Snapshot freshness matters for both paths: a poller-tracked member whose
// window reset has already passed but whose stored utilization is still
// frozen at 1.0 (the poller only tracks the active member, so a failed-off
// member's entry freezes at its last good reset) must read *not* blocking —
// otherwise a transient overload 429 on a recovered member is falsely
// parked. The reset-freshness guard lives in windowBlocks for the no-status
// branch, mirroring storeExhaustedUntilLocked's behaviour on the recovery
// side (#125).
func (c *Controller) isGenuineExhaustionSignal(nick string, respSnap quota.Snapshot) bool {
	now := c.now()
	// Resolve the backend once to key the long-window predicate. Fail closed
	// (longBlocks=true) when the nick can't be resolved — a runtime-removed
	// member should still honour a genuine 7d rejection rather than have it
	// silently dropped (issue #192).
	longBlocks := true
	idx := c.indexOf(nick)
	if idx >= 0 {
		longBlocks = poller.LongWindowBlocksExhaustion(c.backendAt(idx).BaseURL)
	}
	if snapRejects(respSnap, now, longBlocks) {
		return true
	}
	if c.store != nil && idx >= 0 {
		if snapRejects(c.store.Get(c.backendAt(idx).QuotaKey()), now, longBlocks) {
			return true
		}
	}
	return false
}

// snapRejects reports whether snap shows the backend actually rate-limited:
// an overall "rejected" unified status, or either unified window blocking
// (see windowBlocks — a per-window "rejected", or, absent a status, a
// utilization at the cap with a reset still in the future).
//
// now is the controller's clock reading; it gates the no-status util-only
// branch so a frozen at-cap snapshot whose reset has already passed reads
// not blocking. The status branch ignores now — an explicit "rejected" is
// authoritative regardless of reset arithmetic.
//
// longBlocks reports whether the backend's long (7d/monthly) window is a
// genuine chat-blocking signal (poller.LongWindowBlocksExhaustion). When
// false — Z.AI/Zhipu, whose monthly slot is a web-search/reader/zread tool
// quota, not chat throughput (issue #192) — the 7d term is dropped so a
// filled tool quota can't park a chat-healthy member. Callers fail closed
// (pass true) when the backend can't be resolved.
func snapRejects(snap quota.Snapshot, now time.Time, longBlocks bool) bool {
	return snap.UnifiedStatus == unifiedStatusRejected ||
		windowBlocks(snap.Unified5hUtilization, snap.Unified5hStatus, snap.Unified5hReset, now) ||
		(longBlocks && windowBlocks(snap.Unified7dUtilization, snap.Unified7dStatus, snap.Unified7dReset, now))
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
	prev := c.curNick
	if !c.isUnavailableLocked(prev) {
		c.notifyMutate()
		return record429Result{to: prev}
	}
	if next, ok := c.firstHealthyNickLocked(); ok {
		c.setActiveMemberLocked(next)
		return record429Result{to: next, switched: next != prev}
	}
	next, soonest := c.soonestNickLocked()
	c.setActiveMemberLocked(next)
	return record429Result{to: next, retryAfter: c.waitUntil(soonest), allExhausted: true}
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
		// Skip disabled / removed members — they are unreachable regardless
		// of upstream state.
		if c.disabled[nick] || c.removedMembers[nick] {
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
			fmt.Fprintf(c.logOut, "auto[%s]: recovery probe for %s failed: %v (park retained)\n", c.pool, t.nick, err)
			continue
		}
		// snapRejects (post-#125) shares the freshness predicate with the
		// park-decision path. If the snapshot no longer rejects, the member
		// is healthy — unmark and update lastActed to suppress re-probe
		// thrash until the cooldown.
		c.mu.Lock()
		recoveredNow := c.now()
		if !snapRejects(snap, recoveredNow, poller.LongWindowBlocksExhaustion(b.BaseURL)) {
			delete(c.exhausted, t.nick)
			c.notifyMutate()
			if recovered == "" {
				recovered = t.nick
			}
			fmt.Fprintf(c.logOut, "auto[%s]: recovery probe for %s returned healthy; unparked\n", c.pool, t.nick)
		} else {
			fmt.Fprintf(c.logOut, "auto[%s]: recovery probe for %s still exhausted; park retained\n", c.pool, t.nick)
		}
		c.probeInFlight[t.quotaKey] = false
		c.mu.Unlock()
	}
	return recovered
}

// clearProbeInFlight clears the in-flight flag for quotaKey under c.mu.
// Used by the error path of tryRecoverParked.
func (c *Controller) clearProbeInFlight(quotaKey string) {
	c.mu.Lock()
	c.probeInFlight[quotaKey] = false
	c.mu.Unlock()
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

// liveParkActiveLocked reports whether a live-429 park is currently holding
// nick out of rotation: an entry in c.exhausted whose reset has not elapsed and
// which the fresh store has not reconciled away (issue #145). It is the gate
// for MemberStatus.Parked / the per-nick clear button (issue #147) — the exact
// condition under which ClearExhaustedNick has a park to drop AND that park is
// what is keeping the member parked. Store-sourced exhaustion is deliberately
// excluded: clearing the live park cannot move it. Caller holds c.mu.
func (c *Controller) liveParkActiveLocked(nick string) bool {
	reset, ok := c.exhausted[nick]
	if !ok || !c.now().Before(reset) {
		return false
	}
	return !c.storeReconcilesParkLocked(nick)
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
	}
	if sReset, sOK := c.storeExhaustedUntilLocked(nick); sOK {
		if !ok || sReset.After(reset) {
			reset, ok = sReset, true
		}
	}
	return reset, ok
}

// storeReconcilesParkLocked reports whether the polled quota store is fresh
// and healthy enough to retire a member's stale live-429 park (issue #145).
// It is true only when the store holds the member's data (HasData), that
// snapshot is recent (within storeSnapshotFreshness of now), AND it shows no
// blocking window (snapRejects == false). It returns false for a nil store,
// an unknown nick, an empty snapshot (store.Get on a missing key returns a
// stamped-but-empty snapshot whose !snapRejects would otherwise read healthy),
// a frozen/stale snapshot (the poller refreshes only the active member, so a
// failed-off member's entry freezes — it must keep aging by wall-clock), or a
// snapshot whose window still blocks. Because !snapRejects is strictly more
// conservative than the storeExhaustedUntilLocked union, this short-circuit
// and that union can never both fire. Caller holds c.mu; the store has its
// own lock and never calls back into the controller.
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

// storeExhaustedUntilLocked reports nick's window reset when the quota store
// shows a unified window actually blocking (see windowBlocks: a "rejected"
// status, or — absent a status — utilization at the cap) with a reset still
// in the future. It considers the 5h window always, and the 7d/long window
// only when that window is a genuine chat-blocking signal
// (poller.LongWindowBlocksExhaustion). Each contributes only when its own
// window blocks and its own reset is ahead, and when both qualify the later
// reset wins, so the returned time is always anchored to the window that
// actually flagged the member — never the 7d reset for a 5h-only exhaustion
// or vice versa.
// Checking 7d matters for poller-tracked backends (MiniMaxi / Ark), which
// report a weekly cap through the dashboard API and emit no clean
// proxy-path 429 to catch a 7d-exhausted-but-5h-healthy member the reactive
// way. Z.AI/Zhipu is the exception: its monthly slot is a web-search/reader/
// zread tool quota, not chat throughput, so its long window is skipped here
// (issue #192).
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
	b := c.backendAt(idx)
	snap := c.store.Get(b.QuotaKey())
	now := c.now()
	reset, ok := time.Time{}, false
	// The long window contributes only when it is a genuine chat-blocking
	// signal. For Z.AI/Zhipu its monthly slot is a web-search/reader/zread
	// tool quota (issue #192), so drop it here — a filled tool quota must
	// not proactively park a chat-healthy member. The 5h window always
	// participates.
	longBlocks := poller.LongWindowBlocksExhaustion(b.BaseURL)
	windows := [...]struct {
		util   *float64
		status string
		reset  *time.Time
		use    bool
	}{
		{snap.Unified5hUtilization, snap.Unified5hStatus, snap.Unified5hReset, true},
		{snap.Unified7dUtilization, snap.Unified7dStatus, snap.Unified7dReset, longBlocks},
	}
	for _, w := range windows {
		if !w.use {
			continue
		}
		// windowBlocks is the single source of truth for the freshness
		// contract: the no-status branch requires reset != nil AND
		// now.Before(*reset), so a frozen at-cap snapshot whose reset has
		// already passed reads as not blocking here. The status branch
		// returns true regardless of reset (an explicit "rejected" is
		// authoritative and refreshed on every response), so w.reset may
		// still be nil past the gate — skip those windows since we have no
		// future reset to anchor.
		if !windowBlocks(w.util, w.status, w.reset, now) {
			continue
		}
		if w.reset == nil {
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
//     which can lock an entire pool out as "all exhausted". A "rejected"
//     status whose reset has already passed reads as not blocking — the
//     same freshness guard the no-status util branch applies, so a frozen
//     post-#134 snapshot can't keep a recovered backend parked forever
//     (issue #134 deadlock). A "rejected" status with a nil reset still
//     blocks: the snapshot is genuinely authoritative about the window
//     state and we have no reset to bound its freshness.
//   - When the window has no status (poller-tracked z.ai / MiniMaxi / Ark,
//     which report only a utilization fraction), fall back to the cap, but
//     ONLY while the window's reset is still in the future. The poller
//     only tracks the active member, so a failed-off member's entry freezes
//     at its last good reset; once that reset passes the entry is stale and
//     must read not blocking — otherwise a transient overload 429 on a
//     recovered member is falsely parked. This freshness guard is the same
//     one storeExhaustedUntilLocked applies on the recovery side (#125).
func windowBlocks(util *float64, status string, reset *time.Time, now time.Time) bool {
	if status != "" {
		if status != unifiedStatusRejected {
			return false
		}
		// "rejected" with no reset is authoritative (snapshot has no
		// freshness bound). "rejected" with a reset respects it — once the
		// reset has passed the snapshot is stale and reads as not blocking
		// (issue #134).
		return reset == nil || now.Before(*reset)
	}
	return util != nil && *util >= exhaustionUtilizationThreshold &&
		reset != nil && now.Before(*reset)
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
	b := c.backendAt(idx)
	snap := c.store.Get(b.QuotaKey())
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

	// The long window's length is provider-aware: Z.AI/Zhipu's long slot
	// carries a monthly TIME_LIMIT window, so dividing its reset by 7 days
	// would clamp the elapsed fraction to 0 and collapse the lead to raw
	// utilization (issue #140). Resolve the length from the same provider
	// mapping that supplies the column label.
	lead5h, has5h = computeLead(snap.Unified5hUtilization, snap.Unified5hReset, window5h)
	// The long window feeds routing pressure only when it is a genuine
	// chat-blocking signal. For Z.AI/Zhipu the monthly slot is a
	// web-search/reader/zread tool quota (issue #192), so leave has7d false
	// and drive balance-mode pressure from the 5h window alone — a filled
	// tool quota must not skew chat routing.
	if poller.LongWindowBlocksExhaustion(b.BaseURL) {
		lead7d, has7d = computeLead(snap.Unified7dUtilization, snap.Unified7dReset, poller.LongWindowFor(b.BaseURL))
	}

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

// balanceSwitchLocked returns the nick of the member to switch to when the
// active member's overall lead exceeds the best candidate's lead by at least
// balanceGap and the dwell timer has elapsed. Returns ("", false) when no
// switch is warranted. Covers all members in the unified collection —
// runtime-added members participate in balance consideration (issue #185).
// Caller holds c.mu.
//
// Among eligible candidates with the same best lead (including the common
// all-zero / no-snapshot case), the one with the smallest lastSelectedSeq
// wins: the member that was least recently active is preferred, spreading
// 5-hour cycles across pool members rather than repeatedly re-selecting
// the lexically-first nick.
func (c *Controller) balanceSwitchLocked() (string, bool) {
	if !c.lastBalanceSwitch.IsZero() && c.now().Sub(c.lastBalanceSwitch) < c.balanceDwell {
		return "", false
	}
	curOverall, _, _, _, _ := c.memberLeadsLocked(c.curNick)

	bestNick := ""
	bestLead := curOverall
	var bestSeq uint64
	for _, m := range c.members {
		if m.Nick == c.curNick || c.isUnavailableLocked(m.Nick) {
			continue
		}
		candOverall, _, _, _, _ := c.memberLeadsLocked(m.Nick)
		if curOverall-candOverall < c.balanceGap {
			continue
		}
		seq := c.lastSelectedSeq[m.Nick]
		if bestNick == "" || candOverall < bestLead || (candOverall == bestLead && seq < bestSeq) {
			bestLead = candOverall
			bestNick = m.Nick
			bestSeq = seq
		}
	}
	if bestNick == "" {
		return "", false
	}
	return bestNick, true
}

// soonestNickLocked returns the nick and reset time of the member that
// frees up first. Only meaningful when every member is exhausted (the
// all-dry case); falls back to curNick if the map is somehow empty.
// Caller holds c.mu.
func (c *Controller) soonestNickLocked() (string, time.Time) {
	best, bestSet := c.curNick, false
	var bestReset time.Time
	for _, m := range c.members {
		if c.disabled[m.Nick] || c.removedMembers[m.Nick] {
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
		Pool:       c.pool,
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
		if c.disabled[nick] || c.removedMembers[nick] {
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

// poolWindowLabelsFor returns the per-pool rolling-window label hint the
// UI consumes to render the long-window column. The default is the
// Anthropic-style "5h" / "7d". Z.AI's long window is monthly (issue
// #138), so a Z.AI backend gets "5h" / "monthly". Unknown providers fall
// back to the default; an empty base URL is treated as no provider.
//
// This is duplicated with main.WindowLabelsFor so the auto package does
// not have to import the main package. The two mappings are intentionally
// identical: they are both a one-line switch on the provider name, and
// adding a new provider touches both at once. A test in
// cmd/agent-quota-gateway/main_test.go covers the consumer side.
func poolWindowLabelsFor(baseURL string) PoolWindowLabels {
	l := poller.WindowLabelsFor(baseURL)
	return PoolWindowLabels{Short: l.Short, Long: l.Long}
}
