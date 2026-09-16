// Package quota captures Anthropic rate-limit headers from upstream
// responses and exposes the most recent snapshot per backend key.
//
// The gateway is the only place quota state lives. It does not issue
// synthetic probes — every snapshot is the side effect of a real
// request that a client made. Storage is intentionally a single latest
// snapshot per key (no history, no time series); the read endpoint
// returns whatever the last response taught us.
package quota

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Anthropic unified rate-limit response headers. Names are
// case-insensitive per RFC 7230, and http.Header.Get already handles
// canonical lookup.
//
// The unified scheme is what subscription / OAuth (Claude Code) tokens
// report: usage against rolling 5-hour and 7-day windows, expressed as
// a utilization fraction (0..1) plus a status. This is the quota the
// gateway exists to meter. The older anthropic-ratelimit-requests-* and
// -tokens-* headers (per-minute RPM/TPM throttles on API-key traffic)
// are intentionally not captured — they are a throughput rate, not the
// subscription budget.
const (
	HeaderUnifiedStatus              = "anthropic-ratelimit-unified-status"
	HeaderUnifiedReset               = "anthropic-ratelimit-unified-reset"
	HeaderUnifiedRepresentativeClaim = "anthropic-ratelimit-unified-representative-claim"

	HeaderUnified5hStatus      = "anthropic-ratelimit-unified-5h-status"
	HeaderUnified5hUtilization = "anthropic-ratelimit-unified-5h-utilization"
	HeaderUnified5hReset       = "anthropic-ratelimit-unified-5h-reset"

	HeaderUnified7dStatus      = "anthropic-ratelimit-unified-7d-status"
	HeaderUnified7dUtilization = "anthropic-ratelimit-unified-7d-utilization"
	HeaderUnified7dReset       = "anthropic-ratelimit-unified-7d-reset"

	HeaderUnifiedFallbackPercentage    = "anthropic-ratelimit-unified-fallback-percentage"
	HeaderUnifiedOverageStatus         = "anthropic-ratelimit-unified-overage-status"
	HeaderUnifiedOverageDisabledReason = "anthropic-ratelimit-unified-overage-disabled-reason"

	HeaderOrgID = "anthropic-organization-id"
)

// Snapshot is the latest known quota state for a single backend key.
//
// Fields are zero/nil when the corresponding header was absent on the
// most recent response. The unified scheme is reported per rolling
// window (5h, 7d); a given response may carry some windows and not
// others, so partial snapshots are normal. Utilization is a *float64 so
// JSON consumers can distinguish "header missing" (field absent) from a
// real 0.0 (window untouched — full quota available).
type Snapshot struct {
	// Backend is the cache key the snapshot was filed under: the nick of
	// the backend the request resolved to, or "default" as a fallback.
	Backend string `json:"backend"`

	// UnifiedStatus is the overall allow/reject decision; UnifiedReset
	// is when the representative window resets; RepresentativeClaim
	// names which window (e.g. "five_hour") drove that decision.
	UnifiedStatus              string     `json:"unified_status,omitempty"`
	UnifiedReset               *time.Time `json:"unified_reset,omitempty"`
	UnifiedRepresentativeClaim string     `json:"unified_representative_claim,omitempty"`

	Unified5hStatus      string     `json:"unified_5h_status,omitempty"`
	Unified5hUtilization *float64   `json:"unified_5h_utilization,omitempty"`
	Unified5hReset       *time.Time `json:"unified_5h_reset,omitempty"`
	// Unified5hWindowMinutes is the window length the upstream itself
	// reported (x-codex-primary-window-minutes), in minutes. Carried for
	// consumers that need the true length — balance-lead elapsed-fraction
	// math — because Codex window lengths are not contractual (issue #304).
	// Deliberately NOT part of HasData / hasQuotaWindow admission: it is
	// duration metadata, not window state, so it neither admits a snapshot
	// nor alone refreshes one.
	Unified5hWindowMinutes *int `json:"unified_5h_window_minutes,omitempty"`

	Unified7dStatus      string     `json:"unified_7d_status,omitempty"`
	Unified7dUtilization *float64   `json:"unified_7d_utilization,omitempty"`
	Unified7dReset       *time.Time `json:"unified_7d_reset,omitempty"`
	// Unified7dWindowMinutes is the long-window analogue of
	// Unified5hWindowMinutes (x-codex-secondary-window-minutes).
	Unified7dWindowMinutes *int `json:"unified_7d_window_minutes,omitempty"`

	UnifiedFallbackPercentage    *float64 `json:"unified_fallback_percentage,omitempty"`
	UnifiedOverageStatus         string   `json:"unified_overage_status,omitempty"`
	UnifiedOverageDisabledReason string   `json:"unified_overage_disabled_reason,omitempty"`

	// unified5hResetIsFallback / unified7dResetIsFallback record whether the
	// corresponding reset was resolved from x-codex-*-reset-after-seconds
	// (true — a drift-prone now+secs computed at observe time) rather than an
	// absolute x-codex-*-reset-at (false — precise). Codex-only (issue #311):
	// Anthropic extraction and the poller never set them. Unexported on
	// purpose so they can never surface in the JSON wire shape or HasData;
	// OverlayCodex and mergeSnapshot consult them to keep a fresh fallback
	// from overwriting a reset that is already known. The json:"-" tag is
	// inert on an unexported field (encoding/json skips it regardless) and
	// documents non-serialization.
	unified5hResetIsFallback bool `json:"-"`
	unified7dResetIsFallback bool `json:"-"`

	// OrgID is the Anthropic organization that owns the backend's account,
	// copied from the anthropic-organization-id response header. Present
	// only when that header was on the snapshot-driving response.
	OrgID string `json:"org_id,omitempty"`

	// AsOf is the gateway-side time the snapshot was recorded. The
	// reset fields are upstream-supplied absolute times; AsOf is the
	// "we last heard from Anthropic" timestamp.
	AsOf time.Time `json:"as_of"`
}

// Extract parses the Anthropic rate-limit headers off resp and returns
// a Snapshot stamped with the current time.
//
// The returned Snapshot's Backend field is left empty — Extract does
// not know which key the caller intends to file the snapshot under.
// The wiring in main.go derives the key from the resolved backend nick
// before calling Store.Put. Headers that are absent or unparseable
// yield nil fields; we never invent zero values for missing data.
func Extract(resp *http.Response) Snapshot {
	s := Snapshot{AsOf: time.Now().UTC()}
	if resp == nil {
		return s
	}
	h := resp.Header
	s.UnifiedStatus = h.Get(HeaderUnifiedStatus)
	s.UnifiedReset = parseUnixTime(h.Get(HeaderUnifiedReset))
	s.UnifiedRepresentativeClaim = h.Get(HeaderUnifiedRepresentativeClaim)

	s.Unified5hStatus = h.Get(HeaderUnified5hStatus)
	s.Unified5hUtilization = parseFloat(h.Get(HeaderUnified5hUtilization))
	s.Unified5hReset = parseUnixTime(h.Get(HeaderUnified5hReset))

	s.Unified7dStatus = h.Get(HeaderUnified7dStatus)
	s.Unified7dUtilization = parseFloat(h.Get(HeaderUnified7dUtilization))
	s.Unified7dReset = parseUnixTime(h.Get(HeaderUnified7dReset))

	s.UnifiedFallbackPercentage = parseFloat(h.Get(HeaderUnifiedFallbackPercentage))
	s.UnifiedOverageStatus = h.Get(HeaderUnifiedOverageStatus)
	s.UnifiedOverageDisabledReason = h.Get(HeaderUnifiedOverageDisabledReason)

	s.OrgID = h.Get(HeaderOrgID)
	return s
}

// HasData reports whether the snapshot carries any upstream-derived
// fields. A snapshot with only Backend and AsOf set is empty — the
// /_gateway/quota endpoint uses this to distinguish "no traffic yet"
// from "traffic returned no rate-limit headers".
func (s Snapshot) HasData() bool {
	return s.UnifiedStatus != "" ||
		s.UnifiedReset != nil ||
		s.UnifiedRepresentativeClaim != "" ||
		s.Unified5hStatus != "" ||
		s.Unified5hUtilization != nil ||
		s.Unified5hReset != nil ||
		s.Unified7dStatus != "" ||
		s.Unified7dUtilization != nil ||
		s.Unified7dReset != nil ||
		s.UnifiedFallbackPercentage != nil ||
		s.UnifiedOverageStatus != "" ||
		s.UnifiedOverageDisabledReason != "" ||
		s.OrgID != ""
}

// Store holds the most recent Snapshot per backend key. The zero value
// is not usable; call NewStore.
type Store struct {
	mu       sync.RWMutex
	data     map[string]Snapshot
	onChange func() // called (non-blocking) after every Put; nil by default
}

// NewStore returns an empty Store ready for concurrent use.
func NewStore() *Store {
	return &Store{data: make(map[string]Snapshot)}
}

// SetOnChange installs a callback invoked (non-blocking) after every Put.
// Used by the persister to coalesce writes without importing this package.
func (s *Store) SetOnChange(fn func()) {
	s.mu.Lock()
	s.onChange = fn
	s.mu.Unlock()
}

// Put records s under key, replacing any prior snapshot for that key.
// Put overwrites the snapshot's Backend field with key so the value
// returned by Get always reports the key it was filed under.
func (s *Store) Put(key string, snap Snapshot) {
	snap.Backend = key
	s.mu.Lock()
	s.data[key] = snap
	onChange := s.onChange
	s.mu.Unlock()
	if onChange != nil {
		onChange()
	}
}

// Merge records snap under key, but preserves any previously-cached field
// that snap does not carry. For every field that distinguishes "absent"
// from a real zero — nil *float64, nil *time.Time, empty string — a nil/
// empty value on snap retains the prior snapshot's value; only fields
// actually present on snap overwrite. Backend (forced to key) and AsOf
// always come from snap.
//
// This is the write path for the two live writers (the response observer
// and the poller). A given upstream response or poll reports only the
// windows it touched, so partial snapshots are normal; a wholesale Put
// would blank the reset/utilization fields the missing windows had taught
// us, flashing the UI reset cells to "-" until a complete response landed
// (issue #163). Merge keeps those fields. The hasQuotaWindow / HasData
// gates upstream of this call still skip a fully-empty response, so a
// no-window response neither bumps AsOf nor overwrites anything (the #121
// org-id-only guard is unaffected).
//
// A stale-but-preserved reset is safe for the routing logic: windowBlocks
// and the lead computation both gate on the reset still being in the
// future, so a past reset reads as not-blocking exactly as a nil one did.
func (s *Store) Merge(key string, snap Snapshot) {
	snap.Backend = key
	s.mu.Lock()
	if prev, ok := s.data[key]; ok {
		snap = mergeSnapshot(prev, snap)
	}
	s.data[key] = snap
	onChange := s.onChange
	s.mu.Unlock()
	if onChange != nil {
		onChange()
	}
}

// mergeSnapshot returns next with every absent field (nil pointer or empty
// string) filled from prev. Backend and AsOf are always next's — the merge
// preserves learned window data, not the "when we last heard" timestamp.
//
// Codex fallback-precision gate (issue #311): a reset flagged as a
// reset-after-seconds fallback is resolved as now+secs at observe time, so it
// drifts on every response. A fallback reset on next therefore never
// overwrites a reset prev already knows, whatever prev's precision — this
// runs under Store.Merge's write lock, which is what makes the gate hold when
// two responses to the same nick interleave the observer's
// read-prev→overlay→merge sequence (a stale-prev overlay merged after a
// precise merge must not regress it). Precise (unflagged) and nil resets keep
// the plain fill-forward rules; the flag only ever originates in ExtractCodex,
// so Anthropic-extraction and poller snapshots merge bit-identically to before.
func mergeSnapshot(prev, next Snapshot) Snapshot {
	out := next
	if out.UnifiedStatus == "" {
		out.UnifiedStatus = prev.UnifiedStatus
	}
	if out.UnifiedReset == nil {
		out.UnifiedReset = prev.UnifiedReset
	}
	if out.UnifiedRepresentativeClaim == "" {
		out.UnifiedRepresentativeClaim = prev.UnifiedRepresentativeClaim
	}
	if out.Unified5hStatus == "" {
		out.Unified5hStatus = prev.Unified5hStatus
	}
	if out.Unified5hUtilization == nil {
		out.Unified5hUtilization = prev.Unified5hUtilization
	}
	if out.Unified5hReset == nil {
		out.Unified5hReset = prev.Unified5hReset
		out.unified5hResetIsFallback = prev.unified5hResetIsFallback
	} else if out.unified5hResetIsFallback && prev.Unified5hReset != nil {
		// next carries a drift-prone fallback and prev already knows a reset
		// (precise or not): keep prev's. The reset-non-nil condition is
		// structural here — the else branch only runs with a non-nil next
		// reset — so a flag-only malformed snapshot can never trigger the
		// gate (plan-review round 1 note).
		out.Unified5hReset = prev.Unified5hReset
		out.unified5hResetIsFallback = prev.unified5hResetIsFallback
	}
	if out.Unified5hWindowMinutes == nil {
		out.Unified5hWindowMinutes = prev.Unified5hWindowMinutes
	}
	if out.Unified7dStatus == "" {
		out.Unified7dStatus = prev.Unified7dStatus
	}
	if out.Unified7dUtilization == nil {
		out.Unified7dUtilization = prev.Unified7dUtilization
	}
	if out.Unified7dReset == nil {
		out.Unified7dReset = prev.Unified7dReset
		out.unified7dResetIsFallback = prev.unified7dResetIsFallback
	} else if out.unified7dResetIsFallback && prev.Unified7dReset != nil {
		out.Unified7dReset = prev.Unified7dReset
		out.unified7dResetIsFallback = prev.unified7dResetIsFallback
	}
	if out.Unified7dWindowMinutes == nil {
		out.Unified7dWindowMinutes = prev.Unified7dWindowMinutes
	}
	if out.UnifiedFallbackPercentage == nil {
		out.UnifiedFallbackPercentage = prev.UnifiedFallbackPercentage
	}
	if out.UnifiedOverageStatus == "" {
		out.UnifiedOverageStatus = prev.UnifiedOverageStatus
	}
	if out.UnifiedOverageDisabledReason == "" {
		out.UnifiedOverageDisabledReason = prev.UnifiedOverageDisabledReason
	}
	if out.OrgID == "" {
		out.OrgID = prev.OrgID
	}
	return out
}

// Get returns the snapshot recorded for key. When no snapshot has been
// recorded, Get returns an empty Snapshot with Backend=key and a fresh
// AsOf timestamp so consumers always see a parseable JSON object.
func (s *Store) Get(key string) Snapshot {
	s.mu.RLock()
	snap, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return Snapshot{Backend: key, AsOf: time.Now().UTC()}
	}
	return snap
}

// Snapshot returns a copy of all currently stored snapshots. Used by the
// persister to capture the full store state for serialisation.
func (s *Store) Snapshot() map[string]Snapshot {
	s.mu.RLock()
	out := make(map[string]Snapshot, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	s.mu.RUnlock()
	return out
}

// parseFloat returns a pointer to the parsed float64, or nil when the
// input is empty or not a valid number. The pointer lets the caller
// tell "header absent" from a real 0.0 (a window at zero utilization is
// full quota, not missing data).
func parseFloat(v string) *float64 {
	if v == "" {
		return nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil
	}
	return &f
}

// parseUnixTime parses a Unix-seconds timestamp into an absolute time.
// The unified reset headers carry epoch seconds (e.g. "1781352600"),
// not RFC 3339. Unparseable values yield nil rather than a default, so
// a malformed upstream cannot quietly look like "reset at the Unix
// epoch" to downstream consumers.
func parseUnixTime(v string) *time.Time {
	if v == "" {
		return nil
	}
	secs, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil
	}
	t := time.Unix(secs, 0).UTC()
	return &t
}
