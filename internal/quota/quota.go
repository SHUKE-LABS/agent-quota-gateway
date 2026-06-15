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

	Unified7dStatus      string     `json:"unified_7d_status,omitempty"`
	Unified7dUtilization *float64   `json:"unified_7d_utilization,omitempty"`
	Unified7dReset       *time.Time `json:"unified_7d_reset,omitempty"`

	UnifiedFallbackPercentage    *float64 `json:"unified_fallback_percentage,omitempty"`
	UnifiedOverageStatus         string   `json:"unified_overage_status,omitempty"`
	UnifiedOverageDisabledReason string   `json:"unified_overage_disabled_reason,omitempty"`

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
