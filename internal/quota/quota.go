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

// Anthropic rate-limit response headers. Names are case-insensitive
// per RFC 7230, and http.Header.Get already handles canonical lookup.
// See: https://docs.anthropic.com/en/api/rate-limits#response-headers
const (
	HeaderRequestsLimit     = "anthropic-ratelimit-requests-limit"
	HeaderRequestsRemaining = "anthropic-ratelimit-requests-remaining"
	HeaderRequestsReset     = "anthropic-ratelimit-requests-reset"
	HeaderTokensLimit       = "anthropic-ratelimit-tokens-limit"
	HeaderTokensRemaining   = "anthropic-ratelimit-tokens-remaining"
	HeaderTokensReset       = "anthropic-ratelimit-tokens-reset"
	HeaderOrgID             = "anthropic-organization-id"
)

// Snapshot is the latest known quota state for a single backend key.
//
// Pointer fields are nil when the corresponding header was absent on
// the most recent response. Anthropic's tier-dependent header set means
// partial snapshots are normal — for example, the token-bucket pair may
// be present while the request-bucket pair is not. JSON consumers can
// distinguish "header missing" (field absent in JSON) from "zero" by
// the nil case.
type Snapshot struct {
	// Backend is the cache key the snapshot was filed under. It is the
	// value of X-Mux-Backend-Nick on the inbound request, or "default"
	// when that header was empty.
	Backend string `json:"backend"`

	RequestsLimit     *int64     `json:"requests_limit,omitempty"`
	RequestsRemaining *int64     `json:"requests_remaining,omitempty"`
	RequestsReset     *time.Time `json:"requests_reset,omitempty"`

	TokensLimit     *int64     `json:"tokens_limit,omitempty"`
	TokensRemaining *int64     `json:"tokens_remaining,omitempty"`
	TokensReset     *time.Time `json:"tokens_reset,omitempty"`

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
// The wiring in main.go derives the key from X-Mux-Backend-Nick before
// calling Store.Put. Headers that are absent or unparseable yield nil
// fields; we never invent zero values for missing data.
func Extract(resp *http.Response) Snapshot {
	s := Snapshot{AsOf: time.Now().UTC()}
	if resp == nil {
		return s
	}
	h := resp.Header
	s.RequestsLimit = parseInt(h.Get(HeaderRequestsLimit))
	s.RequestsRemaining = parseInt(h.Get(HeaderRequestsRemaining))
	s.RequestsReset = parseTime(h.Get(HeaderRequestsReset))
	s.TokensLimit = parseInt(h.Get(HeaderTokensLimit))
	s.TokensRemaining = parseInt(h.Get(HeaderTokensRemaining))
	s.TokensReset = parseTime(h.Get(HeaderTokensReset))
	s.OrgID = h.Get(HeaderOrgID)
	return s
}

// HasData reports whether the snapshot carries any upstream-derived
// fields. A snapshot with only Backend and AsOf set is empty — the
// /_gateway/quota endpoint uses this to distinguish "no traffic yet"
// from "traffic returned no rate-limit headers".
func (s Snapshot) HasData() bool {
	return s.RequestsLimit != nil ||
		s.RequestsRemaining != nil ||
		s.RequestsReset != nil ||
		s.TokensLimit != nil ||
		s.TokensRemaining != nil ||
		s.TokensReset != nil ||
		s.OrgID != ""
}

// Store holds the most recent Snapshot per backend key. The zero value
// is not usable; call NewStore.
type Store struct {
	mu   sync.RWMutex
	data map[string]Snapshot
}

// NewStore returns an empty Store ready for concurrent use.
func NewStore() *Store {
	return &Store{data: make(map[string]Snapshot)}
}

// Put records s under key, replacing any prior snapshot for that key.
// Put overwrites the snapshot's Backend field with key so the value
// returned by Get always reports the key it was filed under.
func (s *Store) Put(key string, snap Snapshot) {
	snap.Backend = key
	s.mu.Lock()
	s.data[key] = snap
	s.mu.Unlock()
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

// parseInt returns a pointer to the parsed int64, or nil when the
// input is empty or not a valid integer. We use a pointer so the
// caller can tell "header absent" from "header present and zero".
func parseInt(v string) *int64 {
	if v == "" {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

// parseTime parses an RFC 3339 timestamp. Anthropic's reset headers
// are documented to use this format; unparseable values yield nil
// rather than a default, so a malformed upstream cannot quietly look
// like "reset at the Unix epoch" to downstream consumers.
func parseTime(v string) *time.Time {
	if v == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return nil
	}
	return &t
}
