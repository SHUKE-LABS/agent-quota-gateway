package quota

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ChatGPT-Codex metered-window response headers. OpenAI's Codex backend
// (chatgpt.com/backend-api/codex) reports its plan windows on EVERY response
// — including the 429 that signals depletion — via this family, instead of
// Anthropic's anthropic-ratelimit-unified-* headers. Names and semantics are
// pinned from openai/codex's own parser (codex-rs/codex-api/src/rate_limits.rs,
// confirmed in issue #304):
//
//   - x-codex-{primary,secondary}-used-percent — integer percent of the window
//   - x-codex-{primary,secondary}-window-minutes — the window's length
//   - x-codex-{primary,secondary}-reset-at — absolute unix-seconds reset
//   - x-codex-{primary,secondary}-reset-after-seconds — relative reset; some
//     older/community builds emit this instead of reset-at (tolerated, lower
//     priority)
//   - x-codex-rate-limit-reached-type — backend-classified limit state
//     (e.g. usage_limit_reached); read by internal/auto's 429 classifier but
//     deliberately never stored: it names no window, and filed as a status it
//     would poison windowBlocks' "rejected" branch (which treats a rejected
//     status with no reset as authoritative forever).
//
// Primary is the 5-hour window and secondary the weekly one in every observed
// deployment, but the lengths are not contractual (5h-cap removal experiments,
// reset banking) — which is why window-minutes is parsed and stored rather
// than hardcoded anywhere (issue #304 review round 2).
const (
	HeaderCodexRateLimitReachedType = "x-codex-rate-limit-reached-type"

	HeaderCodexPrimaryUsedPercent       = "x-codex-primary-used-percent"
	HeaderCodexPrimaryWindowMinutes     = "x-codex-primary-window-minutes"
	HeaderCodexPrimaryResetAt           = "x-codex-primary-reset-at"
	HeaderCodexPrimaryResetAfterSeconds = "x-codex-primary-reset-after-seconds"

	HeaderCodexSecondaryUsedPercent       = "x-codex-secondary-used-percent"
	HeaderCodexSecondaryWindowMinutes     = "x-codex-secondary-window-minutes"
	HeaderCodexSecondaryResetAt           = "x-codex-secondary-reset-at"
	HeaderCodexSecondaryResetAfterSeconds = "x-codex-secondary-reset-after-seconds"
)

// CodexReachedTypeUsageLimit is the reached-type value that marks a genuine
// plan-limit depletion (the ChatGPT "You've hit your usage limit" 429). Other
// values may exist (workspace credit states); they do not classify on their
// own — a capped window's metered headers are the fallback signal.
const CodexReachedTypeUsageLimit = "usage_limit_reached"

// CodexReachedType returns the response's x-codex-rate-limit-reached-type
// value, trimmed, or "" when absent. Case is preserved; callers compare
// case-insensitively.
func CodexReachedType(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Header.Get(HeaderCodexRateLimitReachedType))
}

// ExtractCodex parses the x-codex-* metered-window family into a partial
// Snapshot: primary maps onto the 5h fields, secondary onto the 7d fields,
// and used-percent becomes a utilization fraction (percent/100). The snapshot
// is poller-style — no status field — so downstream reads treat "utilization
// at the cap with a future reset" as the blocking signal, exactly like a
// z.ai / MiniMaxi / Ark dashboard snapshot.
//
// reset-at (absolute unix seconds) is precise; reset-after-seconds is a
// fallback — now+secs resolved against the observe-time now — flagged as such
// so a fresh fallback never overwrites a reset that is already known (issue
// #311). Absent or unparseable headers leave nil fields — never invented
// values.
//
// The Backend field is left empty (same contract as Extract): the caller
// files the snapshot under the key it chooses.
func ExtractCodex(h http.Header, now time.Time) Snapshot {
	s := Snapshot{}
	s.Unified5hUtilization = parseCodexPercent(h.Get(HeaderCodexPrimaryUsedPercent))
	s.Unified5hReset, s.unified5hResetIsFallback = parseCodexReset(h.Get(HeaderCodexPrimaryResetAt), h.Get(HeaderCodexPrimaryResetAfterSeconds), now)
	s.Unified5hWindowMinutes = parseCodexWindowMinutes(h.Get(HeaderCodexPrimaryWindowMinutes))
	s.Unified7dUtilization = parseCodexPercent(h.Get(HeaderCodexSecondaryUsedPercent))
	s.Unified7dReset, s.unified7dResetIsFallback = parseCodexReset(h.Get(HeaderCodexSecondaryResetAt), h.Get(HeaderCodexSecondaryResetAfterSeconds), now)
	s.Unified7dWindowMinutes = parseCodexWindowMinutes(h.Get(HeaderCodexSecondaryWindowMinutes))
	return s
}

// OverlayCodex merges a codex-derived snapshot c onto s with prev-aware
// precedence; the response observer uses this to combine the codex windows
// with (the, for a chatgpt.com member, always empty) Anthropic header
// extraction before filing one snapshot — a merge at the source rather than
// two Store writes. prev is the store's last-known snapshot for the key
// (quotaObserver passes store.Get(key)); on a fresh key it is the zero
// Snapshot and every rule below degenerates to today's behavior.
//
// Two field classes with different rules (issue #311 — the 5h window went
// missing and the 5h reset drifted because the pre-#311 overlay was
// prev-blind):
//
//   - utilization and window-minutes: c is authoritative when present; a
//     field absent from both s and c carries forward from prev, mirroring
//     mergeSnapshot's #163 fill semantics at the overlay step.
//
//   - resets: precise values (absolute reset-at, fallback flag false) always
//     win; a drift-prone reset-after-seconds fallback is adopted only when no
//     prior reset is known, and never overwrites one that is — each fallback
//     is now+secs resolved at observe time, so letting it overwrite would
//     drift the stored reset on every response.
//
// The prev read is best-effort and may be stale under concurrent responses
// to the same nick; these reset rules are therefore advisory at this layer.
// The authoritative fallback gate is mergeSnapshot's, which runs under
// Store.Merge's write lock — the serialization point (plan-review round 1).
func (s *Snapshot) OverlayCodex(prev, c Snapshot) {
	if c.Unified5hUtilization != nil {
		s.Unified5hUtilization = c.Unified5hUtilization
	} else if s.Unified5hUtilization == nil && prev.Unified5hUtilization != nil {
		s.Unified5hUtilization = prev.Unified5hUtilization
	}
	s.Unified5hReset, s.unified5hResetIsFallback = overlayCodexReset(
		s.Unified5hReset, s.unified5hResetIsFallback,
		prev.Unified5hReset, prev.unified5hResetIsFallback,
		c.Unified5hReset, c.unified5hResetIsFallback)
	if c.Unified5hWindowMinutes != nil {
		s.Unified5hWindowMinutes = c.Unified5hWindowMinutes
	} else if s.Unified5hWindowMinutes == nil && prev.Unified5hWindowMinutes != nil {
		s.Unified5hWindowMinutes = prev.Unified5hWindowMinutes
	}
	if c.Unified7dUtilization != nil {
		s.Unified7dUtilization = c.Unified7dUtilization
	} else if s.Unified7dUtilization == nil && prev.Unified7dUtilization != nil {
		s.Unified7dUtilization = prev.Unified7dUtilization
	}
	s.Unified7dReset, s.unified7dResetIsFallback = overlayCodexReset(
		s.Unified7dReset, s.unified7dResetIsFallback,
		prev.Unified7dReset, prev.unified7dResetIsFallback,
		c.Unified7dReset, c.unified7dResetIsFallback)
	if c.Unified7dWindowMinutes != nil {
		s.Unified7dWindowMinutes = c.Unified7dWindowMinutes
	} else if s.Unified7dWindowMinutes == nil && prev.Unified7dWindowMinutes != nil {
		s.Unified7dWindowMinutes = prev.Unified7dWindowMinutes
	}
}

// overlayCodexReset applies OverlayCodex's reset-class precedence for one
// window and returns the resulting value and fallback flag. See OverlayCodex
// for the rationale; the cases in order are: precise wins, first-time
// fallback adoption, fallback never overwrites a known reset, carry absent
// forward from prev, and leave s untouched when neither c nor prev
// contributes.
func overlayCodexReset(sVal *time.Time, sFB bool, pVal *time.Time, pFB bool, cVal *time.Time, cFB bool) (*time.Time, bool) {
	switch {
	case cVal != nil && !cFB:
		return cVal, false
	case cVal != nil && pVal == nil:
		return cVal, cFB
	case cVal != nil:
		return pVal, pFB
	case sVal == nil && pVal != nil:
		return pVal, pFB
	default:
		return sVal, sFB
	}
}

// parseCodexPercent turns an integer-percent header into a utilization
// fraction, or nil when absent/unparseable.
func parseCodexPercent(v string) *float64 {
	if v == "" {
		return nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return nil
	}
	util := f / 100.0
	return &util
}

// parseCodexReset resolves a window's reset and reports whether the value is
// a fallback: absolute unix seconds from resetAt when usable (precise, false);
// else now+seconds from resetAfter (fallback, true — drift-prone, only
// adopted when no prior reset is known); else (nil, false).
func parseCodexReset(resetAt, resetAfter string, now time.Time) (*time.Time, bool) {
	if t := parseUnixTime(resetAt); t != nil {
		return t, false
	}
	if resetAfter == "" {
		return nil, false
	}
	secs, err := strconv.ParseInt(strings.TrimSpace(resetAfter), 10, 64)
	if err != nil || secs < 0 {
		return nil, false
	}
	t := now.Add(time.Duration(secs) * time.Second).UTC()
	return &t, true
}

// parseCodexWindowMinutes parses a window length in minutes; nil for
// absent/unparseable/non-positive values.
func parseCodexWindowMinutes(v string) *int {
	if v == "" {
		return nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n <= 0 {
		return nil
	}
	return &n
}
