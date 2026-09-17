package auto

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// codexBackendHost is the ChatGPT backend-api host that serves the Codex
// Responses protocol. Codex seats are ChatGPT-subscription accounts, so their
// depletion arrives as a 429 with the x-codex-* metered family (issue #304) —
// a signature no Anthropic classifier knows.
const codexBackendHost = "chatgpt.com"

// IsCodexBackend reports whether b points at the ChatGPT-Codex backend. The
// host identity gates both the 429 exhaustion classifier in ModifyResponse
// and the response observer's codex-window overlay in main.go: arbitrary
// non-ChatGPT vendors emitting lookalike x-codex-* headers must never be
// parked or filed off them (same host-classifier style as
// isNativeAnthropicBackend; issue #304 AC4). Exported because main.go's
// observer shares the gate.
func IsCodexBackend(b backend.Backend) bool {
	return isCodexBackend(b)
}

func isCodexBackend(b backend.Backend) bool {
	u, err := url.Parse(b.BaseURL)
	return err == nil && strings.EqualFold(u.Hostname(), codexBackendHost)
}

// codexExhaustion429 classifies a 429 from a chatgpt.com member from its
// response headers alone (issue #304). It reports whether the 429 is genuine
// plan depletion and, when it is, the park bound and the windowFact flag for
// parkAndFailoverWithSource.
//
// Genuineness: at least one window whose used-percent is at the cap. The
// x-codex-rate-limit-reached-type: usage_limit_reached marker alone does NOT
// park — that #304 fallback survived on wall-clock only (windowFact=false,
// no capped window to reconcile against), so a single marker-bearing 429
// with sub-cap windows parked the last enabled seat of a one-member pool
// until a manual /_gateway/clear, even though the seat demonstrably still
// had quota (issue #314; reversing the round-3 fallback). A marker-bearing
// 429 with sub-cap windows is routed by ModifyResponse to the same-member
// throttle instead. A 429 with no capped window and no marker is a
// policy/punishment 429 — not genuine, caller keeps it in rotation.
//
// Bound: every capped window contributes — its own reset when usable (parsed
// and still in the future), else the conservative now+defaultExhaustionWindow
// when its reset is missing, malformed, or already elapsed — and the park
// bound is the LATEST contribution. A seat capped in both windows cannot
// serve until both clear, so the binding reset is the latest one (decision
// recorded on issue #304; consistent with storeBlockBoundLocked's "latest
// bound among contributing windows"). A capped window with an unusable reset
// contributes the fallback so it can never be silently outrun by another
// window's earlier precise reset (review round 2).
//
// windowFact (the retirable-park flag threaded through record429WithSource):
// true only when the bound is entirely precise resets. A bound containing
// any fallback contribution gets false — protected like a 401/403 park,
// because the same response's sub-cap windows (and any fresh-but-pre-429
// snapshot) are not health evidence: the fallback exists precisely because
// the metered windows could not pin a reset. storeReconcilesParkLocked must
// not be able to retire either park leg until the bound elapses or an
// operator clears it (review round 3). A fully-precise bound keeps true: its
// own response merges as at-cap (blocking) into the store, and a later fresh
// sub-cap snapshot from a sibling pool's real traffic means the seat
// demonstrably serves again — early retirement is then correct.
//
// The caller passes its clock so tests pin now; all reads are of resp's
// headers — the shared quota store is deliberately NOT consulted, so a
// headerless policy 429 can never be parked off a prior capped snapshot
// (AC3; the z.ai branch applies the same ordering rationale).
func codexExhaustion429(resp *http.Response, now time.Time) (reset time.Time, windowFact bool, genuine bool) {
	snap := quota.ExtractCodex(resp.Header, now)

	windows := [...]struct {
		util  *float64
		reset *time.Time
	}{
		{snap.Unified5hUtilization, snap.Unified5hReset},
		{snap.Unified7dUtilization, snap.Unified7dReset},
	}
	var bound time.Time
	have := false
	anyFallback := false
	for _, w := range windows {
		if w.util == nil || *w.util < exhaustionUtilizationThreshold {
			// Not at the cap: the window contributes neither genuineness
			// nor a bound. Its (sub-cap) reading is metered state, not a
			// depletion verdict.
			continue
		}
		candidate := now.Add(defaultExhaustionWindow)
		if w.reset != nil && w.reset.After(now) {
			candidate = *w.reset
		} else {
			anyFallback = true
		}
		if !have || candidate.After(bound) {
			bound = candidate
		}
		have = true
	}
	if !have {
		// No capped window: not genuine depletion. A reached-type marker on
		// top is the caller's throttle signal, never a park bound (#314).
		return time.Time{}, false, false
	}
	return bound, !anyFallback, true
}

// codexThrottleRetryAfter returns the Retry-After (seconds) the reached-type
// sub-cap throttle should advertise: the upstream value clamped into
// [rateLimitBackoffMinSeconds, rateLimitBackoffMaxSeconds], defaulting to
// the band top when the header is absent, malformed, or out of range — the
// same clamp transientRateLimit429 applies to Anthropic per-minute throttles
// (issue #314).
func codexThrottleRetryAfter(resp *http.Response) int {
	if raw := resp.Header.Get("Retry-After"); raw != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
			return clampRetryAfter(n)
		}
	}
	return rateLimitBackoffMaxSeconds
}
