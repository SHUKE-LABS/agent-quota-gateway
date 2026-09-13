package auto

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// codexController builds a single-pool controller whose pool default upstream
// is the ChatGPT-Codex backend, so IsCodexBackend recognises its members (a
// pure host match — no network). Mirrors zaiController; store may be nil.
func codexController(t *testing.T, clock *fixedClock, logOut io.Writer, store *quota.Store, nicks ...string) *Controller {
	t.Helper()
	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"AUTO_BASE_URL", "https://chatgpt.com/backend-api/codex")
	for _, n := range nicks {
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_"+strings.ToUpper(n), "cred-"+n)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return NewController(reg, "auto", 0, store, clock.now, logOut)
}

// codexWin describes one x-codex-* window on a synthetic response. The zero
// value omits every header of that window.
type codexWin struct {
	percent       string        // used-percent, e.g. "100"; "" omits
	minutes       string        // window-minutes; "" omits
	resetIn       time.Duration // >0 → reset-at = now+resetIn
	resetAtRaw    string        // overrides resetIn verbatim (may be garbage)
	resetAfterRaw string        // reset-after-seconds; used only when no reset-at wins
}

func resp429Codex(b backend.Backend, clock *fixedClock, primary, secondary codexWin, reachedType string) *http.Response {
	ctx := backend.WithBackend(context.Background(), b)
	req := httptest.NewRequest(http.MethodPost, "/responses", nil).WithContext(ctx)
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	if reachedType != "" {
		h.Set(quota.HeaderCodexRateLimitReachedType, reachedType)
	}
	setWin := func(prefix string, w codexWin) {
		if w.percent != "" {
			h.Set("x-codex-"+prefix+"-used-percent", w.percent)
		}
		if w.minutes != "" {
			h.Set("x-codex-"+prefix+"-window-minutes", w.minutes)
		}
		switch {
		case w.resetAtRaw != "":
			h.Set("x-codex-"+prefix+"-reset-at", w.resetAtRaw)
		case w.resetIn > 0:
			h.Set("x-codex-"+prefix+"-reset-at", strconv.FormatInt(clock.now().Add(w.resetIn).Unix(), 10))
		case w.resetAfterRaw != "":
			h.Set("x-codex-"+prefix+"-reset-after-seconds", w.resetAfterRaw)
		}
	}
	setWin("primary", primary)
	setWin("secondary", secondary)
	body := `{"error":{"type":"usage_limit_reached","message":"You've hit your usage limit and your plan will reset at the start of your next window."}}`
	return &http.Response{
		StatusCode:    http.StatusTooManyRequests,
		Header:        h,
		Request:       req,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// codexParkState reads a nick's live-park/credential-park state under c.mu
// for assertions.
func codexParkState(t *testing.T, c *Controller, nick string) (exhaustedUntil time.Time, exhausted bool, cpReset time.Time, cpOK bool, cpWindowFact bool) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.exhausted[nick]; ok {
		exhaustedUntil, exhausted = r, true
	}
	if e, ok := c.credentialPark[nick]; ok {
		cpReset, cpOK, cpWindowFact = e.reset, true, e.windowFact
	}
	return
}

// TestModifyResponse_codexReachedTypeCappedParksPrecisely: the canonical
// depletion shape — usage_limit_reached with the primary (5h) window at the
// cap and a future reset — parks until that reset, propagates as a
// store-unrepresentable windowFact park (fully-precise bound), and fails
// over with the standard switch 503 (issue #304 AC1).
func TestModifyResponse_codexReachedTypeCappedParksPrecisely(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := codexController(t, clock, &logBuf, nil, "a", "b", "c")

	resp := resp429Codex(c.resolve(t, "a"), clock,
		codexWin{percent: "100", minutes: "300", resetIn: 2 * time.Hour},
		codexWin{percent: "40", minutes: "10080", resetIn: 80 * time.Hour},
		quota.CodexReachedTypeUsageLimit)

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (switch)", resp.StatusCode)
	}
	if got := resp.Header.Get("x-codex-primary-used-percent"); got != "" {
		t.Errorf("x-codex-* header not stripped from synthetic 503: %q", got)
	}
	if got := c.Current(); got != "b" {
		t.Errorf("Current()=%q, want b (advanced off the depleted seat)", got)
	}
	reset, exhausted, cpReset, cpOK, cpWindowFact := codexParkState(t, c, "a")
	if !exhausted {
		t.Fatalf("a not parked after codex exhaustion 429")
	}
	if want := clock.now().Add(2 * time.Hour); !reset.Equal(want) {
		t.Errorf("park reset=%v, want the primary reset %v", reset, want)
	}
	if !cpOK {
		t.Fatalf("no credentialPark entry: codex parks are store-unrepresentable (no status → 5m freshness)")
	}
	if !cpReset.Equal(reset) {
		t.Errorf("credentialPark reset=%v, want %v (propagated)", cpReset, reset)
	}
	if !cpWindowFact {
		t.Errorf("windowFact=false for a fully-precise bound, want true (retirable on later real evidence)")
	}
	if log := logBuf.String(); !strings.Contains(log, "auto[auto]: a -> b (a hit 429)") {
		t.Errorf("switch not logged; got %q", log)
	}
}

// TestModifyResponse_codexSecondaryCappedParksWeekly: only the weekly window
// at the cap — the bound is the secondary reset (issue #304).
func TestModifyResponse_codexSecondaryCappedParksWeekly(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := codexController(t, clock, io.Discard, nil, "a", "b")

	resp := resp429Codex(c.resolve(t, "a"), clock,
		codexWin{percent: "55", resetIn: time.Hour},
		codexWin{percent: "100", resetIn: 80 * time.Hour},
		"")

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	reset, exhausted, _, _, _ := codexParkState(t, c, "a")
	if !exhausted {
		t.Fatalf("a not parked")
	}
	if want := clock.now().Add(80 * time.Hour); !reset.Equal(want) {
		t.Errorf("park reset=%v, want the secondary reset %v", reset, want)
	}
	if got := c.Current(); got != "b" {
		t.Errorf("Current()=%q, want b", got)
	}
}

// TestModifyResponse_codexBothCappedTakesLaterReset: both windows capped with
// usable resets — the seat cannot serve until BOTH clear, so the park runs to
// the LATEST reset (decision recorded on issue #304).
func TestModifyResponse_codexBothCappedTakesLaterReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := codexController(t, clock, io.Discard, nil, "a", "b")

	resp := resp429Codex(c.resolve(t, "a"), clock,
		codexWin{percent: "100", resetIn: 2 * time.Hour},
		codexWin{percent: "100", resetIn: 80 * time.Hour},
		quota.CodexReachedTypeUsageLimit)

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	reset, exhausted, _, cpOK, cpWindowFact := codexParkState(t, c, "a")
	if !exhausted {
		t.Fatalf("a not parked")
	}
	if want := clock.now().Add(80 * time.Hour); !reset.Equal(want) {
		t.Errorf("park reset=%v, want the later (secondary) reset %v", reset, want)
	}
	if !cpOK || !cpWindowFact {
		t.Errorf("credentialPark ok=%v windowFact=%v, want ok+true (fully-precise bound)", cpOK, cpWindowFact)
	}
}

// TestModifyResponse_codexMixedCapsFallbackDominates: a capped window whose
// reset is unusable (elapsed, malformed, or missing) contributes the
// conservative now+5h, and the bound is the latest across contributions — a
// capped window can never be silently outrun by another window's earlier
// precise reset (review round 2). Includes the case where the fallback
// outruns the precise reset.
func TestModifyResponse_codexMixedCapsFallbackDominates(t *testing.T) {
	for _, tc := range []struct {
		name          string
		primary       codexWin
		secondary     codexWin
		reached       string
		wantPreciseIn time.Duration // 0 → want now+defaultExhaustionWindow
	}{
		{
			name:          "elapsed secondary reset",
			primary:       codexWin{percent: "100", resetIn: 2 * time.Hour},
			secondary:     codexWin{percent: "100", resetAtRaw: "1699999000"}, // now - 1000s
			wantPreciseIn: defaultExhaustionWindow,
		},
		{
			name:      "malformed primary reset",
			primary:   codexWin{percent: "100", resetAtRaw: "soon-ish"},
			secondary: codexWin{percent: "100", resetIn: 80 * time.Hour},
			reached:   quota.CodexReachedTypeUsageLimit,
			// The secondary's precise 80h outruns the 5h fallback.
			wantPreciseIn: 80 * time.Hour,
		},
		{
			name:          "missing resets entirely",
			primary:       codexWin{percent: "100"},
			secondary:     codexWin{percent: "100"},
			wantPreciseIn: defaultExhaustionWindow,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			c := codexController(t, clock, io.Discard, nil, "a", "b")

			resp := resp429Codex(c.resolve(t, "a"), clock, tc.primary, tc.secondary, tc.reached)
			if err := c.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse: %v", err)
			}
			reset, exhausted, _, _, cpWindowFact := codexParkState(t, c, "a")
			if !exhausted {
				t.Fatalf("a not parked")
			}
			want := clock.now().Add(tc.wantPreciseIn)
			if !reset.Equal(want) {
				t.Errorf("park reset=%v, want %v", reset, want)
			}
			if cpWindowFact {
				t.Errorf("windowFact=true for a fallback-containing bound, want false (protected)")
			}
		})
	}
}

// TestModifyResponse_codexReachedTypeOnlyFallsBackTo5h: usage_limit_reached
// with no capped window — the percents lag the verdict — parks for the
// conservative default window (issue #304).
func TestModifyResponse_codexReachedTypeOnlyFallsBackTo5h(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := codexController(t, clock, io.Discard, nil, "a", "b")

	resp := resp429Codex(c.resolve(t, "a"), clock,
		codexWin{percent: "50", resetIn: time.Hour},
		codexWin{},
		quota.CodexReachedTypeUsageLimit)

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	reset, exhausted, cpReset, cpOK, cpWindowFact := codexParkState(t, c, "a")
	if !exhausted {
		t.Fatalf("a not parked")
	}
	if want := clock.now().Add(defaultExhaustionWindow); !reset.Equal(want) || !cpReset.Equal(want) {
		t.Errorf("park reset=%v credentialPark=%v, want both %v", reset, cpReset, want)
	}
	if !cpOK || cpWindowFact {
		t.Errorf("credentialPark ok=%v windowFact=%v, want ok+false (fallback bound protected)", cpOK, cpWindowFact)
	}
}

// TestModifyResponse_codexReachedTypeParkSurvivesFreshSubCapSnapshot is the
// round-3 route-level regression: a reached-type 429 whose windows read
// sub-cap must stay parked for the fallback window even though the same
// response's (or any earlier) windows sit FRESH in the quota store — the
// state storeReconcilesParkLocked would otherwise read as healthy. The
// windowFact=false lifecycle: unavailable before the bound, selectable again
// only after it elapses (or an explicit /_gateway/clear).
func TestModifyResponse_codexReachedTypeParkSurvivesFreshSubCapSnapshot(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := codexController(t, clock, io.Discard, store, "a", "b")

	resp := resp429Codex(c.resolve(t, "a"), clock,
		codexWin{percent: "50", minutes: "300", resetIn: 4 * time.Hour},
		codexWin{percent: "20", minutes: "10080", resetIn: 79 * time.Hour},
		quota.CodexReachedTypeUsageLimit)

	// Observer-equivalent: the 429's own windows land in the store (fresh,
	// sub-cap — the shape reconciliation would treat as healthy).
	snap := quota.ExtractCodex(resp.Header, clock.now())
	snap.AsOf = clock.now()
	store.Merge("a", snap)

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	blockedAt := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		_, blocked := c.exhaustedUntilLocked("a")
		return blocked
	}

	clock.t = clock.now().Add(1 * time.Minute) // snapshot still fresh (≤5m)
	if !blockedAt() {
		t.Fatalf("park reconciled away by a fresh sub-cap snapshot at +1m; the reached-type marker must outrank its own lagging percents")
	}
	if b, _, exhausted := c.ResolveAuto(); exhausted || b.Nick != "b" {
		t.Errorf("ResolveAuto at +1m routed to %q exhausted=%v, want healthy b (a must stay parked)", b.Nick, exhausted)
	}

	clock.t = clock.now().Add(4*time.Hour + 58*time.Minute) // +4h59m total
	if !blockedAt() {
		t.Fatalf("park lapsed before the 5h fallback bound at +4h59m")
	}

	clock.t = clock.now().Add(2 * time.Minute) // +5h01m total: bound elapsed
	if blockedAt() {
		t.Errorf("member still blocked after the fallback bound elapsed at +5h01m")
	}
}

// TestModifyResponse_codexNoSignatureStaysPolicy: a codex 429 with NO
// metered signature keeps today's policy-429 behavior — no park, no
// failover, upstream body forwarded on a 503 (issue #304 AC3).
func TestModifyResponse_codexNoSignatureStaysPolicy(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := codexController(t, clock, &logBuf, nil, "a", "b")

	resp := resp429Codex(c.resolve(t, "a"), clock, codexWin{}, codexWin{}, "")
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (policy shape)", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "usage_limit_reached") {
		t.Errorf("503 body dropped the upstream policy message: %q", body)
	}
	reset, exhausted, _, cpOK, _ := codexParkState(t, c, "a")
	if exhausted || cpOK || !reset.IsZero() {
		t.Errorf("policy 429 parked the member: exhausted=%v credentialPark=%v reset=%v", exhausted, cpOK, reset)
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a (no failover on a policy 429)", got)
	}
	if log := logBuf.String(); !strings.Contains(log, "policy 429 (no exhaustion signal) — not parking") {
		t.Errorf("policy branch not logged; got %q", log)
	}
}

// TestModifyResponse_codexHeaderless429WithSeededStoreAddsNothing is the AC3
// boundary regression: the response handling for a headerless codex 429 adds
// no live park, no credential park, and no sticky rotation — even when the
// store already holds a FRESH CAPPED snapshot for the nick. The backend is
// resolved while healthy (before seeding) so the assertion is about what
// ModifyResponse does, not about pre-existing routing behavior. A fresh
// capped snapshot is itself a store-driven block on the routing side
// (ResolveAuto promotes store-exhausted members before forwarding;
// isUnavailableLocked unions the store bound) — that is pre-existing generic
// behavior for any no-status member, identical to z.ai, unchanged by this
// diff and out of this test's scope (review round 2).
func TestModifyResponse_codexHeaderless429WithSeededStoreAddsNothing(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := codexController(t, clock, io.Discard, store, "a", "b")

	b := c.resolve(t, "a") // healthy resolve, before any store state

	// Seed the store the way a prior capped response's observer pass would
	// have: fresh AsOf, at-cap 5h window with a future reset.
	util := 1.0
	reset := clock.now().Add(2 * time.Hour)
	seed := quota.Snapshot{AsOf: clock.now(), Unified5hUtilization: &util, Unified5hReset: &reset}
	store.Merge("a", seed)

	resp := resp429Codex(b, clock, codexWin{}, codexWin{}, "")
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (policy shape)", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "usage_limit_reached") {
		t.Errorf("503 body dropped the upstream policy message: %q", body)
	}
	resetA, exhausted, _, cpOK, _ := codexParkState(t, c, "a")
	if exhausted || cpOK || !resetA.IsZero() {
		t.Errorf("response handling added a park: exhausted=%v credentialPark=%v reset=%v", exhausted, cpOK, resetA)
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a (no rotation from response handling)", got)
	}
}

// TestModifyResponse_codexHeadersOnNonCodexHostIgnored: the same exhaustion
// headers on a non-chatgpt.com member never enter the codex path — the
// generic classifier (which does not read x-codex-*) treats it as a policy
// 429 (issue #304 AC4 host gate).
func TestModifyResponse_codexHeadersOnNonCodexHostIgnored(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b") // api.anthropic.com members

	resp := resp429Codex(c.resolve(t, "a"), clock,
		codexWin{percent: "100", resetIn: 2 * time.Hour},
		codexWin{percent: "100", resetIn: 80 * time.Hour},
		quota.CodexReachedTypeUsageLimit)

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "usage_limit_reached") {
		t.Errorf("body not preserved on the policy path: %q", body)
	}
	reset, exhausted, _, cpOK, _ := codexParkState(t, c, "a")
	if exhausted || cpOK || !reset.IsZero() {
		t.Errorf("non-codex host parked off x-codex-* headers: exhausted=%v credentialPark=%v", exhausted, cpOK)
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a", got)
	}
	if log := logBuf.String(); !strings.Contains(log, "policy 429 (no exhaustion signal)") {
		t.Errorf("expected the generic policy branch; got %q", log)
	}
}

// TestIsCodexBackend pins the host classifier: exact host chatgpt.com
// (case-insensitive), any path; anything else — including lookalike subdomains
// and other vendors — is not codex.
func TestIsCodexBackend(t *testing.T) {
	yes := []string{
		"https://chatgpt.com/backend-api/codex",
		"https://CHATGPT.com/backend-api/codex",
		"https://chatgpt.com",
	}
	no := []string{
		"https://api.anthropic.com",
		"https://api.openai.com/v1",
		"https://chatgpt.com.evil.example/backend-api/codex",
		"https://sub.chatgpt.com/backend-api/codex",
		"://not-a-url",
	}
	for _, raw := range yes {
		if !IsCodexBackend(backend.Backend{BaseURL: raw}) {
			t.Errorf("IsCodexBackend(%q)=false, want true", raw)
		}
	}
	for _, raw := range no {
		if IsCodexBackend(backend.Backend{BaseURL: raw}) {
			t.Errorf("IsCodexBackend(%q)=true, want false", raw)
		}
	}
}

// TestMemberLeads_codexWindowMinutesHonored: balance-lead elapsed-fraction
// math uses the window length the upstream reported, not the fixed 5h/7d
// defaults — Codex window lengths are not contractual (issue #304 review
// round 2). Same utilization/reset, 120-minute reported window → a
// meaningful lead; no minutes header → the 300-minute default collapses it.
func TestMemberLeads_codexWindowMinutesHonored(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := codexController(t, clock, io.Discard, store, "a", "b")

	util := 0.8
	reset := clock.now().Add(60 * time.Minute)
	mins := 120
	snap := quota.Snapshot{
		AsOf:                   clock.now(),
		Unified5hUtilization:   &util,
		Unified5hReset:         &reset,
		Unified5hWindowMinutes: &mins,
	}
	store.Merge("a", snap)

	c.mu.Lock()
	overall, lead5h, _, has5h, _ := c.memberLeadsLocked("a")
	c.mu.Unlock()

	if !has5h {
		t.Fatalf("has5h=false, want lead data")
	}
	// elapsed = 1 - 60/120 = 0.5 → lead = 0.8 - 0.5 = 0.3
	if want := 0.3; lead5h < want-1e-9 || lead5h > want+1e-9 {
		t.Errorf("lead5h=%v, want %v (elapsed fraction from the 120m reported window)", lead5h, want)
	}
	if overall < lead5h-1e-9 {
		t.Errorf("overall=%v below lead5h=%v", overall, lead5h)
	}

	// Without the minutes field the same reading falls back to the 5h
	// default: elapsed = 1 - 60/300 = 0.8 → lead = 0. Merged under a second
	// nick because Merge carries a learned minutes value forward (issue
	// #163 semantics) — removing the header never blanks it.
	snap.Unified5hWindowMinutes = nil
	snap.AsOf = clock.now()
	store.Merge("b", snap)
	c.mu.Lock()
	_, lead5h, _, has5h, _ = c.memberLeadsLocked("b")
	c.mu.Unlock()
	if !has5h {
		t.Fatalf("has5h=false for the no-minutes snapshot")
	}
	if want := 0.0; lead5h < want-1e-9 || lead5h > want+1e-9 {
		t.Errorf("lead5h=%v, want %v (default 300m window)", lead5h, want)
	}
}
