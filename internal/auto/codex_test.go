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

// TestModifyResponse_codexReachedTypeSubCapThrottlesSameMember: a
// usage_limit_reached 429 whose windows are sub-cap does NOT park (the #304
// reached-type-only 5h fallback parked the last enabled seat of a
// one-member pool until a manual /_gateway/clear — issue #314): the member
// stays in rotation and the client gets the transient same-member 503 with
// the short back-off band. Also pins the Retry-After clamp (upstream value
// clamped into the band, band top when absent/malformed) and the sub-cap
// shape WITHOUT the marker staying on the policy path.
func TestModifyResponse_codexReachedTypeSubCapThrottlesSameMember(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := codexController(t, clock, &logBuf, nil, "a", "b")

	resp := resp429Codex(c.resolve(t, "a"), clock,
		codexWin{percent: "9", minutes: "300", resetIn: 2 * time.Hour},
		codexWin{percent: "60", minutes: "10080", resetIn: 79 * time.Hour},
		quota.CodexReachedTypeUsageLimit)

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (same-member throttle)", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if got := string(body); got != `{"error":"backend throttled; same member"}` {
		t.Errorf("503 body=%q, want the throttle shape", got)
	}
	if got := resp.Header.Get("x-codex-primary-used-percent"); got != "" {
		t.Errorf("x-codex-* header not stripped from synthetic 503: %q", got)
	}
	secs, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || secs < rateLimitBackoffMinSeconds || secs > rateLimitBackoffMaxSeconds {
		t.Errorf("Retry-After=%q (parsed %d, err %v), want within [%d,%d]", resp.Header.Get("Retry-After"), secs, err, rateLimitBackoffMinSeconds, rateLimitBackoffMaxSeconds)
	}
	if got := resp.Header.Get("Retry-After"); got != strconv.Itoa(rateLimitBackoffMaxSeconds) {
		t.Errorf("Retry-After=%q with no upstream header, want the band top %d", got, rateLimitBackoffMaxSeconds)
	}
	reset, exhausted, _, cpOK, _ := codexParkState(t, c, "a")
	if exhausted || cpOK || !reset.IsZero() {
		t.Errorf("reached-type sub-cap 429 parked the member: exhausted=%v credentialPark=%v reset=%v", exhausted, cpOK, reset)
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a (no failover on the throttle)", got)
	}
	if log := logBuf.String(); !strings.Contains(log, "throttling same member, not parking") {
		t.Errorf("throttle branch not logged; got %q", log)
	}

	// Retry-After clamp: an upstream value is honoured only inside the band.
	for _, tc := range []struct {
		name     string
		upstream string
		want     int
	}{
		{"in band honoured", "2", 2},
		{"above band clamped", "120", rateLimitBackoffMaxSeconds},
		{"below band clamped", "0", rateLimitBackoffMinSeconds},
		{"malformed defaults to band top", "soon-ish", rateLimitBackoffMaxSeconds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			c := codexController(t, clock, io.Discard, nil, "a", "b")
			resp := resp429Codex(c.resolve(t, "a"), clock,
				codexWin{percent: "9", resetIn: 2 * time.Hour},
				codexWin{percent: "60", resetIn: 79 * time.Hour},
				quota.CodexReachedTypeUsageLimit)
			resp.Header.Set("Retry-After", tc.upstream)
			if err := c.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse: %v", err)
			}
			if got := resp.Header.Get("Retry-After"); got != strconv.Itoa(tc.want) {
				t.Errorf("Retry-After=%q for upstream %q, want %d", got, tc.upstream, tc.want)
			}
		})
	}

	// Sub-cap windows WITHOUT the reached-type marker: policy shape — the
	// upstream body is forwarded on the 503, still no park, no failover.
	resp2 := resp429Codex(c.resolve(t, "a"), clock,
		codexWin{percent: "9", resetIn: 2 * time.Hour},
		codexWin{percent: "60", resetIn: 79 * time.Hour},
		"")
	if err := c.ModifyResponse(resp2); err != nil {
		t.Fatalf("ModifyResponse (no marker): %v", err)
	}
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (policy shape)", resp2.StatusCode)
	}
	body2, _ := io.ReadAll(resp2.Body)
	if !strings.Contains(string(body2), "usage_limit_reached") {
		t.Errorf("503 body dropped the upstream policy message: %q", body2)
	}
	reset2, exhausted2, _, cpOK2, _ := codexParkState(t, c, "a")
	if exhausted2 || cpOK2 || !reset2.IsZero() {
		t.Errorf("sub-cap no-marker 429 parked the member: exhausted=%v credentialPark=%v", exhausted2, cpOK2)
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a (policy path never fails over)", got)
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

// A headerless Codex 429 plus a fresh capped snapshot now parks the seat:
// the failed original response supplies the missing exhaustion evidence.
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
	if !strings.Contains(string(body), "backend switching") {
		t.Errorf("503 body=%q, want switch response", body)
	}
	resetA, exhausted, _, cpOK, _ := codexParkState(t, c, "a")
	if !exhausted || !cpOK || !resetA.Equal(reset) {
		t.Errorf("response handling park: exhausted=%v credentialPark=%v reset=%v, want capped-window reset", exhausted, cpOK, resetA)
	}
	if got := c.Current(); got != "b" {
		t.Errorf("Current()=%q, want b (failed full window rotates)", got)
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
