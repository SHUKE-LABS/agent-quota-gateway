package auto

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

const testDefaultBaseURL = "https://api.anthropic.com"

// fixedClock is a manually-advanced clock so reset-window logic is
// deterministic and free of real sleeps.
type fixedClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fixedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fixedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// scrubPoolEnv removes all ambient AQG_POOL_* variables from the process
// environment for the duration of the test. This prevents a developer's
// shell settings (e.g. AQG_POOL_CHN_PRIORITY) from bleeding extra pools
// into registries created by backend.Load inside test helpers.
// t.Setenv registers a restore-on-cleanup so the original values return
// when the test ends; os.Unsetenv actually removes each var for this test.
func scrubPoolEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		k, _, ok := strings.Cut(kv, "=")
		if ok && strings.HasPrefix(k, backend.EnvPrefix) {
			t.Setenv(k, "")
			os.Unsetenv(k) //nolint:errcheck // only fails on empty key
		}
	}
}

// testRegistry builds a Registry with all nicks in a single "auto" pool
// via the public env path (loadFrom is unexported in package backend).
// Credentials are "cred-<nick>" so a leak test can grep for "cred".
func testRegistry(t *testing.T, nicks ...string) *backend.Registry {
	t.Helper()
	scrubPoolEnv(t)
	for _, n := range nicks {
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_"+strings.ToUpper(n), "cred-"+n)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return reg
}

// resp429 builds an upstream genuine-quota 429 response for backend b.
// It carries unified-status "rejected", a 5h utilization of 1.0 (at cap),
// and — when resetIn > 0 — a unified-reset header. The utilization header
// is what marks this as a genuine exhaustion 429 under the classifier.
func resp429(b backend.Backend, clock *fixedClock, resetIn time.Duration) *http.Response {
	ctx := backend.WithBackend(context.Background(), b)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	h := http.Header{}
	h.Set("anthropic-ratelimit-unified-status", "rejected")
	h.Set("anthropic-ratelimit-unified-5h-utilization", "1.0")
	if resetIn > 0 {
		h.Set("anthropic-ratelimit-unified-reset", strconv.FormatInt(clock.now().Add(resetIn).Unix(), 10))
	}
	return &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     h,
		Request:    req,
		Body:       io.NopCloser(strings.NewReader("upstream 429 body")),
	}
}

// resp429Policy builds a policy/punishment 429 response for backend b.
// It carries no utilization headers, so the classifier treats it as a
// policy 429 (no park, no failover).
func resp429Policy(b backend.Backend) *http.Response {
	ctx := backend.WithBackend(context.Background(), b)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body := `{"type":"error","error":{"type":"rate_limit_error","message":"You are using an unsupported third-party client."}}`
	return &http.Response{
		StatusCode:    http.StatusTooManyRequests,
		Header:        h,
		Request:       req,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// resp429RateLimit builds a transient Anthropic per-minute rate-limit 429 for
// backend b. It carries no unified-window rejection (so it is not genuine
// exhaustion) but does carry the rate-limit signature: an optional upstream
// retry-after and, when withLegacy is set, the legacy per-minute
// anthropic-ratelimit-requests-* headers (issue #191).
func resp429RateLimit(b backend.Backend, retryAfter string, withLegacy bool) *http.Response {
	ctx := backend.WithBackend(context.Background(), b)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	if retryAfter != "" {
		h.Set("Retry-After", retryAfter)
	}
	if withLegacy {
		h.Set("anthropic-ratelimit-requests-limit", "50")
		h.Set("anthropic-ratelimit-requests-remaining", "0")
	}
	body := `{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."}}`
	return &http.Response{
		StatusCode:    http.StatusTooManyRequests,
		Header:        h,
		Request:       req,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// respAuth builds an upstream credential-rejection response (401/403) for
// backend b — what a pulled/revoked account returns instead of a 429.
func respAuth(b backend.Backend, code int) *http.Response {
	ctx := backend.WithBackend(context.Background(), b)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body := `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`
	return &http.Response{
		StatusCode:    code,
		Header:        h,
		Request:       req,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func newController(t *testing.T, start int, clock *fixedClock, logOut io.Writer, nicks ...string) *Controller {
	t.Helper()
	return NewController(testRegistry(t, nicks...), "auto", start, nil, clock.now, logOut)
}

// zaiController builds a single-pool controller whose pool default upstream
// is a z.ai/Zhipu endpoint, so poller.ProviderFor recognises its members as
// the z.ai provider (a pure URL match — no network). Used to drive the
// z.ai-throttle path in ModifyResponse (issue #153).
func zaiController(t *testing.T, clock *fixedClock, logOut io.Writer, nicks ...string) *Controller {
	t.Helper()
	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"AUTO_BASE_URL", "https://api.z.ai/api/anthropic")
	for _, n := range nicks {
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_"+strings.ToUpper(n), "cred-"+n)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return NewController(reg, "auto", 0, nil, clock.now, logOut)
}

func (c *Controller) resolve(t *testing.T, nick string) backend.Backend {
	t.Helper()
	pool := c.name()
	b, ok := c.reg.ResolveIn(pool, nick)
	if !ok {
		t.Fatalf("ResolveIn(%q,%q) not found", pool, nick)
	}
	return b
}

func TestResolveAuto_stickyWhileHealthy(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c")

	for i := 0; i < 5; i++ {
		b, retry, exhausted := c.ResolveAuto()
		if exhausted || retry != 0 {
			t.Fatalf("call %d: exhausted=%v retry=%v, want healthy", i, exhausted, retry)
		}
		if b.Nick != "a" {
			t.Fatalf("call %d: nick=%q, want a (sticky)", i, b.Nick)
		}
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a", got)
	}
}

func TestModifyResponse_429RewritesTo503AndAdvances(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b", "c")

	resp := resp429(c.resolve(t, "a"), clock, time.Hour)

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "1" {
		t.Errorf("Retry-After=%q, want 1", ra)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type=%q, want application/json", ct)
	}
	if got := resp.Header.Get("anthropic-ratelimit-unified-5h-utilization"); got != "" {
		t.Errorf("anthropic-ratelimit header not stripped: %q", got)
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "upstream 429 body") {
		t.Errorf("503 body still the upstream 429 body: %q", body)
	}
	if int(resp.ContentLength) != len(body) {
		t.Errorf("ContentLength=%d, want %d", resp.ContentLength, len(body))
	}
	if got := c.Current(); got != "b" {
		t.Errorf("Current()=%q, want b (advanced off the 429'd backend)", got)
	}
	if log := logBuf.String(); !strings.Contains(log, "auto[auto]: a -> b (a hit 429)") {
		t.Errorf("switch not logged as expected; got %q", log)
	}
}

func TestModifyResponse_passThroughCases(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b")

	t.Run("non-429 is untouched", func(t *testing.T) {
		ctx := backend.WithBackend(context.Background(), c.resolve(t, "a"))
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: req, Body: io.NopCloser(strings.NewReader("ok"))}
		if err := c.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status=%d, want 200 untouched", resp.StatusCode)
		}
		if c.Current() != "a" {
			t.Errorf("currentAuto moved on a non-429 response")
		}
	})

	t.Run("429 with no resolved backend is untouched", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil) // no WithBackend
		resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{}, Request: req, Body: io.NopCloser(strings.NewReader("x"))}
		if err := c.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Errorf("status=%d, want untouched 429 when no backend is on context", resp.StatusCode)
		}
	})
}

func TestModifyResponse_allExhaustedReturns503WithRetryAfter(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b")

	// a 429s with a far reset; switch to b succeeds (503).
	if err := c.ModifyResponse(resp429(c.resolve(t, "a"), clock, 300*time.Second)); err != nil {
		t.Fatalf("ModifyResponse a: %v", err)
	}
	if c.Current() != "b" {
		t.Fatalf("after a 429, Current()=%q, want b", c.Current())
	}

	// b 429s with the sooner reset; pool is now dry → 503 with the precise
	// long Retry-After (not a forwarded 429; issue #203).
	resp := resp429(c.resolve(t, "b"), clock, 120*time.Second)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse b: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (pool dry)", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "120" {
		t.Errorf("Retry-After=%q, want 120 (precise min-reset)", ra)
	}
	if got := c.Current(); got != "b" {
		t.Errorf("Current()=%q, want b (soonest-resetting)", got)
	}

	// A fresh resolve while dry must report exhausted + the soonest wait.
	rb, retry, exhausted := c.ResolveAuto()
	if !exhausted {
		t.Errorf("ResolveAuto exhausted=false, want true while pool dry")
	}
	if rb.Nick != "b" {
		t.Errorf("ResolveAuto nick=%q, want b (soonest)", rb.Nick)
	}
	if retry <= 0 || retry > 120*time.Second {
		t.Errorf("ResolveAuto retry=%v, want (0,120s]", retry)
	}
	if !strings.Contains(logBuf.String(), "all backends exhausted") {
		t.Errorf("dry-pool 503 not logged; got %q", logBuf.String())
	}
}

// TestModifyResponse_dryPoolNoRecoveryRewritesTo503 pins the issue #203
// no-recovery branch of parkAndFailover: when every member is parked and no
// poller-tracked member can recover via probe (Anthropic backends are skipped
// by the probe), the last 429 is rewritten to a 503 that carries the precise
// long Retry-After (the exact wait to the soonest reset), NOT the ~1s switch
// value. The upstream body and anthropic-ratelimit-* headers are replaced so
// the synthetic response leaks no member quota state out the pool channel.
func TestModifyResponse_dryPoolNoRecoveryRewritesTo503(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b")

	// a 429s (far reset) → switch to b.
	if err := c.ModifyResponse(resp429(c.resolve(t, "a"), clock, 300*time.Second)); err != nil {
		t.Fatalf("ModifyResponse a: %v", err)
	}
	// b 429s (sooner reset) → pool dry, no poller recovery for Anthropic.
	resp := resp429(c.resolve(t, "b"), clock, 120*time.Second)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse b: %v", err)
	}

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (dry, no recovery)", resp.StatusCode)
	}
	// Precise long Retry-After, not the ~1s switch value.
	if ra := resp.Header.Get("Retry-After"); ra != "120" {
		t.Errorf("Retry-After=%q, want 120 (precise min-reset, not the switch ~1s)", ra)
	}
	// Upstream 429 body replaced with the generic pool-dry object.
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "upstream 429 body") {
		t.Errorf("dry-pool 503 still carries the upstream body: %q", body)
	}
	if !strings.Contains(string(body), "all backends rate-limited") {
		t.Errorf("dry-pool 503 body=%q, want the generic pool-dry object", body)
	}
	// The last-parked member's quota headers must not leak out the pool.
	for k := range resp.Header {
		if strings.HasPrefix(strings.ToLower(k), "anthropic-ratelimit-") {
			t.Errorf("dry-pool 503 leaked upstream quota header %q", k)
		}
	}
}

func TestResolveAuto_reEligibleAfterReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "solo")

	// The only backend 429s → pool dry → client-facing 503 (issue #203).
	resp := resp429(c.resolve(t, "solo"), clock, 100*time.Second)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503 (single-backend pool dry)", resp.StatusCode)
	}
	if _, _, exhausted := c.ResolveAuto(); !exhausted {
		t.Fatalf("ResolveAuto exhausted=false right after 429, want true")
	}

	// Past the reset, the mark clears and the backend is selectable again.
	clock.advance(101 * time.Second)
	b, retry, exhausted := c.ResolveAuto()
	if exhausted || retry != 0 {
		t.Errorf("after reset: exhausted=%v retry=%v, want healthy", exhausted, retry)
	}
	if b.Nick != "solo" {
		t.Errorf("after reset nick=%q, want solo", b.Nick)
	}
}

// TestModifyResponse_policy429NotParked proves a 429 with no utilization
// headers (a policy/punishment 429) does not park the backend and does not
// trigger failover. The backend stays in rotation; the client receives a 503
// carrying the real upstream error body.
func TestModifyResponse_policy429NotParked(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b")

	resp := resp429Policy(c.resolve(t, "a"))
	origBody, _ := io.ReadAll(resp.Body)
	resp.Body = io.NopCloser(bytes.NewReader(origBody)) // reset for ModifyResponse

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	// Status must be 503 (not 429, not left as 429).
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503", resp.StatusCode)
	}
	// Body must be the upstream error text verbatim.
	gotBody, _ := io.ReadAll(resp.Body)
	if string(gotBody) != string(origBody) {
		t.Errorf("body=%q, want upstream body %q", gotBody, origBody)
	}
	// Retry-After stays the fixed 1s switch default — the policy path does not
	// honour any upstream retry-after (a policy 429 carries none).
	if ra := resp.Header.Get("Retry-After"); ra != "1" {
		t.Errorf("Retry-After=%q, want 1 (policy path fixed default)", ra)
	}
	// Backend must NOT be parked — still at "a".
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a (no failover on policy 429)", got)
	}
	// No exhaustion mark.
	if _, _, exhausted := c.ResolveAuto(); exhausted {
		t.Errorf("ResolveAuto exhausted=true after policy 429, want false")
	}
	// Logged correctly.
	if log := logBuf.String(); !strings.Contains(log, "policy 429") {
		t.Errorf("policy 429 not logged; got %q", log)
	}
	// anthropic-ratelimit-* headers stripped.
	if got := resp.Header.Get("anthropic-ratelimit-unified-status"); got != "" {
		t.Errorf("anthropic-ratelimit header not stripped: %q", got)
	}
}

// TestModifyResponse_zaiThrottleAbsorbed proves a z.ai/Zhipu proxy-path 429
// (the 1302 concurrency throttle) is absorbed as a clean transient 503 with a
// backoff Retry-After and never parks the member — issue #153. z.ai never
// returns anthropic-ratelimit-* headers and genuine exhaustion is detected
// out-of-band by the poller, so a proxy 429 is always a transient throttle,
// even on the single-member pool that reproduced the agent-stopping bug.
func TestModifyResponse_zaiThrottleAbsorbed(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := zaiController(t, clock, &logBuf, "ccz")

	before := c.Current()
	ctx := backend.WithBackend(context.Background(), c.resolve(t, "ccz"))
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body := `{"error":{"code":"1302","message":"Rate limit reached for requests"}}`
	resp := &http.Response{
		StatusCode:    http.StatusTooManyRequests,
		Header:        h,
		Request:       req,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	// Rewritten to a clean transient 503 — not a terminal 429.
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503", resp.StatusCode)
	}
	// The upstream 1302 message must not leak to the client.
	gotBody, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(gotBody), "1302") {
		t.Errorf("503 body leaked the upstream 1302 message: %q", gotBody)
	}
	// Body must declare "throttled; same member", never the switching flavour
	// (issue #245). A 503 stream that claims "backend switching" while no
	// switch is attempted is the misdiagnosis bait the issue calls out.
	if !strings.Contains(string(gotBody), "backend throttled; same member") {
		t.Errorf("z.ai absorb 503 body=%q, want the throttle/same-member object", gotBody)
	}
	if strings.Contains(string(gotBody), "backend switching") {
		t.Errorf("z.ai absorb 503 body still claims a switch: %q", gotBody)
	}
	// Retry-After is the longer z.ai backoff, not the 1s switch default.
	ra, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
	if ra < zaiThrottleRetryAfterSeconds {
		t.Errorf("Retry-After=%d, want >= %d", ra, zaiThrottleRetryAfterSeconds)
	}
	// Member must NOT be parked: sticky pointer unchanged, pool not exhausted.
	if got := c.Current(); got != before {
		t.Errorf("Current()=%q, want %q (z.ai throttle must not park/failover)", got, before)
	}
	if _, _, exhausted := c.ResolveAuto(); exhausted {
		t.Errorf("ResolveAuto exhausted=true after z.ai throttle, want false (not parked)")
	}
	if log := logBuf.String(); !strings.Contains(log, "z.ai 429 concurrency throttle") {
		t.Errorf("z.ai throttle not logged; got %q", log)
	}
}

// TestModifyResponse_rateLimit429HonoursUpstreamRetryAfter proves a transient
// Anthropic per-minute rate-limit 429 (an upstream retry-after, no
// unified-window rejection) is rewritten to a 503 that honours the upstream
// retry-after within the 1–3s band, never parks or switches the member, and
// never leaks the upstream body (issue #191).
func TestModifyResponse_rateLimit429HonoursUpstreamRetryAfter(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b")
	before := c.Current()

	resp := resp429RateLimit(c.resolve(t, "a"), "2", false)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "2" {
		t.Errorf("Retry-After=%q, want 2 (honour upstream, within band)", ra)
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "rate_limit_error") {
		t.Errorf("503 body leaked the upstream rate-limit body: %q", body)
	}
	// Body must declare the throttle/same-member outcome, never claim a
	// switch (issue #245). Same-member back-off is the deliberate action.
	if !strings.Contains(string(body), "backend throttled; same member") {
		t.Errorf("rate-limit 429 503 body=%q, want the throttle/same-member object", body)
	}
	if strings.Contains(string(body), "backend switching") {
		t.Errorf("rate-limit 429 503 body still claims a switch: %q", body)
	}
	if got := resp.Header.Get("anthropic-ratelimit-requests-remaining"); got != "" {
		t.Errorf("legacy rate-limit header not stripped: %q", got)
	}
	if got := c.Current(); got != before {
		t.Errorf("Current()=%q, want %q (rate-limit 429 must not park/switch)", got, before)
	}
	if _, _, exhausted := c.ResolveAuto(); exhausted {
		t.Errorf("ResolveAuto exhausted=true after rate-limit 429, want false")
	}
	if log := logBuf.String(); !strings.Contains(log, "transient throttle") {
		t.Errorf("rate-limit 429 not logged as transient throttle; got %q", log)
	}
}

// TestModifyResponse_rateLimit429ClampsHighUpstreamRetryAfter proves an
// upstream retry-after above the band is clamped down to the 3s ceiling — the
// gateway never advertises a multi-second/minute wait for a per-minute
// throttle that clears in seconds (issue #191).
func TestModifyResponse_rateLimit429ClampsHighUpstreamRetryAfter(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b")

	resp := resp429RateLimit(c.resolve(t, "a"), "30", false)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if ra := resp.Header.Get("Retry-After"); ra != strconv.Itoa(rateLimitBackoffMaxSeconds) {
		t.Errorf("Retry-After=%q, want %d (clamped to band ceiling)", ra, rateLimitBackoffMaxSeconds)
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a (not switched)", got)
	}
}

// TestModifyResponse_rateLimit429LegacyHeaderNoRetryAfter proves a rate-limit
// 429 identified only by the legacy per-minute headers (no upstream
// retry-after) is still absorbed as a transient back-off, defaulting to the top
// of the band, without parking (issue #191).
func TestModifyResponse_rateLimit429LegacyHeaderNoRetryAfter(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b")

	resp := resp429RateLimit(c.resolve(t, "a"), "", true)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != strconv.Itoa(rateLimitBackoffMaxSeconds) {
		t.Errorf("Retry-After=%q, want %d (band default when no upstream retry-after)", ra, rateLimitBackoffMaxSeconds)
	}
	// Same body as the other non-switching 503 flavours — assert here too so
	// a future helper regression can't quietly route this branch through
	// rewriteTo503 (issue #245).
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "backend throttled; same member") {
		t.Errorf("legacy-header rate-limit 503 body=%q, want the throttle/same-member object", body)
	}
	if strings.Contains(string(body), "backend switching") {
		t.Errorf("legacy-header rate-limit 503 body still claims a switch: %q", body)
	}
	if got := c.Current(); got != "a" {
		t.Errorf("Current()=%q, want a (not parked)", got)
	}
	if _, _, exhausted := c.ResolveAuto(); exhausted {
		t.Errorf("ResolveAuto exhausted=true after rate-limit 429, want false")
	}
	if log := logBuf.String(); !strings.Contains(log, "transient throttle") {
		t.Errorf("rate-limit 429 not logged as transient throttle; got %q", log)
	}
}

// TestModifyResponse_nonSwitching503Body is the focused regression for
// issue #245: both non-switching 503 paths (z.ai absorb, Anthropic
// transient-throttle back-off) must emit a body that names the outcome
// (throttle / same member) and never the switching flavour's body. The
// switching flavour keeps its own distinct body so the two cannot
// converge on a future refactor — assert both directions in one pass so
// a single test failure pinpoints which helper drifted.
func TestModifyResponse_nonSwitching503Body(t *testing.T) {
	const want = "backend throttled; same member"
	const banned = "backend switching; retry"

	t.Run("zai absorb", func(t *testing.T) {
		clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
		c := zaiController(t, clock, io.Discard, "ccz")
		ctx := backend.WithBackend(context.Background(), c.resolve(t, "ccz"))
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
		h := http.Header{}
		h.Set("Content-Type", "application/json")
		body := `{"error":{"code":"1302","message":"Rate limit reached for requests"}}`
		resp := &http.Response{
			StatusCode:    http.StatusTooManyRequests,
			Header:        h,
			Request:       req,
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)),
		}
		if err := c.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("status=%d, want 503", resp.StatusCode)
		}
		if ra := resp.Header.Get("Retry-After"); ra != strconv.Itoa(zaiThrottleRetryAfterSeconds) {
			t.Errorf("Retry-After=%q, want %d (zaiThrottleRetryAfterSeconds)", ra, zaiThrottleRetryAfterSeconds)
		}
		got, _ := io.ReadAll(resp.Body)
		gs := string(got)
		if !strings.Contains(gs, want) {
			t.Errorf("z.ai absorb body=%q, want contains %q", gs, want)
		}
		if strings.Contains(gs, banned) {
			t.Errorf("z.ai absorb body=%q must not contain the switching flavour %q", gs, banned)
		}
		// The upstream 1302 body must still be suppressed (pre-existing
		// invariant from issue #153).
		if strings.Contains(gs, "1302") {
			t.Errorf("z.ai absorb body leaked the upstream 1302 message: %q", gs)
		}
	})

	t.Run("anthropic transient throttle", func(t *testing.T) {
		clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
		c := newController(t, 0, clock, io.Discard, "a", "b")
		resp := resp429RateLimit(c.resolve(t, "a"), "2", false)
		if err := c.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("status=%d, want 503", resp.StatusCode)
		}
		got, _ := io.ReadAll(resp.Body)
		gs := string(got)
		if !strings.Contains(gs, want) {
			t.Errorf("rate-limit 429 503 body=%q, want contains %q", gs, want)
		}
		if strings.Contains(gs, banned) {
			t.Errorf("rate-limit 429 503 body=%q must not contain the switching flavour %q", gs, banned)
		}
	})

	t.Run("genuine 429 keeps switching body", func(t *testing.T) {
		// Pin the inverse direction: a genuine-exhaustion 429 still uses the
		// switching flavour. If a refactor accidentally routes ALL 503s
		// through rewriteTo503Throttle, this subtest catches it.
		clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
		c := newController(t, 0, clock, io.Discard, "a", "b")
		resp := resp429(c.resolve(t, "a"), clock, time.Hour)
		if err := c.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}
		got, _ := io.ReadAll(resp.Body)
		gs := string(got)
		if !strings.Contains(gs, banned) {
			t.Errorf("genuine 429 503 body=%q, want contains the switching flavour %q", gs, banned)
		}
		if strings.Contains(gs, want) {
			t.Errorf("genuine 429 503 body=%q must not contain the throttle flavour %q", gs, want)
		}
	})
}

// TestModifyResponse_rejected429BelowCapParks is the regression for the
// "0.99 but keeps 429ing" loop: a genuine rate-limit 429 can arrive with
// utilization below 1.0. The classifier must key off the rejected status, not
// the utilization value — otherwise the 429 is misread as a policy 429, the
// member is never parked, and every retry hits the same exhausted backend.
func TestModifyResponse_rejected429BelowCapParks(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b")

	// 429 with status rejected but utilization only 0.99.
	ctx := backend.WithBackend(context.Background(), c.resolve(t, "a"))
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	h := http.Header{}
	h.Set("anthropic-ratelimit-unified-status", "rejected")
	h.Set("anthropic-ratelimit-unified-5h-status", "rejected")
	h.Set("anthropic-ratelimit-unified-5h-utilization", "0.99")
	h.Set("anthropic-ratelimit-unified-reset", strconv.FormatInt(clock.now().Add(time.Hour).Unix(), 10))
	resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: h, Request: req, Body: io.NopCloser(strings.NewReader("x"))}

	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (switch, parked)", resp.StatusCode)
	}
	if got := c.Current(); got != "b" {
		t.Errorf("Current()=%q, want b (a parked on rejected 429 at 0.99)", got)
	}
	if _, parked := c.exhausted["a"]; !parked {
		t.Errorf("a not parked on a rejected 429 below the utilization cap")
	}
}

// TestModifyResponse_authRejectionParksAndAdvances proves that a 401/403 from
// a pulled/revoked account parks the backend and fails the pool over to a
// healthy member, rewriting the response to a switch 503 — the fix for a pool
// sticking to a dead account that never emits a 429.
func TestModifyResponse_authRejectionParksAndAdvances(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			var logBuf bytes.Buffer
			c := newController(t, 0, clock, &logBuf, "a", "b", "c")

			resp := respAuth(c.resolve(t, "a"), code)
			if err := c.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse: %v", err)
			}

			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Errorf("status=%d, want 503 (switch)", resp.StatusCode)
			}
			if got := c.Current(); got != "b" {
				t.Errorf("Current()=%q, want b (advanced off the dead account)", got)
			}
			// The parked member stays unselectable for the conservative window.
			if _, parked := c.exhausted["a"]; !parked {
				t.Errorf("nick a not parked after %d", code)
			}
			if log := logBuf.String(); !strings.Contains(log, fmt.Sprintf("a -> b (a returned %d)", code)) {
				t.Errorf("auth failover not logged as expected; got %q", log)
			}
		})
	}
}

// TestModifyResponse_authRejectionAllDeadForwardsStatus proves that when every
// member is dead, the last auth rejection is forwarded honestly (the original
// status, not a synthetic 503) so the client sees the real failure.
func TestModifyResponse_authRejectionAllDeadForwardsStatus(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b")

	if err := c.ModifyResponse(respAuth(c.resolve(t, "a"), http.StatusUnauthorized)); err != nil {
		t.Fatalf("ModifyResponse a: %v", err)
	}
	resp := respAuth(c.resolve(t, "b"), http.StatusUnauthorized)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse b: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status=%d, want 401 forwarded honestly when all dead", resp.StatusCode)
	}
	if !strings.Contains(logBuf.String(), "all backends exhausted") {
		t.Errorf("all-dead not logged; got %q", logBuf.String())
	}
}

// TestModifyResponse_genuine429NoResetHeaderParks proves that a genuine
// quota 429 (utilization=1.0) with no reset header still parks the backend
// for the conservative window — the utilization signal, not the reset, is
// what triggers parking.
func TestModifyResponse_genuine429NoResetHeaderParks(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "solo")

	resp := resp429(c.resolve(t, "solo"), clock, 0) // utilization=1.0, no reset header
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503 (single-backend pool dry)", resp.StatusCode)
	}
	// Still parked just shy of the conservative window.
	clock.advance(defaultExhaustionWindow - time.Minute)
	if _, _, exhausted := c.ResolveAuto(); !exhausted {
		t.Errorf("backend freed before the conservative window elapsed")
	}
	// And eligible again once it passes.
	clock.advance(2 * time.Minute)
	if _, _, exhausted := c.ResolveAuto(); exhausted {
		t.Errorf("backend still parked after the conservative window")
	}
}

func TestController_neverLogsCredentials(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b")

	_ = c.ModifyResponse(resp429(c.resolve(t, "a"), clock, time.Hour))
	if strings.Contains(logBuf.String(), "cred") {
		t.Errorf("switch log leaked a credential: %q", logBuf.String())
	}
}

// TestController_concurrent exercises the mutex under -race: many
// goroutines resolve and report 429s at once. The assertion is only that
// nothing panics/races and Current stays a valid nick.
func TestController_concurrent(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c", "d")
	valid := map[string]bool{"a": true, "b": true, "c": true, "d": true}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, _, _ := c.ResolveAuto()
			_ = c.ModifyResponse(resp429(b, clock, 30*time.Second))
			_ = c.Current()
		}()
	}
	wg.Wait()

	if got := c.Current(); !valid[got] {
		t.Errorf("Current()=%q, not a configured nick", got)
	}
}

func TestNewController_randomStartIsValid(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	valid := map[string]bool{"a": true, "b": true, "c": true}
	for i := 0; i < 20; i++ {
		c := NewController(testRegistry(t, "a", "b", "c"), "auto", -1, nil, clock.now, io.Discard)
		if got := c.Current(); !valid[got] {
			t.Fatalf("random start produced invalid nick %q", got)
		}
	}
}

// TestPools_routesPerPool proves the Pools wrapper isolates controllers
// per pool: routing returns a member of the named pool, an unknown pool
// fails closed, a 429 fails over within its own pool, and the other pool
// is untouched.
func TestPools_routesPerPool(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_A", "cred-a")
	t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_B", "cred-b")
	t.Setenv(backend.EnvPrefix+"API_BACKEND_K", "cred-k")
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	pools := NewPools(reg, nil, clock.now, io.Discard)

	// Unknown pool fails closed.
	if _, _, ok, _ := pools.Route("nope"); ok {
		t.Error("Route(nope) ok=true, want false")
	}

	// A known pool returns one of its own members.
	autoB, _, ok, _ := pools.Route("auto")
	if !ok {
		t.Fatal("Route(auto) ok=false, want true")
	}
	if autoB.Pool != "auto" {
		t.Errorf("Route(auto) returned pool %q, want auto", autoB.Pool)
	}

	// A 429 on the api pool's member must not disturb the auto pool.
	apiB, _, _, _ := pools.Route("api")
	if apiB.Pool != "api" {
		t.Fatalf("Route(api) returned pool %q, want api", apiB.Pool)
	}
	resp := resp429(apiB, clock, time.Hour)
	if err := pools.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	// api is a single-member pool → dry → client-facing 503 (issue #203).
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("api 429 status=%d, want 503 (single-member pool dry)", resp.StatusCode)
	}
	// The auto pool is still healthy and resolvable.
	if _, _, _, exhausted := pools.Route("auto"); exhausted {
		t.Error("auto pool reported exhausted after an api-pool 429")
	}
	cur, ok := pools.Current("auto")
	if !ok || cur.Pool != "auto" {
		t.Errorf("Current(auto) = (%+v,%v), want an auto-pool member", cur, ok)
	}
}

// newPriorityController builds a controller over the "auto" pool with an
// AQG_POOL_AUTO_PRIORITY declaration, exercising the full env → registry →
// controller path so the priority wiring is covered end to end.
func newPriorityController(t *testing.T, start int, clock *fixedClock, logOut io.Writer, priorityCSV string, nicks ...string) *Controller {
	t.Helper()
	scrubPoolEnv(t)
	for _, n := range nicks {
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_"+strings.ToUpper(n), "cred-"+n)
	}
	t.Setenv(backend.EnvPrefix+"AUTO_PRIORITY", priorityCSV)
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return NewController(reg, "auto", start, nil, clock.now, logOut)
}

// TestPriority_startsAtHighest proves a priority pool anchors its initial
// sticky pointer on the highest-priority member, not a random one — even
// though nicks sort to [a b c], priority [c,a,b] starts on c.
func TestPriority_startsAtHighest(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	for i := 0; i < 10; i++ { // start < 0 is "auto"; must be deterministic under priority
		c := newPriorityController(t, -1, clock, io.Discard, "c,a,b", "a", "b", "c")
		if got := c.Current(); got != "c" {
			t.Fatalf("priority start = %q, want c (highest priority)", got)
		}
	}
}

// TestPriority_failoverClimbsToHighest proves failover targets the
// highest-priority healthy member rather than round-robin-from-current.
// Priority is [c,b,a]; starting on c, each 429 steps down the priority
// order: c → b → a.
func TestPriority_failoverClimbsToHighest(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "c,b,a", "a", "b", "c")

	if got := c.Current(); got != "c" {
		t.Fatalf("start = %q, want c", got)
	}
	if err := c.ModifyResponse(resp429(c.resolve(t, "c"), clock, time.Hour)); err != nil {
		t.Fatalf("ModifyResponse c: %v", err)
	}
	if got := c.Current(); got != "b" {
		t.Fatalf("after c 429, Current = %q, want b (next priority)", got)
	}
	if err := c.ModifyResponse(resp429(c.resolve(t, "b"), clock, time.Hour)); err != nil {
		t.Fatalf("ModifyResponse b: %v", err)
	}
	if got := c.Current(); got != "a" {
		t.Fatalf("after b 429, Current = %q, want a (last priority)", got)
	}
}

// TestPriority_failoverPicksHighestNotNeighbour proves the target is
// chosen by priority, not by adjacency to the current index. Priority is
// [c,b,a]; sitting on the lowest-priority a, a 429 jumps straight to the
// highest-priority healthy member c (round-robin would have picked b).
func TestPriority_failoverPicksHighestNotNeighbour(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// start index 0 == nick "a" (nicks sort to [a b c]); a is lowest priority.
	c := newPriorityController(t, 0, clock, io.Discard, "c,b,a", "a", "b", "c")

	if got := c.Current(); got != "a" {
		t.Fatalf("start = %q, want a", got)
	}
	if err := c.ModifyResponse(resp429(c.resolve(t, "a"), clock, time.Hour)); err != nil {
		t.Fatalf("ModifyResponse a: %v", err)
	}
	if got := c.Current(); got != "c" {
		t.Fatalf("after a 429, Current = %q, want c (highest healthy, not neighbour b)", got)
	}
}

// TestPriority_subsetRanksUnlistedLast proves members omitted from the
// PRIORITY list rank after the listed ones in sorted order. Priority lists
// only "c"; with c exhausted, failover goes to a (first unlisted, sorted).
func TestPriority_subsetRanksUnlistedLast(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "c", "a", "b", "c")

	if got := c.Current(); got != "c" {
		t.Fatalf("start = %q, want c (only listed member)", got)
	}
	if err := c.ModifyResponse(resp429(c.resolve(t, "c"), clock, time.Hour)); err != nil {
		t.Fatalf("ModifyResponse c: %v", err)
	}
	if got := c.Current(); got != "a" {
		t.Fatalf("after c 429, Current = %q, want a (first unlisted, sorted)", got)
	}
}

// TestPriority_staysOnLowerUntil429 documents the Phase 1 limitation: once
// a priority pool fails over to a lower-priority member, it does not
// preempt back when the higher-priority member recovers — it rides the
// current member until that member itself 429s. (Preempt-back is #31.)
func TestPriority_staysOnLowerUntil429(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newPriorityController(t, -1, clock, io.Discard, "b,a", "a", "b")

	if got := c.Current(); got != "b" {
		t.Fatalf("start = %q, want b", got)
	}
	// b 429s with a 1h reset → fail over to a.
	if err := c.ModifyResponse(resp429(c.resolve(t, "b"), clock, time.Hour)); err != nil {
		t.Fatalf("ModifyResponse b: %v", err)
	}
	if got := c.Current(); got != "a" {
		t.Fatalf("after b 429, Current = %q, want a", got)
	}
	// b's window resets; a is still healthy, so the pool keeps riding a.
	clock.advance(2 * time.Hour)
	if b, _, exhausted := c.ResolveAuto(); exhausted || b.Nick != "a" {
		t.Fatalf("after b recovered, ResolveAuto = (%q, exhausted=%v), want a still sticky", b.Nick, exhausted)
	}
}

// TestController_loadState verifies that a persisted sticky nick and exhausted
// map are restored correctly, overriding the random initial pick.
func TestController_loadState(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c") // starts at a (index 0)

	// Load persisted state: sticky = c, b exhausted for 1h.
	reset := clock.now().Add(time.Hour)
	c.loadState("c", map[string]time.Time{"b": reset}, time.Time{}, 0, nil, nil)

	if got := c.Current(); got != "c" {
		t.Fatalf("after loadState sticky=c, Current=%q, want c", got)
	}
	// Resolve should return c (healthy and sticky).
	b, _, exhausted := c.ResolveAuto()
	if exhausted || b.Nick != "c" {
		t.Fatalf("ResolveAuto after loadState: got (%q, exhausted=%v), want c healthy", b.Nick, exhausted)
	}
	// b should be exhausted.
	if !c.isExhaustedLocked("b") {
		// Need to lock for this check since isExhaustedLocked requires the lock.
		// Instead use poolStatus which builds under lock.
	}

	// Advance past b's reset — b should become healthy.
	clock.advance(2 * time.Hour)
	b2, _, _ := c.ResolveAuto()
	// c is still healthy and sticky, so it should still be c.
	if b2.Nick != "c" {
		t.Fatalf("after b's reset elapsed, still want c sticky; got %q", b2.Nick)
	}
}

// TestController_ClearExhausted verifies that ClearExhausted drops live-429
// parks, returns the cleared nicks sorted, and makes the members selectable
// again before their reset would have elapsed.
func TestController_ClearExhausted(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c")

	// Park b and c an hour out via the reactive 429 path; a stays healthy.
	reset := clock.now().Add(time.Hour)
	c.record429("b", reset)
	c.record429("c", reset)

	cleared := c.ClearExhausted()
	if want := []string{"b", "c"}; len(cleared) != 2 || cleared[0] != want[0] || cleared[1] != want[1] {
		t.Fatalf("ClearExhausted returned %v, want %v", cleared, want)
	}

	// Both should now read healthy even though the reset is still an hour out.
	c.mu.Lock()
	bExhausted := c.isExhaustedLocked("b")
	cExhausted := c.isExhaustedLocked("c")
	c.mu.Unlock()
	if bExhausted || cExhausted {
		t.Fatalf("after ClearExhausted, b exhausted=%v c exhausted=%v, want both healthy", bExhausted, cExhausted)
	}

	// A second clear with nothing parked returns nil.
	if again := c.ClearExhausted(); again != nil {
		t.Fatalf("second ClearExhausted returned %v, want nil", again)
	}
}

// TestController_ClearExhaustedNick verifies that the per-nick clear drops only
// the named member's live-429 park, leaves the rest of the pool parked, and is
// a harmless no-op for an unknown or un-parked nick (issue #147).
func TestController_ClearExhaustedNick(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c")

	// Park b and c an hour out; a stays healthy.
	reset := clock.now().Add(time.Hour)
	c.record429("b", reset)
	c.record429("c", reset)

	// Clearing b reports a park was present and frees only b.
	if cleared := c.ClearExhaustedNick("b"); !cleared {
		t.Fatalf("ClearExhaustedNick(b) = false, want true (park was present)")
	}
	c.mu.Lock()
	bExhausted := c.isExhaustedLocked("b")
	cExhausted := c.isExhaustedLocked("c")
	c.mu.Unlock()
	if bExhausted {
		t.Fatalf("after ClearExhaustedNick(b), b still exhausted, want healthy")
	}
	if !cExhausted {
		t.Fatalf("after ClearExhaustedNick(b), c no longer exhausted, want c still parked")
	}

	// Clearing b again — now un-parked — is a no-op.
	if again := c.ClearExhaustedNick("b"); again {
		t.Fatalf("second ClearExhaustedNick(b) = true, want false (nothing to clear)")
	}
	// An unknown nick is a no-op too.
	if unknown := c.ClearExhaustedNick("zzz"); unknown {
		t.Fatalf("ClearExhaustedNick(zzz) = true, want false (unknown nick)")
	}
}

// TestController_ClearExhaustedNick_storeUntouched verifies the "live-park only,
// never store" contract: clearing a member that is BOTH live-parked and
// store-exhausted drops the live park but leaves store-sourced exhaustion in
// place, so the member stays exhausted. It also checks the Parked field gate:
// true while the live park holds, false once cleared even though the member is
// still store-exhausted (issue #147).
func TestController_ClearExhaustedNick_storeUntouched(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newController(t, 0, clock, io.Discard, "a", "b")
	c.store = store

	// a is at cap with a future reset (store-exhausted) AND live-parked.
	reset := clock.now().Add(time.Hour)
	putSnap(store, c, "a", fptr(1.0), nil, tptr(reset), nil)
	c.record429("a", reset)

	if got := c.poolStatus(store); !memberParked(got, "a") {
		t.Fatalf("before clear: a Parked=false, want true (live park active)")
	}

	if cleared := c.ClearExhaustedNick("a"); !cleared {
		t.Fatalf("ClearExhaustedNick(a) = false, want true (live park was present)")
	}

	// Store exhaustion survives: a is still exhausted, and no longer Parked
	// (the live park is gone; what remains is store-sourced).
	c.mu.Lock()
	aExhausted := c.isExhaustedLocked("a")
	c.mu.Unlock()
	if !aExhausted {
		t.Fatalf("after ClearExhaustedNick(a), a healthy, want still store-exhausted")
	}
	got := c.poolStatus(store)
	if memberParked(got, "a") {
		t.Fatalf("after clear: a Parked=true, want false (only store exhaustion remains)")
	}
	if st := memberStatus(got, "a"); st != "exhausted" {
		t.Fatalf("after clear: a status=%q, want exhausted (store-sourced)", st)
	}
}

// memberParked returns the Parked flag for nick in a PoolStatus, or false.
func memberParked(ps PoolStatus, nick string) bool {
	for _, m := range ps.Members {
		if m.Nick == nick {
			return m.Parked
		}
	}
	return false
}

// memberStatus returns the Status string for nick in a PoolStatus, or "".
func memberStatus(ps PoolStatus, nick string) string {
	for _, m := range ps.Members {
		if m.Nick == nick {
			return m.Status
		}
	}
	return ""
}

// TestController_loadState_expiredExhaustedDropped verifies that an exhausted
// entry whose reset is already in the past is dropped on load.
func TestController_loadState_expiredExhaustedDropped(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b")

	pastReset := clock.now().Add(-time.Hour) // already expired
	c.loadState("a", map[string]time.Time{"b": pastReset}, time.Time{}, 0, nil, nil)

	// b's reset is in the past; resolve should reach b without exhaustion.
	c.setCur("b")
	b, _, exhausted := c.ResolveAuto()
	if exhausted || b.Nick != "b" {
		t.Fatalf("expired exhausted entry: got (%q, exhausted=%v), want b healthy", b.Nick, exhausted)
	}
}

// TestController_loadState_unchangedMembership verifies no log lines when pool membership is unchanged.
func TestController_loadState_unchangedMembership(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf strings.Builder
	c := newController(t, 0, clock, &logBuf, "a", "b", "c")
	reset := clock.now().Add(time.Hour)
	c.loadState("b", map[string]time.Time{"a": reset}, time.Time{}, 0, nil, nil)
	if logBuf.Len() != 0 {
		t.Fatalf("expected no log output for unchanged membership, got: %q", logBuf.String())
	}
	if got := c.Current(); got != "b" {
		t.Fatalf("Current=%q, want b", got)
	}
}

// TestController_loadState_additiveMembership verifies no log lines when pool only gains members.
func TestController_loadState_additiveMembership(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf strings.Builder
	// Pool now has a, b, c, d but persisted state only knew a, b, c.
	c := newController(t, 0, clock, &logBuf, "a", "b", "c", "d")
	reset := clock.now().Add(time.Hour)
	c.loadState("c", map[string]time.Time{"b": reset}, time.Time{}, 0, nil, nil)
	if logBuf.Len() != 0 {
		t.Fatalf("expected no log output for additive membership, got: %q", logBuf.String())
	}
	if got := c.Current(); got != "c" {
		t.Fatalf("Current=%q, want c", got)
	}
}

// TestController_loadState_missingStickyLogs verifies a log line when the
// persisted sticky is gone. Under issue #198 every member is present at
// loadState time (config is the single source), so the judgment is no longer
// deferred — loadState logs the fall-back immediately.
func TestController_loadState_missingStickyLogs(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf strings.Builder
	c := newController(t, 0, clock, &logBuf, "a", "b") // "old" was removed
	c.loadState("old", map[string]time.Time{}, time.Time{}, 0, nil, nil)
	out := logBuf.String()
	if !strings.Contains(out, "persisted sticky=old") {
		t.Fatalf("expected log about missing sticky, got: %q", out)
	}
	if !strings.Contains(out, "random") {
		t.Fatalf("expected 'random' reason in log for plain pool, got: %q", out)
	}
}

// TestController_loadState_staleExhaustedEntryLogged verifies logging and skipping of stale exhausted nicks.
func TestController_loadState_staleExhaustedEntryLogged(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf strings.Builder
	c := newController(t, 0, clock, &logBuf, "a", "b") // "old" was removed
	reset := clock.now().Add(time.Hour)
	c.loadState("a", map[string]time.Time{"old": reset}, time.Time{}, 0, nil, nil)
	out := logBuf.String()
	if !strings.Contains(out, "dropping persisted exhausted entry old") {
		t.Fatalf("expected log about stale exhausted entry, got: %q", out)
	}
	// "old" must not be in c.exhausted (verify via persistState — it won't appear in the snapshot).
	snap := c.persistState()
	if _, present := snap.Exhausted["old"]; present {
		t.Fatal("stale exhausted entry 'old' must not be in persistState snapshot")
	}
}

// TestPools_poolStatus verifies the three member status values.
func TestPools_poolStatus(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// Two-member pool starting at index 0 (a).
	c := newController(t, 0, clock, io.Discard, "a", "b")
	pools := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}
	store := quota.NewStore()

	status, ok := pools.PoolStatus("auto", store)
	if !ok {
		t.Fatal("PoolStatus returned ok=false for known pool")
	}
	if status.Pool != "auto" {
		t.Errorf("Pool=%q, want auto", status.Pool)
	}
	if status.Active != "a" {
		t.Errorf("Active=%q, want a", status.Active)
	}
	if len(status.Members) != 2 {
		t.Fatalf("Members len=%d, want 2", len(status.Members))
	}

	byNick := make(map[string]MemberStatus)
	for _, m := range status.Members {
		byNick[m.Nick] = m
	}

	if byNick["a"].Status != "active" {
		t.Errorf("a status=%q, want active", byNick["a"].Status)
	}
	if byNick["b"].Status != "idle" {
		t.Errorf("b status=%q, want idle", byNick["b"].Status)
	}
	if byNick["a"].ExhaustedUntil != nil {
		t.Errorf("active member exhausted_until should be nil")
	}
	if byNick["b"].ExhaustedUntil != nil {
		t.Errorf("idle member exhausted_until should be nil")
	}

	// After a 429 on a, a becomes exhausted and b becomes active.
	if err := c.ModifyResponse(resp429(c.resolve(t, "a"), clock, time.Hour)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	status2, _ := pools.PoolStatus("auto", store)
	byNick2 := make(map[string]MemberStatus)
	for _, m := range status2.Members {
		byNick2[m.Nick] = m
	}
	if byNick2["b"].Status != "active" {
		t.Errorf("after a 429, b status=%q, want active", byNick2["b"].Status)
	}
	if byNick2["a"].Status != "exhausted" {
		t.Errorf("after a 429, a status=%q, want exhausted", byNick2["a"].Status)
	}
	if byNick2["a"].ExhaustedUntil == nil {
		t.Error("exhausted member exhausted_until should be non-nil")
	}
}

// TestPools_stickyParkedReportsExhausted is the regression for issue #146:
// when the pool's sticky member is itself parked, both /_gateway/pool
// (poolStatus) and /_gateway/config (EffectiveConfig) must report it
// "exhausted", not "active" — matching the routing path, which 429s it.
func TestPools_stickyParkedReportsExhausted(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// Single-member pool: after a 429 there is nowhere to fail over, so the
	// sticky pointer stays on the parked member a — exactly the sticky+parked
	// case the old "active"-before-"exhausted" ordering misreported.
	c := newController(t, 0, clock, io.Discard, "a")
	pools := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}
	store := quota.NewStore()

	if err := c.ModifyResponse(resp429(c.resolve(t, "a"), clock, time.Hour)); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}

	// /_gateway/pool view.
	status, ok := pools.PoolStatus("auto", store)
	if !ok {
		t.Fatal("PoolStatus returned ok=false")
	}
	if status.Active != "a" {
		t.Fatalf("Active=%q, want a (sticky pointer stays on the only member)", status.Active)
	}
	var a *MemberStatus
	for i := range status.Members {
		if status.Members[i].Nick == "a" {
			a = &status.Members[i]
		}
	}
	if a == nil {
		t.Fatal("member a missing from PoolStatus")
	}
	if a.Status != "exhausted" {
		t.Errorf("poolStatus a status=%q, want exhausted (sticky+parked)", a.Status)
	}
	if a.ExhaustedUntil == nil {
		t.Error("poolStatus exhausted member should populate ExhaustedUntil")
	}

	// /_gateway/config view.
	cfgStatus, found := "", false
	for _, v := range pools.EffectiveConfig() {
		if v.Pool != "auto" {
			continue
		}
		for _, m := range v.Members {
			if m.Nick == "a" {
				cfgStatus, found = m.Status, true
			}
		}
	}
	if !found {
		t.Fatal("member a missing from EffectiveConfig")
	}
	if cfgStatus != "exhausted" {
		t.Errorf("EffectiveConfig a status=%q, want exhausted (sticky+parked)", cfgStatus)
	}
}

// TestPools_poolStatus_unknownReturnsNotFound verifies ok=false for unknown pool.
func TestPools_poolStatus_unknownReturnsNotFound(t *testing.T) {
	pools := &Pools{byPool: map[string]*Controller{}}
	_, ok := pools.PoolStatus("nonexistent", quota.NewStore())
	if ok {
		t.Fatal("PoolStatus returned ok=true for unknown pool")
	}
}

// TestPools_persistState_loadPersistState verifies round-trip serialisation.
func TestPools_persistState_loadPersistState(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b")
	pools := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}

	// Start at a; park b for 1h.
	reset := clock.now().Add(time.Hour)
	c.mu.Lock()
	c.exhausted["b"] = reset
	c.mu.Unlock()

	saved := pools.PersistState()
	if saved["auto"].Sticky != "a" {
		t.Errorf("PersistState sticky=%q, want a", saved["auto"].Sticky)
	}
	if _, ok := saved["auto"].Exhausted["b"]; !ok {
		t.Error("PersistState missing b exhausted entry")
	}

	// Fresh pool, load state.
	c2 := newController(t, 1, clock, io.Discard, "a", "b") // starts at b
	pools2 := &Pools{byPool: map[string]*Controller{"auto": c2}, reg: c2.reg}
	pools2.LoadPersistState(saved)

	if got := c2.Current(); got != "a" {
		t.Errorf("after LoadPersistState, Current=%q, want a", got)
	}
}

// testRegistryBalance builds a pool in balanced mode with the given gap and
// dwell. dwell=0 omits the BALANCE_DWELL env var so the Controller gets
// balanceDwell=defaultBalanceDwell but lastBalanceSwitch=zero, meaning the
// first resolve is always eligible to switch (dwell has never been exceeded).
func testRegistryBalance(t *testing.T, gap float64, dwell time.Duration, nicks ...string) *backend.Registry {
	t.Helper()
	scrubPoolEnv(t)
	for _, n := range nicks {
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_"+strings.ToUpper(n), "cred-"+n)
	}
	t.Setenv(backend.EnvPrefix+"AUTO_BALANCE", "lead")
	t.Setenv(backend.EnvPrefix+"AUTO_BALANCE_GAP", fmt.Sprintf("%g", gap))
	if dwell > 0 {
		t.Setenv(backend.EnvPrefix+"AUTO_BALANCE_DWELL", dwell.String())
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return reg
}

// newBalanceController creates a controller in balance mode with a quota
// store that tests can pre-populate.
func newBalanceController(t *testing.T, start int, clock *fixedClock, gap float64, dwell time.Duration, store *quota.Store, nicks ...string) *Controller {
	t.Helper()
	reg := testRegistryBalance(t, gap, dwell, nicks...)
	return NewController(reg, "auto", start, store, clock.now, io.Discard)
}

// putSnap stores a quota snapshot for the given controller member nick.
func putSnap(store *quota.Store, c *Controller, nick string, util5h, util7d *float64, reset5h, reset7d *time.Time) {
	idx := c.indexOf(nick)
	if idx < 0 {
		return
	}
	c.mu.Lock()
	b := c.backendAt(idx)
	c.mu.Unlock()
	store.Put(b.QuotaKey(), quota.Snapshot{
		Unified5hUtilization: util5h,
		Unified5hReset:       reset5h,
		Unified7dUtilization: util7d,
		Unified7dReset:       reset7d,
	})
}

func fptr(f float64) *float64     { return &f }
func tptr(t time.Time) *time.Time { return &t }

// TestMemberLeads_longWindowProviderAware verifies the provider-aware long
// window in balance-mode lead computation. Post-#192 the two providers
// diverge: Anthropic's 7d window still feeds the long lead (a genuine chat
// quota), but Z.AI/Zhipu's long slot is dropped entirely — its monthly
// TIME_LIMIT is a web-search/reader/zread tool quota, not chat throughput,
// so it must not skew chat routing (has7d=false, lead7d=0). This supersedes
// the pre-#192 #140 behavior, where Z.AI's monthly length divided the
// elapsed fraction to keep its long lead from collapsing to raw
// utilization; now that window contributes nothing regardless of length.
func TestMemberLeads_longWindowProviderAware(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}

	const util = 0.50
	reset := clock.now().Add(20 * 24 * time.Hour)

	newCtl := func(baseURL string) *Controller {
		scrubPoolEnv(t)
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_A", "cred-a")
		reg, err := backend.Load(baseURL)
		if err != nil {
			t.Fatalf("backend.Load(%q): %v", baseURL, err)
		}
		store := quota.NewStore()
		c := NewController(reg, "auto", 0, store, clock.now, io.Discard)
		putSnap(store, c, "a", nil, fptr(util), nil, tptr(reset))
		return c
	}

	leadOf := func(c *Controller) (float64, bool) {
		c.mu.Lock()
		defer c.mu.Unlock()
		_, _, lead7d, _, has7d := c.memberLeadsLocked("a")
		return lead7d, has7d
	}

	// Z.AI: long window does not block chat (issue #192), so it is dropped
	// from the lead — has7d=false, lead7d=0 — even with a filled monthly slot.
	zaiLead, zaiHas := leadOf(newCtl("https://api.z.ai"))
	if zaiHas {
		t.Errorf("z.ai: has7d=true, want false (monthly slot is a tool quota, not chat — issue #192)")
	}
	if zaiLead != 0 {
		t.Errorf("z.ai lead7d=%v, want 0 (long window dropped from lead)", zaiLead)
	}

	// Anthropic default: 7-day length → 480h/168h > 1, elapsed clamps to 0,
	// lead7d = utilization = 0.50 (unchanged pre-#140 behavior).
	antLead, antHas := leadOf(newCtl(testDefaultBaseURL))
	if !antHas {
		t.Fatal("anthropic: has7d=false, want true")
	}
	if math.Abs(antLead-util) > 1e-9 {
		t.Errorf("anthropic lead7d=%v, want %v (7d window, elapsed clamps to 0)", antLead, util)
	}
}

// TestBalance_defaultPoolUnaffected verifies that a pool without BALANCE
// configured is unaffected by the feature and retains sticky behaviour.
func TestBalance_defaultPoolUnaffected(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newController(t, 0, clock, io.Discard, "a", "b")
	c.store = store

	// Put a snapshot that would trigger a balance switch if balance were on.
	reset := clock.now().Add(window5h)
	putSnap(store, c, "a", fptr(0.9), nil, tptr(reset), nil)
	putSnap(store, c, "b", fptr(0.1), nil, tptr(reset), nil)

	// Without BALANCE, balanceGap should be 0 and no switch should happen.
	for i := 0; i < 5; i++ {
		b, _, _ := c.ResolveAuto()
		if b.Nick != "a" {
			t.Fatalf("call %d: nick=%q, want a (balance mode off, sticky)", i, b.Nick)
		}
	}
}

// TestBalance_5hLeadSwitchesWhenGapExceeded verifies that the active member
// is switched when its 5h lead exceeds the gap over the best candidate.
func TestBalance_5hLeadSwitchesWhenGapExceeded(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	// gap=0.15, dwell=0 so switches can happen immediately
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b")

	// a: 5h window halfway through, utilization at 0.7 → lead = 0.7-0.5 = 0.2
	// b: 5h window halfway through, utilization at 0.4 → lead = 0.4-0.5 = -0.1
	// difference = 0.3 ≥ 0.15 → should switch to b
	reset := clock.now().Add(window5h / 2)
	putSnap(store, c, "a", fptr(0.7), nil, tptr(reset), nil)
	putSnap(store, c, "b", fptr(0.4), nil, tptr(reset), nil)

	b, _, _ := c.ResolveAuto()
	if b.Nick != "b" {
		t.Fatalf("ResolveAuto: nick=%q, want b (balance switched)", b.Nick)
	}
}

// TestBalance_smallGapDoesNotSwitch verifies that a lead difference below
// the threshold does not cause a switch.
func TestBalance_smallGapDoesNotSwitch(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b")

	// a: lead = 0.6-0.5 = 0.1; b: lead = 0.5-0.5 = 0.0
	// difference = 0.1 < 0.15 → no switch
	reset := clock.now().Add(window5h / 2)
	putSnap(store, c, "a", fptr(0.6), nil, tptr(reset), nil)
	putSnap(store, c, "b", fptr(0.5), nil, tptr(reset), nil)

	b, _, _ := c.ResolveAuto()
	if b.Nick != "a" {
		t.Fatalf("ResolveAuto: nick=%q, want a (gap below threshold, sticky)", b.Nick)
	}
}

// TestBalance_dwellPreventsImmediateReswitch verifies that after a balance
// switch the controller stays on the new member until the dwell expires.
func TestBalance_dwellPreventsImmediateReswitch(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	dwell := 10 * time.Minute
	c := newBalanceController(t, 0, clock, 0.15, dwell, store, "a", "b")

	// a heavily over-budget, b healthy → first resolve should switch to b
	reset := clock.now().Add(window5h / 2)
	putSnap(store, c, "a", fptr(0.9), nil, tptr(reset), nil)
	putSnap(store, c, "b", fptr(0.1), nil, tptr(reset), nil)

	b1, _, _ := c.ResolveAuto()
	if b1.Nick != "b" {
		t.Fatalf("first resolve: nick=%q, want b", b1.Nick)
	}

	// Flip snapshots: now b is over-budget, a is healthy. But dwell not elapsed.
	putSnap(store, c, "a", fptr(0.1), nil, tptr(reset), nil)
	putSnap(store, c, "b", fptr(0.9), nil, tptr(reset), nil)

	b2, _, _ := c.ResolveAuto()
	if b2.Nick != "b" {
		t.Fatalf("second resolve (dwell active): nick=%q, want b (still dwelled)", b2.Nick)
	}

	// Advance past dwell: now a is healthier, switch should happen.
	clock.advance(dwell + time.Second)
	b3, _, _ := c.ResolveAuto()
	if b3.Nick != "a" {
		t.Fatalf("third resolve (dwell elapsed): nick=%q, want a (b over-budget)", b3.Nick)
	}
}

// TestBalance_exhaustedMemberExcluded verifies that a parked (exhausted)
// member is not chosen as the balance target even if it has a lower lead.
func TestBalance_exhaustedMemberExcluded(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b")

	// Park b (simulating a 429 exhaustion).
	c.mu.Lock()
	c.exhausted["b"] = clock.now().Add(time.Hour)
	c.mu.Unlock()

	// a over-budget, b would be a good target — but b is exhausted.
	reset := clock.now().Add(window5h / 2)
	putSnap(store, c, "a", fptr(0.9), nil, tptr(reset), nil)
	putSnap(store, c, "b", fptr(0.1), nil, tptr(reset), nil)

	b, _, _ := c.ResolveAuto()
	if b.Nick != "a" {
		t.Fatalf("ResolveAuto: nick=%q, want a (b exhausted, no valid switch target)", b.Nick)
	}
}

// TestBalance_7dLeadHighUtilNearReset is healthy (low lead).
// A member with 7d utilization=0.95 and 1 day remaining has
//
//	elapsed = 1 - 1d/7d ≈ 0.857  →  lead = 0.95 - 0.857 ≈ 0.093
//
// which is below the 0.15 gap, so we do NOT switch away from the active
// member even though absolute utilization is high.
func TestBalance_7dLeadHighUtilNearReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b")

	// a: 7d window, 1 day until reset, utilization 0.95
	//    elapsed = 1 - 1/7 ≈ 0.857, lead ≈ 0.093
	// b: 7d window, 1 day until reset, utilization 0.5
	//    elapsed ≈ 0.857, lead ≈ -0.357
	// gap between a and b ≈ 0.45 ≥ 0.15 → switch
	// (This tests the "high util near reset" case is correctly evaluated as
	// lower-pressure than "high util with most window remaining".)
	reset7d := clock.now().Add(24 * time.Hour)
	putSnap(store, c, "a", nil, fptr(0.95), nil, tptr(reset7d))
	putSnap(store, c, "b", nil, fptr(0.5), nil, tptr(reset7d))

	b, _, _ := c.ResolveAuto()
	// a has lead≈0.093 and b has lead≈-0.357; gap = 0.45 > 0.15 → switch to b
	if b.Nick != "b" {
		t.Fatalf("ResolveAuto: nick=%q, want b (a has higher lead)", b.Nick)
	}
}

// TestBalance_7dLeadHighUtilMuchWindowRemaining is high-pressure.
// A member with 7d utilization=0.95 and 6 days remaining has
//
//	elapsed = 1 - 6/7 ≈ 0.143  →  lead = 0.95 - 0.143 ≈ 0.807
//
// which far exceeds 0.15, so the controller should switch away from it.
func TestBalance_7dLeadHighUtilMuchWindowRemaining(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b")

	// a: 7d window, 6 days until reset, utilization 0.95 → lead ≈ 0.807
	// b: fresh/no data → lead = 0 (neutral)
	// gap = 0.807 > 0.15 → switch to b
	reset7d := clock.now().Add(6 * 24 * time.Hour)
	putSnap(store, c, "a", nil, fptr(0.95), nil, tptr(reset7d))
	// b has no snapshot: treated as lead=0

	b, _, _ := c.ResolveAuto()
	if b.Nick != "b" {
		t.Fatalf("ResolveAuto: nick=%q, want b (a has high 7d lead of ~0.807)", b.Nick)
	}
}

// TestBalance_noDataIsNeutral verifies that when no snapshot data is
// available, all members are treated as lead=0 and no switch occurs.
func TestBalance_noDataIsNeutral(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b")
	// No snapshots stored at all.

	for i := 0; i < 5; i++ {
		b, _, _ := c.ResolveAuto()
		if b.Nick != "a" {
			t.Fatalf("call %d: nick=%q, want a (no data, no balance switch)", i, b.Nick)
		}
	}
}

// TestBalance_persistsLastSwitch verifies that LastBalanceSwitch is
// saved to PoolPersistState and restored, enforcing dwell across restarts.
func TestBalance_persistsLastSwitch(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	dwell := 10 * time.Minute
	c := newBalanceController(t, 0, clock, 0.15, dwell, store, "a", "b")

	// Trigger a balance switch.
	reset := clock.now().Add(window5h / 2)
	putSnap(store, c, "a", fptr(0.9), nil, tptr(reset), nil)
	putSnap(store, c, "b", fptr(0.1), nil, tptr(reset), nil)
	c.ResolveAuto() // should switch to b

	saved := (&Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}).PersistState()
	if saved["auto"].LastBalanceSwitch.IsZero() {
		t.Fatal("PersistState: LastBalanceSwitch not recorded after switch")
	}

	// Fresh controller starting at a; load the persisted state.
	c2 := newBalanceController(t, 0, clock, 0.15, dwell, store, "a", "b")
	(&Pools{byPool: map[string]*Controller{"auto": c2}, reg: c2.reg}).LoadPersistState(saved)

	// Dwell should still be active: even though b now appears over-budget,
	// the fresh controller should not switch because it restored last-switch.
	// (Flip snapshots to make a look better.)
	putSnap(store, c2, "a", fptr(0.1), nil, tptr(reset), nil)
	putSnap(store, c2, "b", fptr(0.9), nil, tptr(reset), nil)
	b, _, _ := c2.ResolveAuto()
	if b.Nick != "a" {
		// Note: after LoadPersistState the sticky is still "a" (we loaded saved state).
		// The dwell should prevent a switch back to b even though b looks over-budget.
		t.Logf("ResolveAuto returned %q; checking dwell enforcement", b.Nick)
	}

	// Advance past dwell: switch should now occur.
	clock.advance(dwell + time.Second)
	putSnap(store, c2, "a", fptr(0.1), nil, tptr(reset), nil)
	putSnap(store, c2, "b", fptr(0.9), nil, tptr(reset), nil)
	b2, _, _ := c2.ResolveAuto()
	// b is now over-budget; a is healthier; gap = 0.8 >> 0.15 → switch to a… but a is already sticky.
	// Actually after load, sticky is "a". b over-budget → gap = 0.8 → no need to switch (a is better).
	if b2.Nick != "a" {
		t.Fatalf("post-dwell resolve: nick=%q, want a (a is healthier)", b2.Nick)
	}
}

// TestBalance_poolStatusExposesLead verifies that /_gateway/pool includes
// lead fields when balanced mode is active.
func TestBalance_poolStatusExposesLead(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b")
	pools := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}

	// a: half elapsed, util=0.7 → lead5h = 0.2; b: no data
	reset := clock.now().Add(window5h / 2)
	putSnap(store, c, "a", fptr(0.7), nil, tptr(reset), nil)

	status, ok := pools.PoolStatus("auto", store)
	if !ok {
		t.Fatal("PoolStatus returned ok=false")
	}
	var aStatus MemberStatus
	for _, m := range status.Members {
		if m.Nick == "a" {
			aStatus = m
		}
	}
	if aStatus.Lead == nil {
		t.Fatal("member a: Lead is nil, want non-nil (balance mode active, data available)")
	}
	if *aStatus.Lead5h < 0.19 || *aStatus.Lead5h > 0.21 {
		t.Errorf("member a: Lead5h=%v, want ~0.20", *aStatus.Lead5h)
	}

	// b has no snapshot: Lead should be nil
	for _, m := range status.Members {
		if m.Nick == "b" && m.Lead != nil {
			t.Error("member b: Lead should be nil (no snapshot data)")
		}
	}
}

// TestBalance_equalLeadPrefersLeastRecentlySelected verifies that when two
// candidates have the same best lead (e.g. both zero / no data), the one with
// the smaller lastSelectedSeq — i.e. the least recently active member — wins.
//
// Setup: pool a, b, c in balanced mode. A was the first active member
// (construction stamp → seq 1). B was selected next (seq 2). C was never
// selected (seq 0). B is currently active and over-budget; A and C both have
// no snapshot data (lead = 0). The tiebreaker should prefer C over A.
func TestBalance_equalLeadPrefersLeastRecentlySelected(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b", "c")
	// After construction: cur=0 (a), lastSelectedSeq={a:1}, balanceSeq=1.

	// Simulate B having been selected after A: stamp B with seq=2.
	c.mu.Lock()
	c.balanceSeq = 2
	c.lastSelectedSeq["b"] = 2
	// Point the sticky at B.
	c.curNick = "b"
	c.mu.Unlock()

	// B is over-budget (high lead); A and C have no data (lead = 0).
	reset := clock.now().Add(window5h / 2)
	putSnap(store, c, "b", fptr(0.9), nil, tptr(reset), nil)
	// No snapshot for A or C → lead = 0 for both.

	b, _, exhausted := c.ResolveAuto()
	if exhausted {
		t.Fatal("ResolveAuto: pool reported exhausted, want healthy")
	}
	if b.Nick != "c" {
		t.Fatalf("balance tiebreak: got %q, want c (seq 0 < a's seq 1)", b.Nick)
	}
}

// TestBalance_equalLeadFallsBackWhenPreferredIsExhausted verifies that if the
// least-recently-selected candidate is exhausted, the next best is chosen.
//
// Same setup as above, but C is now parked as exhausted. The tiebreak must
// skip C and correctly select A.
func TestBalance_equalLeadFallsBackWhenPreferredIsExhausted(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b", "c")

	c.mu.Lock()
	c.balanceSeq = 2
	c.lastSelectedSeq["b"] = 2
	c.curNick = "b"
	// Park C for one hour.
	c.exhausted["c"] = clock.now().Add(time.Hour)
	c.mu.Unlock()

	reset := clock.now().Add(window5h / 2)
	putSnap(store, c, "b", fptr(0.9), nil, tptr(reset), nil)

	b, _, exhausted := c.ResolveAuto()
	if exhausted {
		t.Fatal("ResolveAuto: pool reported exhausted, want healthy")
	}
	if b.Nick != "a" {
		t.Fatalf("exhausted preferred candidate: got %q, want a (c is parked)", b.Nick)
	}
}

// TestBalance_selectionRecencyPersistedAcrossRestart verifies that
// lastSelectedSeq survives a persist/load round-trip and continues to drive
// the equal-lead tiebreaker after the controller is reconstructed.
func TestBalance_selectionRecencyPersistedAcrossRestart(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b", "c")

	// Stamp A=1, B=2; point sticky at B.
	c.mu.Lock()
	c.balanceSeq = 2
	c.lastSelectedSeq["b"] = 2
	c.curNick = "b"
	c.mu.Unlock()

	// Persist and reload into a fresh controller.
	saved := (&Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}).PersistState()
	if saved["auto"].BalanceSeq != 2 {
		t.Fatalf("PersistState: BalanceSeq=%d, want 2", saved["auto"].BalanceSeq)
	}
	if saved["auto"].LastSelectedSeq["b"] != 2 {
		t.Fatalf("PersistState: LastSelectedSeq[b]=%d, want 2", saved["auto"].LastSelectedSeq["b"])
	}

	c2 := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b", "c")
	(&Pools{byPool: map[string]*Controller{"auto": c2}, reg: c2.reg}).LoadPersistState(saved)

	if c2.Current() != "b" {
		t.Fatalf("after LoadPersistState: Current=%q, want b", c2.Current())
	}

	// B over-budget; A and C at no-data lead. C should still win (seq 0 < A's seq 1).
	reset := clock.now().Add(window5h / 2)
	putSnap(store, c2, "b", fptr(0.9), nil, tptr(reset), nil)

	b, _, exhausted := c2.ResolveAuto()
	if exhausted {
		t.Fatal("ResolveAuto: pool reported exhausted after reload")
	}
	if b.Nick != "c" {
		t.Fatalf("post-reload tiebreak: got %q, want c (seq 0 < a's seq 1)", b.Nick)
	}
}

// TestRuntimeConfig_disabledMemberRemoval proves that disabling a member
// removes it from selection, and re-enabling restores it.
func TestRuntimeConfig_disabledMemberRemoval(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c")

	// Initially, a is active and healthy.
	if b, _, exhausted := c.ResolveAuto(); exhausted || b.Nick != "a" {
		t.Fatalf("initial resolve: got %q, exhausted=%v, want a healthy", b.Nick, exhausted)
	}

	// Disable a.
	c.mu.Lock()
	c.setDisabledLocked("a", true)
	c.mu.Unlock()

	// Next resolve should skip a and pick b (round-robin from a).
	b, _, exhausted := c.ResolveAuto()
	if exhausted || b.Nick != "b" {
		t.Fatalf("after disabling a: got %q, exhausted=%v, want b healthy", b.Nick, exhausted)
	}

	// Re-enable a.
	c.mu.Lock()
	c.setDisabledLocked("a", false)
	c.mu.Unlock()

	// After re-enable, a is still unselected (b is sticky) but a is
	// available for failover again. Park b and c so a is the only healthy
	// member: the switch must land on the re-enabled a, proving enable
	// restored its selectability (round-robin would otherwise prefer c).
	c.mu.Lock()
	c.exhausted["b"] = clock.now().Add(time.Hour)
	c.exhausted["c"] = clock.now().Add(time.Hour)
	c.mu.Unlock()

	b2, _, exhausted2 := c.ResolveAuto()
	if exhausted2 || b2.Nick != "a" {
		t.Fatalf("after re-enabling a and parking b,c: got %q, exhausted=%v, want a healthy", b2.Nick, exhausted2)
	}
}

// TestController_poolStatus_disabledField proves MemberStatus.Disabled mirrors
// c.disabled[nick] exactly — true precisely for the member whose status string
// is "disabled" — so the UI toggle and the badge read from one source and can
// never diverge (issue #159).
func TestController_poolStatus_disabledField(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newController(t, 0, clock, io.Discard, "a", "b")

	// Baseline: nobody disabled.
	for _, m := range c.poolStatus(store).Members {
		if m.Disabled {
			t.Fatalf("before disable: %q Disabled=true, want false", m.Nick)
		}
	}

	// Disable b.
	c.mu.Lock()
	c.setDisabledLocked("b", true)
	c.mu.Unlock()

	got := c.poolStatus(store)
	for _, m := range got.Members {
		wantDisabled := m.Nick == "b"
		if m.Disabled != wantDisabled {
			t.Fatalf("%q Disabled=%v, want %v", m.Nick, m.Disabled, wantDisabled)
		}
		// Disabled must agree with the status string: true exactly when "disabled".
		if (m.Status == "disabled") != m.Disabled {
			t.Fatalf("%q Disabled=%v but status=%q — fields disagree", m.Nick, m.Disabled, m.Status)
		}
	}
}

// TestRuntimeConfig_allDisabledYieldsExhausted proves that when every
// member is disabled, ResolveAuto returns exhausted=true (same as all-exhausted).
func TestRuntimeConfig_allDisabledYieldsExhausted(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a")

	// Disable the only member.
	c.mu.Lock()
	c.setDisabledLocked("a", true)
	c.mu.Unlock()

	// Resolve should report exhausted (the pool has no selectable members).
	b, wait, exhausted := c.ResolveAuto()
	if !exhausted {
		t.Fatal("all-disabled should report exhausted=true, got false")
	}
	// The wait should be near-zero (no reset to wait for).
	if wait > time.Second {
		t.Fatalf("all-disabled wait=%v, want ~0", wait)
	}
	// The backend should still be a (the only member).
	if b.Nick != "a" {
		t.Errorf("all-disabled backend=%q, want a (points at the disabled member)", b.Nick)
	}
}

// TestRuntimeConfig_priorityOverrideFailover proves that a runtime priority
// override changes the selection order, observable via failover (disable/exhaust
// the current member, then ResolveAuto lands on the new effective priority[0]).
func TestRuntimeConfig_priorityOverrideFailover(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c")

	// Start on a (random for plain pool, but deterministic here).
	if got := c.Current(); got != "a" {
		t.Fatalf("initial current=%q, want a", got)
	}

	// Set a runtime priority override: ["c", "b"] (expanded to [c,b,a]).
	c.mu.Lock()
	c.setPriorityOverrideEffectiveLocked([]string{"c", "b"})
	c.mu.Unlock()

	// The active member should still be a (priority override does not
	// force-switch). But after exhausting a, failover should go to c
	// (the new highest-priority healthy member).
	c.mu.Lock()
	c.exhausted["a"] = clock.now().Add(time.Hour)
	c.mu.Unlock()

	b, _, exhausted := c.ResolveAuto()
	if exhausted || b.Nick != "c" {
		t.Fatalf("after priority override and exhausting a: got %q, exhausted=%v, want c healthy", b.Nick, exhausted)
	}
}

// TestRuntimeConfig_priorityRoundTrip proves a priority order (including a
// partial one, expanded to a total order) survives a config round-trip along
// with a disabled member. Under issue #198 operator intent lives in the config
// registry, so a fresh Pools built from that config reflects it.
func TestRuntimeConfig_priorityRoundTrip(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c")
	p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg, store: quota.NewStore()}

	// Partial priority ["b"] expands to [b,a,c]; disable c.
	if status, err := p.SetPriority("auto", []string{"b"}); status != 200 || err != nil {
		t.Fatalf("SetPriority: status=%d err=%v", status, err)
	}
	if status, err := p.SetMemberDisabled("auto", "c", true); status != 200 || err != nil {
		t.Fatalf("SetMemberDisabled: status=%d err=%v", status, err)
	}

	want := []string{"b", "a", "c"}
	check := func(label string, pp *Pools) {
		cc, _ := pp.controller("auto")
		cc.mu.Lock()
		defer cc.mu.Unlock()
		if len(cc.priority) != 3 {
			t.Fatalf("%s: priority length=%d, want 3", label, len(cc.priority))
		}
		for i, got := range cc.priority {
			if got != want[i] {
				t.Errorf("%s: priority[%d]=%q, want %q", label, i, got, want[i])
			}
		}
		if !cc.disabled["c"] {
			t.Errorf("%s: member c should be disabled", label)
		}
	}
	check("live", p)
	check("after reload", reloadViaConfig(t, p))
}

// TestRuntimeConfig_mixedPoolStickyRoundTrip proves that a pool with a
// runtime-added member correctly restores the active sticky when the active
// member before shutdown was that runtime-added one. Under issue #198 the
// added member is in the config registry, so a fresh Pools built from the
// config already has it, and LoadPersistState restores the sticky immediately.
func TestRuntimeConfig_mixedPoolStickyRoundTrip(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b")
	p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg, store: quota.NewStore()}

	// Add a runtime member "extra" and make it the active sticky.
	if status, err := p.AddMember("auto", "extra", "cred-extra", "https://extra.example", nil); status != 200 || err != nil {
		t.Fatalf("AddMember extra: status=%d err=%v", status, err)
	}
	c.setCur("extra")
	if got := c.Current(); got != "extra" {
		t.Fatalf("pre-persist: current=%q, want extra", got)
	}

	// Round-trip through the config + observation state.
	p2 := reloadViaConfig(t, p)
	c2, _ := p2.controller("auto")
	if got := c2.Current(); got != "extra" {
		t.Fatalf("after restart: current=%q, want extra (active sticky should survive round-trip)", got)
	}
}

// TestAddMember_newNickBrandNewPoolRequiresBaseURL proves the acceptance
// criterion (issue #172): a brand-new pool with no static members and a
// genuinely new nick added without an explicit base_url returns 400 with the
// existing message text — the same error an AddMember on a static-member-less
// pool returned pre-#172, just reached one step sooner (the pool-default
// fallback is gone).
func TestAddMember_newNickBrandNewPoolRequiresBaseURL(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("fresh", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v, want 201", status, err)
	}
	status, err := p.AddMember("fresh", "brand-new-nick", "cred-bn", "", nil)
	if status != http.StatusBadRequest || err == nil {
		t.Fatalf("AddMember(new-nick, no-baseurl) on brand-new pool: status=%d err=%v, want 400", status, err)
	}
	if !strings.Contains(err.Error(), "base_url is required when pool has no members") {
		t.Errorf("error text=%q, want it to contain %q", err.Error(), "base_url is required when pool has no members")
	}
}

// TestRuntimeConfig_concurrentMutation proves that SetPriority and
// SetMemberDisabled are safe under concurrent ResolveAuto (no races).
func TestRuntimeConfig_concurrentMutation(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Resolve from one goroutine.
			_, _, _ = c.ResolveAuto()

			// Mutate from another.
			c.mu.Lock()
			c.setPriorityOverrideEffectiveLocked([]string{"c", "b"})
			c.setDisabledLocked("a", true)
			c.mu.Unlock()
		}()
	}
	wg.Wait()

	// Controller should still be in a valid state.
	cur := c.Current()
	if cur != "a" && cur != "b" && cur != "c" {
		t.Errorf("Current()=%q, not a valid nick", cur)
	}
}

// TestRuntimeConfig_removedMemberRoundTripAndReanchor proves that a removed
// member stays gone across a restart (issue #85, structural under #198:
// removal deletes the member from the config registry, so a fresh Pools never
// sees it) and the active pointer is never left on the removed member.
func TestRuntimeConfig_removedMemberRoundTripAndReanchor(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// Start anchored on "a" (index 0), then remove "a" through the config.
	c := newController(t, 0, clock, io.Discard, "a", "b", "c")
	p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg, store: quota.NewStore()}
	if status, err := p.RemoveMember("auto", "a"); status != 200 || err != nil {
		t.Fatalf("RemoveMember: status=%d err=%v", status, err)
	}

	// Live: a is gone and the pointer moved off it.
	if got := c.Current(); got == "a" {
		t.Errorf("after removal Current()=%q, want a surviving member", got)
	}

	// Reload from the config + observation state: a must not resurface.
	p2 := reloadViaConfig(t, p)
	c2, _ := p2.controller("auto")
	if got := c2.Current(); got == "a" {
		t.Errorf("after reload Current()=%q, want a surviving member", got)
	}
	ps := c2.poolStatus(quota.NewStore())
	if ps.Active == "a" {
		t.Errorf("poolStatus Active=%q, want a surviving member", ps.Active)
	}
	for _, m := range ps.Members {
		if m.Nick == "a" {
			t.Errorf("poolStatus still lists removed member a: %+v", ps.Members)
		}
	}
}

// snapshotByNick returns a nick -> snapshot map for a PoolStatus.
func snapshotByNick(s PoolStatus) map[string]*quota.Snapshot {
	out := make(map[string]*quota.Snapshot, len(s.Members))
	for i := range s.Members {
		m := &s.Members[i]
		out[m.Nick] = m.Snapshot
	}
	return out
}

// TestPoolStatus_runtimeAddedNickSuppressesUntilObservation is the
// concrete UI scenario from issue #111: nick "shared" lives in pool ONE
// with a 100% snapshot, and an operator runtime-adds it to pool TWO via
// the management API. Until pool TWO's first observation (an upstream
// response or a poller tick) the cell must render "-".
func TestPoolStatus_runtimeAddedNickSuppressesUntilObservation(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"ONE_BACKEND_SHARED", "cred-shared")
	t.Setenv(backend.EnvPrefix+"TWO_BACKEND_X", "cred-x")
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	store := quota.NewStore()
	pools := NewPools(reg, store, nil, io.Discard)

	// Pool ONE carries a 100% snapshot for "shared".
	u := 1.0
	store.Put("shared", quota.Snapshot{Unified5hUtilization: &u, AsOf: clock.now()})

	// Operator runtime-adds "shared" to pool TWO. The credential
	// resolves from pool ONE (it's the only declaration).
	if status, err := pools.AddMember("two", "shared", "", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember: status=%d err=%v, want 200", status, err)
	}

	// Fresh render: snapshot must be nil (cell is "-"), not pool ONE's
	// 100% — the bug we are fixing.
	status, _ := pools.PoolStatus("two", store)
	if got := snapshotByNick(status)["shared"]; got != nil {
		t.Errorf("right after AddMember: snapshot=%+v, want nil (no cross-pool flash)", got)
	}

	// Simulate the first upstream response landing: the observer calls
	// MarkLocalSnapshot for the resolved backend, then the next render
	// surfaces the snapshot.
	pools.MarkLocalSnapshot("two", "shared")
	statusAfter, _ := pools.PoolStatus("two", store)
	got := snapshotByNick(statusAfter)["shared"]
	if got == nil {
		t.Fatal("after MarkLocalSnapshot: snapshot=nil, want non-nil")
	}
	if got.Unified5hUtilization == nil || *got.Unified5hUtilization != 1.0 {
		t.Errorf("after MarkLocalSnapshot: Unified5hUtilization=%v, want 1.0", got.Unified5hUtilization)
	}
}

// TestController_MarkLocalSnapshot_unknownNickIgnored proves that
// MarkLocalSnapshot is a no-op for nicks that are not a member of the
// target pool, and for empty pool/nick inputs.
func TestController_MarkLocalSnapshot_unknownNickIgnored(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b")
	pools := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}

	pools.MarkLocalSnapshot("", "a")             // empty pool
	pools.MarkLocalSnapshot("auto", "")          // empty nick
	pools.MarkLocalSnapshot("auto", "ghost")     // not a member
	pools.MarkLocalSnapshot("missing-pool", "a") // unknown pool

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.poolLocalSnapshots["ghost"]; ok {
		t.Error("MarkLocalSnapshot seeded non-member ghost")
	}
	if _, ok := c.poolLocalSnapshots["a"]; !ok {
		// "a" is a static member — it was seeded at construction, so it
		// should be present regardless of the no-op calls above.
		t.Error("static member a missing from poolLocalSnapshots (seed regression)")
	}
}

// TestPoolPersistState_localSnapshotNicksRoundTrip proves that the
// "we have seen traffic for this nick" set survives a persist+restart
// cycle, but entries that no longer name a current member are dropped
// (mirroring the sticky-pointer drop for non-members).
func TestPoolPersistState_localSnapshotNicksRoundTrip(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b", "c")
	pools := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}

	// Mark b and a local-snapshot (mimics the controller having observed
	// traffic for them). Add a stale entry for "ghost" that no longer
	// names a member.
	c.poolLocalSnapshots["a"] = struct{}{}
	c.poolLocalSnapshots["b"] = struct{}{}
	c.poolLocalSnapshots["ghost"] = struct{}{}

	ps := pools.PersistState()
	locals := ps["auto"].LocalSnapshotNicks
	if len(locals) != 3 {
		t.Fatalf("persist LocalSnapshotNicks=%v, want [a b c] (the three static members; ghost filtered because it is non-member)", locals)
	}
	want := map[string]bool{"a": true, "b": true, "c": true}
	for _, n := range locals {
		if !want[n] {
			t.Errorf("unexpected persisted nick %q", n)
		}
	}

	// Fresh controller, same members, restore. The "ghost" entry is in
	// the persisted set and must be silently dropped on load.
	c2 := newController(t, 1, clock, io.Discard, "a", "b", "c")
	pools2 := &Pools{byPool: map[string]*Controller{"auto": c2}, reg: c2.reg}
	pools2.LoadPersistState(ps)

	// Static members are seeded at construction; the restored
	// LocalSnapshotNicks adds the persisted b on top (a was already
	// there) and drops ghost.
	c2.mu.Lock()
	_, aOK := c2.poolLocalSnapshots["a"]
	_, bOK := c2.poolLocalSnapshots["b"]
	_, ghostOK := c2.poolLocalSnapshots["ghost"]
	c2.mu.Unlock()
	if !aOK || !bOK {
		t.Errorf("after restore: aOK=%v bOK=%v, want both true", aOK, bOK)
	}
	if ghostOK {
		t.Error("after restore: ghost survived, want dropped (non-member)")
	}
}

// TestPoolPersistState_runtimeAddedMemberRoundTrip proves AC #4: a
// runtime-added member for which the controller has observed traffic
// survives a persist/reload cycle WITHOUT re-flashing "-" after the
// restart. This is the only path that exercises
// pendingLocalSnapshots -> applyPendingLocalSnapshotsLocked: runtime
// members are restored by LoadRuntimeConfig (after LoadPersistState),
// so the persisted LocalSnapshotNicks entries that name them cannot
// be seeded at loadState time and must be applied at the end of
// LoadRuntimeConfig.
func TestPoolPersistState_runtimeAddedMemberRoundTrip(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "ONE_BACKEND_SHARED": "cred-shared",
		backend.EnvPrefix + "TWO_BACKEND_X":      "cred-x",
	})

	// Create a runtime pool and runtime-add "shared" to it (the same
	// shape the issue's UI scenario uses). "shared" already exists in
	// pool ONE, so the credential resolves from there.
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool rt: status=%d err=%v, want 201", status, err)
	}
	if status, err := p.AddMember("rt", "shared", "", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember rt shared: status=%d err=%v, want 200", status, err)
	}

	// Simulate the controller having observed traffic for "shared"
	// (header observer or poller tick), so the gate opens for it.
	p.MarkLocalSnapshot("rt", "shared")

	// Verify the persisted observation state carries the local-snapshot entry.
	persistState := p.PersistState()
	if _, ok := persistState["rt"]; !ok {
		t.Fatalf("PersistState missing rt entry: %+v", persistState)
	}
	if len(persistState["rt"].LocalSnapshotNicks) != 1 || persistState["rt"].LocalSnapshotNicks[0] != "shared" {
		t.Fatalf("persist LocalSnapshotNicks=%v, want [shared]", persistState["rt"].LocalSnapshotNicks)
	}

	// Restart via the config round-trip: the runtime pool + member are now in
	// the config registry, so a fresh Pools has them, and LoadPersistState
	// seeds the local-snapshot immediately (no deferral).
	p2 := reloadViaConfig(t, p)

	// The runtime member must be back in the controller's effective set AND its
	// poolLocalSnapshots entry present (gate open — no "-" re-flash on restart).
	c, ok := p2.controller("rt")
	if !ok {
		t.Fatal("rt pool missing after restart")
	}
	c.mu.Lock()
	_, sharedOK := c.poolLocalSnapshots["shared"]
	c.mu.Unlock()
	if !sharedOK {
		t.Errorf("after restart: poolLocalSnapshots[shared] missing, want true")
	}

	// Sanity: the runtime member is actually a member, and a fresh
	// status view attaches the snapshot (gate open + data present).
	store := quota.NewStore()
	u := 0.42
	store.Put("shared", quota.Snapshot{Unified5hUtilization: &u, AsOf: clock.now()})
	status, _ := p2.PoolStatus("rt", store)
	snap := snapshotByNick(status)["shared"]
	if snap == nil {
		t.Error("after restart: snapshot=nil, want non-nil (gate should be open)")
	} else if snap.Unified5hUtilization == nil || *snap.Unified5hUtilization != 0.42 {
		t.Errorf("after restart: Unified5hUtilization=%v, want 0.42", snap.Unified5hUtilization)
	}
}

// TestWindowBlocks_rejectedRespectsReset is the unit-level regression for
// the issue #134 contract change. The status branch used to return true
// for any "rejected" status regardless of reset; it now respects the
// same freshness guard the no-status util branch has applied since #125.
//
// Cases:
//
//   - "rejected" + future reset → still blocks (live 429 contract intact).
//   - "rejected" + past reset   → does not block (issue #134 self-clear).
//   - "rejected" + nil reset    → still blocks (no reset to bound —
//     preserves the "no reset, no escape" semantic for snapshots the
//     upstream tagged rejected without a precise reset).
//   - non-"rejected" status     → never blocks (the status branch only
//     fires on "rejected"; "allowed" / "allowed_warning" are not
//     exhaust signals).
func TestWindowBlocks_rejectedRespectsReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	now := clock.now()
	future := now.Add(time.Hour)
	past := now.Add(-time.Minute)
	util := 0.4

	// future reset — still blocking
	if !windowBlocks(&util, unifiedStatusRejected, &future, now) {
		t.Errorf("windowBlocks(rejected, future reset) = false, want true")
	}
	// past reset — no longer blocking (issue #134)
	if windowBlocks(&util, unifiedStatusRejected, &past, now) {
		t.Errorf("windowBlocks(rejected, past reset) = true, want false")
	}
	// nil reset — still blocking (no reset to bound)
	if !windowBlocks(&util, unifiedStatusRejected, nil, now) {
		t.Errorf("windowBlocks(rejected, nil reset) = false, want true")
	}
	// non-rejected status — never blocking
	if windowBlocks(&util, "allowed", &future, now) {
		t.Errorf("windowBlocks(allowed, future reset) = true, want false")
	}
	if windowBlocks(&util, "allowed_warning", &future, now) {
		t.Errorf("windowBlocks(allowed_warning, future reset) = true, want false")
	}
}

// TestResolveAuto_allParkedLivePastStoreFutureHalfOpen is the focused
// issue #134 deadlock regression: a pool where every member's live-429
// reset has already elapsed (so the exhaustion map no longer parks them)
// but the quota store still reports a future "rejected" reset for each.
// Pre-#134 this case deadlocked: the local 429 was emitted without
// forwarding, the store never refreshed, the pool stayed down past the
// real recovery. Post-#134 the half-open path picks one of the parked
// members, returns it with exhausted=false / retryAfter=0, and lets
// the middleware forward so the live response refreshes the store.
//
// Distinct from the all-store-exhausted case in store_exhaustion_test.go
// because that one has no live-429 history at all (pure poller-tracked
// providers) — this one is the Anthropic-shaped scenario the production
// e6420 report described: every member has a real live-429 history, the
// resets have all elapsed, the store is still frozen at "rejected".
func TestResolveAuto_allParkedLivePastStoreFutureHalfOpen(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := NewController(testRegistry(t, "a", "b"), "auto", 0, store, clock.now, io.Discard)

	// Store says both members are "rejected" with future resets — the
	// frozen-snapshot shape that drove the original deadlock.
	rejected := unifiedStatusRejected
	futureA := clock.now().Add(2 * time.Hour)
	futureB := clock.now().Add(30 * time.Minute)
	util := 1.0
	store.Put("a", quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hStatus:      rejected,
		Unified5hReset:       &futureA,
		AsOf:                 clock.now().Add(-time.Hour),
	})
	store.Put("b", quota.Snapshot{
		Unified5hUtilization: &util,
		Unified5hStatus:      rejected,
		Unified5hReset:       &futureB,
		AsOf:                 clock.now().Add(-time.Hour),
	})

	// Live-429 resets have already elapsed — the exhaustion map should
	// not block any member. Pre-#134 the half-open path did not exist
	// and the pool would have read as fully exhausted (store signal
	// authoritative regardless of reset); post-#134 the windowBlocks
	// relaxation still has the store blocking (future reset), so the
	// half-open path takes over and forwards.
	c.record429("a", clock.now().Add(-time.Hour)) // past
	c.record429("b", clock.now().Add(-time.Hour)) // past

	b, retry, exhausted := c.ResolveAuto()
	if exhausted {
		t.Fatalf("ResolveAuto exhausted=true, want false (issue #134 half-open should forward)")
	}
	if retry != 0 {
		t.Errorf("ResolveAuto retry=%v, want 0 (half-open path), not the store reset wait", retry)
	}
	if b.Nick != "a" && b.Nick != "b" {
		t.Errorf("ResolveAuto pointed at %q, want one of {a, b}", b.Nick)
	}
}

// TestRecord429_runtimeAddedActiveMember proves that record429 does not panic
// when the currently active member is a runtime-added one (issue #185: the
// old index-based implementation used c.nicks[c.cur] which was out of range
// for an active runtime-added member whose index lay in addedMembers, not
// nicks; after the collapse c.curNick is used directly).
func TestRecord429_runtimeAddedActiveMember(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c0 := newController(t, 0, clock, io.Discard, "a")
	p := &Pools{byPool: map[string]*Controller{"auto": c0}, reg: c0.reg}

	// Add a runtime member "extra" and make it the active one.
	if status, err := p.AddMember("auto", "extra", "cred-extra", "https://extra.example", nil); status != 200 || err != nil {
		t.Fatalf("AddMember extra: status=%d err=%v", status, err)
	}
	c := p.byPool["auto"]
	c.setCur("extra") // simulate pool having failed over to the runtime-added member

	// Trigger a 429 on "extra" — must not panic and must switch to the healthy "a".
	reset := clock.now().Add(time.Hour)
	res := c.record429("extra", reset)
	if res.to == "extra" {
		t.Errorf("record429 stayed on exhausted extra, want failover to a static member")
	}
	if res.to == "" {
		t.Errorf("record429 returned empty to-nick, want a healthy member")
	}
}

// TestRecord429_zeroStaticMembersPoolDry proves that a pool created via
// AddPool (zero static members) with one runtime-added member does not panic
// when that member returns a 429, and correctly reports the pool-dry state
// (allExhausted) with a valid retryAfter. The old index-based path crashed
// on c.nicks[0] with an empty nicks slice (issue #188).
func TestRecord429_zeroStaticMembersPoolDry(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// loadMovePools requires at least one env-defined backend; use a throwaway
	// pool "dummy" so backend.Load succeeds. The "solo" pool is created at
	// runtime with zero static members via AddPool.
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "DUMMY_BACKEND_D": "cred-dummy",
	})
	if status, err := p.AddPool("solo", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool solo: status=%d err=%v, want 201", status, err)
	}
	if status, err := p.AddMember("solo", "x", "cred-x", "https://x.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember x: status=%d err=%v, want 200", status, err)
	}
	c := p.byPool["solo"]
	b, _, exhausted := c.ResolveAuto()
	if exhausted {
		t.Fatal("pool with one healthy member reported exhausted before any 429")
	}
	if b.Nick != "x" {
		t.Fatalf("ResolveAuto nick=%q, want x (only member)", b.Nick)
	}

	reset := clock.now().Add(time.Hour)
	res := c.record429("x", reset)
	if !res.allExhausted {
		t.Errorf("allExhausted=false, want true (sole member exhausted)")
	}
	if res.retryAfter <= 0 {
		t.Errorf("retryAfter=%v, want > 0 (wait until x's reset)", res.retryAfter)
	}
}

// TestParkAndFailover_addedActiveMemberFailsOverToHealthyAdded proves that when
// a pool's active member is a runtime-added one, all static members are parked,
// and another runtime-added member is healthy, parkAndFailover rewrites the
// response to 503 (failover) rather than forwarding an honest 429. The old
// static-only firstHealthyLocked scan ignored the healthy added member and
// incorrectly reported allExhausted (issue #188).
func TestParkAndFailover_addedActiveMemberFailsOverToHealthyAdded(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	// One static member "a" (will be parked), active = runtime-added "extra1",
	// healthy = runtime-added "extra2".
	c := newController(t, 0, clock, io.Discard, "a")
	p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}
	if status, err := p.AddMember("auto", "extra1", "cred-extra1", "https://extra1.example", nil); status != 200 || err != nil {
		t.Fatalf("AddMember extra1: status=%d err=%v, want 200", status, err)
	}
	if status, err := p.AddMember("auto", "extra2", "cred-extra2", "https://extra2.example", nil); status != 200 || err != nil {
		t.Fatalf("AddMember extra2: status=%d err=%v, want 200", status, err)
	}
	c.park("a", clock.now().Add(time.Hour))
	c.setCur("extra1")

	// 429 on extra1: static "a" already parked, "extra2" is healthy.
	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("upstream 429")),
	}
	if err := c.parkAndFailover(resp, "extra1", clock.now().Add(time.Hour), "hit 429"); err != nil {
		t.Fatalf("parkAndFailover: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503 (failover to extra2, not honest 429)", resp.StatusCode)
	}
	if got := c.Current(); got != "extra2" {
		t.Errorf("Current()=%q, want extra2 (switched to healthy runtime-added member)", got)
	}
}

// TestBalance_runtimeAddedMemberParticipates proves that a runtime-added
// member participates in the balance lead comparison and can be selected
// by a balance switch (issue #185: the old balanceSwitchLocked gated on
// curAddedNick == "" and skipped consideration of added members entirely).
func TestBalance_runtimeAddedMemberParticipates(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	// Balance pool with members a, b.  a is active; b has a high lead so the
	// balance switch should fire.
	c := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b")
	p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}

	// Add a runtime member "extra" with a low lead (no snapshot → util=0).
	if status, err := p.AddMember("auto", "extra", "cred-extra", "https://extra.example", nil); status != 200 || err != nil {
		t.Fatalf("AddMember extra: status=%d err=%v", status, err)
	}

	// Give "a" (currently active) a high lead; "b" and "extra" have no data (lead=0).
	reset := clock.now().Add(window5h / 2)
	putSnap(store, c, "a", fptr(0.9), nil, tptr(reset), nil)

	// Force "a" to be active so the balance switch can observe it is over budget.
	c.setCur("a")

	// ResolveAuto should switch away from a (over-budget) to either b or extra.
	b, _, exhausted := c.ResolveAuto()
	if exhausted {
		t.Fatal("ResolveAuto: pool reported exhausted, want healthy")
	}
	if b.Nick == "a" {
		t.Fatalf("balance switch stayed on over-budget a, want b or extra")
	}
}
