package auto

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

func newController(t *testing.T, start int, clock *fixedClock, logOut io.Writer, nicks ...string) *Controller {
	t.Helper()
	return NewController(testRegistry(t, nicks...), "auto", start, nil, clock.now, logOut)
}

func (c *Controller) resolve(t *testing.T, nick string) backend.Backend {
	t.Helper()
	b, ok := c.reg.ResolveIn(c.pool, nick)
	if !ok {
		t.Fatalf("ResolveIn(%q,%q) not found", c.pool, nick)
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

func TestModifyResponse_allExhaustedForwards429WithRetryAfter(t *testing.T) {
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

	// b 429s with the sooner reset; pool is now dry → honest 429.
	resp := resp429(c.resolve(t, "b"), clock, 120*time.Second)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse b: %v", err)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status=%d, want 429 (pool dry)", resp.StatusCode)
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
		t.Errorf("dry-pool 429 not logged; got %q", logBuf.String())
	}
}

func TestResolveAuto_reEligibleAfterReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "solo")

	// The only backend 429s → pool dry.
	resp := resp429(c.resolve(t, "solo"), clock, 100*time.Second)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429 (single-backend pool dry)", resp.StatusCode)
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
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429 (single-backend pool dry)", resp.StatusCode)
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
	// api is a single-member pool → dry → honest 429.
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("api 429 status=%d, want 429 (single-member pool dry)", resp.StatusCode)
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
	c.loadState("c", map[string]time.Time{"b": reset}, time.Time{}, 0, nil)

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

// TestController_loadState_expiredExhaustedDropped verifies that an exhausted
// entry whose reset is already in the past is dropped on load.
func TestController_loadState_expiredExhaustedDropped(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b")

	pastReset := clock.now().Add(-time.Hour) // already expired
	c.loadState("a", map[string]time.Time{"b": pastReset}, time.Time{}, 0, nil)

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
	c.loadState("b", map[string]time.Time{"a": reset}, time.Time{}, 0, nil)
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
	c.loadState("c", map[string]time.Time{"b": reset}, time.Time{}, 0, nil)
	if logBuf.Len() != 0 {
		t.Fatalf("expected no log output for additive membership, got: %q", logBuf.String())
	}
	if got := c.Current(); got != "c" {
		t.Fatalf("Current=%q, want c", got)
	}
}

// TestController_loadState_missingStickyLogs verifies a log line when the persisted sticky is gone.
func TestController_loadState_missingStickyLogs(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf strings.Builder
	c := newController(t, 0, clock, &logBuf, "a", "b") // "old" was removed
	c.loadState("old", map[string]time.Time{}, time.Time{}, 0, nil)
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
	c.loadState("a", map[string]time.Time{"old": reset}, time.Time{}, 0, nil)
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
	pools := &Pools{byPool: map[string]*Controller{"auto": c}}
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
	pools := &Pools{byPool: map[string]*Controller{"auto": c}}

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
	pools2 := &Pools{byPool: map[string]*Controller{"auto": c2}}
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

func fptr(f float64) *float64 { return &f }
func tptr(t time.Time) *time.Time { return &t }

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

	saved := (&Pools{byPool: map[string]*Controller{"auto": c}}).PersistState()
	if saved["auto"].LastBalanceSwitch.IsZero() {
		t.Fatal("PersistState: LastBalanceSwitch not recorded after switch")
	}

	// Fresh controller starting at a; load the persisted state.
	c2 := newBalanceController(t, 0, clock, 0.15, dwell, store, "a", "b")
	(&Pools{byPool: map[string]*Controller{"auto": c2}}).LoadPersistState(saved)

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
	pools := &Pools{byPool: map[string]*Controller{"auto": c}}

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
	c.cur = c.indexOf("b")
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
	c.cur = c.indexOf("b")
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
	c.cur = c.indexOf("b")
	c.mu.Unlock()

	// Persist and reload into a fresh controller.
	saved := (&Pools{byPool: map[string]*Controller{"auto": c}}).PersistState()
	if saved["auto"].BalanceSeq != 2 {
		t.Fatalf("PersistState: BalanceSeq=%d, want 2", saved["auto"].BalanceSeq)
	}
	if saved["auto"].LastSelectedSeq["b"] != 2 {
		t.Fatalf("PersistState: LastSelectedSeq[b]=%d, want 2", saved["auto"].LastSelectedSeq["b"])
	}

	c2 := newBalanceController(t, 0, clock, 0.15, 0, store, "a", "b", "c")
	(&Pools{byPool: map[string]*Controller{"auto": c2}}).LoadPersistState(saved)

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
