package auto

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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

// testRegistry builds a Registry with all nicks in a single "auto" pool
// via the public env path (loadFrom is unexported in package backend).
// Credentials are "cred-<nick>" so a leak test can grep for "cred".
func testRegistry(t *testing.T, nicks ...string) *backend.Registry {
	t.Helper()
	for _, n := range nicks {
		t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_"+strings.ToUpper(n), "cred-"+n)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return reg
}

// resp429 builds an upstream 429 response for backend b, carrying a
// unified-reset header resetIn from the clock's current time (resetIn <= 0
// omits the header).
func resp429(b backend.Backend, clock *fixedClock, resetIn time.Duration) *http.Response {
	ctx := backend.WithBackend(context.Background(), b)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	h := http.Header{}
	h.Set("anthropic-ratelimit-unified-status", "rejected")
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
	resp.Header.Set("anthropic-ratelimit-unified-5h-utilization", "1.0")

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

// TestModifyResponse_missingResetHeaderParks proves a 429 with no usable
// reset still parks the backend (failover proceeds) rather than looping
// back onto the dead backend.
func TestModifyResponse_missingResetHeaderParks(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "solo")

	resp := resp429(c.resolve(t, "solo"), clock, 0) // no reset header
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429", resp.StatusCode)
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
	c.loadState("c", map[string]time.Time{"b": reset})

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

// TestController_loadState_expiredExhaustedDropped verifies that an exhausted
// entry whose reset is already in the past is dropped on load.
func TestController_loadState_expiredExhaustedDropped(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newController(t, 0, clock, io.Discard, "a", "b")

	pastReset := clock.now().Add(-time.Hour) // already expired
	c.loadState("a", map[string]time.Time{"b": pastReset})

	// b's reset is in the past; resolve should reach b without exhaustion.
	c.setCur("b")
	b, _, exhausted := c.ResolveAuto()
	if exhausted || b.Nick != "b" {
		t.Fatalf("expired exhausted entry: got (%q, exhausted=%v), want b healthy", b.Nick, exhausted)
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
