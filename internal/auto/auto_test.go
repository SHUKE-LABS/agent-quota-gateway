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
	return NewController(testRegistry(t, nicks...), "auto", start, clock.now, logOut)
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
		c := NewController(testRegistry(t, "a", "b", "c"), "auto", -1, clock.now, io.Discard)
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
	pools := NewPools(reg, clock.now, io.Discard)

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
