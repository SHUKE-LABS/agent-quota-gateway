package auto

import (
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

func issue316Controller(t *testing.T, baseURL string, clock *fixedClock, store *quota.Store) *Controller {
	t.Helper()
	scrubPoolEnv(t)
	t.Setenv(backend.EnvPrefix+"AUTO_BASE_URL", baseURL)
	t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_A", "cred-a")
	t.Setenv(backend.EnvPrefix+"AUTO_BACKEND_B", "cred-b")
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return NewController(reg, "auto", 0, store, clock.now, io.Discard)
}

func TestStatuslessFullSnapshotAloneEligible(t *testing.T) {
	for _, tc := range []struct {
		name, baseURL string
		long          bool
	}{
		{"z.ai 5h", "https://api.z.ai/api/anthropic", false},
		{"MiniMaxi 5h", "https://api.minimaxi.com/v1", false},
		{"MiniMaxi long", "https://api.minimaxi.com/v1", true},
		{"Codex response meter", "https://chatgpt.com/backend-api/codex", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			store := quota.NewStore()
			c := issue316Controller(t, tc.baseURL, clock, store)
			full, reset := 1.0, clock.now().Add(time.Hour)
			snap := quota.Snapshot{AsOf: clock.now()}
			if tc.long {
				snap.Unified7dUtilization, snap.Unified7dReset = &full, &reset
			} else {
				snap.Unified5hUtilization, snap.Unified5hReset = &full, &reset
			}
			store.Put("a", snap)
			if b, _, exhausted := c.ResolveAuto(); exhausted || b.Nick != "a" {
				t.Fatalf("ResolveAuto = %q, exhausted=%v; want eligible a", b.Nick, exhausted)
			}
			if got := memberStatus(c.poolStatus(store, nil, nil), "a"); got == "exhausted" {
				t.Fatalf("poolStatus(a) = %q; want eligible", got)
			}
			c.record429("a", reset)
			if cleared, _ := c.ClearExhaustedNick("a"); !cleared {
				t.Fatal("ClearExhaustedNick did not clear the independent park")
			}
			c.setCur("a")
			if b, _, exhausted := c.ResolveAuto(); exhausted || b.Nick != "a" {
				t.Fatalf("after clear, ResolveAuto = %q, exhausted=%v; full snapshot recreated park", b.Nick, exhausted)
			}
			if got := memberStatus(c.poolStatus(store, nil, nil), "a"); got == "exhausted" {
				t.Fatalf("after clear, poolStatus(a) = %q; want eligible", got)
			}
		})
	}
}

func TestStatuslessFullWindowAndFailedUpstreamParks(t *testing.T) {
	for _, tc := range []struct {
		name, baseURL string
		status        int
		long          bool
		reset         bool
	}{
		{"z.ai 1302 at cap", "https://api.z.ai/api/anthropic", 429, false, true},
		{"MiniMaxi weekly 503", "https://api.minimaxi.com/v1", 503, true, true},
		{"Codex 500 fallback", "https://chatgpt.com/backend-api/codex", 500, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			store := quota.NewStore()
			c := issue316Controller(t, tc.baseURL, clock, store)
			full, asOf := 1.0, clock.now().Add(-time.Minute)
			want := asOf.Add(defaultExhaustionWindow)
			snap := quota.Snapshot{AsOf: asOf}
			if tc.long {
				snap.Unified7dUtilization = &full
			} else {
				snap.Unified5hUtilization = &full
			}
			if tc.reset {
				want = clock.now().Add(2 * time.Hour)
				if tc.long {
					snap.Unified7dReset = &want
				} else {
					snap.Unified5hReset = &want
				}
			}
			store.Put("a", snap)
			resp := statuslessFailure(t, c, "a", tc.status)
			if resp.StatusCode != http.StatusServiceUnavailable || c.Current() != "b" {
				t.Fatalf("response=%d active=%q; want 503 and failover to b", resp.StatusCode, c.Current())
			}
			if got, ok := c.exhaustedUntil("a"); !ok || !got.Equal(want) {
				t.Fatalf("park = %v,%v; want %v,true", got, ok, want)
			}
		})
	}
}

func TestStatuslessFailureNeedsFreshFullEligibleWindow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		util   float64
		age    time.Duration
		long   bool
		status int
	}{
		{"success at cap", 1, 0, false, 200},
		{"sub-cap failure", 0.99, 0, false, 500},
		{"stale cap failure", 1, -6 * time.Minute, false, 500},
		{"z.ai monthly tool cap", 1, 0, true, 500},
		{"z.ai 1302 below cap", 0.99, 0, false, 429},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			store := quota.NewStore()
			c := issue316Controller(t, "https://api.z.ai/api/anthropic", clock, store)
			reset := clock.now().Add(time.Hour)
			snap := quota.Snapshot{AsOf: clock.now().Add(tc.age)}
			if tc.long {
				snap.Unified7dUtilization, snap.Unified7dReset = &tc.util, &reset
			} else {
				snap.Unified5hUtilization, snap.Unified5hReset = &tc.util, &reset
			}
			store.Put("a", snap)
			statuslessFailure(t, c, "a", tc.status)
			if until, ok := c.exhaustedUntil("a"); ok {
				t.Fatalf("parked until %v without fresh eligible full window and failed response", until)
			}
			if got := c.Current(); got != "a" {
				t.Fatalf("active=%q; want a", got)
			}
		})
	}
}

func TestCodexFullWindowOnSameFailedResponseParks(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := issue316Controller(t, "https://chatgpt.com/backend-api/codex", clock, nil)
	b := c.resolve(t, "a")
	req := httptest.NewRequest(http.MethodPost, "/responses", nil).WithContext(backend.WithBackend(context.Background(), b))
	reset := clock.now().Add(2 * time.Hour)
	resp := &http.Response{StatusCode: 500, Header: http.Header{}, Request: req, Body: io.NopCloser(strings.NewReader("failed"))}
	resp.Header.Set(quota.HeaderCodexPrimaryUsedPercent, "100")
	resp.Header.Set(quota.HeaderCodexPrimaryResetAt, strconv.FormatInt(reset.Unix(), 10))
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatal(err)
	}
	if got, ok := c.exhaustedUntil("a"); !ok || !got.Equal(reset) || c.Current() != "b" {
		t.Fatalf("same-response meter: park=%v,%v active=%q; want %v,true,b", got, ok, c.Current(), reset)
	}
}

func TestAnthropic529AtFullStatuslessSnapshotStaysTransient(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	c := newController(t, 0, clock, io.Discard, "a", "b")
	c.store = store
	full, reset := 1.0, clock.now().Add(time.Hour)
	store.Put("a", quota.Snapshot{Unified5hUtilization: &full, Unified5hReset: &reset, AsOf: clock.now()})
	if err := c.ModifyResponse(resp529(c.resolve(t, "a"))); err != nil {
		t.Fatal(err)
	}
	if got := c.Current(); got != "a" {
		t.Fatalf("Current()=%q, want a (native 529 remains same-member)", got)
	}
	if until, ok := c.exhaustedUntil("a"); ok {
		t.Fatalf("native 529 parked member until %v", until)
	}
}
