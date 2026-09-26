package auto

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

func TestIssue337_sortedDefaultRoutingAndWorkerWindow(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerRegistryWithConcurrency(t, 2, "c", "a", "b")
	p := NewPools(reg, nil, clock.now, io.Discard)
	c := p.byPool["auto"]

	if got, _, exhausted := c.ResolveAuto(); exhausted || got.Nick != "a" {
		t.Fatalf("initial ordinary route=%q exhausted=%v, want a/false", got.Nick, exhausted)
	}
	for _, tc := range []struct{ worker, want string }{{"worker-a", "a"}, {"worker-b", "b"}} {
		if got, _, exhausted := c.ResolveWorker(tc.worker); exhausted || got.Nick != tc.want {
			t.Fatalf("initial %s route=%q exhausted=%v, want %s/false", tc.worker, got.Nick, exhausted, tc.want)
		}
	}
	if got, _, exhausted := c.ResolveWorker("worker-c"); exhausted || (got.Nick != "a" && got.Nick != "b") {
		t.Fatalf("new worker in initial window=%q exhausted=%v, want a or b", got.Nick, exhausted)
	}

	c.park("a", clock.now().Add(time.Hour))
	if got, _, exhausted := c.ResolveAuto(); exhausted || got.Nick != "b" {
		t.Fatalf("ordinary failover with a unavailable=%q exhausted=%v, want b/false", got.Nick, exhausted)
	}
	if got, _, exhausted := c.ResolveWorker("worker-a"); exhausted || got.Nick != "c" {
		t.Fatalf("worker-a reassignment=%q exhausted=%v, want c in window {b,c}", got.Nick, exhausted)
	}
	if got, _, exhausted := c.ResolveWorker("worker-b"); exhausted || got.Nick != "b" {
		t.Fatalf("unaffected worker-b moved to %q exhausted=%v, want b", got.Nick, exhausted)
	}

	clock.advance(time.Hour)
	pre := newPreemptor([]*Controller{c}, quota.NewStore(), 0, clock.now, io.Discard)
	if got := c.Current(); got != "b" {
		t.Fatalf("ordinary route switched before background preempt tick: %q, want sticky b", got)
	}
	pre.tick()
	if got := c.Current(); got != "a" {
		t.Fatalf("ordinary route after reset-driven preempt=%q, want a", got)
	}
	if got, _, exhausted := c.ResolveWorker("worker-a"); exhausted || (got.Nick != "a" && got.Nick != "b") {
		t.Fatalf("worker-a after recovery=%q exhausted=%v, want reassignment into {a,b}", got.Nick, exhausted)
	}
	if got, _, exhausted := c.ResolveWorker("worker-b"); exhausted || got.Nick != "b" {
		t.Fatalf("unaffected worker-b after recovery=%q exhausted=%v, want b", got.Nick, exhausted)
	}
}

func TestIssue337_restoresFallbackUntilSortedPreemptBack(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerRegistryWithConcurrency(t, 2, "c", "a", "b")
	p := NewPools(reg, nil, clock.now, io.Discard)
	reset := clock.now().Add(time.Hour)
	p.LoadPersistState(map[string]PoolPersistState{"auto": {
		Sticky:         "b",
		Exhausted:      map[string]time.Time{"a": reset},
		WorkerAffinity: map[string]string{"worker-a": "c", "worker-b": "b"},
		WorkerCursor:   "c",
	}})
	c := p.byPool["auto"]
	if got := c.Current(); got != "b" {
		t.Fatalf("restored sticky=%q, want fallback b", got)
	}
	if got, _, exhausted := c.ResolveWorker("worker-a"); exhausted || got.Nick != "c" {
		t.Fatalf("restored worker-a=%q exhausted=%v, want valid fallback c", got.Nick, exhausted)
	}
	if got, _, exhausted := c.ResolveWorker("worker-b"); exhausted || got.Nick != "b" {
		t.Fatalf("restored worker-b=%q exhausted=%v, want b", got.Nick, exhausted)
	}
	pre := NewPreemptor(p, quota.NewStore(), 0, clock.now, io.Discard)
	pre.tick()
	if got := c.Current(); got != "b" {
		t.Fatalf("restored fallback switched before reset: %q, want b", got)
	}
	clock.advance(time.Hour)
	pre.tick()
	if got := c.Current(); got != "a" {
		t.Fatalf("restored fallback after reset=%q, want sorted head a", got)
	}
	if got, _, exhausted := c.ResolveWorker("worker-a"); exhausted || (got.Nick != "a" && got.Nick != "b") {
		t.Fatalf("restored worker-a after recovery=%q exhausted=%v, want reassignment into {a,b}", got.Nick, exhausted)
	}
	if got, _, exhausted := c.ResolveWorker("worker-b"); exhausted || got.Nick != "b" {
		t.Fatalf("unaffected restored worker-b moved to %q exhausted=%v, want b", got.Nick, exhausted)
	}
}

func TestIssue337_sortedDefaultRuntimeMutations(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerRegistryWithConcurrency(t, 2, "c", "a", "b")
	p := NewPools(reg, nil, clock.now, io.Discard)
	c := p.byPool["auto"]
	if status, err := p.AddMember("auto", "d", "cred-d", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("add to sorted-default pool without placement: status=%d err=%v", status, err)
	}
	c.mu.Lock()
	order := append([]string(nil), c.effectiveOrderLocked()...)
	c.mu.Unlock()
	if got := order; len(got) != 4 || got[0] != "a" || got[1] != "b" || got[2] != "c" || got[3] != "d" {
		t.Fatalf("order after add=%v, want [a b c d]", got)
	}
	for _, tc := range []struct{ worker, want string }{{"worker-a", "a"}, {"worker-b", "b"}} {
		if got, _, exhausted := c.ResolveWorker(tc.worker); exhausted || got.Nick != tc.want {
			t.Fatalf("initial %s route=%q exhausted=%v, want %s", tc.worker, got.Nick, exhausted, tc.want)
		}
	}
	if status, err := p.SetMemberDisabled("auto", "a", true); status != http.StatusOK || err != nil {
		t.Fatalf("disable a: status=%d err=%v", status, err)
	}
	if got, _, exhausted := c.ResolveAuto(); exhausted || got.Nick != "b" {
		t.Fatalf("ordinary route after disabling a=%q exhausted=%v, want b", got.Nick, exhausted)
	}
	if got, _, exhausted := c.ResolveWorker("worker-a"); exhausted || got.Nick != "c" {
		t.Fatalf("affected worker after disabling a=%q exhausted=%v, want reassignment to c in {b,c}", got.Nick, exhausted)
	}
	if got, _, exhausted := c.ResolveWorker("worker-b"); exhausted || got.Nick != "b" {
		t.Fatalf("unaffected worker after disabling a=%q exhausted=%v, want b", got.Nick, exhausted)
	}
	if status, err := p.SetMemberDisabled("auto", "a", false); status != http.StatusOK || err != nil {
		t.Fatalf("enable a: status=%d err=%v", status, err)
	}
	pre := NewPreemptor(p, quota.NewStore(), 0, clock.now, io.Discard)
	if got := c.Current(); got != "b" {
		t.Fatalf("route switched before preempt tick after enable: %q, want sticky b", got)
	}
	pre.tick()
	if got := c.Current(); got != "a" {
		t.Fatalf("route after preempt tick following enable=%q, want a", got)
	}
	if got, _, exhausted := c.ResolveWorker("worker-a"); exhausted || (got.Nick != "a" && got.Nick != "b") {
		t.Fatalf("worker after re-enable=%q exhausted=%v, want reassignment into {a,b}", got.Nick, exhausted)
	}
	if status, err := p.SetConcurrency("auto", 3); status != http.StatusOK || err != nil {
		t.Fatalf("set concurrency: status=%d err=%v", status, err)
	}
	if got, _, exhausted := c.ResolveWorker("worker-new"); exhausted || (got.Nick != "b" && got.Nick != "c" && got.Nick != "d") {
		t.Fatalf("new worker after concurrency change=%q exhausted=%v, want member in available effective-order window", got.Nick, exhausted)
	}
}

func TestIssue337_concurrencyOneUsesSortedGlobalSticky(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerRegistryWithConcurrency(t, 1, "c", "a", "b")
	c := NewController(reg, "auto", -1, nil, clock.now, io.Discard)
	for _, worker := range []string{"worker-a", "worker-b"} {
		if got, _, exhausted := c.ResolveWorker(worker); exhausted || got.Nick != "a" {
			t.Fatalf("%s route=%q exhausted=%v, want shared sorted sticky a", worker, got.Nick, exhausted)
		}
	}
	if len(c.workerAffinity) != 0 || c.workerCursor != "" {
		t.Fatalf("concurrency one recorded worker state: affinity=%v cursor=%q", c.workerAffinity, c.workerCursor)
	}
}

func TestIssue337_initialSortedRouteSkipsDisabledHead(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerRegistryWithConcurrency(t, 1, "c", "a", "b")
	spec := reg.Spec()
	pool := spec.Pools["auto"]
	member := pool.Members["a"]
	member.Disabled = true
	pool.Members["a"] = member
	spec.Pools["auto"] = pool
	reg, err := backend.BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec disabled default member: %v", err)
	}
	c := NewController(reg, "auto", -1, nil, clock.now, io.Discard)
	if got := c.Current(); got != "b" {
		t.Fatalf("initial sticky=%q, want first available sorted member b", got)
	}
	if got, _, exhausted := c.ResolveAuto(); exhausted || got.Nick != "b" {
		t.Fatalf("initial ordinary route=%q exhausted=%v, want b/false", got.Nick, exhausted)
	}
}
