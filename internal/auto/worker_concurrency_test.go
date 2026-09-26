package auto

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

func workerPriorityRegistry(t *testing.T, concurrency int, priority string, nicks ...string) *backend.Registry {
	t.Helper()
	reg := workerRegistryWithConcurrency(t, concurrency, nicks...)
	spec := reg.Spec()
	pool := spec.Pools["auto"]
	pool.Priority = strings.Split(priority, ",")
	spec.Pools["auto"] = pool
	updated, err := backend.BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec priority registry: %v", err)
	}
	return updated
}

func TestResolveWorker_concurrencyOneUsesGlobalPriorityRoute(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerPriorityRegistry(t, 1, "a,b,c", "a", "b", "c")
	c := NewController(reg, "auto", 0, nil, clock.now, io.Discard)
	for _, worker := range []string{"worker-one", "worker-two"} {
		if got, _, exhausted := c.ResolveWorker(worker); exhausted || got.Nick != "a" {
			t.Fatalf("%s on healthy priority head = %q exhausted=%v, want a/false", worker, got.Nick, exhausted)
		}
	}
	if len(c.workerAffinity) != 0 || c.workerCursor != "" {
		t.Fatalf("N=1 recorded worker state: affinity=%v cursor=%q", c.workerAffinity, c.workerCursor)
	}

	c.park("a", clock.now().Add(time.Hour))
	for _, worker := range []string{"worker-one", "worker-two"} {
		if got, _, exhausted := c.ResolveWorker(worker); exhausted || got.Nick != "b" {
			t.Fatalf("%s after a exhausted = %q exhausted=%v, want global failover b/false", worker, got.Nick, exhausted)
		}
	}
	clock.advance(time.Hour)
	if !c.PreemptTo("a") {
		t.Fatal("PreemptTo(a) failed after a recovered")
	}
	for _, worker := range []string{"worker-one", "worker-two"} {
		if got, _, exhausted := c.ResolveWorker(worker); exhausted || got.Nick != "a" {
			t.Fatalf("%s after global preempt = %q exhausted=%v, want a/false", worker, got.Nick, exhausted)
		}
	}
	if len(c.workerAffinity) != 0 || c.workerCursor != "" {
		t.Errorf("N=1 recorded worker state after failover: affinity=%v cursor=%q", c.workerAffinity, c.workerCursor)
	}
}

func TestWorkerAffinity_concurrencyOneDropsPersistedAndReconciledState(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerPriorityRegistry(t, 1, "a,b", "a", "b")
	p := NewPools(reg, nil, clock.now, io.Discard)
	p.LoadPersistState(map[string]PoolPersistState{"auto": {
		Sticky:         "a",
		WorkerAffinity: map[string]string{"worker-one": "b"},
		WorkerCursor:   "b",
	}})
	c := p.byPool["auto"]
	if state := c.persistState(); len(state.WorkerAffinity) != 0 || state.WorkerCursor != "" {
		t.Errorf("N=1 persisted stale worker state: %+v", state)
	}
	c.mu.Lock()
	c.workerAffinity["worker-memory"] = "b"
	c.workerCursor = "b"
	c.reconcileLocked(reg)
	if len(c.workerAffinity) != 0 || c.workerCursor != "" {
		t.Errorf("N=1 reconcile retained worker state: affinity=%v cursor=%q", c.workerAffinity, c.workerCursor)
	}
	c.mu.Unlock()
}

func TestWorkerAffinity_concurrencyWindowAndReclaim(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := NewController(workerPriorityRegistry(t, 2, "a,b,c", "a", "b", "c"), "auto", 0, nil, clock.now, io.Discard)
	for _, tc := range []struct{ worker, want string }{{"worker-a", "a"}, {"worker-b", "b"}} {
		if got, _, exhausted := c.ResolveWorker(tc.worker); exhausted || got.Nick != tc.want {
			t.Fatalf("initial %s = %q exhausted=%v, want %s/false", tc.worker, got.Nick, exhausted, tc.want)
		}
	}
	if got := c.workerAffinity["worker-b"]; got != "b" {
		t.Fatalf("worker-b assignment=%q, want b; c must receive no initial worker", got)
	}

	c.park("a", clock.now().Add(time.Hour))
	if got, _, exhausted := c.ResolveWorker("worker-a"); exhausted || got.Nick != "c" {
		t.Fatalf("worker-a with a unavailable = %q exhausted=%v, want fallback c", got.Nick, exhausted)
	}
	if got, _, exhausted := c.ResolveWorker("worker-b"); exhausted || got.Nick != "b" {
		t.Fatalf("worker-b moved with window = %q exhausted=%v, want b", got.Nick, exhausted)
	}

	clock.advance(time.Hour)
	if got, _, exhausted := c.ResolveWorker("worker-a"); exhausted || got.Nick != "a" {
		t.Fatalf("worker-a after a recovered = %q exhausted=%v, want reassignment into {a,b}", got.Nick, exhausted)
	}
	if got, _, exhausted := c.ResolveWorker("worker-b"); exhausted || got.Nick != "b" {
		t.Fatalf("worker-b moved after a recovered = %q exhausted=%v, want b", got.Nick, exhausted)
	}
}

func TestWorkerStatus_reportsWindowAndDeferredAffinity(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b", "c"), nil, clock.now, io.Discard)
	for _, tc := range []struct{ worker, want string }{{"worker-z", "a"}, {"worker-y", "b"}, {"worker-a", "a"}} {
		if got, _, _, exhausted := p.RouteWorker("auto", tc.worker); exhausted || got.Nick != tc.want {
			t.Fatalf("initial %s = %q exhausted=%v, want %s", tc.worker, got.Nick, exhausted, tc.want)
		}
	}

	status, ok := p.PoolStatus("auto", quota.NewStore(), nil)
	if !ok {
		t.Fatal("PoolStatus(auto) missing")
	}
	byNick := make(map[string]MemberStatus, len(status.Members))
	for _, member := range status.Members {
		byNick[member.Nick] = member
	}
	for nick, want := range map[string]bool{"a": true, "b": true, "c": false} {
		if byNick[nick].InWindow != want {
			t.Errorf("%s in_window=%v, want %v", nick, byNick[nick].InWindow, want)
		}
	}
	if got := strings.Join(byNick["a"].Workers, ","); got != "worker-a,worker-z" {
		t.Errorf("a workers=%v, want sorted [worker-a worker-z]", byNick["a"].Workers)
	}
	if got := strings.Join(byNick["b"].Workers, ","); got != "worker-y" {
		t.Errorf("b workers=%v, want [worker-y]", byNick["b"].Workers)
	}
	if len(byNick["c"].Workers) != 0 {
		t.Errorf("c workers=%v, want none", byNick["c"].Workers)
	}

	p.byPool["auto"].park("b", clock.now().Add(time.Hour))
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	byNick = make(map[string]MemberStatus, len(status.Members))
	for _, member := range status.Members {
		byNick[member.Nick] = member
	}
	if byNick["b"].InWindow {
		t.Error("exhausted b remains in the worker window")
	}
	if !byNick["c"].InWindow {
		t.Error("available c did not enter the worker window after b was exhausted")
	}
	if got := strings.Join(byNick["b"].Workers, ","); got != "worker-y" {
		t.Errorf("b workers before next request=%v, want [worker-y]", byNick["b"].Workers)
	}
	if got, _, _, exhausted := p.RouteWorker("auto", "worker-a"); exhausted || got.Nick != "a" {
		t.Errorf("worker-a in window moved to %q exhausted=%v, want a", got.Nick, exhausted)
	}
	if got, _, _, exhausted := p.RouteWorker("auto", "worker-y"); exhausted || got.Nick != "c" {
		t.Errorf("worker-y deferred reassignment=%q exhausted=%v, want c", got.Nick, exhausted)
	}

	if code, err := p.SetConcurrency("auto", 1); code != http.StatusOK || err != nil {
		t.Fatalf("SetConcurrency(1): status=%d err=%v", code, err)
	}
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	for _, member := range status.Members {
		if member.InWindow {
			t.Errorf("N=1 %s reports in_window=true", member.Nick)
		}
		if len(member.Workers) != 0 {
			t.Errorf("N=1 %s reports workers=%v", member.Nick, member.Workers)
		}
	}
	wire, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal N=1 status: %v", err)
	}
	if strings.Contains(string(wire), `"workers"`) {
		t.Errorf("N=1 status contains a workers field: %s", wire)
	}
}

func TestWorkerStatus_servingRequiresStickyOrAssignedInWindow(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := NewController(workerPriorityRegistry(t, 3, "a,b,c,d,e,f", "a", "b", "c", "d", "e", "f"), "auto", 0, nil, clock.now, io.Discard)
	c.curNick = "a"
	c.workerAffinity = map[string]string{
		"worker-b": "b",
		"worker-d": "d",
		"worker-e": "e",
		"worker-f": "f",
	}
	c.exhausted["f"] = clock.now().Add(time.Hour)
	p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}
	if code, err := p.SetMemberDisabled("auto", "e", true); code != http.StatusOK || err != nil {
		t.Fatalf("SetMemberDisabled(e): status=%d err=%v", code, err)
	}

	status, ok := p.PoolStatus("auto", quota.NewStore(), nil)
	if !ok {
		t.Fatal("PoolStatus(auto) missing")
	}
	if status.Active != "a" {
		t.Fatalf("active=%q, want global sticky nick a", status.Active)
	}
	byNick := make(map[string]MemberStatus, len(status.Members))
	for _, member := range status.Members {
		byNick[member.Nick] = member
	}
	for nick, want := range map[string]string{
		"a": "serving", // global sticky target needs no worker affinity
		"b": "serving", // assigned worker is inside the window
		"c": "idle",    // in the window, but no assigned worker
		"d": "idle",    // stale assignment is pending outside the window
		"e": "disabled",
		"f": "exhausted",
	} {
		if got := byNick[nick].Status; got != want {
			t.Errorf("%s status=%q, want %q", nick, got, want)
		}
	}
	if byNick["c"].InWindow != true || len(byNick["c"].Workers) != 0 {
		t.Errorf("window-only member c=%+v, want eligible with no assignment", byNick["c"])
	}
	if byNick["d"].InWindow || strings.Join(byNick["d"].Workers, ",") != "worker-d" {
		t.Errorf("stale assignment d=%+v, want out-of-window with worker-d still visible", byNick["d"])
	}
	for nick, worker := range map[string]string{"e": "worker-e", "f": "worker-f"} {
		if strings.Join(byNick[nick].Workers, ",") != worker {
			t.Errorf("%s workers=%v, want stale assignment %s", nick, byNick[nick].Workers, worker)
		}
	}

	configStatuses := make(map[string]string)
	for _, view := range p.EffectiveConfig() {
		for _, member := range view.Members {
			configStatuses[member.Nick] = member.Status
		}
	}
	for nick, member := range byNick {
		if got := configStatuses[nick]; got != member.Status {
			t.Errorf("config status for %s=%q, pool status=%q", nick, got, member.Status)
		}
	}

	if code, err := p.SetConcurrency("auto", 1); code != http.StatusOK || err != nil {
		t.Fatalf("SetConcurrency(1): status=%d err=%v", code, err)
	}
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	byNick = make(map[string]MemberStatus, len(status.Members))
	for _, member := range status.Members {
		byNick[member.Nick] = member
	}
	for nick, want := range map[string]string{
		"a": "serving",
		"b": "idle",
		"c": "idle",
		"d": "idle",
		"e": "disabled",
		"f": "exhausted",
	} {
		if got := byNick[nick].Status; got != want {
			t.Errorf("concurrency 1: %s status=%q, want %q", nick, got, want)
		}
	}
}

func TestWorkerStatus_disabledAndExhaustedOverrideStickyWorkers(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	for _, tc := range []struct {
		name string
		want string
		set  func(*Controller)
	}{
		{name: "disabled", want: "disabled", set: func(c *Controller) { c.disabled["b"] = true }},
		{name: "exhausted", want: "exhausted", set: func(c *Controller) { c.exhausted["b"] = clock.now().Add(time.Hour) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewController(workerPriorityRegistry(t, 2, "a,b,c", "a", "b", "c"), "auto", 0, nil, clock.now, io.Discard)
			c.curNick = "b"
			c.workerAffinity = map[string]string{"worker-b": "b"}
			tc.set(c)
			p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}

			status, ok := p.PoolStatus("auto", quota.NewStore(), nil)
			if !ok {
				t.Fatal("PoolStatus(auto) missing")
			}
			if got := memberStatus(status, "b"); got != tc.want {
				t.Errorf("sticky member b status=%q, want %q", got, tc.want)
			}
			workers := []string(nil)
			for _, member := range status.Members {
				if member.Nick == "b" {
					workers = member.Workers
				}
			}
			if got := strings.Join(workers, ","); got != "worker-b" {
				t.Errorf("b workers=%q, want worker-b", got)
			}
			configStatus := ""
			for _, view := range p.EffectiveConfig() {
				for _, member := range view.Members {
					if member.Nick == "b" {
						configStatus = member.Status
					}
				}
			}
			if configStatus != tc.want {
				t.Errorf("config status for b=%q, want %q", configStatus, tc.want)
			}
		})
	}
}

func TestSetConcurrency_reassignsOnlyWorkersOutsideLoweredWindow(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 3, "a", "b", "c"), nil, clock.now, io.Discard)
	for _, tc := range []struct{ worker, want string }{{"worker-a", "a"}, {"worker-b", "b"}, {"worker-c", "c"}} {
		if got, _, _, exhausted := p.RouteWorker("auto", tc.worker); exhausted || got.Nick != tc.want {
			t.Fatalf("initial %s = %q exhausted=%v, want %s", tc.worker, got.Nick, exhausted, tc.want)
		}
	}
	if code, err := p.SetConcurrency("auto", 2); code != http.StatusOK || err != nil {
		t.Fatalf("SetConcurrency(2): status=%d err=%v", code, err)
	}
	status, _ := p.PoolStatus("auto", quota.NewStore(), nil)
	byNick := make(map[string]MemberStatus, len(status.Members))
	for _, member := range status.Members {
		byNick[member.Nick] = member
	}
	for _, nick := range []string{"a", "b"} {
		if !byNick[nick].InWindow {
			t.Errorf("%s is outside the lowered worker window", nick)
		}
	}
	if byNick["c"].InWindow || strings.Join(byNick["c"].Workers, ",") != "worker-c" {
		t.Errorf("c before worker request: %+v, want outside window with worker-c pending", byNick["c"])
	}
	if got, _, _, exhausted := p.RouteWorker("auto", "worker-c"); exhausted || got.Nick == "c" {
		t.Errorf("worker-c reassigned to %q exhausted=%v, want an in-window member", got.Nick, exhausted)
	}
	if got, _, _, exhausted := p.RouteWorker("auto", "worker-a"); exhausted || got.Nick != "a" {
		t.Errorf("worker-a moved from retained window member to %q exhausted=%v, want a", got.Nick, exhausted)
	}
	if got, _, _, exhausted := p.RouteWorker("auto", "worker-b"); exhausted || got.Nick != "b" {
		t.Errorf("worker-b moved from retained window member to %q exhausted=%v, want b", got.Nick, exhausted)
	}
}

func TestWorkerAffinity_concurrencyTwoCyclesExactlyWithinWindow(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := NewController(workerPriorityRegistry(t, 2, "a,b,c", "a", "b", "c"), "auto", 0, nil, clock.now, io.Discard)
	counts := map[string]int{}
	for i, want := range []string{"a", "b", "a", "b"} {
		worker := fmt.Sprintf("worker-%d", i+1)
		got, _, exhausted := c.ResolveWorker(worker)
		if exhausted || got.Nick != want {
			t.Fatalf("%s initial assignment = %q exhausted=%v, want %s/false", worker, got.Nick, exhausted, want)
		}
		counts[got.Nick]++
	}
	if counts["a"] != 2 || counts["b"] != 2 || counts["c"] != 0 {
		t.Errorf("initial window counts = %v, want a:2 b:2 c:0", counts)
	}
}

func TestWorkerAffinity_reclaimsSecondWindowMemberAndReturnsAfterReset(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerPriorityRegistry(t, 2, "a,b,c", "a", "b", "c"), nil, clock.now, io.Discard)
	for i, want := range []string{"a", "b", "a", "b"} {
		worker := fmt.Sprintf("worker-%d", i+1)
		if got, _, ok, exhausted := p.RouteWorker("auto", worker); !ok || exhausted || got.Nick != want {
			t.Fatalf("%s initial assignment = %q exhausted=%v, want %s/false", worker, got.Nick, exhausted, want)
		}
	}
	c := p.byPool["auto"]
	c.park("b", clock.now().Add(time.Hour))
	for worker, want := range map[string]string{"worker-1": "a", "worker-2": "c", "worker-3": "a", "worker-4": "c"} {
		if got, _, _, exhausted := p.RouteWorker("auto", worker); exhausted || got.Nick != want {
			t.Errorf("%s with b unavailable = %q exhausted=%v, want %s/false", worker, got.Nick, exhausted, want)
		}
	}

	clock.advance(time.Hour)
	for _, worker := range []string{"worker-2", "worker-4"} {
		if got, _, _, exhausted := p.RouteWorker("auto", worker); exhausted || (got.Nick != "a" && got.Nick != "b") {
			t.Errorf("%s after b recovered = %q exhausted=%v, want reassignment into {a,b}", worker, got.Nick, exhausted)
		}
	}
	for _, worker := range []string{"worker-1", "worker-3"} {
		if got, _, _, exhausted := p.RouteWorker("auto", worker); exhausted || got.Nick != "a" {
			t.Errorf("%s already in window moved after b recovered = %q exhausted=%v, want a/false", worker, got.Nick, exhausted)
		}
	}
}

func TestWorkerAffinity_singleFallbackMemberAndDryRetry(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerPriorityRegistry(t, 2, "a,b,c", "a", "b", "c"), nil, clock.now, io.Discard)
	c := p.byPool["auto"]
	c.park("a", clock.now().Add(time.Hour))
	c.park("b", clock.now().Add(2*time.Hour))
	for i := 1; i <= 4; i++ {
		worker := fmt.Sprintf("worker-%d", i)
		if got, _, _, exhausted := p.RouteWorker("auto", worker); exhausted || got.Nick != "c" {
			t.Errorf("%s with only c available = %q exhausted=%v, want c/false", worker, got.Nick, exhausted)
		}
	}

	c.park("c", clock.now().Add(3*time.Hour))
	h := backend.WorkerNamespaceMiddleware(backend.Middleware(p, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream handler called while every worker-window member is exhausted")
	})))
	req := httptest.NewRequest(http.MethodPost, "/_aqg/w/worker-dry/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer auto")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("namespaced all-exhausted status = %d, want 503; body=%q", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Retry-After"); got != "3600" {
		t.Errorf("namespaced Retry-After = %q, want soonest reset 3600", got)
	}
}

func TestWorkerAffinity_concurrencyAboveMemberCountUsesAllMembers(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := NewController(workerRegistryWithConcurrency(t, 4, "a", "b", "c"), "auto", 0, nil, clock.now, io.Discard)
	for i, want := range []string{"a", "b", "c", "a"} {
		worker := "worker-" + string(rune('a'+i))
		if got, _, exhausted := c.ResolveWorker(worker); exhausted || got.Nick != want {
			t.Errorf("%s = %q exhausted=%v, want %s/false", worker, got.Nick, exhausted, want)
		}
	}
}

func TestWorkerAffinity_concurrencyDoesNotChangeUnnamespacedRoute(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b", "c"), nil, clock.now, io.Discard)
	p.byPool["auto"].setCur("c")
	if got, _, ok, exhausted := p.Route("auto"); !ok || exhausted || got.Nick != "c" {
		t.Fatalf("unnamespaced route = %q ok=%v exhausted=%v, want c/true/false", got.Nick, ok, exhausted)
	}
	if got, _, ok, exhausted := p.RouteWorker("auto", "worker-a"); !ok || exhausted || got.Nick != "a" {
		t.Fatalf("namespaced route = %q ok=%v exhausted=%v, want window member a", got.Nick, ok, exhausted)
	}
	if got, _, ok, exhausted := p.Route("auto"); !ok || exhausted || got.Nick != "c" {
		t.Errorf("unnamespaced route after worker assignment = %q ok=%v exhausted=%v, want c", got.Nick, ok, exhausted)
	}
}

func TestWorkerAffinity_runtimePriorityAndMemberRemovalReassignOnNextRequest(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b", "c"), nil, clock.now, io.Discard)
	for _, tc := range []struct{ worker, want string }{{"worker-a", "a"}, {"worker-b", "b"}} {
		if got, _, _, _ := p.RouteWorker("auto", tc.worker); got.Nick != tc.want {
			t.Fatalf("initial %s = %q, want %s", tc.worker, got.Nick, tc.want)
		}
	}
	if status, err := p.SetPriority("auto", []string{"c"}); status != http.StatusOK || err != nil {
		t.Fatalf("SetPriority: status=%d err=%v", status, err)
	}
	if got, _, _, _ := p.RouteWorker("auto", "worker-a"); got.Nick != "a" {
		t.Errorf("worker still in new window moved to %q, want a", got.Nick)
	}
	if got, _, _, _ := p.RouteWorker("auto", "worker-b"); got.Nick != "c" {
		t.Errorf("worker outside new priority window = %q, want c", got.Nick)
	}

	plain := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b"), nil, clock.now, io.Discard)
	for _, tc := range []struct{ worker, want string }{{"worker-a", "a"}, {"worker-b", "b"}} {
		if got, _, _, _ := plain.RouteWorker("auto", tc.worker); got.Nick != tc.want {
			t.Fatalf("initial plain %s = %q, want %s", tc.worker, got.Nick, tc.want)
		}
	}
	if status, err := plain.AddMember("auto", "c", "cred-c", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember: status=%d err=%v", status, err)
	}
	if status, err := plain.RemoveMember("auto", "a"); status != http.StatusOK || err != nil {
		t.Fatalf("RemoveMember: status=%d err=%v", status, err)
	}
	if got, _, _, _ := plain.RouteWorker("auto", "worker-a"); got.Nick != "c" {
		t.Errorf("worker assigned to removed a = %q, want c", got.Nick)
	}
	if got, _, _, _ := plain.RouteWorker("auto", "worker-b"); got.Nick != "b" {
		t.Errorf("unaffected worker moved after removal = %q, want b", got.Nick)
	}
}

func TestWorkerAffinity_persistedTargetOutsideWindowReassignsAfterLoad(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerPriorityRegistry(t, 2, "a,b,c", "a", "b", "c")
	p := NewPools(reg, nil, clock.now, io.Discard)
	p.LoadPersistState(map[string]PoolPersistState{"auto": {
		WorkerAffinity: map[string]string{"worker-c": "c"},
		WorkerCursor:   "c",
	}})
	if got, _, _, exhausted := p.RouteWorker("auto", "worker-c"); exhausted || (got.Nick != "a" && got.Nick != "b") {
		t.Fatalf("restored c assignment = %q exhausted=%v, want reassignment into {a,b}", got.Nick, exhausted)
	}
}

func TestWorkerAffinity_concurrentPriorityChangesAndRoutes(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerPriorityRegistry(t, 2, "a,b,c", "a", "b", "c"), nil, clock.now, io.Discard)
	const workers = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		worker := fmt.Sprintf("worker-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 40; j++ {
				if got, _, ok, exhausted := p.RouteWorker("auto", worker); !ok || exhausted || (got.Nick != "a" && got.Nick != "b" && got.Nick != "c") {
					t.Errorf("concurrent route for %s = %q ok=%v exhausted=%v", worker, got.Nick, ok, exhausted)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		orders := [][]string{{"a"}, {"b", "c"}, {"a", "b", "c"}}
		for i := 0; i < 40; i++ {
			if status, err := p.SetPriority("auto", orders[i%len(orders)]); status != http.StatusOK || err != nil {
				t.Errorf("concurrent SetPriority: status=%d err=%v", status, err)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	if status, err := p.SetPriority("auto", []string{"c"}); status != http.StatusOK || err != nil {
		t.Fatalf("final SetPriority: status=%d err=%v", status, err)
	}
	for i := 0; i < workers; i++ {
		worker := fmt.Sprintf("worker-%d", i)
		if got, _, ok, exhausted := p.RouteWorker("auto", worker); !ok || exhausted || (got.Nick != "c" && got.Nick != "a") {
			t.Errorf("%s after final priority window {c,a} = %q ok=%v exhausted=%v", worker, got.Nick, ok, exhausted)
		}
	}
}
