package auto

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

func issue361Workers(ps PoolStatus, nick string) []string {
	for _, member := range ps.Members {
		if member.Nick == nick {
			return member.Workers
		}
	}
	return nil
}

func TestIssue361_concurrencyOneListsResolvedWorkersAndRetainsFailoverAttribution(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 1, "a", "b"), nil, clock.now, io.Discard)
	c := p.byPool["auto"]

	for _, worker := range []string{"worker-z", "worker-a"} {
		if got, _, _, exhausted := p.RouteWorker("auto", worker); exhausted || got.Nick != "a" {
			t.Fatalf("initial %s route=%q exhausted=%v, want a/false", worker, got.Nick, exhausted)
		}
	}
	status, ok := p.PoolStatus("auto", quota.NewStore(), nil)
	if !ok {
		t.Fatal("PoolStatus(auto) missing")
	}
	if got := strings.Join(issue361Workers(status, "a"), ","); got != "worker-a,worker-z" {
		t.Fatalf("concurrency-1 workers on a=%q, want worker-a,worker-z", got)
	}
	for _, member := range status.Members {
		if member.InWindow {
			t.Errorf("concurrency-1 %s reports in_window=true", member.Nick)
		}
	}

	c.park("a", clock.now().Add(time.Hour))
	if got, _, _, exhausted := p.Route("auto"); exhausted || got.Nick != "b" {
		t.Fatalf("global failover route=%q exhausted=%v, want b/false", got.Nick, exhausted)
	}
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	if got := strings.Join(issue361Workers(status, "a"), ","); got != "worker-a,worker-z" {
		t.Fatalf("workers before their next request=%q, want worker-a,worker-z", got)
	}

	if got, _, _, exhausted := p.RouteWorker("auto", "worker-z"); exhausted || got.Nick != "b" {
		t.Fatalf("worker-z after failover=%q exhausted=%v, want b/false", got.Nick, exhausted)
	}
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	if got := issue361Workers(status, "a"); strings.Join(got, ",") != "worker-a" {
		t.Errorf("a workers after worker-z request=%v, want [worker-a]", got)
	}
	if got := issue361Workers(status, "b"); strings.Join(got, ",") != "worker-z" {
		t.Errorf("b workers after worker-z request=%v, want [worker-z]", got)
	}
}

func TestIssue361_poolDryRecordsLivenessButNotAnAttribution(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}

	p := NewPools(workerRegistryWithConcurrency(t, 1, "a"), nil, clock.now, io.Discard)
	c := p.byPool["auto"]
	c.park("a", clock.now().Add(time.Hour))
	if _, _, _, exhausted := p.RouteWorker("auto", "worker-dry"); !exhausted {
		t.Fatal("pool-dry worker route was not exhausted")
	}
	status, _ := p.PoolStatus("auto", quota.NewStore(), nil)
	if got := issue361Workers(status, "a"); len(got) != 0 {
		t.Fatalf("worker with only pool-dry request listed as %v, want none", got)
	}

	p = NewPools(workerRegistryWithConcurrency(t, 1, "a"), nil, clock.now, io.Discard)
	c = p.byPool["auto"]
	if _, _, _, exhausted := p.RouteWorker("auto", "worker-wait"); exhausted {
		t.Fatal("initial worker route unexpectedly exhausted")
	}
	c.park("a", clock.now().Add(time.Hour))
	if _, _, _, exhausted := p.RouteWorker("auto", "worker-wait"); !exhausted {
		t.Fatal("pool-dry retry was not exhausted")
	}
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	if got := strings.Join(issue361Workers(status, "a"), ","); got != "worker-wait" {
		t.Fatalf("pool-dry retry changed attribution to %q, want worker-wait", got)
	}
}

func TestIssue361_idleExpiryAppliesToBothConcurrencyModes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		concurrency int
	}{
		{name: "concurrency-1", concurrency: 1},
		{name: "concurrency-2", concurrency: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			p := NewPools(workerRegistryWithConcurrency(t, tc.concurrency, "a", "b"), nil, clock.now, io.Discard)
			if got, _, _, exhausted := p.RouteWorker("auto", "worker-idle"); exhausted || got.Nick == "" {
				t.Fatalf("initial route=%q exhausted=%v", got.Nick, exhausted)
			}
			clock.advance(59 * time.Minute)
			status, _ := p.PoolStatus("auto", quota.NewStore(), nil)
			found := false
			for _, member := range status.Members {
				found = found || len(member.Workers) > 0
			}
			if !found {
				t.Fatal("worker disappeared before one-hour idle TTL")
			}
			clock.advance(2 * time.Minute)
			status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
			for _, member := range status.Members {
				if len(member.Workers) != 0 {
					t.Errorf("expired worker remains on %s: %v", member.Nick, member.Workers)
				}
			}
		})
	}
}

func TestIssue361_returningWorkerAfterExpiryGetsFreshAssignment(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b"), nil, clock.now, io.Discard)

	for _, tc := range []struct {
		worker string
		want   string
	}{
		{worker: "worker-returning", want: "a"},
		{worker: "worker-anchor", want: "b"},
		{worker: "worker-live", want: "a"},
	} {
		if got, _, _, exhausted := p.RouteWorker("auto", tc.worker); exhausted || got.Nick != tc.want {
			t.Fatalf("initial %s route=%q exhausted=%v, want %s/false", tc.worker, got.Nick, exhausted, tc.want)
		}
	}

	// Keep a different worker on the returning worker's original member while
	// the returning worker and the anchor age past the idle window.
	clock.advance(59 * time.Minute)
	if got, _, _, exhausted := p.RouteWorker("auto", "worker-live"); exhausted || got.Nick != "a" {
		t.Fatalf("live worker refresh route=%q exhausted=%v, want a/false", got.Nick, exhausted)
	}
	clock.advance(2 * time.Minute)
	returningAt := clock.now()
	if got, _, _, exhausted := p.RouteWorker("auto", "worker-returning"); exhausted || got.Nick != "b" {
		t.Fatalf("returning worker route=%q exhausted=%v, want fresh b/false", got.Nick, exhausted)
	}

	c := p.byPool["auto"]
	c.mu.Lock()
	seen, seenOK := c.workerLastSeen["worker-returning"]
	affinity, affinityOK := c.workerAffinity["worker-returning"]
	c.mu.Unlock()
	if !seenOK || !seen.Equal(returningAt) {
		t.Fatalf("returning worker last_seen=%v (present=%v), want %v", seen, seenOK, returningAt)
	}
	if !affinityOK || affinity != "b" {
		t.Fatalf("returning worker affinity=%q (present=%v), want fresh b", affinity, affinityOK)
	}

	status, _ := p.PoolStatus("auto", quota.NewStore(), nil)
	if got := strings.Join(issue361Workers(status, "b"), ","); got != "worker-returning" {
		t.Fatalf("fresh attribution immediately after return=%q, want worker-returning", got)
	}
	clock.advance(59 * time.Minute)
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	if got := strings.Join(issue361Workers(status, "b"), ","); got != "worker-returning" {
		t.Fatalf("fresh attribution before new TTL=%q, want worker-returning", got)
	}
	clock.advance(2 * time.Minute)
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	if got := issue361Workers(status, "b"); len(got) != 0 {
		t.Fatalf("fresh attribution after new TTL=%v, want none", got)
	}
}

func TestIssue361_expiredAffinitiesStopBalancingAndPersisting(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := NewController(workerPriorityRegistry(t, 2, "a,b", "a", "b"), "auto", 0, nil, clock.now, io.Discard)
	start := clock.now()
	c.workerAffinity = map[string]string{"worker-old-1": "a", "worker-old-2": "a", "worker-live": "b"}
	c.workerLastSeen = map[string]time.Time{
		"worker-old-1": start,
		"worker-old-2": start,
		"worker-live":  start.Add(30 * time.Minute),
	}
	c.workerCursor = "a"
	clock.advance(61 * time.Minute)

	if got, _, exhausted := c.ResolveWorker("worker-new"); exhausted || got.Nick != "a" {
		t.Fatalf("new worker route=%q exhausted=%v, want a/false after expired a assignments are removed", got.Nick, exhausted)
	}
	if _, ok := c.workerAffinity["worker-old-1"]; ok {
		t.Error("expired worker-old-1 still affects affinity")
	}
	if _, ok := c.workerAffinity["worker-old-2"]; ok {
		t.Error("expired worker-old-2 still affects affinity")
	}
	saved := c.persistState()
	if _, ok := saved.WorkerAffinity["worker-old-1"]; ok {
		t.Error("expired worker-old-1 persisted")
	}
	if _, ok := saved.WorkerAffinity["worker-old-2"]; ok {
		t.Error("expired worker-old-2 persisted")
	}
	if saved.WorkerAffinity["worker-live"] != "b" || saved.WorkerAffinity["worker-new"] != "a" {
		t.Errorf("persisted live affinities=%v, want worker-live:b and worker-new:a", saved.WorkerAffinity)
	}
	wire, err := json.Marshal(saved)
	if err != nil {
		t.Fatalf("marshal persisted state: %v", err)
	}
	if strings.Contains(string(wire), "last_seen") || strings.Contains(string(wire), "worker_member") {
		t.Fatalf("persisted state gained liveness fields: %s", wire)
	}
}

func TestIssue361_pruningNotifiesOnlyPersistedAffinityDeletion(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := NewController(workerRegistryWithConcurrency(t, 2, "a", "b"), "auto", 0, nil, clock.now, io.Discard)
	if _, _, exhausted := c.ResolveWorker("worker-status"); exhausted {
		t.Fatal("initial status worker route exhausted")
	}
	mutations := 0
	c.onMutate = func() { mutations++ }
	clock.advance(61 * time.Minute)
	p := &Pools{byPool: map[string]*Controller{"auto": c}, reg: c.reg}
	if _, ok := p.PoolStatus("auto", quota.NewStore(), nil); !ok {
		t.Fatal("PoolStatus(auto) missing")
	}
	if mutations != 1 {
		t.Fatalf("status pruning notifications=%d, want 1 persisted affinity deletion", mutations)
	}

	c = NewController(workerRegistryWithConcurrency(t, 2, "a", "b"), "auto", 0, nil, clock.now, io.Discard)
	if _, _, exhausted := c.ResolveWorker("worker-snapshot"); exhausted {
		t.Fatal("initial snapshot worker route exhausted")
	}
	mutations = 0
	c.onMutate = func() { mutations++ }
	clock.advance(61 * time.Minute)
	saved := c.persistState()
	if mutations != 0 {
		t.Fatalf("snapshot pruning notifications=%d, want 0", mutations)
	}
	if len(saved.WorkerAffinity) != 0 {
		t.Fatalf("snapshot retained expired affinity: %v", saved.WorkerAffinity)
	}
}

func TestIssue361_loadedAffinityGetsFreshIdleWindow(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerRegistryWithConcurrency(t, 2, "a", "b")
	p := NewPools(reg, nil, clock.now, io.Discard)
	if _, _, _, exhausted := p.RouteWorker("auto", "worker-restart"); exhausted {
		t.Fatal("initial route exhausted")
	}
	saved := p.PersistState()
	reloaded := NewPools(reg, nil, clock.now, io.Discard)
	reloaded.LoadPersistState(saved)
	status, _ := reloaded.PoolStatus("auto", quota.NewStore(), nil)
	if got := strings.Join(issue361Workers(status, saved["auto"].WorkerAffinity["worker-restart"]), ","); got != "worker-restart" {
		t.Fatalf("loaded affinity workers=%q, want worker-restart", got)
	}
	clock.advance(59 * time.Minute)
	status, _ = reloaded.PoolStatus("auto", quota.NewStore(), nil)
	found := false
	for _, member := range status.Members {
		found = found || len(member.Workers) > 0
	}
	if !found {
		t.Fatal("loaded affinity expired before one hour from load")
	}
	clock.advance(2 * time.Minute)
	status, _ = reloaded.PoolStatus("auto", quota.NewStore(), nil)
	for _, member := range status.Members {
		if len(member.Workers) != 0 {
			t.Errorf("loaded affinity remains after fresh hour: %v", member.Workers)
		}
	}
}

func TestIssue361_concurrencyChangeAndMemberRemovalClearSingleModeAttribution(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 1, "a", "b"), nil, clock.now, io.Discard)
	if _, _, _, exhausted := p.RouteWorker("auto", "worker-change"); exhausted {
		t.Fatal("initial route exhausted")
	}
	status, _ := p.PoolStatus("auto", quota.NewStore(), nil)
	if len(issue361Workers(status, "a")) != 1 {
		t.Fatalf("initial concurrency-1 attribution=%v, want one worker", issue361Workers(status, "a"))
	}
	if code, err := p.SetConcurrency("auto", 2); code != 200 || err != nil {
		t.Fatalf("SetConcurrency(2): status=%d err=%v", code, err)
	}
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	for _, member := range status.Members {
		if len(member.Workers) != 0 {
			t.Errorf("after 1->2 %s workers=%v, want none", member.Nick, member.Workers)
		}
	}
	if _, _, _, exhausted := p.RouteWorker("auto", "worker-change"); exhausted {
		t.Fatal("concurrency-2 route exhausted")
	}
	if code, err := p.SetConcurrency("auto", 1); code != 200 || err != nil {
		t.Fatalf("SetConcurrency(1): status=%d err=%v", code, err)
	}
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	for _, member := range status.Members {
		if len(member.Workers) != 0 {
			t.Errorf("after 2->1 %s workers=%v, want none", member.Nick, member.Workers)
		}
	}

	p = NewPools(workerRegistryWithConcurrency(t, 1, "a", "b"), nil, clock.now, io.Discard)
	if _, _, _, exhausted := p.RouteWorker("auto", "worker-removed"); exhausted {
		t.Fatal("removal test initial route exhausted")
	}
	if code, err := p.RemoveMember("auto", "a"); code != 200 || err != nil {
		t.Fatalf("RemoveMember(a): status=%d err=%v", code, err)
	}
	status, _ = p.PoolStatus("auto", quota.NewStore(), nil)
	for _, member := range status.Members {
		if len(member.Workers) != 0 {
			t.Errorf("after member removal %s workers=%v, want none", member.Nick, member.Workers)
		}
	}
}
