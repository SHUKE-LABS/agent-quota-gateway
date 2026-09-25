package auto

import (
	"encoding/json"
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

func workerRegistryWithConcurrency(t *testing.T, concurrency int, nicks ...string) *backend.Registry {
	t.Helper()
	reg := testRegistry(t, nicks...)
	spec := reg.Spec()
	pool := spec.Pools["auto"]
	pool.Concurrency = &concurrency
	spec.Pools["auto"] = pool
	updated, err := backend.BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec with concurrency: %v", err)
	}
	return updated
}

func newWorkerPriorityController(t *testing.T, concurrency, start int, clock *fixedClock, logOut io.Writer, priorityCSV string, nicks ...string) *Controller {
	t.Helper()
	c := newPriorityController(t, start, clock, logOut, priorityCSV, nicks...)
	c.workerConcurrency = concurrency
	return c
}

func TestWorkerAffinity_firstUseRoundRobinAndHardAffinity(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 3, "c", "a", "b"), nil, clock.now, io.Discard)

	for _, tc := range []struct{ worker, want string }{{"agent-a", "a"}, {"agent-b", "b"}, {"agent-c", "c"}} {
		b, _, ok, exhausted := p.RouteWorker("auto", tc.worker)
		if !ok || exhausted || b.Nick != tc.want {
			t.Errorf("first route for %s = (%q, ok=%v, exhausted=%v), want %q", tc.worker, b.Nick, ok, exhausted, tc.want)
		}
	}
	// The global pointer may move independently. A healthy assignment stays put.
	c := p.byPool["auto"]
	c.setCur("c")
	b, _, ok, exhausted := p.RouteWorker("auto", "agent-a")
	if !ok || exhausted || b.Nick != "a" {
		t.Errorf("repeat route for agent-a = (%q, ok=%v, exhausted=%v), want a", b.Nick, ok, exhausted)
	}
}

func TestWorkerAffinity_concurrentFirstRequestsConverge(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 3, "a", "b", "c"), nil, clock.now, io.Discard)
	const sameWorkerRequests = 64
	results := make(chan string, sameWorkerRequests+2)
	var wg sync.WaitGroup
	for i := 0; i < sameWorkerRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, _, ok, exhausted := p.RouteWorker("auto", "agent-shared")
			if !ok || exhausted {
				results <- "<unavailable>"
				return
			}
			results <- b.Nick
		}()
	}
	wg.Wait()
	close(results)

	var assigned string
	for nick := range results {
		if nick == "<unavailable>" {
			t.Fatal("same-worker route unexpectedly unavailable")
		}
		if assigned == "" {
			assigned = nick
		} else if nick != assigned {
			t.Fatalf("concurrent first requests split agent-shared across %q and %q", assigned, nick)
		}
	}

	// Two distinct first-use requests consume successive healthy cycle slots.
	distinctResults := make(chan string, 2)
	var distinct sync.WaitGroup
	for _, worker := range []string{"agent-other-a", "agent-other-b"} {
		worker := worker
		distinct.Add(1)
		go func() {
			defer distinct.Done()
			b, _, ok, exhausted := p.RouteWorker("auto", worker)
			if !ok || exhausted {
				distinctResults <- "<unavailable>"
				return
			}
			distinctResults <- b.Nick
		}()
	}
	distinct.Wait()
	close(distinctResults)
	var distinctNicks []string
	for nick := range distinctResults {
		distinctNicks = append(distinctNicks, nick)
	}
	if len(distinctNicks) != 2 || distinctNicks[0] == "<unavailable>" || distinctNicks[1] == "<unavailable>" || distinctNicks[0] == distinctNicks[1] {
		t.Errorf("distinct workers assigned %v, want two different healthy members", distinctNicks)
	}
}

func TestWorkerAffinity_preemptMovesOnlyGlobalSticky(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	c := newWorkerPriorityController(t, 2, -1, clock, io.Discard, "a,b", "a", "b")
	if b, _, exhausted := c.ResolveWorker("agent-a"); exhausted || b.Nick != "a" {
		t.Fatalf("agent-a initial assignment = %q, exhausted=%v, want a", b.Nick, exhausted)
	}
	if b, _, exhausted := c.ResolveWorker("agent-b"); exhausted || b.Nick != "b" {
		t.Fatalf("agent-b initial assignment = %q, exhausted=%v, want b", b.Nick, exhausted)
	}
	c.setCur("b")
	if !c.PreemptTo("a") {
		t.Fatal("PreemptTo(a) failed, want global sticky to preempt to the healthy preferred member")
	}
	if got := c.Current(); got != "a" {
		t.Errorf("global sticky=%q, want a after preemption", got)
	}
	if b, _, exhausted := c.ResolveWorker("agent-b"); exhausted || b.Nick != "b" {
		t.Errorf("agent-b assignment=%q, exhausted=%v, want hard affinity b after preemption", b.Nick, exhausted)
	}
}
func TestWorkerAffinity_unavailableMemberReassignsOnlyAffectedWorker(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b"), nil, clock.now, io.Discard)
	for _, tc := range []struct {
		worker string
		want   string
	}{{"agent-a", "a"}, {"agent-b", "b"}} {
		b, _, ok, exhausted := p.RouteWorker("auto", tc.worker)
		if !ok || exhausted || b.Nick != tc.want {
			t.Fatalf("%s initial assignment=%q, want %s", tc.worker, b.Nick, tc.want)
		}
	}
	c := p.byPool["auto"]
	c.park("a", clock.now().Add(time.Hour))
	b, _, ok, exhausted := p.RouteWorker("auto", "agent-a")
	if !ok || exhausted || b.Nick != "b" {
		t.Fatalf("agent-a replacement=%q, ok=%v exhausted=%v, want healthy b", b.Nick, ok, exhausted)
	}
	if got, _, _, _ := p.RouteWorker("auto", "agent-b"); got.Nick != "b" {
		t.Errorf("unrelated agent-b moved to %q, want b", got.Nick)
	}
}

func TestWorkerAffinity_storeQuotaExhaustionReassignsOnlyAffectedWorker(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	store := quota.NewStore()
	p := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b"), store, clock.now, io.Discard)
	for _, tc := range []struct {
		worker string
		want   string
	}{{"agent-a", "a"}, {"agent-b", "b"}} {
		b, _, ok, exhausted := p.RouteWorker("auto", tc.worker)
		if !ok || exhausted || b.Nick != tc.want {
			t.Fatalf("%s initial assignment=%q, want %s", tc.worker, b.Nick, tc.want)
		}
	}
	reset := clock.now().Add(time.Hour)
	store.Put("a", quota.Snapshot{Unified5hStatus: "rejected", Unified5hReset: &reset, AsOf: clock.now()})
	if got, _, ok, exhausted := p.RouteWorker("auto", "agent-a"); !ok || exhausted || got.Nick != "b" {
		t.Errorf("agent-a after quota rejection=%q, ok=%v exhausted=%v, want b", got.Nick, ok, exhausted)
	}
	if got, _, _, _ := p.RouteWorker("auto", "agent-b"); got.Nick != "b" {
		t.Errorf("unrelated agent-b moved to %q, want b", got.Nick)
	}
}

func TestWorkerAffinity_realFailureInvalidatesAllMappingsToNick(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			p := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b"), nil, clock.now, io.Discard)
			for _, worker := range []string{"agent-one", "agent-two", "agent-three"} {
				if _, _, ok, exhausted := p.RouteWorker("auto", worker); !ok || exhausted {
					t.Fatalf("initial assignment for %s failed", worker)
				}
			}
			member, _ := p.reg.ResolveIn("auto", "a")
			var resp *http.Response
			if status == http.StatusTooManyRequests {
				resp = resp429(member, clock, time.Hour)
			} else {
				req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
				req = req.WithContext(backend.WithBackend(req.Context(), member))
				resp = &http.Response{StatusCode: status, Header: make(http.Header), Request: req, Body: io.NopCloser(strings.NewReader("rejected"))}
			}
			if err := p.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse: %v", err)
			}
			for _, worker := range []string{"agent-one", "agent-two", "agent-three"} {
				if got, _, _, _ := p.RouteWorker("auto", worker); got.Nick != "b" {
					t.Errorf("%s after real failure = %q, want reassignment to b", worker, got.Nick)
				}
			}
		})
	}
}

func TestWorkerAffinity_sharedCredentialFailureInvalidatesSiblingPool(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			scrubPoolEnv(t)
			t.Setenv(backend.EnvPrefix+"ONE_BACKEND_A", "cred-a")
			t.Setenv(backend.EnvPrefix+"ONE_BACKEND_B", "cred-b")
			t.Setenv(backend.EnvPrefix+"ONE_CONCURRENCY", "2")
			t.Setenv(backend.EnvPrefix+"TWO_BACKEND_A", "cred-a")
			t.Setenv(backend.EnvPrefix+"TWO_BACKEND_C", "cred-c")
			t.Setenv(backend.EnvPrefix+"TWO_CONCURRENCY", "2")
			reg, err := backend.Load(testDefaultBaseURL)
			if err != nil {
				t.Fatalf("backend.Load: %v", err)
			}
			store := quota.NewStore()
			p := NewPools(reg, store, clock.now, io.Discard)
			if got, _, _, _ := p.RouteWorker("one", "agent-shared"); got.Nick != "a" {
				t.Fatalf("pool one assignment=%q, want a", got.Nick)
			}
			if got, _, _, _ := p.RouteWorker("two", "agent-shared"); got.Nick != "a" {
				t.Fatalf("pool two assignment=%q, want shared nick a", got.Nick)
			}
			if got, _, _, _ := p.RouteWorker("two", "agent-other"); got.Nick != "c" {
				t.Fatalf("pool two unrelated assignment=%q, want c", got.Nick)
			}
			member, _ := reg.ResolveIn("one", "a")
			var resp *http.Response
			if status == http.StatusTooManyRequests {
				resp = resp429(member, clock, time.Hour)
			} else {
				req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
				req = req.WithContext(backend.WithBackend(req.Context(), member))
				resp = &http.Response{StatusCode: status, Header: make(http.Header), Request: req, Body: io.NopCloser(strings.NewReader("rejected"))}
			}
			if err := p.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse: %v", err)
			}
			if status == http.StatusTooManyRequests {
				reset := clock.now().Add(time.Hour)
				store.Put("a", quota.Snapshot{Unified5hStatus: "rejected", Unified5hReset: &reset, AsOf: clock.now()})
			}

			if got, _, _, _ := p.RouteWorker("two", "agent-shared"); got.Nick != "c" {
				t.Errorf("sibling worker after shared failure = %q, want c", got.Nick)
			}
			if got, _, _, _ := p.RouteWorker("two", "agent-other"); got.Nick != "c" {
				t.Errorf("sibling unaffected worker moved to %q, want c", got.Nick)
			}
		})
	}
}

func TestWorkerAffinity_transientFailuresRetainMapping(t *testing.T) {
	t.Run("native Anthropic 529", func(t *testing.T) {
		clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
		p := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b"), nil, clock.now, io.Discard)
		if b, _, _, _ := p.RouteWorker("auto", "agent-a"); b.Nick != "a" {
			t.Fatalf("initial assignment=%q, want a", b.Nick)
		}
		member, _ := p.reg.ResolveIn("auto", "a")
		if err := p.ModifyResponse(resp529(member)); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}
		if got, _, _, _ := p.RouteWorker("auto", "agent-a"); got.Nick != "a" {
			t.Errorf("assignment after 529=%q, want a", got.Nick)
		}
	})

	t.Run("Codex reached-type sub-cap 429", func(t *testing.T) {
		clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
		c := codexController(t, clock, io.Discard, nil, "a", "b")
		c.workerConcurrency = 2
		if b, _, exhausted := c.ResolveWorker("agent-a"); exhausted || b.Nick != "a" {
			t.Fatalf("initial assignment=%q, exhausted=%v, want a", b.Nick, exhausted)
		}
		resp := resp429Codex(c.resolve(t, "a"), clock,
			codexWin{percent: "9", minutes: "300", resetIn: 2 * time.Hour},
			codexWin{percent: "60", minutes: "10080", resetIn: 79 * time.Hour},
			quota.CodexReachedTypeUsageLimit)
		if err := c.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}
		if got, _, exhausted := c.ResolveWorker("agent-a"); exhausted || got.Nick != "a" {
			t.Errorf("assignment after transient 429=%q, exhausted=%v, want a", got.Nick, exhausted)
		}
	})
}

func TestWorkerAffinity_persistsAndDropsUnavailableAssignments(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	reg := workerRegistryWithConcurrency(t, 2, "a", "b")
	p := NewPools(reg, nil, clock.now, io.Discard)
	if got, _, _, _ := p.RouteWorker("auto", "agent-a"); got.Nick != "a" {
		t.Fatalf("agent-a initial assignment=%q, want a", got.Nick)
	}
	if got, _, _, _ := p.RouteWorker("auto", "agent-b"); got.Nick != "b" {
		t.Fatalf("agent-b initial assignment=%q, want b", got.Nick)
	}
	saved := p.PersistState()
	encoded, err := json.Marshal(saved)
	if err != nil {
		t.Fatalf("marshal persisted routing state: %v", err)
	}
	var restored map[string]PoolPersistState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("unmarshal persisted routing state: %v", err)
	}
	c2 := NewPools(reg, nil, clock.now, io.Discard)
	c2.LoadPersistState(restored)
	if got, _, _, _ := c2.RouteWorker("auto", "agent-b"); got.Nick != "b" {
		t.Errorf("restored healthy non-sticky assignment=%q, want b", got.Nick)
	}

	state := map[string]PoolPersistState{"auto": {
		Sticky:         "a",
		Exhausted:      map[string]time.Time{"a": clock.now().Add(time.Hour)},
		WorkerAffinity: map[string]string{"agent-a": "a", "agent-b": "b"},
		WorkerCursor:   "a",
	}}
	c3 := NewPools(reg, nil, clock.now, io.Discard)
	c3.LoadPersistState(state)
	if got, _, _, _ := c3.RouteWorker("auto", "agent-a"); got.Nick != "b" {
		t.Errorf("restored stale agent-a assignment=%q, want reassignment to b", got.Nick)
	}
	c := c3.byPool["auto"]
	c.mu.Lock()
	if got := c.workerAffinity["agent-a"]; got != "b" {
		t.Errorf("reassigned agent-a mapping=%q, want b", got)
	}
	if got := c.workerAffinity["agent-b"]; got != "b" {
		t.Errorf("load changed healthy mapping agent-b=%q, want b", got)
	}
	if c.workerCursor != "b" {
		t.Errorf("load cursor=%q, want first healthy member b", c.workerCursor)
	}
	c.mu.Unlock()
}

func TestWorkerAffinity_runtimeDisableReconcilesAssignments(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	p := NewPools(workerRegistryWithConcurrency(t, 2, "a", "b"), nil, clock.now, io.Discard)
	if got, _, _, _ := p.RouteWorker("auto", "agent-a"); got.Nick != "a" {
		t.Fatalf("agent-a initial assignment=%q, want a", got.Nick)
	}
	if got, _, _, _ := p.RouteWorker("auto", "agent-b"); got.Nick != "b" {
		t.Fatalf("agent-b initial assignment=%q, want b", got.Nick)
	}
	if status, err := p.SetMemberDisabled("auto", "a", true); status != http.StatusOK || err != nil {
		t.Fatalf("disable a: status=%d err=%v", status, err)
	}
	if got, _, _, _ := p.RouteWorker("auto", "agent-a"); got.Nick != "b" {
		t.Errorf("agent-a after disabling a = %q, want b", got.Nick)
	}
	if got, _, _, _ := p.RouteWorker("auto", "agent-b"); got.Nick != "b" {
		t.Errorf("unaffected agent-b after disable = %q, want b", got.Nick)
	}
	if status, err := p.SetMemberDisabled("auto", "a", false); status != http.StatusOK || err != nil {
		t.Fatalf("enable a: status=%d err=%v", status, err)
	}
	if status, err := p.RemoveMember("auto", "b"); status != http.StatusOK || err != nil {
		t.Fatalf("remove b: status=%d err=%v", status, err)
	}
	if got, _, _, _ := p.RouteWorker("auto", "agent-b"); got.Nick != "a" {
		t.Errorf("agent-b after removing b = %q, want a", got.Nick)
	}
}
