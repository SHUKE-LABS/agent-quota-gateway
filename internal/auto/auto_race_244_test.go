package auto

import (
	"sync"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// TestModifyResponse_raceWithMutations is the -race regression for issue
// #244: ModifyResponse previously called isGenuineExhaustionSignal which
// read c.members (via indexOf / backendAt) without c.mu, racing
// reconcileLocked's whole-header reassignment of c.members on every
// AddMember / RemoveMember runtime mutation. The torn-header race could
// panic inside httputil.ReverseProxy.ServeHTTP — http.Server recovers and
// drops the connection, leaving no gateway log line. Separately, when the
// racing read yielded idx<0, the store-snapshot branch was skipped
// entirely and a genuine 429 was misclassified as not-genuine.
//
// The fix resolves the member entry under c.mu in ModifyResponse and
// passes it down to isGenuineExhaustionSignal, so this test now exercises
// the locked path against the same mutator workload.
//
// Failure modes the test catches when run with -race:
//   - data race report on c.members (would happen on the old code)
//   - index-out-of-range panic from the racy backendAt (auto.go:2885)
//   - any unexpected error / panic surfaced via t.Errorf
//
// Membership state is checked after the race window so a torn slice
// header leaves evidence even if the panic was swallowed by the proxy.
func TestModifyResponse_raceWithMutations(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "AUTO_BACKEND_A": "cred-a",
		backend.EnvPrefix + "AUTO_BACKEND_B": "cred-b",
		backend.EnvPrefix + "AUTO_BASE_URL":  "https://api.anthropic.com",
	}
	p := loadMovePools(t, clock, env)
	// Wire a store into the Pools so AddMember → NewController sees one;
	// exercises the locked store-snapshot branch in ModifyResponse.
	store := quota.NewStore()
	p.store = store

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Modifying goroutine: drive 429 responses through ModifyResponse on the
	// currently active member. The active member moves as AddMember/Remove
	// churn the pool; we always resolve the current active nick under the
	// controller's mu so the response carries a valid backend.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			c := p.byPool["auto"]
			c.mu.Lock()
			active := c.curNick
			var regSnap *backend.Registry
			if active != "" {
				regSnap = c.reg
			}
			c.mu.Unlock()
			if regSnap == nil || active == "" {
				continue
			}
			b, ok := regSnap.ResolveIn("auto", active)
			if !ok {
				continue
			}
			if err := c.ModifyResponse(resp429(b, clock, time.Hour)); err != nil {
				t.Errorf("ModifyResponse: %v", err)
				return
			}
		}
	}()

	// Mutator goroutines: AddMember / RemoveMember cycling through extra
	// nicks. This is the workload that drives reconcileLocked's c.members
	// swap and produces the torn-header window the old code raced on.
	extras := []string{"c", "d", "e"}
	wg.Add(2)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			nick := extras[i%len(extras)]
			i++
			// AddMember is a no-op (returns 409) if already present; that
			// is expected churn — the 409 path does not touch c.members.
			_, _ = p.AddMember("auto", nick, "cred-"+nick, "", nil)
		}
	}()
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			nick := extras[i%len(extras)]
			i++
			// 200 or 400 ("not found") are both expected outcomes
			// depending on whether the add-then-remove cycle has the
			// member present; either is fine.
			_, _ = p.RemoveMember("auto", nick)
		}
	}()

	// Run the race window long enough to overlap many reconcile cycles.
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	// After the churn settles, the controller must still be coherent:
	// every member in c.members must also resolve in c.reg (so the
	// member slice and the registry agree — the invariant #244 tore on
	// the old code). curNick must resolve if set (an empty curNick is
	// acceptable if the pool genuinely emptied during the window — the
	// invariant we are pinning is internal consistency, not stickiness).
	c := p.byPool["auto"]
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.curNick != "" {
		if _, ok := c.backendByNickLocked(c.curNick); !ok {
			t.Fatalf("curNick=%q not in c.members — torn slice header", c.curNick)
		}
	}
	regNicks := map[string]bool{}
	for _, n := range c.reg.PoolNicks("auto") {
		regNicks[n] = true
	}
	for _, m := range c.members {
		if !regNicks[m.Nick] {
			t.Errorf("c.members contains %q but c.reg does not — torn slice", m.Nick)
		}
	}
}
