package auto

import (
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// loadPoolsWithStore is loadMovePools wired to a live quota store, so the
// store-exhaustion signal (what the poller fills for poller-tracked members) is
// active for the runtime-pool tests below.
func loadPoolsWithStore(t *testing.T, clock *fixedClock, store *quota.Store, env map[string]string) *Pools {
	t.Helper()
	scrubPoolEnv(t)
	for k, v := range env {
		t.Setenv(k, v)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return NewPools(reg, store, clock.now, io.Discard)
}

// TestRuntimePool_failsOverOnStoreExhaustion proves a pool created at runtime
// fails over between its members on the store-exhaustion signal exactly as an
// env-declared pool does. The poller — now reading its pool set dynamically
// (issue #202) — is what fills that store for poller-tracked members (Z.AI),
// so without this the runtime pool would never detect exhaustion or fail over.
func TestRuntimePool_failsOverOnStoreExhaustion(t *testing.T) {
	clock := newMoveClock()
	store := quota.NewStore()
	p := loadPoolsWithStore(t, clock, store, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})

	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://api.z.ai", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "b", "cred-b", "https://api.z.ai", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember b: status=%d err=%v", status, err)
	}

	// "a" is the active sticky (first member added). The store reports it fully
	// consumed — util 1.0, no status: the shape a poller-tracked Z.AI member
	// produces — so Route must fail the runtime pool over to the healthy "b".
	c, ok := p.controller("rt")
	if !ok {
		t.Fatalf("controller(rt) not found")
	}
	putUtil(t, store, c, "a", 1.0, clock.now().Add(time.Hour))

	b, _, known, exhausted := p.Route("rt")
	if !known {
		t.Fatalf("Route(rt): known=false")
	}
	if exhausted {
		t.Fatalf("Route(rt): exhausted=true, want false (b is healthy)")
	}
	if b.Nick != "b" {
		t.Errorf("Route(rt) picked %q, want b (a is store-exhausted)", b.Nick)
	}
}

// TestRuntimePool_preemptedAfterSetPriority proves a pool created at runtime and
// then given a priority order via SetPriority is picked up by the preemptor,
// which reads the pool set fresh each tick (issue #202), and preempted back to
// its healthy higher-priority member. The preemptor is built BEFORE the pool
// exists, so this asserts dynamic pickup rather than a startup snapshot.
func TestRuntimePool_preemptedAfterSetPriority(t *testing.T) {
	clock := newMoveClock()
	store := quota.NewStore()
	p := loadPoolsWithStore(t, clock, store, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})

	// Preemptor constructed while only the env pool "src" exists.
	pre := NewPreemptor(p, store, 0, clock.now, io.Discard)

	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://api.z.ai", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "b", "cred-b", "https://api.z.ai", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember b: status=%d err=%v", status, err)
	}
	if status, err := p.SetPriority("rt", []string{"a", "b"}); status != http.StatusOK || err != nil {
		t.Fatalf("SetPriority: status=%d err=%v", status, err)
	}

	// Model the pool having fallen over to the lower-priority "b"; "a" is healthy.
	c, ok := p.controller("rt")
	if !ok {
		t.Fatalf("controller(rt) not found")
	}
	c.setCur("b")

	// One tick: the preemptor (built before rt existed) must now see rt and
	// switch back to the healthy higher-priority "a".
	pre.tick()
	if got := c.Current(); got != "a" {
		t.Errorf("Current(rt) = %q, want a (runtime pool preempted back to healthy higher)", got)
	}
}

// poolNames returns the set of pool names present in EffectiveConfig.
func poolNames(t *testing.T, p *Pools) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, v := range p.EffectiveConfig() {
		out[v.Pool] = true
	}
	return out
}

// TestAddPool_createsPlainPool proves a runtime pool is created, surfaces in
// the config view with no members, and accepts a member immediately.
func TestAddPool_createsPlainPool(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})

	if status, err := p.AddPool("New_Pool", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v, want 201", status, err)
	}
	if !poolNames(t, p)["new-pool"] {
		t.Fatalf("new-pool not in config view after creation")
	}
	if got := poolMembers(t, p, "new-pool"); len(got) != 0 {
		t.Errorf("new pool has members %v, want empty", got)
	}

	// Add-member works immediately. base_url is supplied explicitly because a
	// brand-new pool has no default to fall back to (issue #172).
	if status, err := p.AddMember("new-pool", "a", "cred-a", "https://a.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember after AddPool: status=%d err=%v, want 200", status, err)
	}
	am, ok := addedMember(t, p, "new-pool", "a")
	if !ok {
		t.Fatalf("member a not added to new-pool")
	}
	if am.BaseURL != "https://a.example" {
		t.Errorf("member base_url=%q, want explicit https://a.example", am.BaseURL)
	}
}

// TestAddPool_conflictEnvPool proves a runtime pool cannot shadow an env pool.
func TestAddPool_conflictEnvPool(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("src", "plain"); status != http.StatusConflict || err == nil {
		t.Fatalf("AddPool env collision: status=%d err=%v, want 409", status, err)
	}
}

// TestAddPool_conflictRuntimePool proves the same name cannot be created twice.
func TestAddPool_conflictRuntimePool(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("dup", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool first: status=%d err=%v, want 201", status, err)
	}
	if status, err := p.AddPool("dup", ""); status != http.StatusConflict || err == nil {
		t.Fatalf("AddPool duplicate: status=%d err=%v, want 409", status, err)
	}
}

// TestAddPool_rejectsBadInput proves an empty name and an unsupported mode are
// rejected with 400. (base_url is no longer a create-pool field — see
// issue #172 / AddPool signature; URL validation lives in AddMember.)
func TestAddPool_rejectsBadInput(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	cases := []struct {
		name, mode string
	}{
		{"a", "balanced"}, // unsupported mode
		{"", "plain"},     // empty name
	}
	for _, tc := range cases {
		if status, err := p.AddPool(tc.name, tc.mode); status != http.StatusBadRequest || err == nil {
			t.Errorf("AddPool(%q,%q): status=%d err=%v, want 400", tc.name, tc.mode, status, err)
		}
	}
}

// TestAddPool_persistRoundTrip proves a runtime pool and its member round-trip
// through the config (issue #198 folds runtime pools into the config file), and
// that the env pool remains alongside it.
func TestAddPool_persistRoundTrip(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://a.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember: status=%d err=%v", status, err)
	}

	// Restart via the config round-trip: rt (and its member a) plus the env
	// pool src all survive.
	p2 := reloadViaConfig(t, p)
	if !poolNames(t, p2)["rt"] {
		t.Fatalf("rt not present after restart")
	}
	if !poolNames(t, p2)["src"] {
		t.Errorf("env pool src missing after restart")
	}
	if got := poolMembers(t, p2, "rt"); !got["a"] {
		t.Errorf("rt member a lost after restart: %v", got)
	}
	if m, ok := addedMember(t, p2, "rt", "a"); !ok || m.Credential != "cred-a" || m.BaseURL != "https://a.example" {
		t.Errorf("rt member a spec after restart = %+v ok=%v, want cred-a/https://a.example", m, ok)
	}
	_ = p2.PersistState()
}

// TestRuntimePool_stickyPreservedAcrossRestart proves that the active member
// of a runtime-created pool survives a config round-trip restart (the pool and
// its members are config now, and the sticky pointer is restored from the
// observation state).
func TestRuntimePool_stickyPreservedAcrossRestart(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://a.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: status=%d err=%v", status, err)
	}
	if _, _, known, _ := p.Route("rt"); !known {
		t.Fatalf("Route(rt) after AddMember a: known=false")
	}
	if status, err := p.AddMember("rt", "b", "cred-b", "https://b.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember b: status=%d err=%v", status, err)
	}
	// "a" is the active sticky; adding "b" must not switch it.
	if cur, ok := p.Current("rt"); !ok || cur.Pool != "rt" || cur.Nick != "a" {
		t.Fatalf("Current(rt) before restart: %+v ok=%v, want Pool=rt Nick=a", cur, ok)
	}

	p2 := reloadViaConfig(t, p)

	// "a" must still be the active sticky after restart.
	if cur, ok := p2.Current("rt"); !ok || cur.Pool != "rt" || cur.Nick != "a" {
		t.Errorf("Current(rt) after restart: %+v ok=%v, want Pool=rt Nick=a — active member lost", cur, ok)
	}
}

// TestAddPool_routeEmptyPoolExhausted proves routing to a member-less runtime
// pool returns an honest exhausted result instead of panicking.
func TestAddPool_routeEmptyPoolExhausted(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("empty", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	_, _, known, exhausted := p.Route("empty")
	if !known {
		t.Errorf("Route(empty): known=false, want true")
	}
	if !exhausted {
		t.Errorf("Route(empty): exhausted=false, want true (member-less pool)")
	}
	// Current/CurrentBackend over an empty pool must not panic.
	if b, ok := p.Current("empty"); !ok || b.Nick != "" {
		t.Errorf("Current(empty)=%+v ok=%v, want zero backend", b, ok)
	}
}

// TestAddPool_concurrentWithReaders is a race-detector guard: AddPool mutates
// the byPool map while the hot-path readers iterate it. Run with -race.
func TestAddPool_concurrentWithReaders(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})

	store := quota.NewStore()
	const n = 50
	var wg sync.WaitGroup

	// Writers: each creates a distinct pool.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "p" + string(rune('a'+i%26)) + string(rune('a'+i/26))
			_, _ = p.AddPool(name, "")
		}(i)
	}
	// Readers: exercise the map-ranging and single-lookup paths concurrently.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.EffectiveConfig()
			_ = p.AllPoolStatuses(store, nil)
			_, _, _, _ = p.Route("src")
			_ = p.CurrentRegistry()
			_ = p.PersistState()
		}()
	}
	wg.Wait()
}
