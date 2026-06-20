package auto

import (
	"net/http"
	"sync"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

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

	if status, err := p.AddPool("New_Pool", "https://new.example", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v, want 201", status, err)
	}
	if !poolNames(t, p)["new-pool"] {
		t.Fatalf("new-pool not in config view after creation")
	}
	if got := poolMembers(t, p, "new-pool"); len(got) != 0 {
		t.Errorf("new pool has members %v, want empty", got)
	}

	// Add-member works immediately, with base_url omitted (pool default used).
	if status, err := p.AddMember("new-pool", "a", "cred-a", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember after AddPool: status=%d err=%v, want 200", status, err)
	}
	am, ok := addedMember(t, p, "new-pool", "a")
	if !ok {
		t.Fatalf("member a not added to new-pool")
	}
	if am.BaseURL != "https://new.example" {
		t.Errorf("member base_url=%q, want pool default https://new.example", am.BaseURL)
	}
}

// TestAddPool_conflictEnvPool proves a runtime pool cannot shadow an env pool.
func TestAddPool_conflictEnvPool(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("src", "https://x.example", "plain"); status != http.StatusConflict || err == nil {
		t.Fatalf("AddPool env collision: status=%d err=%v, want 409", status, err)
	}
}

// TestAddPool_conflictRuntimePool proves the same name cannot be created twice.
func TestAddPool_conflictRuntimePool(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("dup", "https://x.example", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool first: status=%d err=%v, want 201", status, err)
	}
	if status, err := p.AddPool("dup", "https://y.example", ""); status != http.StatusConflict || err == nil {
		t.Fatalf("AddPool duplicate: status=%d err=%v, want 409", status, err)
	}
}

// TestAddPool_rejectsBadInput proves missing/invalid base_url and unsupported
// mode are rejected with 400.
func TestAddPool_rejectsBadInput(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	cases := []struct {
		name, baseURL, mode string
	}{
		{"a", "", "plain"},                     // missing base_url
		{"b", "not-a-url", "plain"},            // invalid base_url (no scheme/host)
		{"c", "https://x.example", "balanced"}, // unsupported mode
		{"", "https://x.example", "plain"},     // empty name
	}
	for _, tc := range cases {
		if status, err := p.AddPool(tc.name, tc.baseURL, tc.mode); status != http.StatusBadRequest || err == nil {
			t.Errorf("AddPool(%q,%q,%q): status=%d err=%v, want 400", tc.name, tc.baseURL, tc.mode, status, err)
		}
	}
}

// TestAddPool_persistRoundTrip proves a runtime pool round-trips through
// PersistAddedPools / LoadAddedPools as a clean slate (no members), excludes
// env pools from the persisted set, and that the reloaded pool's default
// base_url backs a credential-optional add.
func TestAddPool_persistRoundTrip(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", "https://rt.example", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember: status=%d err=%v", status, err)
	}

	specs := p.PersistAddedPools()
	if _, ok := specs["src"]; ok {
		t.Errorf("PersistAddedPools included env pool src: %v", specs)
	}
	spec, ok := specs["rt"]
	if !ok {
		t.Fatalf("PersistAddedPools missing runtime pool rt: %v", specs)
	}
	if spec.BaseURL != "https://rt.example" {
		t.Errorf("persisted base_url=%q, want https://rt.example", spec.BaseURL)
	}

	// Re-instantiate into a fresh Pools (same env registry).
	clock2 := newMoveClock()
	p2 := loadMovePools(t, clock2, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	p2.LoadAddedPools(specs)
	if !poolNames(t, p2)["rt"] {
		t.Fatalf("rt not re-instantiated after LoadAddedPools")
	}
	if got := poolMembers(t, p2, "rt"); len(got) != 0 {
		t.Errorf("re-instantiated rt has members %v, want clean slate", got)
	}
	// Persisting state over an empty re-instantiated pool must not panic.
	_ = p2.PersistState()

	// The reloaded pool's default base_url backs a credential-optional add.
	if status, err := p2.AddMember("rt", "b", "cred-b", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember to reloaded rt: status=%d err=%v", status, err)
	}
	am, ok := addedMember(t, p2, "rt", "b")
	if !ok || am.BaseURL != "https://rt.example" {
		t.Errorf("reloaded pool default base_url not applied: ok=%v am=%+v", ok, am)
	}
}

// TestRuntimePool_stickyPreservedAcrossRestart proves that the active
// runtime-added member of a member-less pool survives a persist/reload cycle.
// The reload follows the PRODUCTION order (LoadAddedPools -> LoadPersistState ->
// LoadRuntimeConfig, per cmd/agent-quota-gateway/main.go), under which loadState
// runs before added members exist; the deferred sticky must still re-anchor on
// the pre-restart member rather than reanchorLocked's first-healthy pick (#109).
func TestRuntimePool_stickyPreservedAcrossRestart(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", "https://rt.example", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: status=%d err=%v", status, err)
	}
	if _, _, known, _ := p.Route("rt"); !known {
		t.Fatalf("Route(rt) after AddMember a: known=false")
	}
	if status, err := p.AddMember("rt", "b", "cred-b", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember b: status=%d err=%v", status, err)
	}
	// "a" is the active sticky; adding "b" must not switch it.
	if cur, ok := p.Current("rt"); !ok || cur.Pool != "rt" || cur.Nick != "a" {
		t.Fatalf("Current(rt) before restart: %+v ok=%v, want Pool=rt Nick=a", cur, ok)
	}

	addedPools := p.PersistAddedPools()
	persistState := p.PersistState()
	runtimeConfig := p.PersistRuntimeConfig()

	// Re-instantiate in the production load order.
	clock2 := newMoveClock()
	p2 := loadMovePools(t, clock2, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	p2.LoadAddedPools(addedPools)
	p2.LoadPersistState(persistState)
	p2.LoadRuntimeConfig(runtimeConfig)

	// "a" must still be the active sticky after restart.
	if cur, ok := p2.Current("rt"); !ok || cur.Pool != "rt" || cur.Nick != "a" {
		t.Errorf("Current(rt) after restart: %+v ok=%v, want Pool=rt Nick=a — active added member lost", cur, ok)
	}
}

// TestAddPool_loadAddedPoolsDropsEnvCollision proves a persisted runtime pool
// whose name has since reappeared as an env pool is dropped (env wins).
func TestAddPool_loadAddedPoolsDropsEnvCollision(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	// "src" is env-defined; a stale runtime spec for it must not overwrite it.
	p.LoadAddedPools(map[string]AddedPoolSpec{"src": {BaseURL: "https://stale.example"}})
	c, ok := p.controller("src")
	if !ok {
		t.Fatalf("env pool src missing")
	}
	c.mu.Lock()
	got := c.defaultBaseURL
	c.mu.Unlock()
	if got != "" {
		t.Errorf("env pool src got runtime defaultBaseURL=%q, want unset", got)
	}
}

// TestAddPool_routeEmptyPoolExhausted proves routing to a member-less runtime
// pool returns an honest exhausted result instead of panicking.
func TestAddPool_routeEmptyPoolExhausted(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("empty", "https://e.example", ""); status != http.StatusCreated || err != nil {
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
			_, _ = p.AddPool(name, "https://x.example", "")
		}(i)
	}
	// Readers: exercise the map-ranging and single-lookup paths concurrently.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.EffectiveConfig()
			_ = p.AllPoolStatuses(store)
			_, _, _, _ = p.Route("src")
			_ = p.PersistAddedPools()
			_ = p.PersistState()
		}()
	}
	wg.Wait()
}
