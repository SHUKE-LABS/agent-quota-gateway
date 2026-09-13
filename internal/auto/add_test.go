package auto

import (
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
)

// addedMember reads a member's resolved entry directly from the controller's
// member collection so tests can prove what the config produced (credential +
// resolved base_url).
func addedMember(t *testing.T, p *Pools, pool, nick string) (memberEntry, bool) {
	t.Helper()
	c := p.byPool[pool]
	c.mu.Lock()
	defer c.mu.Unlock()
	normalized := backend.NormalizeName(nick)
	for _, m := range c.members {
		if m.Nick == normalized {
			return m, true
		}
	}
	return memberEntry{}, false
}

// TestAdd_resolvesKnownCredentialAndBaseURL proves that adding a known
// subscription by name, with credential and base_url both omitted, resolves
// both from the pool that already holds it and persists the concrete values.
func TestAdd_resolvesKnownCredentialAndBaseURL(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_SHARED": "cred-shared",
		backend.EnvPrefix + "SRC_BASE_URL":       "https://src.example",
		backend.EnvPrefix + "DST_BACKEND_X":      "cred-x",
	})

	if status, err := p.AddMember("dst", "shared", "", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember dst shared (resolve): status=%d err=%v, want 200", status, err)
	}
	am, ok := addedMember(t, p, "dst", "shared")
	if !ok {
		t.Fatalf("shared not added to dst")
	}
	if am.Credential != "cred-shared" {
		t.Errorf("resolved credential=%q, want cred-shared", am.Credential)
	}
	if am.BaseURL != "https://src.example" {
		t.Errorf("resolved+persisted base_url=%q, want https://src.example", am.BaseURL)
	}
}

// TestAdd_bijectionRejectsDifferentCredential proves the nick↔credential
// bijection is now enforced on runtime mutations too (issue #198): adding a
// nick that already exists in another pool, but with a different credential,
// is rejected. Under the old overlay this slipped through (runtime adds were
// not re-validated), which is exactly the drift #198's copy-on-write model
// closes — so the cross-pool "ambiguous credential" state can no longer arise.
func TestAdd_bijectionRejectsDifferentCredential(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "ONE_BACKEND_SHARED": "cred-1",
		backend.EnvPrefix + "TWO_BACKEND_OTHER":  "cred-other",
		backend.EnvPrefix + "DST_BACKEND_X":      "cred-x",
	})

	// "shared" already exists in ONE with cred-1; adding it to TWO with a
	// different credential violates the bijection and is rejected.
	if status, err := p.AddMember("two", "shared", "cred-2", "", nil); status != http.StatusBadRequest || err == nil {
		t.Fatalf("different-credential add: status=%d err=%v, want 400 (bijection)", status, err)
	}
	if _, ok := addedMember(t, p, "two", "shared"); ok {
		t.Errorf("shared was added to two despite the bijection violation")
	}
}

// TestAdd_unknownNickRequiresCredential proves that an omitted credential for a
// nick unknown in every other pool is rejected.
func TestAdd_unknownNickRequiresCredential(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "DST_BACKEND_X": "cred-x",
	})

	if status, err := p.AddMember("dst", "ghost", "", "", nil); status != http.StatusBadRequest || err == nil {
		t.Fatalf("unknown nick: status=%d err=%v, want 400", status, err)
	}
}

// TestAdd_ambiguousBaseURLRejected proves credential and base_url resolve
// independently: a consistent credential resolves, but a base_url that differs
// across pools is rejected (rather than silently picking one).
//
// As above, the static bijection forbids the same nick in two pools with
// different credentials (or with the same credential but different base_urls
// is also a non-issue since the credential matches and resolves), so the
// second occurrence is seeded via the runtime add path.
func TestAdd_ambiguousBaseURLRejected(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "ONE_BACKEND_SHARED": "cred-same",
		backend.EnvPrefix + "ONE_BASE_URL":       "https://a.example",
		backend.EnvPrefix + "TWO_BACKEND_OTHER":  "cred-other",
		backend.EnvPrefix + "DST_BACKEND_X":      "cred-x",
	})

	// Seed "shared" in a second pool with the same credential but a differing
	// base_url.
	if status, err := p.AddMember("two", "shared", "cred-same", "https://b.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("seed AddMember two shared: status=%d err=%v, want 200", status, err)
	}

	// Credential resolves consistently to cred-same, but base_url is ambiguous
	// (a.example vs b.example), so the add is rejected.
	if status, err := p.AddMember("dst", "shared", "", "", nil); status != http.StatusBadRequest || err == nil {
		t.Fatalf("ambiguous base_url: status=%d err=%v, want 400", status, err)
	}
}

// TestAdd_newNickUsesPoolDefaultBaseURL proves that a brand-new nick with an
// omitted base_url persists the target pool's default base_url — never an empty
// string — so the record is self-describing.
func TestAdd_newNickUsesPoolDefaultBaseURL(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "DST_BACKEND_X": "cred-x",
		backend.EnvPrefix + "DST_BASE_URL":  "https://dst.example",
	})

	if status, err := p.AddMember("dst", "fresh", "cred-fresh", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember dst fresh: status=%d err=%v, want 200", status, err)
	}
	am, ok := addedMember(t, p, "dst", "fresh")
	if !ok {
		t.Fatalf("fresh not added to dst")
	}
	if am.BaseURL == "" {
		t.Errorf("persisted base_url is empty, want the pool default")
	}
	if am.BaseURL != "https://dst.example" {
		t.Errorf("persisted base_url=%q, want https://dst.example (pool default)", am.BaseURL)
	}
}

// TestAdd_intoPriorityRequiresPlacement proves adding to a priority pool with no
// existing slot requires an explicit placement that includes the new nick,
// reusing the move path's validation.
func TestAdd_intoPriorityRequiresPlacement(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
		backend.EnvPrefix + "DST_BACKEND_Q": "cred-q",
		backend.EnvPrefix + "DST_PRIORITY":  "p,q",
	}

	// Missing placement -> 400.
	p := loadMovePools(t, clock, env)
	if status, err := p.AddMember("dst", "a", "cred-a", "https://u.example", nil); status != http.StatusBadRequest || err == nil {
		t.Fatalf("missing placement: status=%d err=%v, want 400", status, err)
	}

	// Placement omitting the new nick -> 400.
	if status, err := p.AddMember("dst", "a", "cred-a", "https://u.example", []string{"p", "q"}); status != http.StatusBadRequest || err == nil {
		t.Fatalf("placement without nick: status=%d err=%v, want 400", status, err)
	}

	// Explicit placement including the new nick -> 200, placed at the top.
	if status, err := p.AddMember("dst", "a", "cred-a", "https://u.example", []string{"a", "p", "q"}); status != http.StatusOK || err != nil {
		t.Fatalf("explicit placement: status=%d err=%v, want 200", status, err)
	}
	if got := poolPriority(t, p, "dst"); len(got) == 0 || got[0] != "a" {
		t.Errorf("dst priority=%v, want a first", got)
	}
}

// TestAdd_placementRejectedOnPlainPool proves a plain target must not carry a
// placement (symmetric with the move path).
func TestAdd_placementRejectedOnPlainPool(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "DST_BACKEND_X": "cred-x",
	})
	if status, err := p.AddMember("dst", "a", "cred-a", "https://u.example", []string{"a", "x"}); status != http.StatusBadRequest || err == nil {
		t.Fatalf("placement on plain pool: status=%d err=%v, want 400", status, err)
	}
}

// TestAdd_configNickReAddAfterRemove proves that a config-derived nick that was
// removed can always be re-added without 409 (issue #185: no more static-nick
// duplicate block). The tombstone is cleared and the member rejoins with the
// supplied credential.
func TestAdd_configNickReAddAfterRemove(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "DST_BACKEND_X": "cred-x",
		backend.EnvPrefix + "DST_BACKEND_Y": "cred-y",
	})

	// Remove the config-derived nick "x" from dst.
	if status, err := p.RemoveMember("dst", "x"); status != http.StatusOK || err != nil {
		t.Fatalf("RemoveMember dst x: status=%d err=%v, want 200", status, err)
	}

	// Re-adding "x" must succeed (was removed → tombstone cleared).
	if status, err := p.AddMember("dst", "x", "cred-x", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember dst x (re-add after remove): status=%d err=%v, want 200", status, err)
	}

	// The member should be present again and selectable.
	am, ok := addedMember(t, p, "dst", "x")
	if !ok {
		t.Fatalf("re-added nick x not found in dst members")
	}
	if am.Credential != "cred-x" {
		t.Errorf("credential=%q, want cred-x", am.Credential)
	}
}

// TestAdd_memberBaseURLNotUnanimous proves that AddMember with an omitted
// base_url in a pool whose members hold different effective upstreams
// (issue #248) no longer 400s: the first member's URL is alphabetical, not
// authoritative, so it is not borrowable — the member instead falls back to
// the gateway default upstream (issue #302). The pool is set up so the
// alphabetically first member's BaseURL is the z.ai pool default; the
// assertion pins that the fallback is the registry default
// (testDefaultBaseURL), not either member's URL.
func TestAdd_memberBaseURLNotUnanimous(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		// Pool default is z.ai. "a" inherits it (BaseURL = z.ai);
		// "b" carries a per-member Anthropic override. With "new"
		// absent there is no unanimous URL to borrow.
		backend.EnvPrefix + "MIX_BASE_URL":  "https://api.z.ai/anthropic",
		backend.EnvPrefix + "MIX_BACKEND_A": "cred-a",
		backend.EnvPrefix + "MIX_BACKEND_B": "cred-b|https://api.anthropic.com",
	})

	status, err := p.AddMember("mix", "new", "cred-new", "", nil)
	if status != http.StatusOK || err != nil {
		t.Fatalf("mixed pool AddMember without base_url: status=%d err=%v, want 200", status, err)
	}
	am, ok := addedMember(t, p, "mix", "new")
	if !ok {
		t.Fatalf("new not added to mixed pool")
	}
	if am.BaseURL != testDefaultBaseURL {
		t.Errorf("fallback base_url=%q, want gateway default %q", am.BaseURL, testDefaultBaseURL)
	}
}

// TestAdd_memberBaseURLUnanimous proves the unanimity fallback still fires
// when every existing member shares one effective base_url — a
// single-provider pool is unchanged by #248. Mirrors the legacy behavior
// for the safe case so the migration is invisible to existing operators.
func TestAdd_memberBaseURLUnanimous(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		// Both members inherit the pool's z.ai default — unanimity holds.
		backend.EnvPrefix + "SAME_BASE_URL":  "https://api.z.ai/anthropic",
		backend.EnvPrefix + "SAME_BACKEND_A": "cred-a",
		backend.EnvPrefix + "SAME_BACKEND_B": "cred-b",
	})

	if status, err := p.AddMember("same", "new", "cred-new", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("unanimous pool AddMember without base_url: status=%d err=%v, want 200", status, err)
	}
	am, ok := addedMember(t, p, "same", "new")
	if !ok {
		t.Fatalf("new not added to same pool")
	}
	if am.BaseURL != "https://api.z.ai/anthropic" {
		t.Errorf("inherited base_url=%q, want https://api.z.ai/anthropic", am.BaseURL)
	}
}

// TestCreatePoolWithMember_fallsBackToGatewayDefault proves the create-pool
// half of the issue #302 contract: the combined create (pool + first member)
// with a new nick and no base_url no longer 400s — the first member lands on
// the gateway default upstream, so bootstrapping a fresh pool is a one-call
// operation.
func TestCreatePoolWithMember_fallsBackToGatewayDefault(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	status, err := p.CreatePoolWithMember("fresh", "", "n", "cred-n", "", nil)
	if status != http.StatusCreated || err != nil {
		t.Fatalf("CreatePoolWithMember(no-baseurl): status=%d err=%v, want 201", status, err)
	}
	am, ok := addedMember(t, p, "fresh", "n")
	if !ok {
		t.Fatalf("n not added to fresh pool")
	}
	if am.BaseURL != testDefaultBaseURL {
		t.Errorf("fallback base_url=%q, want gateway default %q", am.BaseURL, testDefaultBaseURL)
	}
}

// TestAdd_defaultBaseURLResolverCallTime proves the resolver consulted by the
// fallback is called per mutation, not captured at startup (issue #302): a
// resolver reading ANTHROPIC_BASE_URL sees the value current at each call, so
// two consecutive adds observe different defaults when the env changes between
// them. The production wiring (env-only mode) is main.envDefaultBaseURL; this
// pins the Pools-side mechanism it plugs into.
func TestAdd_defaultBaseURLResolverCallTime(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	p.SetDefaultBaseURL(func() string {
		if v := os.Getenv(config.EnvAnthropicBaseURL); v != "" {
			return v
		}
		return testDefaultBaseURL
	})

	t.Setenv(config.EnvAnthropicBaseURL, "https://alpha.example.com")
	if status, err := p.AddPool("pa", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool pa: status=%d err=%v, want 201", status, err)
	}
	if status, err := p.AddMember("pa", "a1", "cred-a1", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember pa a1: status=%d err=%v, want 200", status, err)
	}
	if am, ok := addedMember(t, p, "pa", "a1"); !ok || am.BaseURL != "https://alpha.example.com" {
		t.Fatalf("a1 base_url=%+v ok=%v, want https://alpha.example.com", am, ok)
	}

	t.Setenv(config.EnvAnthropicBaseURL, "https://beta.example.com")
	if status, err := p.AddPool("pb", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool pb: status=%d err=%v, want 201", status, err)
	}
	if status, err := p.AddMember("pb", "b1", "cred-b1", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember pb b1: status=%d err=%v, want 200", status, err)
	}
	if am, ok := addedMember(t, p, "pb", "b1"); !ok || am.BaseURL != "https://beta.example.com" {
		t.Fatalf("b1 base_url=%+v ok=%v, want https://beta.example.com", am, ok)
	}
}

// rebuildPoolsFromSpec simulates a restart under the config-single-source
// model (issue #198): the current registry is round-tripped through Spec
// and a fresh Pools is built from it. Mirrors cmd/agent-quota-gateway
// `reloadPools` but lives in the package so add_test.go can simulate a
// restart mid-test without depending on cmd_test-only helpers.
func rebuildPoolsFromSpec(t *testing.T, p *Pools) *Pools {
	t.Helper()
	reg, err := backend.BuildFromSpec(p.CurrentRegistry().Spec(), testDefaultBaseURL)
	if err != nil {
		t.Fatalf("rebuild registry from config: %v", err)
	}
	return NewPools(reg, nil, p.now, io.Discard)
}

// TestAdd_resolvesAcrossRestartFromEnv pins issue #303 AC1 (source=env): a
// nick declared via env on the initial load survives a restart-by-Spec and
// remains resolvable for a bodyless add to a fresh pool. The reset of env
// between the two loads is the actual env-only restart behavior (issue #198:
// env is consulted exactly once, at the very first boot).
func TestAdd_resolvesAcrossRestartFromEnv(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "A_BACKEND_CCZ":   "cred-ccz",
		backend.EnvPrefix + "A_BASE_URL":     "https://a.example",
		backend.EnvPrefix + "B_BACKEND_OTHER": "cred-other",
	}
	p := loadMovePools(t, clock, env)

	// Simulate restart: rebuild from the current spec. A second backend.Load
	// would be a no-op in production (issue #198), but going through Spec
	// exercises the same write-through path a real restart does.
	p = rebuildPoolsFromSpec(t, p)

	if status, err := p.AddMember("b", "ccz", "", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember b ccz (resolve): status=%d err=%v, want 200", status, err)
	}
	am, ok := addedMember(t, p, "b", "ccz")
	if !ok {
		t.Fatalf("ccz not added to b")
	}
	if am.Credential != "cred-ccz" {
		t.Errorf("resolved credential=%q, want cred-ccz", am.Credential)
	}
	if am.BaseURL != "https://a.example" {
		t.Errorf("resolved base_url=%q, want https://a.example", am.BaseURL)
	}
}

// TestAdd_resolvesAcrossRestartFromRuntimeMutation pins issue #303 AC1
// (source=runtime + aqg.json): a nick first added to pool B at runtime —
// which writes through to the registry's Spec (issue #198) — is still
// resolvable from a *third* pool after the registry is rebuilt from that
// Spec. The simulation is the env-only restart loop with a config file
// already on disk: the runtime mutation lands in aqg.json, and the next
// boot reads it back as the source of truth.
func TestAdd_resolvesAcrossRestartFromRuntimeMutation(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "A_BACKEND_CCZ":   "cred-ccz",
		backend.EnvPrefix + "A_BASE_URL":     "https://a.example",
		backend.EnvPrefix + "B_BACKEND_OTHER": "cred-other",
		backend.EnvPrefix + "C_BACKEND_D":     "cred-d",
	}
	p := loadMovePools(t, clock, env)

	// Runtime add: ccz → b (resolves from a). The new registry carries ccz
	// in both a and b.
	if status, err := p.AddMember("b", "ccz", "", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("seed AddMember b ccz: status=%d err=%v, want 200", status, err)
	}

	// Restart via Spec — mirrors aqg.json round-trip.
	p = rebuildPoolsFromSpec(t, p)

	// Bodyless add of ccz to c: crossPoolResolve scans a and b, finds
	// 1 distinct credential + 1 distinct URL → 200.
	if status, err := p.AddMember("c", "ccz", "", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember c ccz (after restart): status=%d err=%v, want 200", status, err)
	}
	am, ok := addedMember(t, p, "c", "ccz")
	if !ok {
		t.Fatalf("ccz not added to c after restart")
	}
	if am.Credential != "cred-ccz" {
		t.Errorf("resolved credential=%q, want cred-ccz", am.Credential)
	}
	if am.BaseURL != "https://a.example" {
		t.Errorf("resolved base_url=%q, want https://a.example", am.BaseURL)
	}
}

// TestAdd_envOnlyRuntimeMutationNotSurvivesRestart pins issue #303 AC1
// (source=runtime-only, not persisted) and the improved error message
// contract: in env-only mode (no aqg.json), a runtime-only nick does not
// survive the next env-only restart, so a subsequent bodyless add of the
// same nick to a fresh pool must surface the (0 creds, 0 URLs) result
// with the registry-scope + recipe message. ccz is intentionally NOT in
// env — it is a pure runtime declaration. The "rebuild" here is the
// env-only restart loop: a fresh loadMovePools reads env again, and the
// runtime intent is gone.
func TestAdd_envOnlyRuntimeMutationNotSurvivesRestart(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "A_BACKEND_X": "cred-x",
		backend.EnvPrefix + "B_BACKEND_Y": "cred-y",
	}
	p := loadMovePools(t, clock, env)

	// Runtime add: ccz with full credentials into b (no other pool carries
	// it, so ccz is a runtime-only declaration in this test).
	if status, err := p.AddMember("b", "ccz", "cred-ccz", "https://b.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("seed AddMember b ccz: status=%d err=%v, want 200", status, err)
	}

	// Env-only restart: fresh loadMovePools reads env again. ccz was never
	// in env, so the new registry has no ccz anywhere — exactly the state
	// an operator hits when their runtime-only nick vanishes on restart.
	p = loadMovePools(t, clock, env)

	status, err := p.AddMember("a", "ccz", "", "", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("AddMember a ccz after env-only restart: status=%d err=%v, want 400", status, err)
	}
	if err == nil {
		t.Fatalf("expected credential-required error, got nil")
	}
	msg := err.Error()
	// Registry scope: every pool the operator could have meant. The pool
	// names here come from the original env, sorted: a, b.
	if !strings.Contains(msg, "(pools: a, b)") {
		t.Errorf("error text %q missing registry scope (pools: a, b)", msg)
	}
	// Recipe: tell the operator the two ways out of the 400.
	if !strings.Contains(msg, "supply credential + base_url, or restore the missing pool from env / aqg.json") {
		t.Errorf("error text %q missing the recipe phrase", msg)
	}
	// No credential leakage.
	if strings.Contains(msg, "cred-") {
		t.Errorf("error text leaked credential: %q", msg)
	}
}

// TestAdd_sameCredentialAcrossPoolsNotAmbiguous pins issue #303 AC4: the
// bijection requires the same credential for the same nick across pools,
// but the *same* credential + the *same* URL across two pools is not an
// ambiguity — crossPoolResolve produces one of each. The runtime add is
// the only way to seed the same nick in two pools today (env would
// collide), so the second declaration is via AddMember with explicit
// values.
func TestAdd_sameCredentialAcrossPoolsNotAmbiguous(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "A_BACKEND_CCZ": "cred-ccz",
		backend.EnvPrefix + "A_BASE_URL":   "https://z.example",
		backend.EnvPrefix + "B_BACKEND_X":  "cred-x",
		backend.EnvPrefix + "C_BACKEND_D":  "cred-d",
	}
	p := loadMovePools(t, clock, env)

	// Seed ccz in b with the *same* credential and URL as a. The bijection
	// is satisfied (one credential, one nick), and the explicit base_url
	// pins the cross-pool URL to a single value.
	if status, err := p.AddMember("b", "ccz", "cred-ccz", "https://z.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("seed AddMember b ccz: status=%d err=%v, want 200", status, err)
	}

	// Bodyless add to c must resolve cleanly — 1 cred, 1 URL.
	if status, err := p.AddMember("c", "ccz", "", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember c ccz (same cred + url): status=%d err=%v, want 200", status, err)
	}
	am, ok := addedMember(t, p, "c", "ccz")
	if !ok {
		t.Fatalf("ccz not added to c")
	}
	if am.Credential != "cred-ccz" {
		t.Errorf("resolved credential=%q, want cred-ccz", am.Credential)
	}
	if am.BaseURL != "https://z.example" {
		t.Errorf("resolved base_url=%q, want https://z.example", am.BaseURL)
	}
}

// TestAdd_differentBaseURLAcrossPoolsIsAmbiguous pins issue #303 AC5:
// credential and base_url resolve independently, and a base_url that
// differs across pools must stay a 400 with the existing ambiguity
// message — the registry-scope improvement is exclusively for the
// (0 creds, 0 URLs) result and must not swallow real conflicts.
func TestAdd_differentBaseURLAcrossPoolsIsAmbiguous(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "A_BACKEND_CCZ": "cred-ccz",
		backend.EnvPrefix + "A_BASE_URL":   "https://z.example",
		backend.EnvPrefix + "C_BACKEND_CCZ": "cred-ccz", // same nick, same cred, DIFFERENT url override
		backend.EnvPrefix + "C_BASE_URL":   "https://c.example",
		backend.EnvPrefix + "B_BACKEND_X":  "cred-x",
	}
	// Note: declaring ccz in both a and c with the same credential but
	// different pool-level base_urls is exactly the bijection-allowed shape
	// the registry accepts. The ambiguity fires when b's bodyless add tries
	// to borrow.
	p := loadMovePools(t, clock, env)

	status, err := p.AddMember("b", "ccz", "", "", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("ambiguous base_url: status=%d err=%v, want 400", status, err)
	}
	if err == nil || !strings.Contains(err.Error(), "base_url for nick ccz is ambiguous across pools") {
		t.Errorf("ambiguity error text=%v, want base_url-for-nick-ambiguous message", err)
	}
}

// TestAdd_unknownNickErrorNamesRegistryScope pins issue #303's
// message-contract sub-bullet directly: the (0 creds, 0 URLs) error names
// the current registry's pools and ends with the recipe. Built as a
// separate test (not folded into the env-only-restart one above) so the
// assertion stays a one-purpose contract on the helper, independent of
// the runtime-mutation lifecycle that test interleaves.
func TestAdd_unknownNickErrorNamesRegistryScope(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "A_BACKEND_X": "cred-x",
		backend.EnvPrefix + "B_BACKEND_Y": "cred-y",
	}
	p := loadMovePools(t, clock, env)

	status, err := p.AddMember("a", "ghost", "", "", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("AddMember a ghost: status=%d err=%v, want 400", status, err)
	}
	if err == nil {
		t.Fatalf("expected credential-required error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "(pools: a, b)") {
		t.Errorf("error text %q missing sorted registry scope (pools: a, b)", msg)
	}
	if !strings.Contains(msg, "supply credential + base_url, or restore the missing pool from env / aqg.json") {
		t.Errorf("error text %q missing the recipe phrase", msg)
	}
	if strings.Contains(msg, "cred-") {
		t.Errorf("error text leaked credential: %q", msg)
	}
}

// TestCreatePool_unknownNickErrorNamesRegistryScope mirrors the above for
// the CreatePoolWithMember path: a bodyless combined create (pool + first
// member) where the nick is unknown produces the same registry-scope +
// recipe message. The two call sites share nickNotResolvableError, so the
// contract holds on both — but a regression on either side is invisible
// from the other test, so each is pinned separately.
func TestCreatePool_unknownNickErrorNamesRegistryScope(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "A_BACKEND_X": "cred-x",
		backend.EnvPrefix + "B_BACKEND_Y": "cred-y",
	}
	p := loadMovePools(t, clock, env)

	status, err := p.CreatePoolWithMember("fresh", "", "ghost", "", "", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("CreatePoolWithMember fresh ghost: status=%d err=%v, want 400", status, err)
	}
	if err == nil {
		t.Fatalf("expected credential-required error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "(pools: a, b)") {
		t.Errorf("error text %q missing sorted registry scope (pools: a, b)", msg)
	}
	if !strings.Contains(msg, "supply credential + base_url, or restore the missing pool from env / aqg.json") {
		t.Errorf("error text %q missing the recipe phrase", msg)
	}
}
