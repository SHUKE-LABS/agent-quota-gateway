package auto

import (
	"net/http"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// addedMember reads a runtime-added member's persisted spec directly (bypassing
// the display-time base_url fallback in backendByNickLocked) so tests can prove
// what was actually stored.
func addedMember(t *testing.T, p *Pools, pool, nick string) (AddedMember, bool) {
	t.Helper()
	c := p.byPool[pool]
	c.mu.Lock()
	defer c.mu.Unlock()
	am, ok := c.addedMembers[backend.NormalizeName(nick)]
	return am, ok
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

// TestAdd_ambiguousCredentialRejected proves that an omitted credential for a
// nick that exists with differing credentials across pools is rejected.
//
// The static-config bijection (a nick must carry the same credential
// everywhere) forbids the same nick in two pools at load time, so the
// cross-pool state is seeded here via the runtime add path — the only
// remaining way the same nick can land in two pools. The runtime ambiguity
// guard under test is unchanged.
func TestAdd_ambiguousCredentialRejected(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "ONE_BACKEND_SHARED": "cred-1",
		backend.EnvPrefix + "TWO_BACKEND_OTHER":  "cred-other",
		backend.EnvPrefix + "DST_BACKEND_X":      "cred-x",
	})

	// Seed "shared" in a second pool with a differing credential.
	if status, err := p.AddMember("two", "shared", "cred-2", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("seed AddMember two shared: status=%d err=%v, want 200", status, err)
	}

	// Adding "shared" to dst with the credential omitted is now ambiguous:
	// ONE holds cred-1, TWO holds cred-2.
	if status, err := p.AddMember("dst", "shared", "", "", nil); status != http.StatusBadRequest || err == nil {
		t.Fatalf("ambiguous credential: status=%d err=%v, want 400", status, err)
	}
	if _, ok := addedMember(t, p, "dst", "shared"); ok {
		t.Errorf("shared was added to dst despite ambiguous credential")
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
