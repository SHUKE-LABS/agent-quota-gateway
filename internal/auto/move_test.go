package auto

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// loadMovePools builds a multi-pool Pools from explicit AQG_POOL_* env, with a
// fixed clock and discarded logs. Ambient pool env is scrubbed first.
func loadMovePools(t *testing.T, clock *fixedClock, env map[string]string) *Pools {
	t.Helper()
	scrubPoolEnv(t)
	for k, v := range env {
		t.Setenv(k, v)
	}
	reg, err := backend.Load(testDefaultBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return NewPools(reg, nil, clock.now, io.Discard)
}

// poolMembers returns the effective-config member nicks for a pool as a set.
func poolMembers(t *testing.T, p *Pools, pool string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, v := range p.EffectiveConfig() {
		if v.Pool == pool {
			for _, m := range v.Members {
				out[m.Nick] = true
			}
		}
	}
	return out
}

// poolPriority returns the effective priority order for a pool.
func poolPriority(t *testing.T, p *Pools, pool string) []string {
	t.Helper()
	for _, v := range p.EffectiveConfig() {
		if v.Pool == pool {
			return v.Priority
		}
	}
	return nil
}

func newMoveClock() *fixedClock {
	return &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
}

// TestMove_plainToPlain moves a subscription between two plain pools: it leaves
// the source, joins the target, and is selectable there.
func TestMove_plainToPlain(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_A": "cred-a",
		backend.EnvPrefix + "SRC_BACKEND_B": "cred-b",
		backend.EnvPrefix + "DST_BACKEND_X": "cred-x",
	})

	status, err := p.MoveMember("src", "a", "dst", nil, false)
	if err != nil || status != http.StatusOK {
		t.Fatalf("MoveMember: status=%d err=%v, want 200", status, err)
	}

	if poolMembers(t, p, "src")["a"] {
		t.Errorf("a still present in src after move")
	}
	if !poolMembers(t, p, "dst")["a"] {
		t.Errorf("a not present in dst after move")
	}

	// Selection reflects the new location: with x removed, dst selects a.
	if _, err := p.RemoveMember("dst", "x"); err != nil {
		t.Fatalf("RemoveMember(dst,x): %v", err)
	}
	dst := p.byPool["dst"]
	b, _, exhausted := dst.ResolveAuto()
	if exhausted || b.Nick != "a" {
		t.Errorf("dst ResolveAuto nick=%q exhausted=%v, want a", b.Nick, exhausted)
	}
	if b.Credential != "cred-a" {
		t.Errorf("moved member credential=%q, want cred-a", b.Credential)
	}
}

// TestMove_intoPriorityRequiresPlacement proves a move into a priority pool with
// no existing slot requires an explicit placement that includes the moved nick.
func TestMove_intoPriorityRequiresPlacement(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_A": "cred-a",
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
		backend.EnvPrefix + "DST_BACKEND_Q": "cred-q",
		backend.EnvPrefix + "DST_PRIORITY":  "p,q",
	}

	// Missing placement -> 400.
	p := loadMovePools(t, clock, env)
	if status, err := p.MoveMember("src", "a", "dst", nil, false); status != http.StatusBadRequest || err == nil {
		t.Fatalf("missing placement: status=%d err=%v, want 400", status, err)
	}

	// Placement omitting the moved nick -> 400.
	if status, err := p.MoveMember("src", "a", "dst", []string{"p", "q"}, false); status != http.StatusBadRequest || err == nil {
		t.Fatalf("placement without nick: status=%d err=%v, want 400", status, err)
	}

	// Explicit placement including the moved nick -> 200, placed at the top.
	if status, err := p.MoveMember("src", "a", "dst", []string{"a", "p", "q"}, false); status != http.StatusOK || err != nil {
		t.Fatalf("explicit placement: status=%d err=%v, want 200", status, err)
	}
	if got := poolPriority(t, p, "dst"); len(got) == 0 || got[0] != "a" {
		t.Errorf("dst priority=%v, want a first", got)
	}

	// Selection reflects the new location: with p and q disabled, dst selects a.
	if _, err := p.SetMemberDisabled("dst", "p", true); err != nil {
		t.Fatalf("disable p: %v", err)
	}
	if _, err := p.SetMemberDisabled("dst", "q", true); err != nil {
		t.Fatalf("disable q: %v", err)
	}
	dst := p.byPool["dst"]
	b, _, exhausted := dst.ResolveAuto()
	if exhausted || b.Nick != "a" {
		t.Errorf("dst ResolveAuto nick=%q exhausted=%v, want a", b.Nick, exhausted)
	}
}

// TestMove_noReanchor proves the move does not force-switch the target's healthy
// active member; the new order applies on the next selection event.
func TestMove_noReanchor(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_A": "cred-a",
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
		backend.EnvPrefix + "DST_BACKEND_Q": "cred-q",
		backend.EnvPrefix + "DST_PRIORITY":  "p,q",
	})

	dst := p.byPool["dst"]
	// Anchor dst on p (a healthy active member).
	if b, _, _ := dst.ResolveAuto(); b.Nick != "p" {
		t.Fatalf("dst initial active=%q, want p", b.Nick)
	}

	// Move a into dst at the top of the order.
	if status, err := p.MoveMember("src", "a", "dst", []string{"a", "p", "q"}, false); status != http.StatusOK || err != nil {
		t.Fatalf("MoveMember: status=%d err=%v", status, err)
	}

	// p is still healthy, so it stays active despite a now ranking higher.
	if got := dst.Current(); got != "p" {
		t.Errorf("dst active=%q after move, want p (no surprise re-anchor)", got)
	}
}

// TestMove_overwriteConflict proves a same-nick target with a differing
// credential/base_url returns 409, and force overwrites in place.
func TestMove_overwriteConflict(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_S": "cred-s",
		backend.EnvPrefix + "DST_BACKEND_D": "cred-d",
	})
	// Source carries an added member a (explicit base).
	if status, err := p.AddMember("src", "a", "k1", "https://u1.example"); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember src a: status=%d err=%v", status, err)
	}
	// Target already has a different a.
	if status, err := p.AddMember("dst", "a", "k2", "https://u2.example"); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember dst a: status=%d err=%v", status, err)
	}

	// Without force -> 409.
	if status, err := p.MoveMember("src", "a", "dst", nil, false); status != http.StatusConflict || err == nil {
		t.Fatalf("conflict move: status=%d err=%v, want 409", status, err)
	}
	// Source untouched by the rejected move.
	if !poolMembers(t, p, "src")["a"] {
		t.Errorf("a missing from src after a rejected move")
	}

	// With force -> 200, target overwritten with the source credential.
	if status, err := p.MoveMember("src", "a", "dst", nil, true); status != http.StatusOK || err != nil {
		t.Fatalf("forced move: status=%d err=%v, want 200", status, err)
	}
	if poolMembers(t, p, "src")["a"] {
		t.Errorf("a still in src after forced move")
	}
	if b, ok := p.byPool["dst"].backendByNickLocked("a"); !ok || b.Credential != "k1" || b.BaseURL != "https://u1.example" {
		t.Errorf("dst a after overwrite = %+v ok=%v, want k1/https://u1.example", b, ok)
	}
}

// TestMove_overwriteMatchSilent proves a same-nick target with matching
// credential + base_url is silently overwritten (no force needed).
func TestMove_overwriteMatchSilent(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_S": "cred-s",
		backend.EnvPrefix + "DST_BACKEND_D": "cred-d",
	})
	if status, err := p.AddMember("src", "a", "k1", "https://u1.example"); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember src a: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("dst", "a", "k1", "https://u1.example"); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember dst a: status=%d err=%v", status, err)
	}

	if status, err := p.MoveMember("src", "a", "dst", nil, false); status != http.StatusOK || err != nil {
		t.Fatalf("matching move: status=%d err=%v, want 200 silent", status, err)
	}
	if poolMembers(t, p, "src")["a"] {
		t.Errorf("a still in src after matching move")
	}
	if !poolMembers(t, p, "dst")["a"] {
		t.Errorf("a missing from dst after matching move")
	}
}

// TestMove_staticTarget covers moving onto a same-nick static target: a match
// is a silent no-op (slot preserved); a differing static target is an
// unresolvable conflict that force cannot override.
func TestMove_staticTarget(t *testing.T) {
	clock := newMoveClock()

	// Matching static target: both pools declare a with the same credential.
	pMatch := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_A": "cred-a",
		backend.EnvPrefix + "DST_BACKEND_A": "cred-a",
	})
	if status, err := pMatch.MoveMember("src", "a", "dst", nil, false); status != http.StatusOK || err != nil {
		t.Fatalf("matching static move: status=%d err=%v, want 200", status, err)
	}
	if poolMembers(t, pMatch, "src")["a"] {
		t.Errorf("a still present in src after matching static move")
	}
	if !poolMembers(t, pMatch, "dst")["a"] {
		t.Errorf("a missing from dst after matching static move")
	}

	// Differing static target: force cannot override an immutable static member.
	pDiff := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_A": "cred-a",
		backend.EnvPrefix + "DST_BACKEND_A": "cred-different",
	})
	if status, _ := pDiff.MoveMember("src", "a", "dst", nil, false); status != http.StatusConflict {
		t.Errorf("differing static move (no force): status=%d, want 409", status)
	}
	if status, _ := pDiff.MoveMember("src", "a", "dst", nil, true); status != http.StatusConflict {
		t.Errorf("differing static move (force): status=%d, want 409 (static is immutable)", status)
	}
}

// TestMove_persistsAcrossRestart proves a move into a priority pool (placed
// runtime-added member) survives a PersistRuntimeConfig -> LoadRuntimeConfig
// restart, including the placement and the source tombstone.
func TestMove_persistsAcrossRestart(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_A": "cred-a",
		backend.EnvPrefix + "SRC_BACKEND_B": "cred-b",
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
		backend.EnvPrefix + "DST_PRIORITY":  "p",
	}
	p := loadMovePools(t, clock, env)
	if status, err := p.MoveMember("src", "a", "dst", []string{"a", "p"}, false); status != http.StatusOK || err != nil {
		t.Fatalf("MoveMember: status=%d err=%v", status, err)
	}
	cfg := p.PersistRuntimeConfig()

	// Fresh pools from the same env, then replay the persisted runtime config.
	p2 := loadMovePools(t, clock, env)
	p2.LoadRuntimeConfig(cfg)

	if poolMembers(t, p2, "src")["a"] {
		t.Errorf("a resurfaced in src after restart")
	}
	if !poolMembers(t, p2, "dst")["a"] {
		t.Errorf("a missing from dst after restart")
	}
	if got := poolPriority(t, p2, "dst"); len(got) == 0 || got[0] != "a" {
		t.Errorf("dst priority after restart=%v, want a first", got)
	}
	// a is selectable in dst after restart once p is disabled.
	if _, err := p2.SetMemberDisabled("dst", "p", true); err != nil {
		t.Fatalf("disable p: %v", err)
	}
	if b, _, exhausted := p2.byPool["dst"].ResolveAuto(); exhausted || b.Nick != "a" {
		t.Errorf("dst ResolveAuto after restart nick=%q exhausted=%v, want a", b.Nick, exhausted)
	}
}

// TestMove_errors covers the validation error paths.
func TestMove_errors(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_A": "cred-a",
		backend.EnvPrefix + "DST_BACKEND_X": "cred-x",
	})

	if status, _ := p.MoveMember("nope", "a", "dst", nil, false); status != http.StatusNotFound {
		t.Errorf("unknown source: status=%d, want 404", status)
	}
	if status, _ := p.MoveMember("src", "a", "nope", nil, false); status != http.StatusNotFound {
		t.Errorf("unknown target: status=%d, want 404", status)
	}
	if status, _ := p.MoveMember("src", "a", "src", nil, false); status != http.StatusBadRequest {
		t.Errorf("same source/target: status=%d, want 400", status)
	}
	if status, _ := p.MoveMember("src", "ghost", "dst", nil, false); status != http.StatusBadRequest {
		t.Errorf("missing source member: status=%d, want 400", status)
	}
	// Placement supplied for a plain target is rejected.
	if status, _ := p.MoveMember("src", "a", "dst", []string{"a"}, false); status != http.StatusBadRequest {
		t.Errorf("placement on plain target: status=%d, want 400", status)
	}
}
