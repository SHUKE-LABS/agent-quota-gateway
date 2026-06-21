package auto

import (
	"net/http"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// TestRemove_fromPriorityPoolPrunesOverride is the core #120 regression: a
// runtime priority override must drop a removed nick on the immediate next
// EffectiveConfig call — no restart needed. Before the fix, removeMemberLocked
// never touched c.priorityOverride, so the UI kept showing the stale entry
// until loadRuntimeConfig finally filtered it on the next restart.
//
// The bug only fires for pools whose priority is in the runtime override
// (c.priorityOverride != nil), not the env-declared c.priority — env-declared
// priority is read once at startup and loadRuntimeConfig does not rewrite it,
// so restart would NOT fix the env case (separate ticket shape). The fixture
// installs the override explicitly via SetPriority.
func TestRemove_fromPriorityPoolPrunesOverride(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
		backend.EnvPrefix + "DST_BACKEND_Q": "cred-q",
		backend.EnvPrefix + "DST_BACKEND_R": "cred-r",
		// No DST_PRIORITY — start as a plain pool, install the override below.
	}
	p := loadMovePools(t, clock, env)

	// Install a runtime priority override [p, q, r] so c.priorityOverride is set.
	if status, err := p.SetPriority("dst", []string{"p", "q", "r"}); status != http.StatusOK || err != nil {
		t.Fatalf("SetPriority: status=%d err=%v", status, err)
	}
	if got := poolPriority(t, p, "dst"); len(got) != 3 || got[0] != "p" || got[1] != "q" || got[2] != "r" {
		t.Fatalf("dst priority before Remove = %v, want [p q r]", got)
	}

	if status, err := p.RemoveMember("dst", "q"); status != http.StatusOK || err != nil {
		t.Fatalf("RemoveMember q: status=%d err=%v", status, err)
	}

	got := poolPriority(t, p, "dst")
	for _, n := range got {
		if n == "q" {
			t.Errorf("dst priority = %v still contains removed nick q", got)
		}
	}
	// Remaining order preserved (no reordering, only filter).
	want := []string{"p", "r"}
	if len(got) != len(want) {
		t.Fatalf("dst priority after Remove = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("dst priority[%d] = %q, want %q (order must be preserved)", i, got[i], want[i])
		}
	}
}

// TestRemove_runtimeAddedFromPriorityPoolPrunesOverride proves the fix
// also covers runtime-added members placed into a priority pool — the
// Add path expands the override over addedMembersLocked (see
// setPriorityOverrideEffectiveLocked at :1724), so the override legitimately
// orders runtime-added nicks. After Remove, the override must drop the
// runtime-added nick too.
func TestRemove_runtimeAddedFromPriorityPoolPrunesOverride(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
		backend.EnvPrefix + "DST_BACKEND_Q": "cred-q",
		backend.EnvPrefix + "DST_PRIORITY":  "p,q",
	}
	p := loadMovePools(t, clock, env)

	// Add a runtime-added member "a" with explicit placement at the top.
	if status, err := p.AddMember("dst", "a", "cred-a", "https://u.example", []string{"a", "p", "q"}); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: status=%d err=%v", status, err)
	}

	// Sanity: priority now lists a first (env plus added-member expansion),
	// then the env order.
	if got := poolPriority(t, p, "dst"); len(got) == 0 || got[0] != "a" {
		t.Fatalf("dst priority after Add = %v, want a first", got)
	}

	if status, err := p.RemoveMember("dst", "a"); status != http.StatusOK || err != nil {
		t.Fatalf("RemoveMember a: status=%d err=%v", status, err)
	}

	got := poolPriority(t, p, "dst")
	for _, n := range got {
		if n == "a" {
			t.Errorf("dst priority = %v still contains removed runtime-added nick a", got)
		}
	}
	// After Remove, the priority list should match the original env-declared
	// order (the Add expanded override was rebuilt over the post-Remove
	// effective set, which is just the env members).
	want := []string{"p", "q"}
	if len(got) != len(want) {
		t.Fatalf("dst priority after Remove = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("dst priority[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestRemove_lastMemberFromPriorityPoolClearsOverride proves that removing
// the last priority-pool member drops the override entirely — the pool
// becomes a plain pool and the UI hides the priority column. The fix sets
// priorityOverride to nil when the filtered list is empty.
func TestRemove_lastMemberFromPriorityPoolClearsOverride(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
	}
	p := loadMovePools(t, clock, env)

	// Install a runtime override covering the only member.
	if status, err := p.SetPriority("dst", []string{"p"}); status != http.StatusOK || err != nil {
		t.Fatalf("SetPriority: status=%d err=%v", status, err)
	}
	if got := poolPriority(t, p, "dst"); len(got) != 1 || got[0] != "p" {
		t.Fatalf("dst priority before Remove = %v, want [p]", got)
	}

	if status, err := p.RemoveMember("dst", "p"); status != http.StatusOK || err != nil {
		t.Fatalf("RemoveMember p: status=%d err=%v", status, err)
	}

	if got := poolPriority(t, p, "dst"); len(got) != 0 {
		t.Errorf("dst priority after removing the last member = %v, want empty (override should be nil)", got)
	}
}

// TestRemove_fromPlainPoolLeavesNilOverride proves the fix is a no-op for
// pools that have no runtime priority override. The pool stays plain; the
// override (which was nil) is still nil after Remove.
func TestRemove_fromPlainPoolLeavesNilOverride(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
		backend.EnvPrefix + "DST_BACKEND_Q": "cred-q",
		// No DST_PRIORITY — dst is a plain pool.
	}
	p := loadMovePools(t, clock, env)

	if got := poolPriority(t, p, "dst"); len(got) != 0 {
		t.Fatalf("dst priority before Remove = %v, want empty (plain pool has no override)", got)
	}

	if status, err := p.RemoveMember("dst", "p"); status != http.StatusOK || err != nil {
		t.Fatalf("RemoveMember p: status=%d err=%v", status, err)
	}

	if got := poolPriority(t, p, "dst"); len(got) != 0 {
		t.Errorf("dst priority after Remove on plain pool = %v, want empty (override stays nil)", got)
	}
}