package auto

import (
	"net/http"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// TestRemove_fromDeclaredOrderPrunesMember is the core #120 regression:
// removing a member must prune the declared order immediately, without a
// restart. The fixture installs an order through SetPriority before removal.
func TestRemove_fromDeclaredOrderPrunesMember(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
		backend.EnvPrefix + "DST_BACKEND_Q": "cred-q",
		backend.EnvPrefix + "DST_BACKEND_R": "cred-r",
		// No DST_PRIORITY — start with the sorted default, declare an order below.
	}
	p := loadMovePools(t, clock, env)

	// Install a runtime declared order [p, q, r].
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

// TestRemove_runtimeAddedFromDeclaredOrderPrunesMember proves removing a
// runtime-added member also prunes it from the declared order.
func TestRemove_runtimeAddedFromDeclaredOrderPrunesMember(t *testing.T) {
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

	// Sanity: the declared order now lists a first, then the env order.
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
	// After Remove, the declaration should match the original env order.
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

// TestRemove_lastMemberClearsDeclaredOrder proves that removing
// the last member also clears the now-empty declared order.
func TestRemove_lastMemberClearsDeclaredOrder(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
	}
	p := loadMovePools(t, clock, env)

	// Install an order covering the only member.
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
		t.Errorf("dst priority after removing the last member = %v, want empty", got)
	}
}

// TestRemove_fromSortedDefaultLeavesDeclarationEmpty proves removal preserves the
// omitted declaration on a pool using the sorted default.
func TestRemove_fromSortedDefaultLeavesDeclarationEmpty(t *testing.T) {
	clock := newMoveClock()
	env := map[string]string{
		backend.EnvPrefix + "DST_BACKEND_P": "cred-p",
		backend.EnvPrefix + "DST_BACKEND_Q": "cred-q",
		// No DST_PRIORITY — dst uses the sorted default.
	}
	p := loadMovePools(t, clock, env)

	if got := poolPriority(t, p, "dst"); len(got) != 0 {
		t.Fatalf("dst priority before Remove = %v, want empty (sorted default has no declaration)", got)
	}

	if status, err := p.RemoveMember("dst", "p"); status != http.StatusOK || err != nil {
		t.Fatalf("RemoveMember p: status=%d err=%v", status, err)
	}

	if got := poolPriority(t, p, "dst"); len(got) != 0 {
		t.Errorf("dst priority after Remove on sorted-default pool = %v, want empty", got)
	}
}
