package backend

import (
	"reflect"
	"strings"
	"testing"
)

// specFixture builds a small multi-pool registry used across the
// copy-on-write mutator tests.
func specFixture(t *testing.T) *Registry {
	t.Helper()
	reg, err := BuildFromSpec(Spec{Pools: map[string]PoolSpec{
		"auto": {
			Members: map[string]MemberSpec{
				"a": {Credential: "cred-a"},
				"b": {Credential: "cred-b"},
			},
			Priority: []string{"a", "b"},
		},
		"z-ai": {
			BaseURL: "https://open.example/anthropic",
			Members: map[string]MemberSpec{
				"x": {Credential: "vendor-x"},
				"y": {Credential: "vendor-y", BaseURL: "https://mirror.example/anthropic"},
			},
		},
	}}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	return reg
}

func TestSpec_roundTripsCleanly(t *testing.T) {
	reg := specFixture(t)
	spec := reg.Spec()

	// A member that inherited the pool default keeps an empty per-member
	// base_url; only the genuine override survives.
	if got := spec.Pools["auto"].Members["a"].BaseURL; got != "" {
		t.Errorf("auto/a inherited base_url should be empty, got %q", got)
	}
	if got := spec.Pools["z-ai"].Members["x"].BaseURL; got != "" {
		t.Errorf("z-ai/x inherited base_url should be empty, got %q", got)
	}
	if got := spec.Pools["z-ai"].Members["y"].BaseURL; got != "https://mirror.example/anthropic" {
		t.Errorf("z-ai/y override base_url = %q, want the mirror", got)
	}

	// Rebuilding from the extracted Spec yields an equivalent registry.
	reg2, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	for _, p := range []string{"auto", "z-ai"} {
		for _, n := range reg.PoolNicks(p) {
			b1, _ := reg.ResolveIn(p, n)
			b2, _ := reg2.ResolveIn(p, n)
			if b1 != b2 {
				t.Errorf("%s/%s round-trip mismatch: %+v vs %+v", p, n, b1, b2)
			}
		}
		if !reflect.DeepEqual(reg.PoolPriority(p), reg2.PoolPriority(p)) {
			t.Errorf("%s priority round-trip mismatch", p)
		}
	}
}

func TestWithMemberSet_addAndRotate(t *testing.T) {
	reg := specFixture(t)

	// Add a brand-new member.
	reg2, err := reg.WithMemberSet("auto", "c", "cred-c", "", false)
	if err != nil {
		t.Fatalf("add member: %v", err)
	}
	if b, ok := reg2.ResolveIn("auto", "c"); !ok || b.Credential != "cred-c" {
		t.Fatalf("new member c missing or wrong: %+v ok=%v", b, ok)
	}
	// Original registry is untouched (copy-on-write).
	if _, ok := reg.ResolveIn("auto", "c"); ok {
		t.Fatal("original registry was mutated")
	}

	// Rotate an existing member's credential in place (#197 core).
	reg3, err := reg2.WithMemberSet("auto", "a", "cred-a-rotated", "", false)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if b, _ := reg3.ResolveIn("auto", "a"); b.Credential != "cred-a-rotated" {
		t.Fatalf("rotation did not take: %+v", b)
	}
}

func TestWithMemberSet_bijectionEnforced(t *testing.T) {
	reg := specFixture(t)
	// Reusing an existing credential under a different nick violates the
	// nick↔credential bijection and must be rejected by the rebuild.
	if _, err := reg.WithMemberSet("auto", "dupe", "cred-b", "", false); err == nil {
		t.Fatal("expected bijection error, got nil")
	}
}

func TestWithMemberRemoved_prunesPriority(t *testing.T) {
	reg := specFixture(t)
	reg2, err := reg.WithMemberRemoved("auto", "a")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, ok := reg2.ResolveIn("auto", "a"); ok {
		t.Fatal("member a still present after removal")
	}
	if got := reg2.PoolPriority("auto"); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("priority not pruned: %v", got)
	}
	if _, err := reg.WithMemberRemoved("auto", "nope"); err == nil {
		t.Fatal("expected error removing unknown member")
	}
}

// TestWithMemberRemoved_drainsDeclaredBasePool proves that removing the last
// member of a pool whose base_url differs from the gateway default drains it to
// a valid zero-member pool — parity with a default-base pool — and that the
// drained pool round-trips through Spec()→BuildFromSpec (restart-safe), rather
// than tripping the memberless-pool guard (issue #200).
func TestWithMemberRemoved_drainsDeclaredBasePool(t *testing.T) {
	reg, err := BuildFromSpec(Spec{Pools: map[string]PoolSpec{
		"zai": { // declared base_url, single member
			BaseURL: "https://open.example/anthropic",
			Members: map[string]MemberSpec{"only": {Credential: "vendor-only"}},
		},
		"auto": { // default-base, single member (parity control)
			Members: map[string]MemberSpec{"solo": {Credential: "cred-solo"}},
		},
	}}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}

	// Declared-base pool drains to empty without error.
	drained, err := reg.WithMemberRemoved("zai", "only")
	if err != nil {
		t.Fatalf("draining declared-base pool: %v", err)
	}
	if !drained.HasPool("zai") {
		t.Fatal("zai pool vanished after draining its last member")
	}
	if nicks := drained.PoolNicks("zai"); len(nicks) != 0 {
		t.Fatalf("zai should be empty after drain, got %v", nicks)
	}

	// Parity: the default-base pool drains the same way.
	if _, err := reg.WithMemberRemoved("auto", "solo"); err != nil {
		t.Fatalf("draining default-base pool: %v", err)
	}

	// Restart-safety: the drained declared-base pool round-trips cleanly.
	if spec := drained.Spec(); spec.Pools["zai"].BaseURL != "" {
		t.Fatalf("drained zai still emits base_url %q; would trip the guard on reload", spec.Pools["zai"].BaseURL)
	}
	rebuilt, err := BuildFromSpec(drained.Spec(), testDefaultBaseURL)
	if err != nil {
		t.Fatalf("rebuild of drained pool: %v", err)
	}
	if !rebuilt.HasPool("zai") || len(rebuilt.PoolNicks("zai")) != 0 {
		t.Fatal("drained zai did not survive Spec()->BuildFromSpec as an empty pool")
	}
}

// TestBuildFromSpec_memberlessBaseURLStillRejected fences the typo guard: a
// genuine load declaring base_url for a pool with zero members is almost
// certainly a typo'd nick and must still fail closed (issue #200 must not
// weaken this for the initial load path).
func TestBuildFromSpec_memberlessBaseURLStillRejected(t *testing.T) {
	_, err := BuildFromSpec(Spec{Pools: map[string]PoolSpec{
		"zai": {BaseURL: "https://open.example/anthropic"}, // no members
		"auto": {
			Members: map[string]MemberSpec{"a": {Credential: "cred-a"}},
		},
	}}, testDefaultBaseURL)
	if err == nil {
		t.Fatal("expected memberless-pool base_url to be rejected on load")
	}
}

func TestWithMemberDisabled(t *testing.T) {
	reg := specFixture(t)
	reg2, err := reg.WithMemberDisabled("auto", "b", true)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if b, _ := reg2.ResolveIn("auto", "b"); !b.Disabled {
		t.Fatal("member b not disabled")
	}
	// Round-trips through Spec.
	if !reg2.Spec().Pools["auto"].Members["b"].Disabled {
		t.Fatal("disabled flag lost in Spec()")
	}
	reg3, _ := reg2.WithMemberDisabled("auto", "b", false)
	if b, _ := reg3.ResolveIn("auto", "b"); b.Disabled {
		t.Fatal("member b not re-enabled")
	}
}

func TestWithPriority_setAndClear(t *testing.T) {
	reg := specFixture(t)
	reg2, err := reg.WithPriority("auto", []string{"b", "a"})
	if err != nil {
		t.Fatalf("set priority: %v", err)
	}
	if got := reg2.PoolPriority("auto"); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("priority = %v", got)
	}
	reg3, err := reg2.WithPriority("auto", nil)
	if err != nil {
		t.Fatalf("clear priority: %v", err)
	}
	if got := reg3.PoolPriority("auto"); got != nil {
		t.Fatalf("priority not cleared: %v", got)
	}
	if _, err := reg.WithPriority("auto", []string{"ghost"}); err == nil {
		t.Fatal("expected error for priority naming a non-member")
	}
}

func TestWithPoolCreated_emptyPoolSurvives(t *testing.T) {
	reg := specFixture(t)
	reg2, err := reg.WithPoolCreated("fresh")
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	if !reg2.HasPool("fresh") {
		t.Fatal("created pool missing")
	}
	if nicks := reg2.PoolNicks("fresh"); len(nicks) != 0 {
		t.Fatalf("fresh pool should be empty, got %v", nicks)
	}
	// A member added later inherits the gateway default upstream.
	reg3, err := reg2.WithMemberSet("fresh", "n1", "cred-n1", "", false)
	if err != nil {
		t.Fatalf("add to fresh: %v", err)
	}
	if b, _ := reg3.ResolveIn("fresh", "n1"); b.BaseURL != testDefaultBaseURL {
		t.Fatalf("fresh member base URL = %q, want gateway default", b.BaseURL)
	}
	if _, err := reg.WithPoolCreated("auto"); err == nil {
		t.Fatal("expected error creating an existing pool")
	}
}

// TestWithPoolRemoved drops a pool and leaves the rest intact; an unknown pool
// errors (issue #232, the inverse of WithPoolCreated). Membership is not
// checked here — the auto layer owns the require-empty 409, matching
// WithMemberRemoved's split of responsibility.
func TestWithPoolRemoved(t *testing.T) {
	reg := specFixture(t)
	reg2, err := reg.WithPoolRemoved("Z_AI") // exercises normalization
	if err != nil {
		t.Fatalf("remove pool: %v", err)
	}
	if reg2.HasPool("z-ai") {
		t.Fatal("removed pool z-ai still present")
	}
	if !reg2.HasPool("auto") {
		t.Fatal("sibling pool auto dropped by unrelated removal")
	}
	// The removal round-trips through Spec()->BuildFromSpec (restart-safe).
	rebuilt, err := BuildFromSpec(reg2.Spec(), testDefaultBaseURL)
	if err != nil {
		t.Fatalf("rebuild from spec: %v", err)
	}
	if rebuilt.HasPool("z-ai") {
		t.Fatal("removed pool z-ai reappeared after Spec()->BuildFromSpec")
	}
	// Unknown pool errors; the original registry is untouched (immutable).
	if _, err := reg.WithPoolRemoved("ghost"); err == nil {
		t.Fatal("expected error removing an unknown pool")
	}
	if !reg.HasPool("z-ai") {
		t.Fatal("source registry mutated by WithPoolRemoved (should be copy-on-write)")
	}
}

// TestWithPoolRenamed_movesEverything proves the rename carries every
// attribute the pool declared (membership, per-member base URL override,
// disabled flag, priority order, balance mode/params) over to the new key
// while leaving every other pool untouched.
func TestWithPoolRenamed_movesEverything(t *testing.T) {
	reg := specFixture(t)

	reg2, err := reg.WithPoolRenamed("auto", "Alpha")
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	if reg2.HasPool("auto") {
		t.Errorf("old name auto still present after rename")
	}
	if !reg2.HasPool("alpha") {
		t.Fatalf("new name alpha missing after rename")
	}
	// Membership survives.
	if got := reg2.PoolNicks("alpha"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("alpha members=%v, want [a b]", got)
	}
	// Priority survives.
	if got := reg2.PoolPriority("alpha"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("alpha priority=%v, want [a b]", got)
	}
	// Other pool untouched.
	if got := reg2.PoolNicks("z-ai"); len(got) != 2 {
		t.Errorf("z-ai members=%v after unrelated rename", got)
	}
	// Original registry immutable.
	if !reg.HasPool("auto") || reg.HasPool("alpha") {
		t.Errorf("source registry mutated by rename (should be copy-on-write)")
	}
}

// TestWithPoolRenamed_carriesBaseURLAndDisabled proves the rename preserves
// the pool's declared base_url and a disabled member's flag — the attributes
// the persisted config file and the active controller depend on.
func TestWithPoolRenamed_carriesBaseURLAndDisabled(t *testing.T) {
	reg := specFixture(t)
	// Disable z-ai/x so the flag has something to carry.
	disabled, err := reg.WithMemberDisabled("z-ai", "x", true)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	reg2, err := disabled.WithPoolRenamed("z-ai", "Vendor")
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	b, ok := reg2.ResolveIn("vendor", "x")
	if !ok {
		t.Fatalf("vendor/x not resolvable after rename")
	}
	if !b.Disabled {
		t.Errorf("vendor/x Disabled=false after rename, want true")
	}
	// y's per-member base_url override must survive.
	y, ok := reg2.ResolveIn("vendor", "y")
	if !ok {
		t.Fatalf("vendor/y not resolvable after rename")
	}
	if y.BaseURL != "https://mirror.example/anthropic" {
		t.Errorf("vendor/y base_url=%q after rename, want the mirror", y.BaseURL)
	}
	// Pool-level base_url round-trips through Spec() and rebuild.
	rebuilt, err := BuildFromSpec(reg2.Spec(), testDefaultBaseURL)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !rebuilt.HasPool("vendor") {
		t.Fatalf("rebuilt registry missing vendor after Spec() round-trip")
	}
}

// TestWithPoolRenamed_errors covers the four error paths:
//   - unknown source pool
//   - empty new name (after normalization)
//   - new name identical to old (after normalization)
//   - new name collides with a different existing pool
func TestWithPoolRenamed_errors(t *testing.T) {
	reg := specFixture(t)
	cases := []struct {
		old, new string
		wantSub  string
	}{
		{"ghost", "renamed", "unknown pool"},
		{"auto", "", "empty after normalization"},
		{"auto", "Auto", "same name"},
		{"auto", "z-ai", "already exists"},
	}
	for _, tc := range cases {
		if _, err := reg.WithPoolRenamed(tc.old, tc.new); err == nil || !strings.Contains(err.Error(), tc.wantSub) {
			t.Errorf("WithPoolRenamed(%q,%q): err=%v, want substring %q", tc.old, tc.new, err, tc.wantSub)
		}
	}
}
