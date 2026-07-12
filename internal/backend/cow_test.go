package backend

import (
	"reflect"
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
