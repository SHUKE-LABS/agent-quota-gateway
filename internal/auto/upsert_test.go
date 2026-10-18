package auto

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// TestUpsertMember_sameCredentialIsAConfigNoOp pins the management contract:
// the redacted/effective base URL and priority sent by a repeat must not
// overwrite declaration-level values, and a no-op must not wake the config
// writer.
func TestUpsertMember_sameCredentialIsAConfigNoOp(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "DST_BASE_URL":  "https://pool.example",
		backend.EnvPrefix + "DST_BACKEND_A": "cred-a",
		backend.EnvPrefix + "DST_BACKEND_B": "cred-b|https://member.example",
		backend.EnvPrefix + "DST_PRIORITY":  "b,a",
	})
	if status, err := p.SetMemberDisabled("dst", "a", true); status != http.StatusOK || err != nil {
		t.Fatalf("disable a: status=%d err=%v, want 200", status, err)
	}

	before := p.CurrentRegistry().Spec()
	changes := 0
	p.SetOnConfigChange(func() { changes++ })

	// Both replacement-only values deliberately disagree with the current
	// declaration. They must be ignored for an identical credential repeat.
	status, err := p.UpsertMember("dst", "a", "cred-a", "https://different.example", []string{"a", "b"})
	if status != http.StatusOK || err != nil {
		t.Fatalf("same-credential upsert: status=%d err=%v, want 200", status, err)
	}

	after := p.CurrentRegistry().Spec()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("same-credential upsert changed config:\nbefore=%+v\nafter=%+v", before, after)
	}
	if changes != 0 {
		t.Fatalf("same-credential upsert signaled %d config changes, want 0", changes)
	}
}

// TestUpsertMember_rotationPreservesDeclaration proves that credential
// rotation changes only the credential for an unshared nick, even when the
// client sends a different base URL and placement from the redacted view.
func TestUpsertMember_rotationPreservesDeclaration(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "DST_BASE_URL":  "https://pool.example",
		backend.EnvPrefix + "DST_BACKEND_A": "cred-a|https://member.example",
		backend.EnvPrefix + "DST_BACKEND_B": "cred-b",
		backend.EnvPrefix + "DST_PRIORITY":  "b,a",
	})
	if status, err := p.SetMemberDisabled("dst", "a", true); status != http.StatusOK || err != nil {
		t.Fatalf("disable a: status=%d err=%v, want 200", status, err)
	}

	before := p.CurrentRegistry().Spec()
	status, err := p.UpsertMember("dst", "a", "cred-a-rotated", "https://different.example", []string{"a", "b"})
	if status != http.StatusOK || err != nil {
		t.Fatalf("rotation: status=%d err=%v, want 200", status, err)
	}

	after := p.CurrentRegistry().Spec()
	oldMember := before.Pools["dst"].Members["a"]
	newMember := after.Pools["dst"].Members["a"]
	if newMember.Credential != "cred-a-rotated" {
		t.Errorf("rotated credential=%q, want cred-a-rotated", newMember.Credential)
	}
	if oldMember.BaseURL != newMember.BaseURL || oldMember.Disabled != newMember.Disabled {
		t.Fatalf("rotation lost declaration fields: before=%+v after=%+v", oldMember, newMember)
	}
	beforePool, afterPool := before.Pools["dst"], after.Pools["dst"]
	if beforePool.BaseURL != afterPool.BaseURL || !reflect.DeepEqual(beforePool.Priority, afterPool.Priority) || !reflect.DeepEqual(beforePool.Concurrency, afterPool.Concurrency) {
		t.Fatalf("rotation changed pool declaration:\nbefore=%+v\nafter=%+v", beforePool, afterPool)
	}
	for nick, want := range beforePool.Members {
		got, ok := afterPool.Members[nick]
		if !ok {
			t.Fatalf("rotation removed member %q", nick)
		}
		if nick == "a" {
			want.Credential = "cred-a-rotated"
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("rotation changed member %q beyond credential: before=%+v after=%+v", nick, want, got)
		}
	}
	if got := poolPriority(t, p, "dst"); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("priority after rotation=%v, want [b a]", got)
	}

	restarted := rebuildPoolsFromSpec(t, p)
	if got, ok := restarted.CurrentRegistry().ResolveIn("dst", "a"); !ok || got.Credential != "cred-a-rotated" || got.Disabled != true {
		t.Fatalf("rotation did not survive config round-trip: backend=%+v ok=%v", got, ok)
	}
}

func TestUpsertMember_sharedNickRefusesRotation(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "ONE_BACKEND_SHARED": "cred-shared",
		backend.EnvPrefix + "TWO_BACKEND_OTHER":  "cred-other",
	})
	if status, err := p.AddMember("two", "shared", "cred-shared", "https://shared.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("seed shared member: status=%d err=%v, want 200", status, err)
	}
	before := p.CurrentRegistry().Spec()

	status, err := p.UpsertMember("one", "shared", "cred-new", "https://different.example", []string{"shared"})
	if status != http.StatusConflict || err == nil {
		t.Fatalf("shared rotation: status=%d err=%v, want 409", status, err)
	}
	if after := p.CurrentRegistry().Spec(); !reflect.DeepEqual(after, before) {
		t.Fatalf("shared rotation changed config:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestUpsertMember_credentialCollisionRefusesRotation(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "DST_BACKEND_A": "cred-a",
		backend.EnvPrefix + "DST_BACKEND_B": "cred-b",
	})
	before := p.CurrentRegistry().Spec()

	status, err := p.UpsertMember("dst", "a", "cred-b", "https://different.example", []string{"a", "b"})
	if status != http.StatusBadRequest || err == nil {
		t.Fatalf("credential collision: status=%d err=%v, want 400", status, err)
	}
	if after := p.CurrentRegistry().Spec(); !reflect.DeepEqual(after, before) {
		t.Fatalf("credential collision changed config:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestUpsertMember_newOrderedMemberUsesPlacement(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "DST_BACKEND_A": "cred-a",
		backend.EnvPrefix + "DST_BACKEND_B": "cred-b",
		backend.EnvPrefix + "DST_PRIORITY":  "a,b",
	})

	status, err := p.UpsertMember("dst", "c", "cred-c", "https://c.example", []string{"a", "b", "c"})
	if status != http.StatusOK || err != nil {
		t.Fatalf("new ordered member: status=%d err=%v, want 200", status, err)
	}
	if got := poolPriority(t, p, "dst"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("priority after new member=%v, want [a b c]", got)
	}
	if member, ok := p.CurrentRegistry().ResolveIn("dst", "c"); !ok || member.Credential != "cred-c" {
		t.Fatalf("new member=%+v ok=%v, want cred-c", member, ok)
	}
}
