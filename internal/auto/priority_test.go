package auto

import (
	"net/http"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// TestSetPriority_runtimeAddedMemberAccepted proves that SetPriority accepts a
// nick that was added at runtime via AddMember, returning 200 and installing
// the override.
func TestSetPriority_runtimeAddedMemberAccepted(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "POOL_BACKEND_A": "cred-a",
		backend.EnvPrefix + "POOL_BACKEND_B": "cred-b",
	})

	if status, err := p.AddMember("pool", "x", "cred-x", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember: status=%d err=%v", status, err)
	}

	// x is runtime-added — SetPriority must accept it.
	status, err := p.SetPriority("pool", []string{"x", "a", "b"})
	if status != http.StatusOK || err != nil {
		t.Fatalf("SetPriority with runtime-added nick: status=%d err=%v, want 200", status, err)
	}

	got := poolPriority(t, p, "pool")
	if len(got) == 0 || got[0] != "x" {
		t.Errorf("priority override not applied: got %v, want x first", got)
	}
}

// TestSetPriority_unlistedRuntimeAddedMemberRanksLast proves that a
// runtime-added member omitted from the declared order is appended last,
// matching the "unlisted members rank last" contract for static members.
func TestSetPriority_unlistedRuntimeAddedMemberRanksLast(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "POOL_BACKEND_A": "cred-a",
		backend.EnvPrefix + "POOL_BACKEND_B": "cred-b",
	})

	if status, err := p.AddMember("pool", "x", "cred-x", "", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember: status=%d err=%v", status, err)
	}

	// Declare order without x — x must be appended last.
	status, err := p.SetPriority("pool", []string{"a", "b"})
	if status != http.StatusOK || err != nil {
		t.Fatalf("SetPriority: status=%d err=%v, want 200", status, err)
	}

	got := poolPriority(t, p, "pool")
	if len(got) < 3 {
		t.Fatalf("effective priority has %d entries, want 3; got %v", len(got), got)
	}
	if got[len(got)-1] != "x" {
		t.Errorf("unlisted runtime-added member not ranked last: got %v, want x last", got)
	}
}

// TestSetPriority_unknownNickStillRejected proves that a nick that is neither
// static nor runtime-added is still rejected with 400.
func TestSetPriority_unknownNickStillRejected(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "POOL_BACKEND_A": "cred-a",
	})

	status, err := p.SetPriority("pool", []string{"a", "ghost"})
	if status != http.StatusBadRequest || err == nil {
		t.Errorf("unknown nick accepted: status=%d err=%v, want 400", status, err)
	}
}
