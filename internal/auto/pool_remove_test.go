package auto

import (
	"net/http"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// TestRemovePool_success proves an empty runtime pool is removed from the live
// registry and no longer routes (Route reports unknown), the inverse of AddPool
// (issue #232).
func TestRemovePool_success(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	if status, err := p.RemovePool("RT"); status != http.StatusOK || err != nil {
		t.Fatalf("RemovePool: status=%d err=%v, want 200", status, err)
	}
	if poolNames(t, p)["rt"] {
		t.Errorf("rt still in config view after removal")
	}
	// Routing to the removed pool fails closed: the controller left byPool, so
	// Route reports unknown (the middleware turns that into 403).
	if _, _, known, _ := p.Route("rt"); known {
		t.Errorf("Route(rt) known=true after removal, want unknown")
	}
	// The env pool is untouched.
	if !poolNames(t, p)["src"] {
		t.Errorf("env pool src missing after unrelated removal")
	}
}

// TestRemovePool_notFound proves an unknown pool returns 404.
func TestRemovePool_notFound(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.RemovePool("ghost"); status != http.StatusNotFound || err == nil {
		t.Fatalf("RemovePool(ghost): status=%d err=%v, want 404", status, err)
	}
	// An empty name normalizes away → 400.
	if status, err := p.RemovePool(""); status != http.StatusBadRequest || err == nil {
		t.Fatalf("RemovePool(\"\"): status=%d err=%v, want 400", status, err)
	}
}

// TestRemovePool_conflictNonEmpty proves a pool with members cannot be deleted
// (409, drain-first); once drained it deletes. No cascade discards credentials.
func TestRemovePool_conflictNonEmpty(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://a.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember: status=%d err=%v", status, err)
	}
	if status, err := p.RemovePool("rt"); status != http.StatusConflict || err == nil {
		t.Fatalf("RemovePool non-empty: status=%d err=%v, want 409", status, err)
	}
	// The pool and its member survive the rejected delete.
	if !poolNames(t, p)["rt"] {
		t.Fatalf("rt vanished after a rejected (409) removal")
	}
	// Drain the member, then the delete succeeds.
	if status, err := p.RemoveMember("rt", "a"); status != http.StatusOK || err != nil {
		t.Fatalf("RemoveMember: status=%d err=%v", status, err)
	}
	if status, err := p.RemovePool("rt"); status != http.StatusOK || err != nil {
		t.Fatalf("RemovePool after drain: status=%d err=%v, want 200", status, err)
	}
}

// TestRemovePool_persistedAcrossRestart proves a deleted pool does not reappear
// after a config round-trip restart (persistence verified), while a sibling
// runtime pool survives.
func TestRemovePool_persistedAcrossRestart(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("gone", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool gone: status=%d err=%v", status, err)
	}
	if status, err := p.AddPool("kept", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool kept: status=%d err=%v", status, err)
	}
	if status, err := p.RemovePool("gone"); status != http.StatusOK || err != nil {
		t.Fatalf("RemovePool gone: status=%d err=%v", status, err)
	}

	p2 := reloadViaConfig(t, p)
	if poolNames(t, p2)["gone"] {
		t.Errorf("deleted pool gone reappeared after restart")
	}
	if !poolNames(t, p2)["kept"] {
		t.Errorf("sibling runtime pool kept missing after restart")
	}
}

// TestRemovePool_deleteLastPool proves deleting the only pool is permitted;
// routing afterward fails closed (known=false → 403 at the HTTP boundary).
func TestRemovePool_deleteLastPool(t *testing.T) {
	clock := newMoveClock()
	// Bootstrap with a single env pool, then drain and delete it.
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "ONLY_BACKEND_X": "cred-x",
	})
	if status, err := p.RemoveMember("only", "x"); status != http.StatusOK || err != nil {
		t.Fatalf("RemoveMember: status=%d err=%v", status, err)
	}
	if status, err := p.RemovePool("only"); status != http.StatusOK || err != nil {
		t.Fatalf("RemovePool last: status=%d err=%v, want 200", status, err)
	}
	if len(poolNames(t, p)) != 0 {
		t.Errorf("pools remain after deleting the last one: %v", poolNames(t, p))
	}
	if _, _, known, _ := p.Route("only"); known {
		t.Errorf("Route(only) known=true after deleting the last pool, want unknown")
	}
}
