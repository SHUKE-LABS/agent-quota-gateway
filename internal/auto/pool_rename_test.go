package auto

import (
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// TestRenamePool_preservesObservation proves a renamed pool keeps its
// sticky pointer, exhausted mark, balance sequence, and local-snapshot set
// attached to the controller under the new name (issue #238 AC: runtime
// observation survives the rename).
func TestRenamePool_preservesObservation(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://a.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: status=%d err=%v", status, err)
	}
	if status, err := p.AddMember("rt", "b", "cred-b", "https://b.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember b: status=%d err=%v", status, err)
	}

	// Stick a: park b at a future reset; record a local-snapshot mark for a.
	c, ok := p.controller("rt")
	if !ok {
		t.Fatalf("controller(rt) not found")
	}
	c.mu.Lock()
	c.curNick = "a"
	c.exhausted["b"] = clock.now().Add(time.Hour)
	c.poolLocalSnapshots["a"] = struct{}{}
	c.lastSelectedSeq["a"] = 7
	c.balanceSeq = 11
	c.mu.Unlock()

	// Rename.
	if status, err := p.RenamePool("rt", "newname"); status != http.StatusOK || err != nil {
		t.Fatalf("RenamePool: status=%d err=%v, want 200", status, err)
	}

	// The old key is gone; the new key resolves to the same controller.
	if _, ok := p.controller("rt"); ok {
		t.Errorf("old byPool key rt still present after rename")
	}
	c2, ok := p.controller("newname")
	if !ok {
		t.Fatalf("new byPool key newname missing after rename")
	}
	if c2 != c {
		t.Errorf("RenamePool replaced the controller; want in-place rewiring")
	}

	// Observation intact under the new name.
	c2.mu.Lock()
	if c2.curNick != "a" {
		t.Errorf("curNick=%q after rename, want a", c2.curNick)
	}
	reset, ok := c2.exhausted["b"]
	if !ok || !reset.Equal(clock.now().Add(time.Hour)) {
		t.Errorf("exhausted[b]=%v ok=%v after rename, want preserved", reset, ok)
	}
	if _, ok := c2.poolLocalSnapshots["a"]; !ok {
		t.Errorf("poolLocalSnapshots[a] lost across rename")
	}
	if c2.lastSelectedSeq["a"] != 7 {
		t.Errorf("lastSelectedSeq[a]=%d after rename, want 7", c2.lastSelectedSeq["a"])
	}
	if c2.balanceSeq != 11 {
		t.Errorf("balanceSeq=%d after rename, want 11", c2.balanceSeq)
	}
	c2.mu.Unlock()

	// Routing by old name fails closed (known=false → 403 at the HTTP
	// boundary); routing by new name still serves the active sticky member.
	if _, _, known, _ := p.Route("rt"); known {
		t.Errorf("Route(rt) known=true after rename, want unknown")
	}
	cur, known := p.Current("newname")
	if !known {
		t.Fatalf("Route(newname) known=false after rename")
	}
	if cur.Nick != "a" {
		t.Errorf("Route(newname).Nick=%q, want a", cur.Nick)
	}
}

// TestRenamePool_siblingUnaffected proves renaming one pool leaves every
// other pool's controller, membership, and observation untouched.
func TestRenamePool_siblingUnaffected(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool rt: %v", err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://a.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: %v", err)
	}

	// Park src/x so we can prove it survives the rename of a sibling.
	srcCtrl, ok := p.controller("src")
	if !ok {
		t.Fatalf("controller(src) not found")
	}
	srcCtrl.mu.Lock()
	srcCtrl.exhausted["x"] = clock.now().Add(time.Minute)
	srcCtrl.mu.Unlock()

	if status, err := p.RenamePool("rt", "newname"); status != http.StatusOK || err != nil {
		t.Fatalf("RenamePool: %v", err)
	}

	srcCtrl2, ok := p.controller("src")
	if !ok {
		t.Fatalf("controller(src) missing after sibling rename")
	}
	srcCtrl2.mu.Lock()
	defer srcCtrl2.mu.Unlock()
	reset, ok := srcCtrl2.exhausted["x"]
	if !ok || !reset.Equal(clock.now().Add(time.Minute)) {
		t.Errorf("sibling src/x exhausted mark lost across rename: %v ok=%v", reset, ok)
	}
}

// TestRenamePool_errors covers the four error paths: unknown old, empty new,
// identical-after-normalize, and conflicting new.
func TestRenamePool_errors(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: %v", err)
	}
	if status, err := p.AddPool("other", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool other: %v", err)
	}

	cases := []struct {
		old, new string
		want     int
	}{
		{"ghost", "renamed", http.StatusNotFound},
		{"rt", "", http.StatusBadRequest},
		{"rt", "RT", http.StatusBadRequest},                // identical after normalize
		{"rt", "other", http.StatusConflict},               // collides with different existing pool
		{"", "renamed", http.StatusBadRequest},             // empty old
	}
	for _, tc := range cases {
		status, err := p.RenamePool(tc.old, tc.new)
		if status != tc.want {
			t.Errorf("RenamePool(%q,%q): status=%d err=%v, want %d", tc.old, tc.new, status, err, tc.want)
		}
	}
}

// TestRenamePool_persistedAcrossRestart proves the rename survives a
// config-roundtrip restart and that the persisted sticky/exhausted state is
// rewritten under the new key (otherwise LoadPersistState on restart drops it).
func TestRenamePool_persistedAcrossRestart(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	// Install the state-dirty callback that the real wiring uses (issue #198
	// state flush). The persister itself is not running, but the callback
	// being wired proves the RenamePool path fires it.
	p.SetOnMutate(func() {})

	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: %v", err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://a.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: %v", err)
	}
	if status, err := p.AddMember("rt", "b", "cred-b", "https://b.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember b: %v", err)
	}

	// Anchor sticky on a and park b so observation has something to carry.
	c, ok := p.controller("rt")
	if !ok {
		t.Fatalf("controller(rt) missing")
	}
	c.mu.Lock()
	c.curNick = "a"
	c.exhausted["b"] = clock.now().Add(time.Hour)
	c.poolLocalSnapshots["a"] = struct{}{}
	c.mu.Unlock()

	// Rename + a PersistState flush to materialise the new key in the state
	// map that LoadPersistState will consume on restart.
	if status, err := p.RenamePool("rt", "newname"); status != http.StatusOK || err != nil {
		t.Fatalf("RenamePool: %v", err)
	}
	state := p.PersistState()
	if _, ok := state["rt"]; ok {
		t.Errorf("persisted state still carries old key rt")
	}
	if _, ok := state["newname"]; !ok {
		t.Fatalf("persisted state missing new key newname")
	}

	// Reload from the registry (operator intent survives), then reapply the
	// captured state — the new Pools must restore observation under newname.
	reg := p.CurrentRegistry()
	p2 := NewPools(reg, quota.NewStore(), clock.now, io.Discard)
	p2.LoadPersistState(state)
	c2, ok := p2.controller("newname")
	if !ok {
		t.Fatalf("controller(newname) missing after restart")
	}
	c2.mu.Lock()
	defer c2.mu.Unlock()
	if c2.curNick != "a" {
		t.Errorf("sticky after restart: curNick=%q, want a", c2.curNick)
	}
	if _, ok := c2.exhausted["b"]; !ok {
		t.Errorf("exhausted[b] lost across restart")
	}
	if _, ok := c2.poolLocalSnapshots["a"]; !ok {
		t.Errorf("poolLocalSnapshots[a] lost across restart")
	}
}

// TestRenamePool_routeAfterRename exercises the controller-lookup atomicity
// claim (issue #238 AC "routing by the old selector fails closed"). A rename
// followed by a Route under both names confirms the byPool swap is observed
// by the read path without an explicit lock.
func TestRenamePool_routeAfterRename(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: %v", err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://a.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: %v", err)
	}

	if status, err := p.RenamePool("rt", "renamed"); status != http.StatusOK || err != nil {
		t.Fatalf("RenamePool: %v", err)
	}

	// Old selector: unknown (Route returns known=false).
	if _, _, known, _ := p.Route("rt"); known {
		t.Errorf("Route(rt) known=true after rename")
	}
	// New selector: serves the renamed pool's sticky backend.
	cur, known := p.Current("renamed")
	if !known {
		t.Fatalf("Route(renamed) known=false after rename")
	}
	if cur.Nick != "a" {
		t.Errorf("Route(renamed).Nick=%q, want a", cur.Nick)
	}
}

// TestRenamePool_concurrentWithRoute drives Route/ModifyResponse against a
// rename under -race. The atomic pool-name pointer plus the under-p.mu byPool
// swap must keep both paths race-clean: a Route that lands before the swap
// sees the old name and fails closed; one that lands after sees the new name
// and serves; one that lands in flight (during the swap window) sees one or
// the other, never a torn map entry.
func TestRenamePool_concurrentWithRoute(t *testing.T) {
	clock := newMoveClock()
	p := loadMovePools(t, clock, map[string]string{
		backend.EnvPrefix + "SRC_BACKEND_X": "cred-x",
	})
	if status, err := p.AddPool("rt", ""); status != http.StatusCreated || err != nil {
		t.Fatalf("AddPool: %v", err)
	}
	if status, err := p.AddMember("rt", "a", "cred-a", "https://a.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember a: %v", err)
	}
	if status, err := p.AddMember("rt", "b", "cred-b", "https://b.example", nil); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember b: %v", err)
	}

	const N = 200
	var wg sync.WaitGroup
	wg.Add(2)
	// Reader: hammers Route + Current + controller lookup.
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			_, _, _, _ = p.Route("rt")
			_, _ = p.Current("rt")
			_, _ = p.controller("rt")
			_, _ = p.controller("renamed")
		}
	}()
	// Writer: renames back and forth so each iteration forces a real swap.
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			if i%2 == 0 {
				_, _ = p.RenamePool("rt", "renamed")
			} else {
				_, _ = p.RenamePool("renamed", "rt")
			}
		}
	}()
	wg.Wait()
}
