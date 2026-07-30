package auto

import (
	"net/http"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// TestRegression_243_AddMemberSurvivesDisable pins the runtime-mutation seam
// reported in #243: after AddMember of a new nick, parking the previously
// active member so the sticky pointer is on the new nick, then
// SetMemberDisabled on the previously active member — the runtime-added
// nick must remain present in both the pool's effective config and the
// controller's member list, and re-adding the same nick must return 409.
//
// Covered shapes:
//   - plain pool (no priority)
//   - priority pool with explicit placement
//   - pool with a per-pool base_url
//
// These are expected to pass on the first run; the point of the test is to
// pin the seam as already correct so a future refactor of reconcileLocked /
// WithMemberDisabled cannot silently regress it.
func TestRegression_243_AddMemberSurvivesDisable(t *testing.T) {
	t.Run("plain pool", func(t *testing.T) {
		clock := newMoveClock()
		env := map[string]string{
			backend.EnvPrefix + "AUTO_BACKEND_A": "cred-a",
			backend.EnvPrefix + "AUTO_BACKEND_B": "cred-b",
		}
		p := loadMovePools(t, clock, env)
		assertAddThenDisableSurvives(t, p, clock, "c", "cred-c", "", nil)
	})

	t.Run("priority pool with explicit placement", func(t *testing.T) {
		clock := newMoveClock()
		env := map[string]string{
			backend.EnvPrefix + "AUTO_BACKEND_A": "cred-a",
			backend.EnvPrefix + "AUTO_BACKEND_B": "cred-b",
			backend.EnvPrefix + "AUTO_PRIORITY":  "a,b",
		}
		p := loadMovePools(t, clock, env)
		assertAddThenDisableSurvives(t, p, clock, "c", "cred-c", "",
			[]string{"a", "c", "b"})
	})

	t.Run("pool with per-pool base_url", func(t *testing.T) {
		clock := newMoveClock()
		env := map[string]string{
			backend.EnvPrefix + "AUTO_BACKEND_A": "cred-a",
			backend.EnvPrefix + "AUTO_BASE_URL":  "https://auto.example",
		}
		p := loadMovePools(t, clock, env)
		// c is a brand-new nick with no base_url; AddMember should resolve
		// to the pool default ("https://auto.example") and persist that
		// concrete value — never an empty string.
		assertAddThenDisableSurvives(t, p, clock, "c", "cred-c", "", nil)

		am, ok := addedMember(t, p, "auto", "c")
		if !ok {
			t.Fatalf("c not in c.members after the seam sequence")
		}
		if am.BaseURL != "https://auto.example" {
			t.Errorf("c.BaseURL=%q, want pool default https://auto.example", am.BaseURL)
		}
	})
}

// assertAddThenDisableSurvives walks the AC3 seam for one pool fixture:
// start state → park previously-active member → AddMember → SetMemberDisabled
// on the previously-active member → assert runtime-added nick survives →
// re-add returns 409.
func assertAddThenDisableSurvives(t *testing.T, p *Pools, clock *fixedClock,
	newNick, cred, baseURL string, placement []string) {
	t.Helper()

	c := p.byPool["auto"]
	prev := c.curNick
	if prev == "" {
		t.Fatalf("fixture did not seed an active member")
	}

	// Park the previously active member so AddMember's reconcile moves the
	// sticky pointer to a different member (this is the path #243 surfaced
	// — the runtime-added nick must not get clobbered by a later disable).
	c.mu.Lock()
	if c.exhausted == nil {
		c.exhausted = make(map[string]time.Time)
	}
	c.exhausted[prev] = clock.now().Add(time.Hour)
	c.mu.Unlock()

	if status, err := p.AddMember("auto", newNick, cred, baseURL, placement); status != http.StatusOK || err != nil {
		t.Fatalf("AddMember %s: status=%d err=%v, want 200", newNick, status, err)
	}

	if status, err := p.SetMemberDisabled("auto", prev, true); status != http.StatusOK || err != nil {
		t.Fatalf("SetMemberDisabled %s: status=%d err=%v, want 200", prev, status, err)
	}

	// Runtime-added nick still present in effective config.
	pm := poolMembers(t, p, "auto")
	if !pm[newNick] {
		t.Errorf("poolMembers missing %s after AddMember+SetMemberDisabled(prev): got %v", newNick, pm)
	}

	// Runtime-added nick still present in the controller's member list.
	if _, ok := addedMember(t, p, "auto", newNick); !ok {
		t.Errorf("c.members missing %s after AddMember+SetMemberDisabled(prev)", newNick)
	}

	// Re-adding the same nick must return 409 (issue #198 bijection).
	if status, err := p.AddMember("auto", newNick, cred, baseURL, placement); status != http.StatusConflict || err == nil {
		t.Errorf("re-AddMember %s: status=%d err=%v, want 409 (already present)", newNick, status, err)
	}

	// For the priority-pool case the new nick must also appear in the
	// effective priority order (placement was honored on the add).
	if len(placement) > 0 {
		pri := poolPriority(t, p, "auto")
		found := false
		for _, n := range pri {
			if n == newNick {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("priority order %v missing newly added %s", pri, newNick)
		}
	}
}
