package auto

import (
	"io"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// reloadViaConfig simulates a restart under the config-single-source model
// (issue #198): the current registry (operator intent) is round-tripped
// through its Spec into a fresh Pools, then runtime observation is restored
// from PersistState — mirroring production startup (config → NewPools →
// LoadPersistState). Env is not re-read, so operator mutations recorded in the
// config survive exactly as across a real restart reading aqg.json.
func reloadViaConfig(t *testing.T, p *Pools) *Pools {
	t.Helper()
	reg, err := backend.BuildFromSpec(p.CurrentRegistry().Spec(), "https://api.anthropic.com")
	if err != nil {
		t.Fatalf("rebuild registry from config: %v", err)
	}
	p2 := NewPools(reg, p.store, nil, io.Discard)
	p2.LoadPersistState(p.PersistState())
	return p2
}

// Test shims for the controller-level mutators that issue #198 moved to the
// registry + Pools layer. Controller-level unit tests still want to set up a
// disabled member or a priority order concisely without constructing a whole
// Pools + registry swap; these replicate the pre-#198 in-place effect (which
// is exactly what reconcileLocked would produce from an equivalent registry).
// Caller holds c.mu, matching the original *Locked contract.

func (c *Controller) setDisabledLocked(nick string, off bool) {
	if off {
		c.disabled[nick] = true
	} else {
		delete(c.disabled, nick)
	}
}

func (c *Controller) setPriorityOverrideEffectiveLocked(order []string) {
	if len(order) == 0 {
		c.priority = nil
	} else {
		c.priority = effectiveOrder(order, c.allMemberNicksLocked())
	}
}
