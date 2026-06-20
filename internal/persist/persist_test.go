package persist

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
)

// TestLoad_missingAddedPoolsIsBackwardCompatible proves a state file written
// before the added_pools field (issue #104) loads cleanly, with AddedPools
// left nil rather than erroring.
func TestLoad_missingAddedPoolsIsBackwardCompatible(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// A legacy state file: pools + snapshots, no added_pools key.
	legacy := `{"pools":{"auto":{"sticky":"a","exhausted":{}}},"snapshots":{}}`
	if err := os.WriteFile(path, []byte(legacy), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	state, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.AddedPools != nil {
		t.Errorf("AddedPools=%v, want nil for a legacy state file", state.AddedPools)
	}
	if _, ok := state.Pools["auto"]; !ok {
		t.Errorf("legacy pools not loaded: %+v", state.Pools)
	}
}

// TestLoad_roundTripsAddedPools proves added_pools survives a marshal/Load
// round-trip with its base_url intact.
func TestLoad_roundTripsAddedPools(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	data, err := json.Marshal(GatewayState{
		AddedPools: map[string]auto.AddedPoolSpec{
			"rt": {BaseURL: "https://rt.example"},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	spec, ok := got.AddedPools["rt"]
	if !ok {
		t.Fatalf("added_pools missing rt after round-trip: %+v", got.AddedPools)
	}
	if spec.BaseURL != "https://rt.example" {
		t.Errorf("rt base_url=%q, want https://rt.example", spec.BaseURL)
	}
}
