package configfile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
)

// TestLoadFile_debug_absentDefaultsOff covers AC3: an existing aqg.json
// without the debug key loads unchanged and defaults to off. The pointer
// DTO + Marshal's "write only when on" rule together preserve the byte
// shape of pre-#301 files.
func TestLoadFile_debug_absentDefaultsOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aqg.json")
	content := `{
		"pools": {
			"auto": {
				"members": { "a": {"credential": "sk-ant-oat-a"} }
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, _, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.DebugLogRequests {
		t.Errorf("DebugLogRequests = true, want false (section absent)")
	}

	// Round-trip: re-marshalled file must not gain a debug section.
	data, err := Marshal(cfg, testRegistry(t))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(data), `"debug"`) {
		t.Errorf("Marshal wrote a debug section for off state:\n%s", data)
	}
}

// TestLoadFile_debug_sectionTrue covers AC2/the file-mode half: the
// section maps into Inputs and lands on Config.DebugLogRequests.
func TestLoadFile_debug_sectionTrue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aqg.json")
	content := `{
		"debug": { "log_requests": true },
		"pools": {
			"auto": { "members": { "a": {"credential": "sk-ant-oat-a"} } }
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, _, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if !cfg.DebugLogRequests {
		t.Errorf("DebugLogRequests = false, want true")
	}
}

// TestLoadFile_debug_explicitFalseMatchesAbsent — explicit false and absent
// must behave identically: same Config, same Marshal output. Protects the
// "zero-value = off" contract.
func TestLoadFile_debug_explicitFalseMatchesAbsent(t *testing.T) {
	for _, content := range []string{
		// absent
		`{"pools":{"auto":{"members":{"a":{"credential":"sk-ant-oat-a"}}}}}`,
		// present, false
		`{"debug":{"log_requests":false},"pools":{"auto":{"members":{"a":{"credential":"sk-ant-oat-a"}}}}}`,
	} {
		path := filepath.Join(t.TempDir(), "aqg.json")
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, _, err := LoadFile(path)
		if err != nil {
			t.Fatalf("LoadFile(%s): %v", content, err)
		}
		if cfg.DebugLogRequests {
			t.Errorf("LoadFile(%s): DebugLogRequests = true, want false", content)
		}
	}
}

// TestMarshal_debug_sectionRoundTrips is the full round-trip: write the
// section on disk via Marshal + LoadFile, then re-marshal and confirm the
// file shape is preserved (the section stays present, no spurious
// top-level keys appear).
func TestMarshal_debug_sectionRoundTrips(t *testing.T) {
	cfg, err := config.Build(config.Inputs{AnthropicBaseURL: "https://api.anthropic.com", DebugLogRequests: true})
	if err != nil {
		t.Fatalf("config.Build: %v", err)
	}
	reg, err := backend.BuildFromSpec(backend.Spec{Pools: map[string]backend.PoolSpec{
		"auto": {Members: map[string]backend.MemberSpec{"a": {Credential: "sk-ant-oat-a"}}},
	}}, cfg.AnthropicBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}

	data, err := Marshal(cfg, reg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := raw["debug"]; !ok {
		t.Fatalf("Marshal dropped the debug section:\n%s", data)
	}
	var debug map[string]any
	if err := json.Unmarshal(raw["debug"], &debug); err != nil {
		t.Fatalf("Unmarshal debug: %v", err)
	}
	if on, _ := debug["log_requests"].(bool); !on {
		t.Errorf("debug.log_requests = %v, want true", debug["log_requests"])
	}

	// Write to disk and reload — the section must survive, and a re-marshal
	// must produce identical bytes (no spurious top-level additions).
	path := filepath.Join(t.TempDir(), "aqg.json")
	if err := WriteAtomic(path, data); err != nil {
		t.Fatal(err)
	}
	cfg2, reg2, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if !cfg2.DebugLogRequests {
		t.Errorf("re-loaded DebugLogRequests = false, want true")
	}
	data2, err := Marshal(cfg2, reg2)
	if err != nil {
		t.Fatalf("Marshal round-trip: %v", err)
	}
	if string(data2) != string(data) {
		t.Errorf("Marshal bytes drifted on round-trip:\n---first---\n%s\n---second---\n%s", data, data2)
	}
}