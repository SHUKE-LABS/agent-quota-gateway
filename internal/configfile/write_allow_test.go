package configfile

import (
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/config"
)

// TestMarshal_sharedWriteAllowRoundTrips proves shared_write_allow (issue
// #349) survives Marshal → LoadFile, so an env-seeded allowlist carried into
// a freshly bootstrapped aqg.json is still enforced on every later boot.
func TestMarshal_sharedWriteAllowRoundTrips(t *testing.T) {
	cfg, err := config.Build(config.Inputs{
		AnthropicBaseURL: "https://api.anthropic.com",
		SharedListenAddr: "100.91.4.56:9949",
		SharedWriteAllow: []string{"100.64.1.2", "fd7a:115c:a1e0::/48"},
	})
	if err != nil {
		t.Fatalf("config.Build: %v", err)
	}
	data, err := Marshal(cfg, testRegistry(t))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"shared_write_allow"`) {
		t.Fatalf("marshaled config missing shared_write_allow:\n%s", data)
	}
	path := filepath.Join(t.TempDir(), "aqg.json")
	if err := WriteAtomic(path, data); err != nil {
		t.Fatalf("WriteAtomic: %v", err)
	}
	cfg2, _, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	want := []netip.Prefix{netip.MustParsePrefix("100.64.1.2/32"), netip.MustParsePrefix("fd7a:115c:a1e0::/48")}
	if len(cfg2.SharedWriteAllow) != len(want) || cfg2.SharedWriteAllow[0] != want[0] || cfg2.SharedWriteAllow[1] != want[1] {
		t.Fatalf("round-tripped SharedWriteAllow = %v, want %v", cfg2.SharedWriteAllow, want)
	}
}

func TestMarshal_omitsUnsetSharedWriteAllow(t *testing.T) {
	data, err := Marshal(testConfig(t), testRegistry(t))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(data), "shared_write_allow") {
		t.Fatalf("unset allowlist must be omitted:\n%s", data)
	}
}

func TestLoadFile_rejectsInvalidSharedWriteAllow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aqg.json")
	content := `{"shared_listen_addr": "100.91.4.56:9949", "shared_write_allow": ["admin.ts.net"], "pools": {}}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadFile(path); err == nil || !strings.Contains(err.Error(), "SHARED_WRITE_ALLOW") {
		t.Fatalf("LoadFile err = %v, want a SHARED_WRITE_ALLOW error", err)
	}
}
