package configfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
)

func TestResolve_precedence(t *testing.T) {
	// Flag > env > default file > env vars

	t.Run("flag takes precedence", func(t *testing.T) {
		path, ok := Resolve("/custom/path.json")
		if !ok || path != "/custom/path.json" {
			t.Errorf("Resolve(flag) = (%v, %v), want (/custom/path.json, true)", path, ok)
		}
	})

	t.Run("env when flag empty", func(t *testing.T) {
		t.Setenv(EnvConfigPath, "/env/path.json")
		path, ok := Resolve("")
		if !ok || path != "/env/path.json" {
			t.Errorf("Resolve(env) = (%v, %v), want (/env/path.json, true)", path, ok)
		}
	})

	t.Run("default file when no flag or env", func(t *testing.T) {
		tmpDir := t.TempDir()
		original, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(original) })

		// Create a default config file in the temp dir
		if err := os.Chdir(tmpDir); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(DefaultConfigPath, []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}

		path, ok := Resolve("")
		if !ok || path != DefaultConfigPath {
			t.Errorf("Resolve(default) = (%v, %v), want (%s, true)", path, ok, DefaultConfigPath)
		}
	})

	t.Run("env vars when no file found", func(t *testing.T) {
		tmpDir := t.TempDir()
		original, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(original) })

		if err := os.Chdir(tmpDir); err != nil {
			t.Fatal(err)
		}
		// Don't create the default file

		path, ok := Resolve("")
		if ok || path != "" {
			t.Errorf("Resolve(no file) = (%v, %v), want (, false)", path, ok)
		}
	})
}

func TestLoadFile_success(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	content := `{
		"base_url": "https://custom.example.com",
		"listen_addr": "127.0.0.1:9000",
		"shared_listen_addr": "",
		"state_file": "",
		"pools": {
			"auto": {
				"base_url": "",
				"members": {
					"a": {"credential": "sk-ant-oat-aaa"},
					"b": {"credential": "sk-ant-oat-bbb"}
				},
				"priority": ["a", "b"],
				"balance": "",
				"balance_gap": 0,
				"balance_dwell": ""
			},
			"balanced": {
				"members": {
					"x": {"credential": "sk-ant-oat-xxx"}
				},
				"balance": "lead",
				"balance_gap": 0.2,
				"balance_dwell": "10m"
			}
		}
	}`

	if err := os.WriteFile(configPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	cfg, registry, err := LoadFile(configPath)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	// Check config
	if cfg.AnthropicBaseURL != "https://custom.example.com" {
		t.Errorf("AnthropicBaseURL = %q, want https://custom.example.com", cfg.AnthropicBaseURL)
	}
	if cfg.ListenAddr != "127.0.0.1:9000" {
		t.Errorf("ListenAddr = %q, want 127.0.0.1:9000", cfg.ListenAddr)
	}

	// Check registry
	if got := registry.PoolNames(); got[0] != "auto" && got[1] != "balanced" {
		t.Errorf("PoolNames = %v, want [auto balanced]", got)
	}
	if got := registry.PoolPriority("auto"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("PoolPriority(auto) = %v, want [a b]", got)
	}
	if got := registry.PoolBalanceGap("balanced"); got != 0.2 {
		t.Errorf("PoolBalanceGap(balanced) = %v, want 0.2", got)
	}
	if got := registry.PoolBalanceDwell("balanced"); got != 10*time.Minute {
		t.Errorf("PoolBalanceDwell(balanced) = %v, want 10m", got)
	}
}

func TestLoadFile_rejectsLooserPermissions(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	if err := os.WriteFile(configPath, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}

	_, _, err := LoadFile(configPath)
	if err == nil {
		t.Error("LoadFile with 0644 should fail")
	}
	if !strings.Contains(err.Error(), "0600") {
		t.Errorf("error should mention 0600 requirement; got: %v", err)
	}
}

func TestLoadFile_rejectsMalformedJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	if err := os.WriteFile(configPath, []byte("{invalid json"), 0600); err != nil {
		t.Fatal(err)
	}

	_, _, err := LoadFile(configPath)
	if err == nil {
		t.Error("LoadFile with invalid JSON should fail")
	}
}

func TestLoadFile_rejectsUnknownFields(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	content := `{
		"base_url": "https://api.anthropic.com",
		"pools": {
			"auto": {
				"members": {
					"a": {"credential": "sk-ant-oat-aaa"}
				},
				"unknown_field": "value"
			}
		}
	}`

	if err := os.WriteFile(configPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	_, _, err := LoadFile(configPath)
	if err == nil {
		t.Error("LoadFile with unknown field should fail")
	}
}

func TestLoadFile_rejectsInvalidBalanceMode(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	content := `{
		"base_url": "https://api.anthropic.com",
		"pools": {
			"sub": {
				"members": {
					"a": {"credential": "sk-ant-oat-aaa"}
				},
				"balance": "round-robin"
			}
		}
	}`

	if err := os.WriteFile(configPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	_, _, err := LoadFile(configPath)
	if err == nil {
		t.Error("LoadFile with invalid balance mode should fail")
	}
}

func TestLoadFile_rejectsOutOfRangeBalanceGap(t *testing.T) {
	// A gap >= 1.0 is unreachable and silently disables balancing; the
	// config-file path shares buildRegistry's check, so it must fail load
	// (issue #215). 15 is the classic percent/fraction mix-up (meant 0.15).
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	content := `{
		"base_url": "https://api.anthropic.com",
		"pools": {
			"sub": {
				"members": {
					"a": {"credential": "sk-ant-oat-aaa"}
				},
				"balance": "lead",
				"balance_gap": 15
			}
		}
	}`

	if err := os.WriteFile(configPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	_, _, err := LoadFile(configPath)
	if err == nil {
		t.Error("LoadFile with balance_gap >= 1.0 should fail")
	}
}

func TestLoadFile_endToEndParity(t *testing.T) {
	// Load the same config via file and via env vars; compare results.
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	content := `{
		"base_url": "https://api.anthropic.com",
		"listen_addr": "127.0.0.1:8080",
		"shared_listen_addr": "",
		"state_file": "",
		"pools": {
			"cfgfile_pool": {
				"base_url": "https://custom.example.com",
				"members": {
					"member_a": {"credential": "cred-a"},
					"member_b": {"credential": "cred-b", "base_url": "https://override.example.com"}
				},
				"priority": ["member_a", "member_b"],
				"balance": "",
				"balance_gap": 0,
				"balance_dwell": ""
			}
		}
	}`

	if err := os.WriteFile(configPath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	// Load via file
	cfgFile, regFile, err := LoadFile(configPath)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	// Load via equivalent env vars
	t.Setenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com")
	t.Setenv("LISTEN_ADDR", "127.0.0.1:8080")
	t.Setenv("SHARED_LISTEN_ADDR", "") // explicitly clear
	t.Setenv("AQG_POOL_CFGFILE_POOL_BASE_URL", "https://custom.example.com")
	t.Setenv("AQG_POOL_CFGFILE_POOL_BACKEND_MEMBER_A", "cred-a")
	t.Setenv("AQG_POOL_CFGFILE_POOL_BACKEND_MEMBER_B", "cred-b|https://override.example.com")
	t.Setenv("AQG_POOL_CFGFILE_POOL_PRIORITY", "member_a,member_b")

	cfgEnv, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	regEnv, err := backend.Load(cfgEnv.AnthropicBaseURL)
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}

	// Compare configs
	if cfgFile.AnthropicBaseURL != cfgEnv.AnthropicBaseURL {
		t.Errorf("AnthropicBaseURL: file=%q env=%q", cfgFile.AnthropicBaseURL, cfgEnv.AnthropicBaseURL)
	}
	if cfgFile.ListenAddr != cfgEnv.ListenAddr {
		t.Errorf("ListenAddr: file=%q env=%q", cfgFile.ListenAddr, cfgEnv.ListenAddr)
	}

	// Compare registries - filter to only our pool to avoid noise from other tests
	const poolName = "cfgfile-pool"

	filePools := filterPools(regFile.PoolNames(), poolName)
	envPools := filterPools(regEnv.PoolNames(), poolName)
	if len(filePools) != 1 || len(envPools) != 1 || filePools[0] != poolName || envPools[0] != poolName {
		t.Logf("All pools - file: %v, env: %v", regFile.PoolNames(), regEnv.PoolNames())
		t.Errorf("PoolNames after filter: file=%v env=%v", filePools, envPools)
	}

	// Compare pool members
	fileNicks := regFile.PoolNicks(poolName)
	envNicks := regEnv.PoolNicks(poolName)
	if len(fileNicks) != len(envNicks) {
		t.Errorf("PoolNicks count: file=%v env=%v", fileNicks, envNicks)
	}

	// Compare priority
	filePri := regFile.PoolPriority(poolName)
	envPri := regEnv.PoolPriority(poolName)
	if len(filePri) != len(envPri) {
		t.Errorf("PoolPriority length: file=%v env=%v", filePri, envPri)
	}
	if len(filePri) > 0 && filePri[0] != envPri[0] {
		t.Errorf("PoolPriority[0]: file=%v env=%v", filePri, envPri)
	}
}

func filterPools(pools []string, keep string) []string {
	var out []string
	for _, p := range pools {
		if p == keep {
			out = append(out, p)
		}
	}
	return out
}

func TestLoadFile_noCredentialInError(t *testing.T) {
	// Sentinel credential that must never appear in error messages.
	// If a future change introduces input-echo behavior in errors, this
	// test will fail loud.
	const sentinelCred = "sk-ant-oat-LEAKCANARY-7Q3K"

	t.Run("bad permissions", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.json")

		content := `{
			"pools": {
				"auto": {
					"members": {
						"a": {"credential": "` + sentinelCred + `"}
					}
				}
			}
		}`

		if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}

		_, _, err := LoadFile(configPath)
		if err == nil {
			t.Fatal("LoadFile with 0644 should fail")
		}
		if !strings.Contains(err.Error(), "0600") {
			t.Errorf("error should mention 0600 requirement; got: %v", err)
		}
		if strings.Contains(err.Error(), sentinelCred) {
			t.Errorf("error message contains credential value: %v", err)
		}
	})

	t.Run("malformed JSON", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.json")

		// Malformed JSON with credential embedded
		content := `{"pools":{"auto":{"members":{"a":{"credential":"` + sentinelCred + `"}}}`
		if err := os.WriteFile(configPath, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}

		_, _, err := LoadFile(configPath)
		if err == nil {
			t.Fatal("LoadFile with malformed JSON should fail")
		}
		if strings.Contains(err.Error(), sentinelCred) {
			t.Errorf("error message contains credential value: %v", err)
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.json")

		content := `{
			"pools": {
				"auto": {
					"members": {
						"a": {"credential": "` + sentinelCred + `"}
					},
					"unknown_field": "x"
				}
			}
		}`

		if err := os.WriteFile(configPath, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}

		_, _, err := LoadFile(configPath)
		if err == nil {
			t.Fatal("LoadFile with unknown field should fail")
		}
		if strings.Contains(err.Error(), sentinelCred) {
			t.Errorf("error message contains credential value: %v", err)
		}
	})

	t.Run("invalid balance mode", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.json")

		content := `{
			"pools": {
				"auto": {
					"members": {
						"a": {"credential": "` + sentinelCred + `"}
					},
					"balance": "round-robin"
				}
			}
		}`

		if err := os.WriteFile(configPath, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}

		_, _, err := LoadFile(configPath)
		if err == nil {
			t.Fatal("LoadFile with invalid balance mode should fail")
		}
		if strings.Contains(err.Error(), sentinelCred) {
			t.Errorf("error message contains credential value: %v", err)
		}
	})
}
