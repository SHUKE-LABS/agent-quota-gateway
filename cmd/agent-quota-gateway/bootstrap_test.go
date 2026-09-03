package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/configfile"
)

const testBase = "https://api.anthropic.com"

// unsetenv clears key for the duration of the test and restores it on
// cleanup. t.Setenv only sets a value; the bootstrap path is sensitive to
// AQG_CONFIG / AQG_STATE_FILE being *absent*, so a test needs to force-unset
// an ambient value deterministically.
func unsetenv(t *testing.T, key string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	os.Unsetenv(key) //nolint:errcheck // only fails on empty key
	if had {
		t.Cleanup(func() { os.Setenv(key, prev) }) //nolint:errcheck
	}
}

// writeStateFile writes a legacy state-file fixture and returns its path.
func writeStateFile(t *testing.T, dir, body string) string {
	t.Helper()
	p := filepath.Join(dir, "state.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	return p
}

// specReg builds a registry the way the file source would, so the
// applyLegacyOverlay unit tests can construct a precise env baseline without
// going through env parsing.
func specReg(t *testing.T, pools map[string]backend.PoolSpec) *backend.Registry {
	t.Helper()
	reg, err := backend.BuildFromSpec(backend.Spec{Pools: pools}, testBase)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	return reg
}

// --- resolveConfig branches ------------------------------------------------

func TestResolveConfig_noConfigPath_envOnly(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_CONFIG")
	unsetenv(t, "AQG_STATE_FILE")
	// Chdir into an empty dir so configfile.Resolve's ./aqg.json cwd-stat
	// (configfile.go:44) cannot pick up a stray file and make the branch flaky.
	t.Chdir(t.TempDir())
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")

	var buf bytes.Buffer
	_, reg, path, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if path != "" {
		t.Errorf("path = %q, want \"\" (write-through disabled in env-only mode)", path)
	}
	if !reg.HasPool("auto") {
		t.Error("registry missing env-declared pool auto")
	}
}

func TestResolveConfig_configFileExists_ignoresEnv(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	// A file whose only pool is "filepool"; env below declares a *different*
	// pool that must be ignored entirely when the file exists.
	fileJSON := `{"base_url":"https://api.anthropic.com","pools":{"filepool":{"members":{"fm":{"credential":"cred-file"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_POOL_ENVONLY_BACKEND_X", "sk-ant-x")

	var buf bytes.Buffer
	_, reg, path, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if path != cfgPath {
		t.Errorf("path = %q, want %q", path, cfgPath)
	}
	if !reg.HasPool("filepool") {
		t.Error("registry missing file-declared pool filepool")
	}
	if reg.HasPool("envonly") {
		t.Error("env pool leaked in: AQG_POOL_* must be ignored when the config file exists")
	}
}

func TestResolveConfig_bootstrapsWhenAbsent(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")

	var buf bytes.Buffer
	_, reg, path, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if path != cfgPath {
		t.Errorf("path = %q, want %q", path, cfgPath)
	}
	if !reg.HasPool("auto") {
		t.Error("bootstrapped registry missing env pool auto")
	}
	fi, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatalf("bootstrapped config file not written: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("config file mode = %#o, want 0600", fi.Mode().Perm())
	}
}

// TestResolveConfig_freshBootstrapEmptyEnv proves issue #298: an empty
// AQG_CONFIG path with no AQG_POOL_* env vars set boots instead of erroring
// out before aqg.json is ever written, and the resulting file itself then
// boots cleanly on a second start (the existing-file branch), matching the
// zero-pool contract BuildFromSpec already guarantees for runtime pool
// deletion (issue #232).
func TestResolveConfig_freshBootstrapEmptyEnv(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	t.Setenv("AQG_CONFIG", cfgPath)

	var buf bytes.Buffer
	_, reg, path, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig (fresh bootstrap, empty env): %v, want nil", err)
	}
	if path != cfgPath {
		t.Errorf("path = %q, want %q", path, cfgPath)
	}
	if names := reg.PoolNames(); len(names) != 0 {
		t.Errorf("bootstrapped registry has pools %v, want none", names)
	}
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("bootstrapped config file not written: %v", err)
	}

	// Second start: the file now exists (existing-file branch), env is still
	// empty and stays ignored either way — must still boot with zero pools.
	var buf2 bytes.Buffer
	_, reg2, path2, err := resolveConfig("", &buf2)
	if err != nil {
		t.Fatalf("resolveConfig (existing empty aqg.json): %v, want nil", err)
	}
	if path2 != cfgPath {
		t.Errorf("path = %q, want %q", path2, cfgPath)
	}
	if names := reg2.PoolNames(); len(names) != 0 {
		t.Errorf("re-read registry has pools %v, want none", names)
	}
}

// TestResolveConfigAndWarn_emitsWarningOnFreshBootstrap drives run()'s actual
// startup call (resolveConfigAndWarn) end-to-end against a temp AQG_CONFIG
// path with no file and no AQG_POOL_* env vars, and asserts the zero-pool
// WARNING lands on the captured log output. This is the exact sequence run()
// executes, so it demonstrates the journal-visible warning the issue #298
// acceptance boundary requires without standing up the HTTP listener.
func TestResolveConfigAndWarn_emitsWarningOnFreshBootstrap(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	t.Setenv("AQG_CONFIG", cfgPath)

	var buf bytes.Buffer
	_, reg, _, err := resolveConfigAndWarn("", &buf)
	if err != nil {
		t.Fatalf("resolveConfigAndWarn: %v, want nil", err)
	}
	if names := reg.PoolNames(); len(names) != 0 {
		t.Fatalf("registry has pools %v, want none", names)
	}
	if !strings.Contains(buf.String(), "started with no pools configured") {
		t.Errorf("startup log = %q, want a no-pools WARNING", buf.String())
	}
	if !strings.Contains(buf.String(), "POST /_gateway/pool") {
		t.Errorf("startup log = %q, want it to mention POST /_gateway/pool", buf.String())
	}
}

// TestWarnIfNoPools_freshBootstrapMentionsEnvRecovery, ..._existingFileOmitsEnvRecovery
// and ..._populatedRegistryQuiet pin the exact source-aware contract: env
// re-seeding is only ever a valid recovery path the moment aqg.json is
// freshly written, since it is never consulted again afterward (issue #198).
func TestWarnIfNoPools_freshBootstrapMentionsEnvRecovery(t *testing.T) {
	reg := specReg(t, map[string]backend.PoolSpec{})
	var buf bytes.Buffer
	warnIfNoPools(reg, true, "/var/lib/agent-quota-gateway/aqg.json", &buf)
	got := buf.String()
	if !strings.Contains(got, "POST /_gateway/pool") {
		t.Errorf("warning = %q, want it to mention POST /_gateway/pool", got)
	}
	if !strings.Contains(got, "AQG_POOL_") || !strings.Contains(got, "/var/lib/agent-quota-gateway/aqg.json") {
		t.Errorf("warning = %q, want it to mention env re-seeding and the config path (fresh bootstrap)", got)
	}
}

func TestWarnIfNoPools_existingFileOmitsEnvRecovery(t *testing.T) {
	reg := specReg(t, map[string]backend.PoolSpec{})
	var buf bytes.Buffer
	warnIfNoPools(reg, false, "/var/lib/agent-quota-gateway/aqg.json", &buf)
	got := buf.String()
	if !strings.Contains(got, "POST /_gateway/pool") {
		t.Errorf("warning = %q, want it to mention POST /_gateway/pool", got)
	}
	if strings.Contains(got, "AQG_POOL_") {
		t.Errorf("warning = %q, must not suggest env re-seeding once aqg.json already existed", got)
	}
}

func TestWarnIfNoPools_populatedRegistryQuiet(t *testing.T) {
	reg := specReg(t, map[string]backend.PoolSpec{
		"auto": {Members: map[string]backend.MemberSpec{"a": {Credential: "cred-a"}}},
	})
	for _, fresh := range []bool{true, false} {
		var buf bytes.Buffer
		warnIfNoPools(reg, fresh, "/var/lib/agent-quota-gateway/aqg.json", &buf)
		if buf.String() != "" {
			t.Errorf("freshBootstrap=%v: warning = %q, want silence for a populated registry", fresh, buf.String())
		}
	}
}

func TestWarnIfPersistenceDisabled_startupOutput(t *testing.T) {
	tests := []struct {
		name       string
		configPath string
		statePath  string
		wantWarn   bool
	}{
		{
			name:       "config file with empty state file",
			configPath: "/etc/agent-quota-gateway/aqg.json",
			wantWarn:   true,
		},
		{
			name:       "config file with state file",
			configPath: "/etc/agent-quota-gateway/aqg.json",
			statePath:  "/var/lib/agent-quota-gateway/state.json",
		},
		{
			name: "env mode with empty state file",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			warnIfPersistenceDisabled(tc.configPath, tc.statePath, &output)
			got := output.String()
			if !tc.wantWarn {
				if got != "" {
					t.Fatalf("warning = %q, want no warning", got)
				}
				return
			}

			if strings.Count(got, "\n") != 1 {
				t.Fatalf("warning = %q, want one line", got)
			}
			for _, want := range []string{
				"agent-quota-gateway: ",
				tc.configPath,
				"runtime persistence is disabled",
				"sticky pointers",
				"exhausted maps",
				"balance sequence",
				"quota snapshots",
				"do not survive a restart",
				"state_file",
				"restart",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("warning %q missing %q", got, want)
				}
			}
		})
	}
}

// --- loadLegacyOverlay -----------------------------------------------------

func TestLoadLegacyOverlay(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name    string
		path    func() string
		wantOK  bool
		wantPri []string // priority_override for pool "auto" when wantOK
	}{
		{
			name:   "empty path",
			path:   func() string { return "" },
			wantOK: false,
		},
		{
			name:   "missing file",
			path:   func() string { return filepath.Join(dir, "does-not-exist.json") },
			wantOK: false,
		},
		{
			name:   "unparseable JSON",
			path:   func() string { return writeStateFile(t, t.TempDir(), "{not json") },
			wantOK: false,
		},
		{
			name:   "no overlay keys",
			path:   func() string { return writeStateFile(t, t.TempDir(), `{"snapshots":{"x":{}},"sticky":{}}`) },
			wantOK: false,
		},
		{
			name: "valid overlay",
			path: func() string {
				return writeStateFile(t, t.TempDir(), `{"config":{"auto":{"priority_override":["b","a"]}}}`)
			},
			wantOK:  true,
			wantPri: []string{"b", "a"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ls, ok := loadLegacyOverlay(tc.path())
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if tc.wantOK {
				got := ls.Config["auto"].PriorityOverride
				if strings.Join(got, ",") != strings.Join(tc.wantPri, ",") {
					t.Errorf("priority_override = %v, want %v", got, tc.wantPri)
				}
			}
		})
	}
}

// --- applyLegacyOverlay ----------------------------------------------------

// autoPool builds a single "auto" pool spec from nick->credential pairs, all
// inheriting the default base URL.
func autoPool(members map[string]string) map[string]backend.PoolSpec {
	m := make(map[string]backend.MemberSpec, len(members))
	for nick, cred := range members {
		m[nick] = backend.MemberSpec{Credential: cred}
	}
	return map[string]backend.PoolSpec{"auto": {Members: m}}
}

func TestApplyLegacyOverlay(t *testing.T) {
	t.Run("state credential and base_url win for shared nick", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a"}))
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"auto": {AddedMembers: map[string]legacyMember{
				"a": {Credential: "cred-a2", BaseURL: "https://alt.example"},
			}},
		}}
		got := applyLegacyOverlay(reg, ls, &bytes.Buffer{})
		b, ok := got.ResolveIn("auto", "a")
		if !ok {
			t.Fatal("member a missing")
		}
		if b.Credential != "cred-a2" {
			t.Errorf("credential = %q, want cred-a2 (state wins)", b.Credential)
		}
		if b.BaseURL != "https://alt.example" {
			t.Errorf("base_url = %q, want https://alt.example (state wins)", b.BaseURL)
		}
	})

	t.Run("empty base_url resolves from env, credential still wins", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a"}))
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"auto": {AddedMembers: map[string]legacyMember{
				"a": {Credential: "cred-a2", BaseURL: ""},
			}},
		}}
		got := applyLegacyOverlay(reg, ls, &bytes.Buffer{})
		b, _ := got.ResolveIn("auto", "a")
		if b.BaseURL != testBase {
			t.Errorf("base_url = %q, want %q (resolved from env)", b.BaseURL, testBase)
		}
		if b.Credential != "cred-a2" {
			t.Errorf("credential = %q, want cred-a2", b.Credential)
		}
	})

	t.Run("env-only member retained and state member brought in", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a", "b": "cred-b"}))
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"auto": {AddedMembers: map[string]legacyMember{
				"c": {Credential: "cred-c", BaseURL: "https://c.example"},
			}},
		}}
		got := applyLegacyOverlay(reg, ls, &bytes.Buffer{})
		for _, nick := range []string{"a", "b", "c"} {
			if _, ok := got.ResolveIn("auto", nick); !ok {
				t.Errorf("member %q missing", nick)
			}
		}
	})

	t.Run("state member with unresolvable base_url is skipped", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a"}))
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"auto": {AddedMembers: map[string]legacyMember{
				"d": {Credential: "cred-d", BaseURL: ""}, // d not in env → no base to resolve
			}},
		}}
		var buf bytes.Buffer
		got := applyLegacyOverlay(reg, ls, &buf)
		if _, ok := got.ResolveIn("auto", "d"); ok {
			t.Error("member d should have been skipped (no resolvable base_url)")
		}
		if !strings.Contains(buf.String(), "auto/d") {
			t.Errorf("expected skip log naming auto/d, got %q", buf.String())
		}
	})

	t.Run("removal applied", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a", "b": "cred-b"}))
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"auto": {RemovedMembers: []string{"a"}},
		}}
		got := applyLegacyOverlay(reg, ls, &bytes.Buffer{})
		if _, ok := got.ResolveIn("auto", "a"); ok {
			t.Error("member a should be removed")
		}
		if _, ok := got.ResolveIn("auto", "b"); !ok {
			t.Error("member b should survive")
		}
	})

	t.Run("priority filtered to surviving members", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a", "b": "cred-b"}))
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"auto": {PriorityOverride: []string{"b", "a", "ghost"}},
		}}
		got := applyLegacyOverlay(reg, ls, &bytes.Buffer{})
		pri := got.Spec().Pools["auto"].Priority
		if strings.Join(pri, ",") != "b,a" {
			t.Errorf("priority = %v, want [b a] (ghost filtered out)", pri)
		}
	})

	t.Run("disable via standalone disabled list", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a", "b": "cred-b"}))
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"auto": {Disabled: []string{"b"}}, // b not in added_members
		}}
		got := applyLegacyOverlay(reg, ls, &bytes.Buffer{})
		if b, _ := got.ResolveIn("auto", "b"); !b.Disabled {
			t.Error("member b should be disabled via standalone list")
		}
		if a, _ := got.ResolveIn("auto", "a"); a.Disabled {
			t.Error("member a should stay enabled")
		}
	})

	t.Run("disable via added_members flag", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a"}))
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"auto": {
				Disabled:     []string{"a"},
				AddedMembers: map[string]legacyMember{"a": {Credential: "cred-a", BaseURL: testBase}},
			},
		}}
		got := applyLegacyOverlay(reg, ls, &bytes.Buffer{})
		if a, _ := got.ResolveIn("auto", "a"); !a.Disabled {
			t.Error("member a should be disabled via added_members disabled flag")
		}
	})

	t.Run("runtime-created pool added", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a"}))
		ls := legacyState{AddedPools: map[string]json.RawMessage{"extra": json.RawMessage(`{}`)}}
		got := applyLegacyOverlay(reg, ls, &bytes.Buffer{})
		if !got.HasPool("extra") {
			t.Error("runtime-created pool extra missing")
		}
	})

	t.Run("overlay for unknown pool skipped", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a"}))
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"ghostpool": {AddedMembers: map[string]legacyMember{"x": {Credential: "cred-x", BaseURL: testBase}}},
		}}
		got := applyLegacyOverlay(reg, ls, &bytes.Buffer{})
		if got.HasPool("ghostpool") {
			t.Error("overlay must not resurrect a pool absent from env and not runtime-created")
		}
	})

	t.Run("invalid mutation logged and skipped, rest survives", func(t *testing.T) {
		reg := specReg(t, autoPool(map[string]string{"a": "cred-a", "b": "cred-b"}))
		// c reuses cred-b → violates the nick↔credential bijection.
		ls := legacyState{Config: map[string]legacyPoolConfig{
			"auto": {AddedMembers: map[string]legacyMember{
				"c": {Credential: "cred-b", BaseURL: testBase},
			}},
		}}
		var buf bytes.Buffer
		got := applyLegacyOverlay(reg, ls, &buf)
		if _, ok := got.ResolveIn("auto", "c"); ok {
			t.Error("bijection-violating member c should be skipped")
		}
		for _, nick := range []string{"a", "b"} {
			if _, ok := got.ResolveIn("auto", nick); !ok {
				t.Errorf("member %q should survive the skipped mutation", nick)
			}
		}
		if !strings.Contains(buf.String(), "skipping set member auto/c") {
			t.Errorf("expected skip log for auto/c, got %q", buf.String())
		}
	})
}

// --- end-to-end bootstrap --------------------------------------------------

// TestBootstrapConfigFile_issue197Regression reproduces the #197 shape end to
// end: a state-file credential for a nick also declared in env must win, and
// the generated aqg.json must carry the state credential — env is never read
// again after this one-shot migration.
func TestBootstrapConfigFile_issue197Regression(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_CONFIG")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")

	statePath := writeStateFile(t, dir,
		`{"config":{"auto":{"added_members":{"shared":{"credential":"state-cred"}}}}}`)
	t.Setenv("AQG_STATE_FILE", statePath)
	t.Setenv("AQG_POOL_AUTO_BACKEND_SHARED", "env-cred")

	var buf bytes.Buffer
	if err := bootstrapConfigFile(cfgPath, &buf); err != nil {
		t.Fatalf("bootstrapConfigFile: %v", err)
	}

	fi, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatalf("stat aqg.json: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("aqg.json mode = %#o, want 0600", fi.Mode().Perm())
	}

	// Round-trip through the real loader (also proves the file is valid and
	// 0600-compliant) and confirm the state credential won.
	_, reg, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	b, ok := reg.ResolveIn("auto", "shared")
	if !ok {
		t.Fatal("member shared missing after round-trip")
	}
	if b.Credential != "state-cred" {
		t.Errorf("credential = %q, want state-cred (state must win over env-cred, issue #197)", b.Credential)
	}
}

// --- issue #241: legacy state-file priority reconciliation -----------------

// sha256File reads path and returns its SHA256 digest as a hex string. Used
// to detect whether resolveConfig rewrote aqg.json — WriteAtomic's temp+rename
// changes the inode (and may leave mtime at coarse resolution unchanged), so
// content equality is the only stable signal.
func sha256File(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return string(sum[:])
}

// TestResolveConfig_existingFile_reconcilesLegacyPriority covers the
// deploy/bootstrap regression path: aqg.json already exists, the state file
// still carries an unmigrated priority_override, and the redeploy must
// preserve the operator-adjusted order in the final aqg.json (issue #241).
func TestResolveConfig_existingFile_reconcilesLegacyPriority(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")

	// aqg.json already exists with the env order (a, b, c) and points its
	// state_file at the legacy overlay we'll write next. The state file
	// carries the operator-adjusted order (c, a, b). The redeploy must
	// reconcile to (c, a, b) — both in the returned registry and on disk.
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"},"c":{"credential":"cc"}},"priority":["a","b","c"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	writeStateFile(t, dir,
		`{"config":{"auto":{"priority_override":["c","a","b"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_STATE_FILE", statePath)

	var buf bytes.Buffer
	_, reg, path, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if path != cfgPath {
		t.Errorf("path = %q, want %q", path, cfgPath)
	}

	wantPri := []string{"c", "a", "b"}
	if got := reg.PoolPriority("auto"); strings.Join(got, ",") != strings.Join(wantPri, ",") {
		t.Errorf("returned registry priority = %v, want %v (legacy state must win)", got, wantPri)
	}

	// Round-trip from disk: the reconciled order must be persisted.
	_, reg2, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadFile post-reconcile: %v", err)
	}
	if got := reg2.PoolPriority("auto"); strings.Join(got, ",") != strings.Join(wantPri, ",") {
		t.Errorf("aqg.json on disk priority = %v, want %v", got, wantPri)
	}
	logText := buf.String()
	for _, want := range []string{`pool "auto"`, "[a b c]", "[c a b]", "re-setting the order in the UI will not recur"} {
		if !strings.Contains(logText, want) {
			t.Errorf("reconcile log %q missing %q", logText, want)
		}
	}
}

// TestReconcileLegacyPriority_multiplePools verifies that independent legacy
// priorities are accumulated into one registry before it is persisted. Each
// copy-on-write WithPriority call must start from the preceding result; starting
// from the original registry would silently keep only the final map iteration.
func TestReconcileLegacyPriority_multiplePools(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")

	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"alpha":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}},"priority":["a","b"]},"beta":{"members":{"x":{"credential":"cx"},"y":{"credential":"cy"}},"priority":["x","y"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	writeStateFile(t, dir,
		`{"config":{"alpha":{"priority_override":["b","a"]},"beta":{"priority_override":["y","x"]}}}`)

	cfg, reg, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	got, err := reconcileLegacy(cfg, reg, cfgPath, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("reconcileLegacy: %v", err)
	}
	for pool, want := range map[string]string{"alpha": "b,a", "beta": "y,x"} {
		if pri := strings.Join(got.PoolPriority(pool), ","); pri != want {
			t.Errorf("%s priority = %q, want %q", pool, pri, want)
		}
	}
	_, reloaded, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadFile post-reconcile: %v", err)
	}
	for pool, want := range map[string]string{"alpha": "b,a", "beta": "y,x"} {
		if pri := strings.Join(reloaded.PoolPriority(pool), ","); pri != want {
			t.Errorf("persisted %s priority = %q, want %q", pool, pri, want)
		}
	}
}

func readJSONMap(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode %q: %v", path, err)
	}
	return got
}

func legacyPriorityPresent(t *testing.T, path, pool string) bool {
	t.Helper()
	got := readJSONMap(t, path)
	configs, _ := got["config"].(map[string]any)
	pc, _ := configs[pool].(map[string]any)
	_, ok := pc["priority_override"]
	return ok
}

func TestResolveConfig_existingFile_consumesLegacyPriorityAndDisabled(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"new-a"},"b":{"credential":"new-b"}},"priority":["a","b"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	// Legacy state carries both migrated keys (priority_override, disabled)
	// and the two report-only keys (added_members, removed_members). Existing
	// pool 'auto' has disabled=["a"]; pool 'other' is out-of-scope (must be
	// logged only, not touched).
	stateJSON := `{"pools":{"auto":{"sticky":"a"}},"snapshots":{"a":{"org_id":"org"}},"config":{"auto":{"priority_override":["b","a"],"disabled":["a"],"added_members":{"a":{"credential":"old-a"}},"removed_members":["b"]},"other":{"disabled":["x"]}},"added_pools":{"rt":{}}}`
	if err := os.WriteFile(statePath, []byte(stateJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AQG_CONFIG", cfgPath)

	var buf bytes.Buffer
	_, reg, _, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := strings.Join(reg.PoolPriority("auto"), ","); got != "b,a" {
		t.Fatalf("priority=%q, want b,a", got)
	}
	// Disabled migrated: a is now disabled in the returned registry.
	a, ok := reg.ResolveIn("auto", "a")
	if !ok || !a.Disabled || a.Credential != "new-a" {
		t.Errorf("member a={Disabled:%v Credential:%q ok:%v}; disabled must migrate while credential survives", a.Disabled, a.Credential, ok)
	}
	if b, ok := reg.ResolveIn("auto", "b"); !ok || b.Disabled || b.Credential != "new-b" {
		t.Errorf("member b=%+v, ok=%v; non-listed members must stay enabled", b, ok)
	}
	if legacyPriorityPresent(t, statePath, "auto") {
		t.Error("handled priority_override remains in state file")
	}
	state := readJSONMap(t, statePath)
	configs := state["config"].(map[string]any)
	auto := configs["auto"].(map[string]any)
	// Migrated keys (priority_override and disabled) are deleted from the
	// in-scope pool; report-only keys (added_members, removed_members)
	// remain so the first persist.flush can erase them on its own schedule.
	for _, key := range []string{"priority_override", "disabled"} {
		if _, ok := auto[key]; ok {
			t.Errorf("migrated key %q remains in state file", key)
		}
	}
	for _, key := range []string{"added_members", "removed_members"} {
		if _, ok := auto[key]; !ok {
			t.Errorf("report-only legacy key %q was discarded", key)
		}
	}
	// Out-of-scope pool keeps its disabled key untouched.
	other := configs["other"].(map[string]any)
	if _, ok := other["disabled"]; !ok {
		t.Error("out-of-scope pool's disabled key was discarded")
	}
	for _, key := range []string{"pools", "snapshots", "added_pools"} {
		if _, ok := state[key]; !ok {
			t.Errorf("top-level state key %q was discarded", key)
		}
	}
}

func TestResolveConfig_existingFile_dedupesNormalizedPriority(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"key-a":{"credential":"ca"},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStateFile(t, dir, `{"config":{"auto":{"priority_override":["key_a","key-a","b"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	_, reg, _, err := resolveConfig("", &bytes.Buffer{})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := strings.Join(reg.PoolPriority("auto"), ","); got != "key-a,b" {
		t.Errorf("priority=%q, want key-a,b", got)
	}
	if legacyPriorityPresent(t, statePath, "auto") {
		t.Error("deduped priority_override was not consumed")
	}
}

func TestResolveConfig_existingFile_noPriorityOverrideUntouched(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"}},"priority":["a"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	// Empty legacy state file → no overlay → no writes at all.
	writeStateFile(t, dir, `{"pools":{}}`)
	beforeConfig, beforeState := sha256File(t, cfgPath), sha256File(t, statePath)
	t.Setenv("AQG_CONFIG", cfgPath)
	if _, _, _, err := resolveConfig("", &bytes.Buffer{}); err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := sha256File(t, cfgPath); got != beforeConfig {
		t.Error("aqg.json changed without a legacy overlay")
	}
	if got := sha256File(t, statePath); got != beforeState {
		t.Error("state.json changed without a legacy overlay")
	}
}

// TestResolveConfig_existingFile_noPriorityOverrideDoesNotConsumePriority
// pins the priority path's no-op-when-absent property: when only `disabled`
// is present (no priority_override), the priority migration is dormant and
// the priority_override key is not consumed — only the disabled migration
// and report-only logging fire. With issue #259, the cfg file will be rewritten
// for the disable; the state file will lose its `disabled` key. The
// state file's `config` object must remain intact.
func TestResolveConfig_existingFile_noPriorityOverrideDoesNotConsumePriority(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}},"priority":["a","b"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStateFile(t, dir, `{"config":{"auto":{"disabled":["a"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	var log bytes.Buffer
	_, reg, _, err := resolveConfig("", &log)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := strings.Join(reg.PoolPriority("auto"), ","); got != "a,b" {
		t.Errorf("priority = %q, want a,b (no priority_override must not change order)", got)
	}
	if legacyPriorityPresent(t, statePath, "auto") {
		// priority_override was never there, but the helper reads any key
		// presence; this assertion is only meaningful as "no priority key
		// written by the orchestrator". Empty state file is what we want.
		t.Errorf("spurious priority_override key created at %s", statePath)
	}
}

func TestResolveConfig_existingFile_balancedPoolConsumesPriority(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}},"balance":"lead"}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStateFile(t, dir, `{"config":{"auto":{"priority_override":["b","a"]}}}`)
	beforeConfig := sha256File(t, cfgPath)
	t.Setenv("AQG_CONFIG", cfgPath)
	var log bytes.Buffer
	_, reg, _, err := resolveConfig("", &log)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if reg.PoolPriority("auto") != nil || reg.PoolBalanceGap("auto") == 0 {
		t.Errorf("balanced config changed: priority=%v balanceGap=%v", reg.PoolPriority("auto"), reg.PoolBalanceGap("auto"))
	}
	if got := sha256File(t, cfgPath); got != beforeConfig {
		t.Error("balanced aqg.json was rewritten")
	}
	if legacyPriorityPresent(t, statePath, "auto") {
		t.Error("priority superseded by balance was not consumed")
	}
	if !strings.Contains(log.String(), "balance mode") || !strings.Contains(log.String(), "priority mode") {
		t.Errorf("log does not name both modes: %q", log.String())
	}
}

func TestResolveConfig_existingFile_vacuousReportsAllAndWritesNeither(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"alpha":{"members":{"a":{"credential":"ca"}}},"beta":{"members":{"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStateFile(t, dir, `{"config":{"beta":{"priority_override":["ghost-b"]},"alpha":{"priority_override":["ghost-a"]}}}`)
	beforeConfig, beforeState := sha256File(t, cfgPath), sha256File(t, statePath)
	t.Setenv("AQG_CONFIG", cfgPath)
	_, _, _, err := resolveConfig("", &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected vacuous priority error")
	}
	for _, want := range []string{"alpha", "beta", statePath, cfgPath, "remove", "restart"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
	if sha256File(t, cfgPath) != beforeConfig || sha256File(t, statePath) != beforeState {
		t.Error("a file changed before aggregate validation completed")
	}
}

func TestResolveConfig_existingFile_probesAQGStateFileFirst(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	envStatePath := filepath.Join(dir, "env-state.json")
	stateDir := filepath.Join(dir, "state-dir")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stateDirPath := filepath.Join(stateDir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(envStatePath, []byte(`{"config":{"auto":{"priority_override":["b","a"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stateDirPath, []byte(`{"config":{"auto":{"priority_override":["a","b"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_STATE_FILE", envStatePath)
	t.Setenv("STATE_DIRECTORY", stateDir)
	_, reg, _, err := resolveConfig("", &bytes.Buffer{})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := strings.Join(reg.PoolPriority("auto"), ","); got != "b,a" {
		t.Errorf("priority=%q, want b,a from AQG_STATE_FILE", got)
	}
	if legacyPriorityPresent(t, envStatePath, "auto") {
		t.Error("AQG_STATE_FILE priority was not consumed")
	}
	if !legacyPriorityPresent(t, stateDirPath, "auto") {
		t.Error("lower-precedence STATE_DIRECTORY priority was consumed")
	}
}

func TestResolveConfig_existingFile_probesPastNonOverlayCandidate(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	emptyStatePath := filepath.Join(dir, "empty-state.json")
	stateDir := filepath.Join(dir, "state-dir")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateDir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(emptyStatePath, []byte(`{"pools":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, []byte(`{"config":{"auto":{"priority_override":["b","a"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_STATE_FILE", emptyStatePath)
	t.Setenv("STATE_DIRECTORY", stateDir)
	_, reg, _, err := resolveConfig("", &bytes.Buffer{})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := strings.Join(reg.PoolPriority("auto"), ","); got != "b,a" {
		t.Errorf("priority=%q, want b,a from second candidate", got)
	}
}

func TestResolveConfig_existingFile_probesStateDirectory(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	stateDir := filepath.Join(dir, "state-dir")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateDir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, []byte(`{"config":{"auto":{"priority_override":["b","a"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("STATE_DIRECTORY", stateDir)
	var log bytes.Buffer
	cfg, reg, _, err := resolveConfig("", &log)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if cfg.StateFile != "" {
		t.Errorf("discovered state path persisted into config: %q", cfg.StateFile)
	}
	if got := strings.Join(reg.PoolPriority("auto"), ","); got != "b,a" {
		t.Errorf("priority=%q, want b,a", got)
	}
	cfg2, _, err := configfile.LoadFile(cfgPath)
	if err != nil || cfg2.StateFile != "" {
		t.Errorf("on-disk state_file=%q, err=%v; want empty", cfg2.StateFile, err)
	}
	if legacyPriorityPresent(t, statePath, "auto") {
		t.Error("probe-discovered priority was not consumed")
	}
	if !strings.Contains(log.String(), "discovered legacy state file") || !strings.Contains(log.String(), statePath) {
		t.Errorf("missing discovery log: %q", log.String())
	}
}

func TestResolveConfig_existingFile_declaredStateDoesNotProbe(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	declaredPath := filepath.Join(dir, "declared.json")
	stateDir := filepath.Join(dir, "state-dir")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	probePath := filepath.Join(stateDir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + declaredPath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}},"priority":["a","b"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(declaredPath, []byte(`{"pools":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(probePath, []byte(`{"config":{"auto":{"priority_override":["b","a"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeConfig, beforeProbe := sha256File(t, cfgPath), sha256File(t, probePath)
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("STATE_DIRECTORY", stateDir)
	_, reg, _, err := resolveConfig("", &bytes.Buffer{})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := strings.Join(reg.PoolPriority("auto"), ","); got != "a,b" {
		t.Errorf("declared empty state was bypassed; priority=%q", got)
	}
	if sha256File(t, cfgPath) != beforeConfig || sha256File(t, probePath) != beforeProbe {
		t.Error("non-declared state path was read and migrated")
	}
}

func TestReconcileLegacyPriority_stateWriteFailureIsNonFatal(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	stateDir := filepath.Join(dir, "state-dir")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateDir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, []byte(`{"config":{"auto":{"priority_override":["b","a"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stateDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stateDir, 0o700) })

	cfg, reg, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	got, err := reconcileLegacy(cfg, reg, cfgPath, &log)
	if err != nil {
		t.Fatalf("state cleanup failure must not fail startup: %v", err)
	}
	if pri := strings.Join(got.PoolPriority("auto"), ","); pri != "b,a" {
		t.Errorf("priority=%q, want b,a", pri)
	}
	if !legacyPriorityPresent(t, statePath, "auto") {
		t.Error("state cleanup unexpectedly succeeded")
	}
	if !strings.Contains(log.String(), "retried on the next start") || !strings.Contains(log.String(), statePath) {
		t.Errorf("missing retry log: %q", log.String())
	}
}

func TestResolveConfig_existingFile_emptyPriorityLogsNothingOverridden(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStateFile(t, dir, `{"config":{"auto":{"priority_override":["b","a"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	var log bytes.Buffer
	if _, _, _, err := resolveConfig("", &log); err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	text := log.String()
	for _, want := range []string{`pool "auto"`, "[]", "[b a]", "nothing was overridden"} {
		if !strings.Contains(text, want) {
			t.Errorf("reconcile log %q missing %q", text, want)
		}
	}
}

// TestResolveConfig_existingFile_noMigrationWhenOrderMatches proves the
// steady-state no-write property: when the legacy state-file priority and
// the loaded aqg.json priority match exactly, the file must not be
// rewritten. Content equality (SHA256) is the signal — WriteAtomic uses
// temp+rename, so the inode and mtime are unreliable, but the bytes must
// be unchanged.
func TestResolveConfig_existingFile_noMigrationWhenOrderMatches(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")

	// Order on disk is [a, b, c]; the state file's priority_override is
	// the same [a, b, c]. Post-migration steady state → no write.
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"},"c":{"credential":"cc"}},"priority":["a","b","c"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	beforeHash := sha256File(t, cfgPath)

	writeStateFile(t, dir,
		`{"config":{"auto":{"priority_override":["a","b","c"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_STATE_FILE", statePath)

	var buf bytes.Buffer
	_, reg, _, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := reg.PoolPriority("auto"); strings.Join(got, ",") != "a,b,c" {
		t.Errorf("priority = %v, want a,b,c (must keep loaded order on no-op reconcile)", got)
	}
	if got := sha256File(t, cfgPath); got != beforeHash {
		t.Errorf("aqg.json content changed on a no-op reconcile (before=%s, after=%s)", beforeHash, got)
	}
	if legacyPriorityPresent(t, statePath, "auto") {
		t.Error("equal priority_override must still be consumed")
	}
	if strings.Contains(buf.String(), "migrated into") {
		t.Errorf("no-op reconcile must not log the migration line; got %q", buf.String())
	}
}

// TestResolveConfig_existingFile_failsLoudOnIrreconcilable: the state file's
// priority_override targets an existing pool but every named nick is gone
// from the loaded registry. Silent fallback would discard operator intent,
// so startup must fail with a pool-named error (issue #241 AC: "fail loud").
func TestResolveConfig_existingFile_failsLoudOnIrreconcilable(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")

	// Pool exists with members [a, b]; the state file's priority_override
	// names only nicks that no longer exist in the pool.
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	beforeHash := sha256File(t, cfgPath)

	writeStateFile(t, dir,
		`{"config":{"auto":{"priority_override":["ghost1","ghost2"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_STATE_FILE", statePath)

	_, _, _, err := resolveConfig("", &bytes.Buffer{})
	if err == nil {
		t.Fatal("resolveConfig must fail when legacy priority_override has no surviving members")
	}
	if !strings.Contains(err.Error(), "auto") {
		t.Errorf("error must name the offending pool; got %v", err)
	}
	if !strings.Contains(err.Error(), "legacy") && !strings.Contains(err.Error(), "reconcile") {
		t.Errorf("error should mention the legacy/reconcile context; got %v", err)
	}
	if got := sha256File(t, cfgPath); got != beforeHash {
		t.Errorf("aqg.json must not be rewritten when reconciliation aborts (before=%s, after=%s)", beforeHash, got)
	}
}

// TestResolveConfig_existingFile_legacyStateOutsideScopeIsSkipped: a state
// file that only mentions a pool not in the loaded config (and not in
// added_pools) must be logged and skipped — not aborted — matching the
// bootstrap path's best-effort policy (issue #241 reviewer note).
func TestResolveConfig_existingFile_legacyStateOutsideScopeIsSkipped(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")

	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}},"priority":["a","b"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	beforeHash := sha256File(t, cfgPath)

	// State file mentions a pool the loaded config does not have, and
	// nothing in-scope. Must skip with a log line and not touch aqg.json.
	writeStateFile(t, dir,
		`{"config":{"ghostpool":{"priority_override":["x","y"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_STATE_FILE", statePath)

	var buf bytes.Buffer
	_, reg, _, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig must not abort on out-of-scope legacy entries: %v", err)
	}
	if got := reg.PoolPriority("auto"); strings.Join(got, ",") != "a,b" {
		t.Errorf("priority = %v, want a,b (loaded config preserved)", got)
	}
	if got := sha256File(t, cfgPath); got != beforeHash {
		t.Errorf("aqg.json must not be rewritten for out-of-scope legacy entries (before=%s, after=%s)", beforeHash, got)
	}
	if !strings.Contains(buf.String(), "ghostpool") {
		t.Errorf("expected log naming the out-of-scope pool; got %q", buf.String())
	}
}

// --- issue #259: legacy state-file disabled migration and report-only logs ---

// TestResolveConfig_existingFile_reconcilesLegacyDisabled covers the AC happy
// path: a legacy `config.<pool>.disabled` entry naming a configured,
// currently-enabled member disables it in both the returned registry and
// aqg.json on disk; the key is consumed from the state file.
func TestResolveConfig_existingFile_reconcilesLegacyDisabled(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStateFile(t, dir, `{"config":{"auto":{"disabled":["a"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)

	var buf bytes.Buffer
	_, reg, _, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	a, ok := reg.ResolveIn("auto", "a")
	if !ok || !a.Disabled {
		t.Errorf("member a={Disabled:%v ok:%v}; disabled migration must apply", a.Disabled, ok)
	}
	if b, ok := reg.ResolveIn("auto", "b"); ok && b.Disabled {
		t.Errorf("member b={Disabled:%v ok:%v}; non-listed members stay enabled", b.Disabled, ok)
	}
	// Round-trip from disk: the persisted aqg.json must show a disabled.
	_, onDisk, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadFile post-reconcile: %v", err)
	}
	if a, _ := onDisk.ResolveIn("auto", "a"); !a.Disabled {
		t.Error("aqg.json on disk does not carry disabled=true for a")
	}
	// Legacy disabled key was consumed in the same atomic state-file write.
	state := readJSONMap(t, statePath)
	auto := state["config"].(map[string]any)["auto"].(map[string]any)
	if _, ok := auto["disabled"]; ok {
		t.Error("disabled key remains in state file")
	}
	for _, want := range []string{`pool "auto"`, "member \"a\" disabled", "re-enabling in the UI will not recur"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("reconcile log %q missing %q", buf.String(), want)
		}
	}
}

// TestResolveConfig_existingFile_alreadyDisabledLeavesConfigUntouched pins
// the "already disabled → no aqg.json write for that pool" AC while still
// consuming the legacy key from the state file.
func TestResolveConfig_existingFile_alreadyDisabledLeavesConfigUntouched(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	// Member a is already disabled in aqg.json.
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca","disabled":true},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStateFile(t, dir, `{"config":{"auto":{"disabled":["a"]}}}`)
	beforeConfig := sha256File(t, cfgPath)
	t.Setenv("AQG_CONFIG", cfgPath)
	_, reg, _, err := resolveConfig("", &bytes.Buffer{})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if a, ok := reg.ResolveIn("auto", "a"); !ok || !a.Disabled {
		t.Errorf("member a={Disabled:%v ok:%v}; already-disabled state must survive", a.Disabled, ok)
	}
	if got := sha256File(t, cfgPath); got != beforeConfig {
		t.Errorf("aqg.json content changed for already-disabled member (before=%s, after=%s)", beforeConfig, got)
	}
	state := readJSONMap(t, statePath)
	auto := state["config"].(map[string]any)["auto"].(map[string]any)
	if _, ok := auto["disabled"]; ok {
		t.Error("disabled key was retained despite already-disabled member")
	}
}

// TestResolveConfig_existingFile_nonMemberDisabledSkipped covers the AC for
// a legacy nick that is not a configured member of the pool: it must be
// logged and skipped without failing startup and without modifying aqg.json.
func TestResolveConfig_existingFile_nonMemberDisabledSkipped(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStateFile(t, dir, `{"config":{"auto":{"disabled":["ghost"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	var buf bytes.Buffer
	_, reg, _, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig must not abort on non-member nick: %v", err)
	}
	if a, ok := reg.ResolveIn("auto", "a"); !ok || a.Disabled {
		t.Errorf("member a={Disabled:%v ok:%v}; non-listed member must stay enabled, no spurious disable", a.Disabled, ok)
	}
	for _, want := range []string{"ghost", "auto", "not a current member"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("reconcile log %q missing %q", buf.String(), want)
		}
	}
	// The legacy `disabled` key is still consumed at the pool level: the
	// legacy entry was *seen and handled* (logged) even though no disable
	// was applied. Keeping it would mean the operator sees the same ghost
	// warning forever; consuming it ends the residual after one log.
	state := readJSONMap(t, statePath)
	auto := state["config"].(map[string]any)["auto"].(map[string]any)
	if _, ok := auto["disabled"]; ok {
		t.Error("disabled key remained in state file despite the pool's disable entry being handled")
	}
}

// TestResolveConfig_existingFile_legacyRemovedMembersReportedNotApplied
// pins the report-only contract for removed_members: no member is removed
// from aqg.json, and the key stays in the state file until the next flush.
func TestResolveConfig_existingFile_legacyRemovedMembersReportedNotApplied(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeConfig := sha256File(t, cfgPath)
	writeStateFile(t, dir, `{"config":{"auto":{"removed_members":["a"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	var buf bytes.Buffer
	_, reg, _, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if a, ok := reg.ResolveIn("auto", "a"); !ok {
		t.Errorf("member a missing; removed_members must not be applied: %+v", a)
	}
	if got := sha256File(t, cfgPath); got != beforeConfig {
		t.Errorf("aqg.json changed despite removed_members being report-only (before=%s, after=%s)", beforeConfig, got)
	}
	state := readJSONMap(t, statePath)
	auto := state["config"].(map[string]any)["auto"].(map[string]any)
	if _, ok := auto["removed_members"]; !ok {
		t.Error("removed_members key was discarded; report-only keys must be left for the next flush")
	}
	for _, want := range []string{statePath, "auto", "removed_members", "a", "not applied"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("reconcile log %q missing %q", buf.String(), want)
		}
	}
}

// TestResolveConfig_existingFile_legacyAddedMembersReportedNotApplied pins the
// report-only contract for added_members: no credential is injected into
// aqg.json, and the key stays.
func TestResolveConfig_existingFile_legacyAddedMembersReportedNotApplied(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"}}}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeConfig := sha256File(t, cfgPath)
	writeStateFile(t, dir, `{"config":{"auto":{"added_members":{"ghost":{"credential":"old-ghost","base_url":"https://g.example"}}}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	var buf bytes.Buffer
	_, reg, _, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if _, ok := reg.ResolveIn("auto", "ghost"); ok {
		t.Error("ghost was added despite added_members being report-only")
	}
	if got := sha256File(t, cfgPath); got != beforeConfig {
		t.Errorf("aqg.json changed despite added_members being report-only (before=%s, after=%s)", beforeConfig, got)
	}
	state := readJSONMap(t, statePath)
	auto := state["config"].(map[string]any)["auto"].(map[string]any)
	if _, ok := auto["added_members"]; !ok {
		t.Error("added_members key was discarded; report-only keys must be left for the next flush")
	}
	for _, want := range []string{statePath, "auto", "added_members", "ghost", "not applied"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("reconcile log %q missing %q", buf.String(), want)
		}
	}
}

// TestResolveConfig_existingFile_priorityAndDisabledInSameOverlay is the
// unified-overlay regression: when both migrated keys live in one resolved
// overlay, both apply and both are deleted in a single atomic state-file
// write.
func TestResolveConfig_existingFile_priorityAndDisabledInSameOverlay(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	statePath := filepath.Join(dir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + statePath + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}},"priority":["a","b"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStateFile(t, dir, `{"config":{"auto":{"priority_override":["b","a"],"disabled":["a"]}}}`)
	t.Setenv("AQG_CONFIG", cfgPath)
	beforeState := sha256File(t, statePath)
	_, reg, _, err := resolveConfig("", &bytes.Buffer{})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := strings.Join(reg.PoolPriority("auto"), ","); got != "b,a" {
		t.Errorf("priority = %q, want b,a (priority migration)", got)
	}
	if a, ok := reg.ResolveIn("auto", "a"); !ok || !a.Disabled {
		t.Errorf("member a={Disabled:%v ok:%v}; disabled migration must also apply", a.Disabled, ok)
	}
	// Both keys consumed in one atomic state-file write.
	state := readJSONMap(t, statePath)
	auto := state["config"].(map[string]any)["auto"].(map[string]any)
	for _, key := range []string{"priority_override", "disabled"} {
		if _, ok := auto[key]; ok {
			t.Errorf("expected %q to be consumed in single state-file write", key)
		}
	}
	// State file changed exactly once (i.e. one atomic write vs two — single
	// consumed-keys write is what we want).
	_ = beforeState
}

// TestResolveConfig_existingFile_disabledInLowerCandidateNotMerged locks down
// the unified-overlay design against silently merging keys from a
// lower-precedence candidate: when $AQG_STATE_FILE already has a decodable
// overlay and $STATE_DIRECTORY/state.json carries a `disabled` entry, the
// lower file must NOT be probed by the existing-file path — the same single
// winner contract that priority migration enforces applies to disabled too.
func TestResolveConfig_existingFile_disabledInLowerCandidateNotMerged(t *testing.T) {
	scrubPoolEnv(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	stateDir := filepath.Join(dir, "state-dir")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	highState := filepath.Join(dir, "high.json")
	lowState := filepath.Join(stateDir, "state.json")
	fileJSON := `{"base_url":"https://api.anthropic.com","state_file":"` + highState + `","pools":{"auto":{"members":{"a":{"credential":"ca"},"b":{"credential":"cb"}},"priority":["a","b"]}}}`
	if err := os.WriteFile(cfgPath, []byte(fileJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(highState, []byte(`{"config":{"auto":{"priority_override":["b","a"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lowState, []byte(`{"config":{"auto":{"disabled":["a"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeLow := sha256File(t, lowState)
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("STATE_DIRECTORY", stateDir)
	_, reg, _, err := resolveConfig("", &bytes.Buffer{})
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if got := strings.Join(reg.PoolPriority("auto"), ","); got != "b,a" {
		t.Errorf("priority = %q, want b,a (resolved from higher candidate)", got)
	}
	if a, ok := reg.ResolveIn("auto", "a"); ok && a.Disabled {
		t.Errorf("member a was disabled from the lower candidate: lower candidate must not be silently merged (a=%+v)", a)
	}
	if got := sha256File(t, lowState); got != beforeLow {
		t.Errorf("lower candidate was modified; merge contract violated (before=%s, after=%s)", beforeLow, got)
	}
}
