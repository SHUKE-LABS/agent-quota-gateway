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
			name:    "valid overlay",
			path:    func() string { return writeStateFile(t, t.TempDir(), `{"config":{"auto":{"priority_override":["b","a"]}}}`) },
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
	if !strings.Contains(buf.String(), "reconciled legacy state-file priority") {
		t.Errorf("expected reconcile log line, got %q", buf.String())
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
	if strings.Contains(buf.String(), "reconciled legacy state-file priority") {
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
