package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
	"github.com/shukebeta/agent-quota-gateway/internal/configfile"
)

// configMux builds a ServeMux with the runtime-config routes wired exactly as
// run() wires them, so the path-pattern handlers can resolve r.PathValue.
// The /_gateway/ui route is exercised by uiMux instead — that handler is
// pools-free and does not belong in this mux.
//
// persistence is the new issue #246 persistence view passed into every
// handler. Existing tests that pre-date #246 don't care about the env-only
// signal, so they accept the zero value (ModePersisted, no Unsaved func)
// by leaving the parameter at its default — that matches the pre-#246
// contract (handlers' bodies stay byte-identical in persisted mode).
func configMux(t *testing.T, pools *auto.Pools) *httptest.Server {
	t.Helper()
	return configMuxWithPersistence(t, pools, configfile.PersistenceState{})
}

// configMuxWithPersistence is configMux with an explicit PersistenceState,
// used by the issue #246 tests to drive the env-only surface.
func configMuxWithPersistence(t *testing.T, pools *auto.Pools, persistence configfile.PersistenceState) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/config", configHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool", createPoolHandler(pools, persistence))
	mux.HandleFunc("DELETE /_gateway/pool/{name}", deletePoolHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/rename", renamePoolHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/priority", priorityHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/disable", disableMemberHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/enable", enableMemberHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/move", moveMemberHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}", addMemberHandler(pools, persistence))
	mux.HandleFunc("DELETE /_gateway/pool/{name}/member/{nick}", removeMemberHandler(pools, persistence))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// uiMux builds a ServeMux that registers only /_gateway/ui. The UI handler
// takes no *auto.Pools — it serves a static embedded asset — so the UI tests
// do not need the full runtime-config mux or any AQG_POOL_* env setup.
func uiMux(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/ui", uiHandler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func loadPools(t *testing.T) *auto.Pools {
	t.Helper()
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	return auto.NewPools(registry, nil, nil, io.Discard)
}

// reloadPools simulates a restart under the config-single-source model (issue
// #198): the current registry (operator intent) is round-tripped through the
// config Spec and a fresh Pools is built from it. Env is NOT re-read, so
// operator mutations recorded in the config survive exactly as they would
// across a real restart reading aqg.json.
func reloadPools(t *testing.T, pools *auto.Pools) *auto.Pools {
	t.Helper()
	reg, err := backend.BuildFromSpec(pools.CurrentRegistry().Spec(), "https://api.anthropic.com")
	if err != nil {
		t.Fatalf("rebuild registry from config: %v", err)
	}
	return auto.NewPools(reg, nil, nil, io.Discard)
}

// TestConfigEndpoint_redactsCredentials proves GET /_gateway/config returns the
// effective configuration with no credential substring anywhere in the body.
func TestConfigEndpoint_redactsCredentials(t *testing.T) {
	const secret = "sk-ant-SECRET-DO-NOT-LEAK"
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", secret)
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", secret+"-b")
	srv := configMux(t, loadPools(t))

	resp, err := http.Get(srv.URL + "/_gateway/config")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if strings.Contains(string(body), "sk-ant-SECRET") {
		t.Fatalf("config response leaked a credential substring: %s", body)
	}
	// The structural fields the view promises must still be present.
	if !strings.Contains(string(body), `"pool":"auto"`) {
		t.Errorf("config response missing the auto pool: %s", body)
	}
	if !strings.Contains(string(body), `"nick":"a"`) {
		t.Errorf("config response missing member nick a: %s", body)
	}
}

// TestCreatePoolEndpoint drives POST /_gateway/pool: a valid request returns
// 201 with the normalized pool name, the pool then surfaces in GET
// /_gateway/config, and a duplicate name returns 409. base_url is no longer
// a create-pool field (issue #172).
func TestCreatePoolEndpoint(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	srv := configMux(t, loadPools(t))

	resp, err := http.Post(srv.URL+"/_gateway/pool", "application/json",
		strings.NewReader(`{"name":"New_Pool"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create pool status=%d, want 201", resp.StatusCode)
	}
	var body struct {
		Pool string `json:"pool"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Pool != "new-pool" {
		t.Errorf("response pool=%q, want normalized new-pool", body.Pool)
	}

	// The pool now appears in the effective config view.
	got := fetchPool(t, srv.URL, "new-pool")
	if got.Pool != "new-pool" {
		t.Errorf("new-pool not in config view: %+v", got)
	}

	// A pre-issue-#172 client that still sends base_url is accepted (extra
	// field is ignored by the decoder) — back-compat assertion.
	resp, err = http.Post(srv.URL+"/_gateway/pool", "application/json",
		strings.NewReader(`{"name":"legacy-client","base_url":"https://ignored.example"}`))
	if err != nil {
		t.Fatalf("post legacy: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("legacy-client (with base_url) status=%d, want 201 (base_url should be ignored)", resp.StatusCode)
	}

	// Duplicate name → 409.
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":"new-pool"}`, http.StatusConflict)
	// Empty name → 400.
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":""}`, http.StatusBadRequest)
	// Collision with an env pool → 409.
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":"auto"}`, http.StatusConflict)
}

func TestCreatePoolWithMemberEndpoint(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	srv := configMux(t, loadPools(t))
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":"fresh","nick":"n","credential":"cred-n","base_url":"https://n.example"}`, http.StatusCreated)
	v := fetchPool(t, srv.URL, "fresh")
	if len(v.Members) != 1 || v.Members[0].Nick != "n" {
		t.Fatalf("combined create members=%+v", v.Members)
	}
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":"bad","nick":"unknown"}`, http.StatusBadRequest)
	assertPoolAbsent(t, srv.URL, "bad")
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":"placement","nick":"p","credential":"cred-p","base_url":"https://p.example","placement":["p"]}`, http.StatusBadRequest)
	assertPoolAbsent(t, srv.URL, "placement")
}

func assertPoolAbsent(t *testing.T, baseURL, name string) {
	t.Helper()
	resp, err := http.Get(baseURL + "/_gateway/config")
	if err != nil {
		t.Fatalf("get config: %v", err)
	}
	defer resp.Body.Close()
	var views []auto.PoolConfigView
	if err := json.NewDecoder(resp.Body).Decode(&views); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	for _, view := range views {
		if view.Pool == name {
			t.Fatalf("invalid create left pool: %s", name)
		}
	}
}

// TestDisableEnableEndpoints drives the disable/enable endpoints and verifies
// the effective config reflects the change, plus the error codes for bad input.
func TestDisableEnableEndpoints(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-b")
	srv := configMux(t, loadPools(t))

	// Disable member a.
	post(t, srv.URL+"/_gateway/pool/auto/member/a/disable", http.StatusOK)
	if !memberDisabled(t, srv.URL, "auto", "a") {
		t.Error("member a should be disabled after the disable call")
	}

	// Re-enable member a.
	post(t, srv.URL+"/_gateway/pool/auto/member/a/enable", http.StatusOK)
	if memberDisabled(t, srv.URL, "auto", "a") {
		t.Error("member a should be enabled after the enable call")
	}

	// Unknown pool -> 404; unknown nick -> 400.
	post(t, srv.URL+"/_gateway/pool/ghost/member/a/disable", http.StatusNotFound)
	post(t, srv.URL+"/_gateway/pool/auto/member/ghost/disable", http.StatusBadRequest)
}

// TestDisableEnableEndpoints_runtimeAdded proves that the disable/enable
// endpoints accept a runtime-added member (the regression in #114 — the
// membership gate previously validated only the static roster), and that the
// disabled flag survives a restart via the runtime-config persist path.
func TestDisableEnableEndpoints_runtimeAdded(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-b")
	pools := loadPools(t)
	srv := configMux(t, pools)

	// Add a runtime member.
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/c",
		`{"credential":"sk-ant-c"}`, http.StatusOK)

	// Disable the runtime-added member: must succeed.
	post(t, srv.URL+"/_gateway/pool/auto/member/c/disable", http.StatusOK)
	if !memberDisabled(t, srv.URL, "auto", "c") {
		t.Error("runtime-added member c should be disabled after the disable call")
	}
	if got := memberStatus(t, srv.URL, "auto", "c"); got != "disabled" {
		t.Errorf("runtime-added member c status=%q, want disabled", got)
	}

	// Re-enable the runtime-added member: must succeed.
	post(t, srv.URL+"/_gateway/pool/auto/member/c/enable", http.StatusOK)
	if memberDisabled(t, srv.URL, "auto", "c") {
		t.Error("runtime-added member c should be enabled after the enable call")
	}

	// Unknown runtime nick still 400.
	post(t, srv.URL+"/_gateway/pool/auto/member/ghost/disable", http.StatusBadRequest)

	// Removing the runtime member and then attempting to disable it must
	// still 400 — the gate is on present (non-removed), not merely known.
	delete(t, srv.URL+"/_gateway/pool/auto/member/c", http.StatusOK)
	post(t, srv.URL+"/_gateway/pool/auto/member/c/disable", http.StatusBadRequest)

	// The disabled flag on a runtime-added member must survive restart.
	// Re-add c, disable, snapshot the runtime config, reload into a fresh
	// Pools, and verify the flag is still set on the restored member.
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/c",
		`{"credential":"sk-ant-c"}`, http.StatusOK)
	post(t, srv.URL+"/_gateway/pool/auto/member/c/disable", http.StatusOK)
	pools2 := reloadPools(t, pools)
	srv2 := configMux(t, pools2)
	if !memberDisabled(t, srv2.URL, "auto", "c") {
		t.Error("runtime-added member c disabled flag did not survive restart")
	}
}

// memberStatus returns the status string for one member in a pool's
// effective config view.
func memberStatus(t *testing.T, baseURL, pool, nick string) string {
	t.Helper()
	for _, m := range fetchPool(t, baseURL, pool).Members {
		if m.Nick == nick {
			return m.Status
		}
	}
	t.Fatalf("member %q not found in pool %q", nick, pool)
	return ""
}

// TestPriorityEndpoint drives the priority endpoint: a valid reorder is applied
// (and expanded to a total order), an unknown nick is rejected 400, and a
// balanced pool is rejected 409.
func TestPriorityEndpoint(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-b")
	t.Setenv("AQG_POOL_AUTO_BACKEND_C", "sk-ant-c")
	// A separate balanced pool to exercise the 409 path.
	t.Setenv("AQG_POOL_BAL_BACKEND_X", "sk-ant-x")
	t.Setenv("AQG_POOL_BAL_BACKEND_Y", "sk-ant-y")
	t.Setenv("AQG_POOL_BAL_BALANCE", "lead")
	srv := configMux(t, loadPools(t))

	// Valid partial reorder: ["c"] expands to c first, then the rest sorted.
	postJSON(t, srv.URL+"/_gateway/pool/auto/priority", `["c"]`, http.StatusOK)
	pri := poolPriority(t, srv.URL, "auto")
	if len(pri) != 3 || pri[0] != "c" {
		t.Errorf("effective priority=%v, want [c a b] (expanded partial override)", pri)
	}

	// Unknown nick -> 400.
	postJSON(t, srv.URL+"/_gateway/pool/auto/priority", `["nope"]`, http.StatusBadRequest)

	// Balanced pool -> 409.
	postJSON(t, srv.URL+"/_gateway/pool/bal/priority", `["x"]`, http.StatusConflict)
}

func post(t *testing.T, url string, wantStatus int) {
	t.Helper()
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Errorf("post %s status=%d, want %d", url, resp.StatusCode, wantStatus)
	}
}

func postJSON(t *testing.T, url, body string, wantStatus int) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Errorf("post %s body=%s status=%d, want %d", url, body, resp.StatusCode, wantStatus)
	}
}

// fetchPool returns the config view for one pool from GET /_gateway/config.
func fetchPool(t *testing.T, baseURL, pool string) auto.PoolConfigView {
	t.Helper()
	resp, err := http.Get(baseURL + "/_gateway/config")
	if err != nil {
		t.Fatalf("get config: %v", err)
	}
	defer resp.Body.Close()
	var views []auto.PoolConfigView
	if err := json.NewDecoder(resp.Body).Decode(&views); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	for _, v := range views {
		if v.Pool == pool {
			return v
		}
	}
	t.Fatalf("pool %q not found in config response", pool)
	return auto.PoolConfigView{}
}

func memberDisabled(t *testing.T, baseURL, pool, nick string) bool {
	t.Helper()
	for _, m := range fetchPool(t, baseURL, pool).Members {
		if m.Nick == nick {
			return m.Disabled
		}
	}
	t.Fatalf("member %q not found in pool %q", nick, pool)
	return false
}

func poolPriority(t *testing.T, baseURL, pool string) []string {
	t.Helper()
	return fetchPool(t, baseURL, pool).Priority
}

// TestAddRemoveEndpoints tests adding and removing runtime pool members.
func TestAddRemoveEndpoints(t *testing.T) {
	const secretC = "sk-ant-secret-c"
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-b")
	pools := loadPools(t)
	srv := configMux(t, pools)

	// Add a runtime member.
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/c", `{"credential":"`+secretC+`"}`, http.StatusOK)

	// Verify the runtime-added member appears in the config view, having
	// inherited a base_url from the pool's static members.
	view := fetchPool(t, srv.URL, "auto")
	found := false
	for _, m := range view.Members {
		if m.Nick == "c" {
			found = true
			if m.BaseURL == "" {
				t.Errorf("added member c has empty base_url, want inherited pool default")
			}
			break
		}
	}
	if !found {
		t.Error("added member c not found in config view")
	}

	// Verify credential is not leaked in config response.
	resp, err := http.Get(srv.URL + "/_gateway/config")
	if err != nil {
		t.Fatalf("get config: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), secretC) {
		t.Error("config response leaked credential from added member")
	}

	// Remove the runtime member.
	delete(t, srv.URL+"/_gateway/pool/auto/member/c", http.StatusOK)

	// Verify the member is gone from config.
	view = fetchPool(t, srv.URL, "auto")
	for _, m := range view.Members {
		if m.Nick == "c" {
			t.Error("removed member c still appears in config view")
		}
	}

	// Remove a static member: removal is permanent deletion, so it disappears
	// from the config roster entirely (not merely flagged disabled). The
	// effective set drives both the view and the selection path, so omission
	// from this roster also means the member is no longer selectable.
	delete(t, srv.URL+"/_gateway/pool/auto/member/a", http.StatusOK)
	view = fetchPool(t, srv.URL, "auto")
	for _, m := range view.Members {
		if m.Nick == "a" {
			t.Error("removed static member a still appears in config view")
		}
	}

	// Removal must survive a restart (#85, now structural under #198). Exercise
	// the config round-trip: the removed members are gone from the config
	// registry, so a fresh Pools built from that config never sees them. Env is
	// not re-read, so "a" cannot resurface — the removal is permanent because
	// the config, not env, is the source of truth.
	pools2 := reloadPools(t, pools)
	srv2 := configMux(t, pools2)
	reloaded := fetchPool(t, srv2.URL, "auto")
	sawSurvivor := false
	for _, m := range reloaded.Members {
		switch m.Nick {
		case "a":
			t.Error("removed static member a reappeared after restart")
		case "c":
			t.Error("removed runtime member c reappeared after restart")
		case "b":
			sawSurvivor = true
			if m.Status == "disabled" {
				t.Errorf("survivor b is %q after restart, want selectable", m.Status)
			}
		}
	}
	if !sawSurvivor {
		t.Error("survivor member b missing from config view after restart")
	}

	// Re-adding a previously removed config-derived nick succeeds (issue #185:
	// no more static-nick 409; tombstone is cleared and member rejoins).
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/a", `{"credential":"sk-ant-a"}`, http.StatusOK)

	// Error cases.
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/b", `{"credential":"sk-ant-x"}`, http.StatusConflict)             // duplicate nick (b is still live)
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/new", `{}`, http.StatusBadRequest)                                // empty credential
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/new", `{"credential":"x","base_url":"!"}`, http.StatusBadRequest) // invalid URL
	delete(t, srv.URL+"/_gateway/pool/ghost/member/a", http.StatusNotFound)                                          // unknown pool
}

// TestDeletePoolEndpoint drives DELETE /_gateway/pool/{name}: an empty runtime
// pool deletes with 200 and disappears from the config view and from a
// round-trip restart; an unknown pool is 404; a pool with members is 409 until
// drained; and a request whose selector names a just-deleted pool fails closed
// with 403 through the real resolver middleware (issue #232).
func TestDeletePoolEndpoint(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	pools := loadPools(t)
	srv := configMux(t, pools)

	// Create an empty runtime pool, then delete it → 200.
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":"rt"}`, http.StatusCreated)
	delete(t, srv.URL+"/_gateway/pool/RT", http.StatusOK) // name normalized server-side

	// Gone from the config view.
	for _, v := range fetchAllPools(t, srv.URL) {
		if v.Pool == "rt" {
			t.Error("deleted pool rt still appears in config view")
		}
	}

	// Unknown pool → 404.
	delete(t, srv.URL+"/_gateway/pool/ghost", http.StatusNotFound)

	// A pool that still has members → 409; drain, then delete succeeds.
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":"full"}`, http.StatusCreated)
	addJSON(t, srv.URL+"/_gateway/pool/full/member/m", `{"credential":"sk-ant-m","base_url":"https://m.example"}`, http.StatusOK)
	delete(t, srv.URL+"/_gateway/pool/full", http.StatusConflict)
	delete(t, srv.URL+"/_gateway/pool/full/member/m", http.StatusOK)
	delete(t, srv.URL+"/_gateway/pool/full", http.StatusOK)

	// The deletion survives a restart: neither rt nor full reappears.
	pools2 := reloadPools(t, pools)
	srv2 := configMux(t, pools2)
	for _, v := range fetchAllPools(t, srv2.URL) {
		if v.Pool == "rt" || v.Pool == "full" {
			t.Errorf("deleted pool %q reappeared after restart", v.Pool)
		}
	}

	// Route-after-delete → 403 through the real resolver middleware. Delete the
	// env pool's member then the pool, and confirm a request bearing its
	// selector fails closed (unknown selector), not routed into stale state.
	delete(t, srv.URL+"/_gateway/pool/auto/member/a", http.StatusOK)
	delete(t, srv.URL+"/_gateway/pool/auto", http.StatusOK)
	mw := backend.Middleware(pools, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next handler reached for a deleted pool's selector")
	}))
	msrv := httptest.NewServer(mw)
	t.Cleanup(msrv.Close)
	req, err := http.NewRequest("POST", msrv.URL+"/v1/messages", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer auto")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("route after delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("route to deleted pool status=%d, want 403", resp.StatusCode)
	}
}

// TestDeletePoolEndpoint_configFilePersists drives the live management flow
// end to end with the production config-writer wiring: bootstrap creates
// aqg.json, the HTTP layer drains + deletes a pool, the debounced config
// writer flushes the new config to disk, and a subsequent configfile.LoadFile
// against the same path proves the deletion survives a real restart. The
// failure mode the issue names — DELETE returns 200 in memory but the on-disk
// config still carries the pool, so it resurrects on the next boot — fails
// the disk-poll step (issue #239).
func TestDeletePoolEndpoint_configFilePersists(t *testing.T) {
	scrubPoolEnv(t)
	unsetenv(t, "AQG_CONFIG")
	unsetenv(t, "AQG_STATE_FILE")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "aqg.json")
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BASE_URL", testBase)

	// Bootstrap: resolveConfig creates aqg.json from env + the empty legacy
	// state overlay, then returns the cfg the config writer marshals against.
	var buf bytes.Buffer
	resolvedCfg, _, path, err := resolveConfig("", &buf)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if path != cfgPath {
		t.Fatalf("config path=%q, want %q", path, cfgPath)
	}
	// Pin the package-level type at setup: the closure below invokes
	// configfile.Marshal on resolvedCfg, but go vet cannot see closure use, so
	// without an explicit reference the import looks dead.
	var _ config.Config = resolvedCfg

	// Rebuild the in-memory registry the way a real process does: re-load
	// aqg.json from disk so the runtime mutation path starts from the same
	// authoritative source a real restart would. The registry handed back by
	// resolveConfig is the bootstrap intermediate; what gets handed to NewPools
	// in main.go is the file-loaded form.
	_, reg, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	pools := auto.NewPools(reg, nil, nil, io.Discard)

	// Production-shaped writer: MarkDirty on every operator mutation, Run on
	// its own goroutine sharing the test's shutdown context. The default
	// 200ms debounce window is the one used in main.go.
	cw := configfile.NewWriter(cfgPath, func() ([]byte, error) {
		return configfile.Marshal(resolvedCfg, pools.CurrentRegistry())
	})
	pools.SetOnConfigChange(cw.MarkDirty)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { cw.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	srv := configMux(t, pools)

	// Env-origin empty pool: drain the only member, then delete the pool.
	// Each step returns 200 (the in-memory contract the existing test already
	// proves); this test additionally asserts the on-disk update lands.
	delete(t, srv.URL+"/_gateway/pool/auto/member/a", http.StatusOK)
	// Pause so the drain's debounced flush (default 200ms) lands before the
	// delete runs. Back-to-back the two mark-dirties would share a single
	// debounce window and a RemovePool regression that drops
	// markConfigDirtyLocked would still produce one correct post-delete flush
	// — the test would pass without exercising the regression. Separating
	// them forces the drain to flush before the delete, so the regression
	// surfaces as a stale "auto": {} left on disk that the next deletion
	// never overwrites.
	waitForDiskOmit(t, cfgPath, `"a"`)
	delete(t, srv.URL+"/_gateway/pool/auto", http.StatusOK)

	// Wait for the debounced writer to land (default 200ms; generous slack).
	waitForDiskOmit(t, cfgPath, `"auto"`)

	// Real-restart simulation: load aqg.json the same way resolveConfig would
	// after a clean process restart, and confirm the deleted pool is gone.
	_, reg2, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("post-delete LoadFile: %v", err)
	}
	if reg2.HasPool("auto") {
		t.Errorf("deleted env-origin pool auto reappeared after disk restart")
	}

	// Runtime-origin empty pool: create a fresh runtime pool via the API and
	// delete it, exercising the create→delete path under the same writer.
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":"rt"}`, http.StatusCreated)
	delete(t, srv.URL+"/_gateway/pool/rt", http.StatusOK)

	waitForDiskOmit(t, cfgPath, `"rt"`)

	_, reg3, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("post-delete LoadFile (rt): %v", err)
	}
	if reg3.HasPool("rt") {
		t.Errorf("deleted runtime-origin pool rt reappeared after disk restart")
	}
}

// waitForDiskOmit polls cfgPath until its body no longer contains needle, or
// fails the test when the window elapses. The window must exceed the
// configwriter's 200ms debounce plus a comfortable margin so a regression that
// drops the on-disk flush fails here rather than passing because the test
// raced the writer.
func waitForDiskOmit(t *testing.T, cfgPath, needle string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		d, err := os.ReadFile(cfgPath)
		if err == nil && !bytes.Contains(d, []byte(needle)) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	raw, _ := os.ReadFile(cfgPath)
	t.Fatalf("aqg.json still contains %s after 3s: %s", needle, string(raw))
}

// fetchAllPools returns the full config view from GET /_gateway/config.
func fetchAllPools(t *testing.T, baseURL string) []auto.PoolConfigView {
	t.Helper()
	resp, err := http.Get(baseURL + "/_gateway/config")
	if err != nil {
		t.Fatalf("get config: %v", err)
	}
	defer resp.Body.Close()
	var views []auto.PoolConfigView
	if err := json.NewDecoder(resp.Body).Decode(&views); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	return views
}

// TestRenamePoolEndpoint drives POST /_gateway/pool/{name}/rename: the happy
// path renames a runtime pool, the rename is reflected in GET /_gateway/config
// and survives a config-roundtrip restart, and the four error paths return
// the documented status codes (404 unknown old, 400 empty / identical new,
// 409 conflict).
func TestRenamePoolEndpoint(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	pools := loadPools(t)
	srv := configMux(t, pools)

	// Happy path: create a runtime pool and rename it.
	postJSON(t, srv.URL+"/_gateway/pool", `{"name":"rt"}`, http.StatusCreated)
	addJSON(t, srv.URL+"/_gateway/pool/rt/member/m", `{"credential":"sk-ant-m","base_url":"https://m.example"}`, http.StatusOK)

	postJSON(t, srv.URL+"/_gateway/pool/rt/rename", `{"name":"renamed"}`, http.StatusOK)

	// Old name gone from config view; new name present with the same member.
	for _, v := range fetchAllPools(t, srv.URL) {
		if v.Pool == "rt" {
			t.Error("old pool name rt still present after rename")
		}
	}
	if !memberPresent(t, srv.URL, "renamed", "m") {
		t.Error("renamed pool missing its member after rename")
	}

	// The rename survives a config-roundtrip restart (env is not re-read).
	pools2 := reloadPools(t, pools)
	srv2 := configMux(t, pools2)
	for _, v := range fetchAllPools(t, srv2.URL) {
		if v.Pool == "rt" {
			t.Error("old name rt reappeared after restart")
		}
	}
	if !memberPresent(t, srv2.URL, "renamed", "m") {
		t.Error("renamed pool lost its member across restart")
	}

	// Error paths.
	postJSON(t, srv.URL+"/_gateway/pool/ghost/rename", `{"name":"renamed2"}`, http.StatusNotFound)    // unknown old
	postJSON(t, srv.URL+"/_gateway/pool/renamed/rename", `{"name":""}`, http.StatusBadRequest)        // empty new
	postJSON(t, srv.URL+"/_gateway/pool/renamed/rename", `{"name":"Renamed"}`, http.StatusBadRequest) // identical after normalize
	postJSON(t, srv.URL+"/_gateway/pool/auto/rename", `{"name":"renamed"}`, http.StatusConflict)      // collides with different existing pool

	// Malformed body.
	post(t, srv.URL+"/_gateway/pool/renamed/rename", http.StatusBadRequest)
}

// TestMoveEndpoint exercises POST /_gateway/pool/{name}/member/{nick}/move:
// the happy path between two plain pools, the validation errors, and the
// 409 → force overwrite path for a conflicting same-nick target.
func TestMoveEndpoint(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-b")
	t.Setenv("AQG_POOL_SPARE_BACKEND_X", "sk-ant-x")
	pools := loadPools(t)
	srv := configMux(t, pools)

	// Happy path: move a from auto to spare.
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/a/move", `{"to":"spare"}`, http.StatusOK)

	if memberPresent(t, srv.URL, "auto", "a") {
		t.Error("a still present in auto after move")
	}
	if !memberPresent(t, srv.URL, "spare", "a") {
		t.Error("a not present in spare after move")
	}

	// Validation errors.
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/b/move", `{"to":"auto"}`, http.StatusBadRequest)      // same pool
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/b/move", `{"to":"ghost"}`, http.StatusNotFound)       // unknown target
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/ghost/move", `{"to":"spare"}`, http.StatusBadRequest) // missing member
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/b/move", `{}`, http.StatusBadRequest)                 // missing target

	// The nick↔credential bijection is now enforced on runtime mutations too
	// (issue #198): the same nick with a different credential is rejected.
	addJSON(t, srv.URL+"/_gateway/pool/spare/member/b", `{"credential":"sk-ant-other"}`, http.StatusBadRequest)

	// Conflict path: spare already has b under the SAME credential but a
	// different base_url → move is a base_url conflict → 409, then force
	// overwrites. (A different credential is impossible under the bijection, so
	// base_url is the only legitimate same-nick conflict.)
	addJSON(t, srv.URL+"/_gateway/pool/spare/member/b", `{"credential":"sk-ant-b","base_url":"https://other.example"}`, http.StatusOK)
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/b/move", `{"to":"spare"}`, http.StatusConflict)
	if !memberPresent(t, srv.URL, "auto", "b") {
		t.Error("b vanished from auto after a rejected (409) move")
	}
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/b/move", `{"to":"spare","force":true}`, http.StatusOK)
	if memberPresent(t, srv.URL, "auto", "b") {
		t.Error("b still in auto after forced move")
	}
	if !memberPresent(t, srv.URL, "spare", "b") {
		t.Error("b not in spare after forced move")
	}
}

// memberPresent reports whether nick appears in the pool's effective config view.
func memberPresent(t *testing.T, baseURL, pool, nick string) bool {
	t.Helper()
	view := fetchPool(t, baseURL, pool)
	for _, m := range view.Members {
		if m.Nick == nick {
			return true
		}
	}
	return false
}

func addJSON(t *testing.T, url, body string, wantStatus int) {
	t.Helper()
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Errorf("post %s body=%s status=%d, want %d", url, body, resp.StatusCode, wantStatus)
	}
}

func delete(t *testing.T, url string, wantStatus int) {
	t.Helper()
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete %s: %v", url, err)
	}
	resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Errorf("delete %s status=%d, want %d", url, resp.StatusCode, wantStatus)
	}
}

// TestUIHandler_servesHTML confirms GET /_gateway/ui returns 200 with the
// HTML content type, the expected mount point, and a no-cache header so an
// upgraded binary takes effect on the next reload.
func TestUIHandler_servesHTML(t *testing.T) {
	srv := uiMux(t)

	resp, err := http.Get(srv.URL + "/_gateway/ui")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("Content-Type=%q, want text/html; charset=utf-8", got)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
		t.Errorf("Cache-Control=%q, want no-cache", got)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	bs := string(body)
	if !strings.Contains(bs, "<title>Agent Quota Gateway") {
		t.Errorf("body missing <title>: %s", bs)
	}
	if !strings.Contains(bs, `<div id="pools">`) {
		t.Errorf("body missing #pools mount point: %s", bs)
	}
	if uiSHA256 == "" {
		t.Error("uiSHA256 not computed at init")
	}
	if got := resp.Header.Get("X-UI-SHA256"); got != uiSHA256 {
		t.Errorf("X-UI-SHA256=%q, want %q", got, uiSHA256)
	}
}

// TestUIHandler_surfacesUnsavedConfig is the regression guard for issue #218:
// the management UI must consume the X-AQG-Unsaved-Config signal it was built
// to display. Before the fix a grep for the header over the embedded page
// returned zero matches — the documented UI-facing signal was silently ignored.
// A static assertion on uiHTML keeps a future copy from dropping the consumer.
func TestUIHandler_surfacesUnsavedConfig(t *testing.T) {
	if !strings.Contains(uiHTML, "X-AQG-Unsaved-Config") {
		t.Error("UI HTML does not reference X-AQG-Unsaved-Config; the unsaved-config warning is not wired (issue #218)")
	}
}

// TestUIHandler_surfacesPersistenceMode is the regression guard for issue #246:
// the bundled UI must also consume the X-AQG-Persistence header on
// /_gateway/config and render a distinct banner when the mode is env_only.
// Without this static guard a future copy could silently drop the new banner
// and env-only mode would revert to reporting a clean save (the issue's
// documented failure mode).
func TestUIHandler_surfacesPersistenceMode(t *testing.T) {
	if !strings.Contains(uiHTML, configfile.HeaderPersistence) {
		t.Errorf("UI HTML does not reference %s; the env-only banner is not wired (issue #246)", configfile.HeaderPersistence)
	}
	if !strings.Contains(uiHTML, "env-only mode") {
		t.Error("UI HTML does not contain the env-only banner copy")
	}
	if !strings.Contains(uiHTML, "envonly") {
		t.Error("UI HTML does not reference the #envonly banner element")
	}
}

// TestUIHandler_methodNotAllowed confirms non-GET methods receive 405 with
// an Allow header, matching the policy of the other /_gateway/* endpoints.
func TestUIHandler_methodNotAllowed(t *testing.T) {
	srv := uiMux(t)

	req, err := http.NewRequest("POST", srv.URL+"/_gateway/ui", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status=%d, want 405", resp.StatusCode)
	}
	if got := resp.Header.Get("Allow"); got != http.MethodGet {
		t.Errorf("Allow=%q, want GET", got)
	}
}

// allowedCredentialRefs are the exact, deliberately-named references to the
// add-subscription form's write-only credential field. They are stripped
// before the forbidden-substring scan so the guard permits the named field
// while still catching any other secret/token/api-key shaped content. The
// strings are matched case-sensitively, so a differently-cased reference
// (e.g. "Credential:") still trips the case-insensitive regex.
var allowedCredentialRefs = []string{
	"'credential'",            // the add-form field name (set via mkInput)
	"'credential (optional)'", // the add-form placeholder naming the optional field
	"credential:",             // the POST body key
	"credential is required",  // the client-side validation message naming the field
}

// scanForForbiddenSecret strips the JS contract comment and the allowlisted
// credential references, then scans for any remaining credential/secret/token/
// api-key shaped substring. It returns the offending token and true when the
// guard should fail. Sharing this with the leak test keeps both honest.
func scanForForbiddenSecret(html string) (string, bool) {
	stripped := stripCredentialContractComment(html)
	for _, ref := range allowedCredentialRefs {
		stripped = strings.ReplaceAll(stripped, ref, "")
	}
	re := regexp.MustCompile(`(?i)credential|secret|token|api[_-]?key`)
	if loc := re.FindStringIndex(stripped); loc != nil {
		return stripped[loc[0]:loc[1]], true
	}
	return "", false
}

// TestUIHandler_noCredentialSubstring is the static guard that catches a
// credential leak before the file is ever served. It scans the embedded
// page for known credential substrings (sk-ant and a case-insensitive
// match on credential|secret|token|api[_-]?key). The JS contract comment
// and the deliberately-named credential field (allowedCredentialRefs) are
// the only allowed exceptions; both are stripped before the scan so a future
// copy that introduces an unrelated secret reference still fails.
func TestUIHandler_noCredentialSubstring(t *testing.T) {
	if strings.Contains(uiHTML, "sk-ant") {
		t.Fatalf("UI HTML contains sk-ant credential substring")
	}
	if tok, found := scanForForbiddenSecret(uiHTML); found {
		t.Fatalf("UI HTML contains forbidden credential substring %q", tok)
	}
}

// TestCredentialGuard_stillCatchesLeaks proves relaxing the guard for the
// named credential field did not blunt it: arbitrary secret/token/api-key
// shaped content — and any credential reference outside the allowlist — is
// still caught, while the allowlisted field references alone are permitted.
func TestCredentialGuard_stillCatchesLeaks(t *testing.T) {
	leaks := []string{
		`var x = "secret-value";`,
		`headers['x-api-key'] = k;`,
		`var s = "token-abc";`,
		`el.dataset.credential = m.credential;`, // a non-allowlisted credential ref
		`{ Credential: c }`,                     // wrong case is not on the allowlist
	}
	for _, s := range leaks {
		if _, found := scanForForbiddenSecret(s); !found {
			t.Errorf("guard failed to catch leak-shaped content: %q", s)
		}
	}
	// The allowlisted references alone must NOT trip the guard.
	clean := `mkInput('password', 'credential', 'credential'); var body = { credential: cred };`
	if tok, found := scanForForbiddenSecret(clean); found {
		t.Errorf("guard tripped on allowlisted credential reference: %q", tok)
	}
}

// stripCredentialContractComment removes the JS line comments that
// document the credential-leak contract, so they do not trip the static
// substring check. The contract is one or more `// ...` lines that start
// with `// Credential contract:` and run until the next blank line or
// non-comment line. Only the lines inside the script block are touched;
// any prose in <p> elements is left in place because the regex would
// match it and trip a real failure.
func stripCredentialContractComment(s string) string {
	const marker = "// Credential contract:"
	for {
		start := strings.Index(s, marker)
		if start < 0 {
			return s
		}
		// Walk back to the start of the line.
		lineStart := strings.LastIndex(s[:start], "\n") + 1
		// Walk forward through consecutive `// ...` lines.
		scan := start
		for {
			nl := strings.Index(s[scan:], "\n")
			if nl < 0 {
				scan = len(s)
				break
			}
			next := scan + nl + 1
			rest := strings.TrimLeft(s[next:], " \t")
			if !strings.HasPrefix(rest, "//") {
				scan = next
				break
			}
			scan = next
		}
		s = s[:lineStart] + s[scan:]
	}
}

// envOnlyState is the single test fixture for issue #246's env-only mode:
// ModeEnvOnly with a no-op Unsaved (the latch is always false for an
// env-only writer; the helper constructor stubs it).
func envOnlyState() configfile.PersistenceState {
	return configfile.PersistenceState{Mode: configfile.ModeEnvOnly, Unsaved: func() bool { return false }}
}

// persistedCleanState mirrors envOnlyState for the persisted, clean case:
// ModePersisted, no flush latch tripped. The handler bodies must stay
// byte-identical to the pre-#246 response shapes for this state.
func persistedCleanState() configfile.PersistenceState {
	return configfile.PersistenceState{Mode: configfile.ModePersisted, Unsaved: func() bool { return false }}
}

// persistedUnsavedState covers the existing two-state behaviour: ModePersisted
// with the unsaved latch tripped, which should still emit the legacy header
// (unchanged by issue #246).
func persistedUnsavedState() configfile.PersistenceState {
	return configfile.PersistenceState{Mode: configfile.ModePersisted, Unsaved: func() bool { return true }}
}

// TestHealthHandler_threeStates covers AC1 (issue #246): the health body
// distinguishes env-only from clean and from unsaved, while keeping
// status code 200 and the top-level "status":"ok" field.
func TestHealthHandler_threeStates(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")

	cases := []struct {
		name      string
		pers      configfile.PersistenceState
		wantBody  string
		wantHead  string // header value, "" to assert absence
		noHeaders []string
	}{
		{
			name:     "persisted clean",
			pers:     persistedCleanState(),
			wantBody: `{"status":"ok"}`,
			wantHead: "persisted",
		},
		{
			name:      "persisted unsaved",
			pers:      persistedUnsavedState(),
			wantBody:  `{"status":"ok","unsaved_config_changes":true}`,
			wantHead:  "persisted",
			noHeaders: nil, // legacy unsaved header is set on the dedicated endpoint, not here
		},
		{
			name:     "env-only",
			pers:     envOnlyState(),
			wantBody: `{"status":"ok","persistence":"env_only"}`,
			wantHead: "env_only",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(healthHandler(tc.pers, nil))
			defer srv.Close()

			resp, err := http.Get(srv.URL + "/_gateway/health")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status=%d, want 200", resp.StatusCode)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if got := strings.TrimSpace(string(body)); got != tc.wantBody {
				t.Errorf("body=%q, want %q", got, tc.wantBody)
			}
			if got := resp.Header.Get(configfile.HeaderPersistence); got != tc.wantHead {
				t.Errorf("%s=%q, want %q", configfile.HeaderPersistence, got, tc.wantHead)
			}
		})
	}
}

// TestConfigEndpoint_threeStates covers AC2: /_gateway/config sets the
// persistence header in every state, sets the legacy unsaved header only
// when the latch is tripped, and the credential-redaction contract is
// preserved across all three.
func TestConfigEndpoint_threeStates(t *testing.T) {
	const secret = "sk-ant-SECRET-DO-NOT-LEAK"
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", secret)

	cases := []struct {
		name        string
		pers        configfile.PersistenceState
		wantHead    string
		wantUnsaved bool
	}{
		{name: "persisted clean", pers: persistedCleanState(), wantHead: "persisted", wantUnsaved: false},
		{name: "persisted unsaved", pers: persistedUnsavedState(), wantHead: "persisted", wantUnsaved: true},
		{name: "env-only", pers: envOnlyState(), wantHead: "env_only", wantUnsaved: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := configMuxWithPersistence(t, loadPools(t), tc.pers)

			resp, err := http.Get(srv.URL + "/_gateway/config")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status=%d, want 200", resp.StatusCode)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if got := resp.Header.Get(configfile.HeaderPersistence); got != tc.wantHead {
				t.Errorf("%s=%q, want %q", configfile.HeaderPersistence, got, tc.wantHead)
			}
			gotUnsaved := resp.Header.Get(configfile.HeaderUnsavedConfig) == "true"
			if gotUnsaved != tc.wantUnsaved {
				t.Errorf("X-AQG-Unsaved-Config=%v, want %v", gotUnsaved, tc.wantUnsaved)
			}
			// Credential contract: no credential substring regardless of mode.
			if strings.Contains(string(body), "sk-ant-SECRET") {
				t.Errorf("config response leaked a credential substring in mode %q", tc.name)
			}
		})
	}
}

// TestMutationHandler_envOnly_statusOKShape covers AC3 for the status-style
// mutation body (issue #246 review note #2): every {"status":"ok"} response
// gains the "persistence" body field and the persistence header in
// env-only mode, and stays byte-identical in persisted mode.
//
// Each case drives its own mutation and asserts on the captured response
// body — no re-fire of a single endpoint as a stand-in. That way an
// enable/priority handler that silently dropped the field would fail
// here instead of passing because the test was reading a disable body.
func TestMutationHandler_envOnly_statusOKShape(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-b")

	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{name: "disable member", method: http.MethodPost, path: "/_gateway/pool/auto/member/a/disable", wantStatus: http.StatusOK},
		{name: "enable member", method: http.MethodPost, path: "/_gateway/pool/auto/member/a/enable", wantStatus: http.StatusOK},
		{name: "set priority", method: http.MethodPost, path: "/_gateway/pool/auto/priority", body: `["b","a"]`, wantStatus: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run("env-only/"+tc.name, func(t *testing.T) {
			srv := configMuxWithPersistence(t, loadPools(t), envOnlyState())
			body, status, header := doMutation(t, srv.URL, tc.method, tc.path, tc.body)
			if status != tc.wantStatus {
				t.Fatalf("status=%d, want %d", status, tc.wantStatus)
			}
			if !strings.Contains(body, `"status":"ok"`) {
				t.Errorf("env-only body missing status field: %s", body)
			}
			if !strings.Contains(body, `"persistence":"env_only"`) {
				t.Errorf("env-only body missing persistence field: %s", body)
			}
			if got := header.Get(configfile.HeaderPersistence); got != "env_only" {
				t.Errorf("%s=%q, want \"env_only\"", configfile.HeaderPersistence, got)
			}
		})

		t.Run("persisted/"+tc.name, func(t *testing.T) {
			srv := configMuxWithPersistence(t, loadPools(t), persistedCleanState())
			body, status, header := doMutation(t, srv.URL, tc.method, tc.path, tc.body)
			if status != tc.wantStatus {
				t.Fatalf("status=%d, want %d", status, tc.wantStatus)
			}
			if !strings.Contains(body, `"status":"ok"`) {
				t.Errorf("persisted body missing status field: %s", body)
			}
			if strings.Contains(body, `"persistence"`) {
				t.Errorf("persisted body unexpectedly carries persistence field: %s", body)
			}
			if got := header.Get(configfile.HeaderPersistence); got != "persisted" {
				t.Errorf("%s=%q, want \"persisted\"", configfile.HeaderPersistence, got)
			}
		})
	}
}

// doMutation fires method/path against baseURL, returning the body as a
// string, the status code, and the response headers. Shared helper for
// the env-only mutation tests so each case captures its own response
// (the existing post/postJSON helpers discard the body).
func doMutation(t *testing.T, baseURL, method, path, body string) (string, int, http.Header) {
	t.Helper()
	var req *http.Request
	var err error
	if body == "" {
		req, err = http.NewRequest(method, baseURL+path, nil)
	} else {
		req, err = http.NewRequest(method, baseURL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	bs, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(bs), resp.StatusCode, resp.Header
}

// TestMutationHandler_envOnly_poolNamedShape covers AC3 for the second
// mutation body shape (issue #246 review note #2): {"pool":"..."} responses
// from createPool and renamePool also gain the persistence field and
// header in env-only mode.
func TestMutationHandler_envOnly_poolNamedShape(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")

	t.Run("env-only/create", func(t *testing.T) {
		srv := configMuxWithPersistence(t, loadPools(t), envOnlyState())
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/_gateway/pool", strings.NewReader(`{"name":"rt"}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("status=%d, want 201", resp.StatusCode)
		}
		if !strings.Contains(string(body), `"persistence":"env_only"`) {
			t.Errorf("env-only body missing persistence field: %s", body)
		}
		if !strings.Contains(string(body), `"pool":"rt"`) {
			t.Errorf("env-only body missing pool field: %s", body)
		}
		if got := resp.Header.Get(configfile.HeaderPersistence); got != "env_only" {
			t.Errorf("%s=%q, want \"env_only\"", configfile.HeaderPersistence, got)
		}
	})

	t.Run("persisted/create", func(t *testing.T) {
		srv := configMuxWithPersistence(t, loadPools(t), persistedCleanState())
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/_gateway/pool", strings.NewReader(`{"name":"rt2"}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("status=%d, want 201", resp.StatusCode)
		}
		if strings.Contains(string(body), `"persistence"`) {
			t.Errorf("persisted body unexpectedly carries persistence field: %s", body)
		}
		if !strings.Contains(string(body), `"pool":"rt2"`) {
			t.Errorf("persisted body missing pool field: %s", body)
		}
		if got := resp.Header.Get(configfile.HeaderPersistence); got != "persisted" {
			t.Errorf("%s=%q, want \"persisted\"", configfile.HeaderPersistence, got)
		}
	})

	t.Run("env-only/rename", func(t *testing.T) {
		srv := configMuxWithPersistence(t, loadPools(t), envOnlyState())
		// Create then rename — both must carry the env-only signal.
		postJSON(t, srv.URL+"/_gateway/pool", `{"name":"rt"}`, http.StatusCreated)
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/_gateway/pool/rt/rename", strings.NewReader(`{"name":"rt-renamed"}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("rename status=%d, want 200", resp.StatusCode)
		}
		if !strings.Contains(string(body), `"persistence":"env_only"`) {
			t.Errorf("env-only rename body missing persistence field: %s", body)
		}
		if !strings.Contains(string(body), `"pool":"rt-renamed"`) {
			t.Errorf("env-only rename body missing pool field: %s", body)
		}
		if got := resp.Header.Get(configfile.HeaderPersistence); got != "env_only" {
			t.Errorf("%s=%q, want \"env_only\"", configfile.HeaderPersistence, got)
		}
	})
}

// TestMutationHandler_envOnly_fullFamily is a sanity sweep over every
// mutation handler (issue #246 review note #2): each one returns a 200
// or 201 in env-only mode and carries the persistence header. Together
// with the body-shape tests above, this confirms AC3 across all 9
// mutation handlers.
//
// The sweep uses two pre-configured pools plus two more set up at
// runtime so deletePool (which needs an empty pool) and moveMember
// (which needs a source + target) have the state their happy paths
// require without the setup leaking into the other assertions.
func TestMutationHandler_envOnly_fullFamily(t *testing.T) {
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-a")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "sk-ant-b")
	t.Setenv("AQG_POOL_PLAIN_BACKEND_X", "sk-ant-x")
	t.Setenv("AQG_POOL_TARGET_BACKEND_T", "sk-ant-t")

	srv := configMuxWithPersistence(t, loadPools(t), envOnlyState())

	happyPaths := []struct {
		method string
		path   string
		body   string
		want   int
	}{
		{"POST", "/_gateway/pool/auto/priority", `["b","a"]`, 200},
		{"POST", "/_gateway/pool/plain/member/x/disable", ``, 200},
		{"POST", "/_gateway/pool/plain/member/x/enable", ``, 200},
		{"POST", "/_gateway/pool/plain/member/y", `{"credential":"sk-ant-y"}`, 200},
		{"DELETE", "/_gateway/pool/plain/member/y", ``, 200},
	}
	for _, hp := range happyPaths {
		body, status, header := doMutation(t, srv.URL, hp.method, hp.path, hp.body)
		if status != hp.want {
			t.Errorf("%s %s status=%d, want %d (body=%s)", hp.method, hp.path, status, hp.want, body)
		}
		if got := header.Get(configfile.HeaderPersistence); got != "env_only" {
			t.Errorf("%s %s: %s=%q, want \"env_only\"", hp.method, hp.path, configfile.HeaderPersistence, got)
		}
	}

	// deletePool: drain the plain pool's only member, then delete the pool.
	// The pool was created by `loadPools` from env; member x is the only
	// member, so removing it leaves the pool empty.
	body, status, header := doMutation(t, srv.URL, http.MethodDelete, "/_gateway/pool/plain/member/x", "")
	if status != http.StatusOK {
		t.Errorf("drain plain/member/x status=%d, want 200 (body=%s)", status, body)
	}
	body, status, header = doMutation(t, srv.URL, http.MethodDelete, "/_gateway/pool/plain", "")
	if status != http.StatusOK {
		t.Errorf("delete pool plain status=%d, want 200 (body=%s)", status, body)
	}
	if got := header.Get(configfile.HeaderPersistence); got != "env_only" {
		t.Errorf("delete pool plain: %s=%q, want \"env_only\"", configfile.HeaderPersistence, got)
	}

	// moveMember: target already exists (env-declared with member t). Move
	// a runtime member y from auto to target; the env-only signal must
	// fire on the 200 response.
	body, status, header = doMutation(t, srv.URL, http.MethodPost, "/_gateway/pool/auto/member/a/move", `{"to":"target"}`)
	if status != http.StatusOK {
		t.Fatalf("move auto/a -> target status=%d, want 200 (body=%s)", status, body)
	}
	if got := header.Get(configfile.HeaderPersistence); got != "env_only" {
		t.Errorf("move auto/a -> target: %s=%q, want \"env_only\"", configfile.HeaderPersistence, got)
	}
}
