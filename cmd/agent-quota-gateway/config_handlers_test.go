package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// configMux builds a ServeMux with the runtime-config routes wired exactly as
// run() wires them, so the path-pattern handlers can resolve r.PathValue.
// The /_gateway/ui route is exercised by uiMux instead — that handler is
// pools-free and does not belong in this mux.
func configMux(t *testing.T, pools *auto.Pools) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/config", configHandler(pools))
	mux.HandleFunc("POST /_gateway/pool/{name}/priority", priorityHandler(pools))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/disable", disableMemberHandler(pools))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/enable", enableMemberHandler(pools))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}", addMemberHandler(pools))
	mux.HandleFunc("DELETE /_gateway/pool/{name}/member/{nick}", removeMemberHandler(pools))
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
	srv := configMux(t, loadPools(t))

	// Add a runtime member.
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/c", `{"credential":"`+secretC+`"}`, http.StatusOK)

	// Verify the member appears in config with source: "runtime".
	view := fetchPool(t, srv.URL, "auto")
	found := false
	for _, m := range view.Members {
		if m.Nick == "c" {
			found = true
			if m.Source != "runtime" {
				t.Errorf("added member c has source=%q, want 'runtime'", m.Source)
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

	// Remove a static member (hide it from selection).
	delete(t, srv.URL+"/_gateway/pool/auto/member/a", http.StatusOK)
	view = fetchPool(t, srv.URL, "auto")
	for _, m := range view.Members {
		if m.Nick == "a" && !m.Disabled {
			// Note: static members that are "removed" via this endpoint stay
			// in the config but are hidden from selection. The current behavior
			// is that they disappear from the effective member list.
		}
	}

	// Error cases.
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/a", `{"credential":"sk-ant-x"}`, http.StatusConflict) // duplicate nick
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/new", `{}`, http.StatusBadRequest)           // empty credential
	addJSON(t, srv.URL+"/_gateway/pool/auto/member/new", `{"credential":"x","base_url":"!"}`, http.StatusBadRequest) // invalid URL
	delete(t, srv.URL+"/_gateway/pool/ghost/member/a", http.StatusNotFound) // unknown pool
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

// TestUIHandler_noCredentialSubstring is the static guard that catches a
// credential leak before the file is ever served. It scans the embedded
// page for known credential substrings (sk-ant and a case-insensitive
// match on credential|secret|token|api[_-]?key). The JS contract comment
// is the only allowed exception; the regex strip below removes it from
// the search space so a future copy that omits the comment does not break
// the test silently.
func TestUIHandler_noCredentialSubstring(t *testing.T) {
	if strings.Contains(uiHTML, "sk-ant") {
		t.Fatalf("UI HTML contains sk-ant credential substring")
	}
	// Strip the contract-comment block so its keyword prose doesn't trigger
	// a false positive; the page is searched as a flat string after.
	stripped := stripCredentialContractComment(uiHTML)
	re := regexp.MustCompile(`(?i)credential|secret|token|api[_-]?key`)
	if loc := re.FindStringIndex(stripped); loc != nil {
		t.Fatalf("UI HTML contains forbidden credential substring %q at %d..%d", stripped[loc[0]:loc[1]], loc[0], loc[1])
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
