package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
)

func managementTestViews() []auto.PoolConfigView {
	return []auto.PoolConfigView{{
		Pool:     "dst",
		Priority: []string{"a", "b"},
		Members: []auto.PoolMemberConfigView{
			{Nick: "a", BaseURL: "https://old.example", Disabled: true, Status: "disabled"},
			{Nick: "b", BaseURL: "https://old.example", Status: "idle"},
		},
	}}
}

func writeManagementJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode management response: %v", err)
	}
}

func TestManagementCLI_memberAddUsesStdinAndAppendsExpandedPriority(t *testing.T) {
	const secret = "credential-must-not-appear"
	var mu sync.Mutex
	var gotBody map[string]any
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_gateway/config":
			writeManagementJSON(t, w, managementTestViews())
		case "/_gateway/pool/dst/member/c":
			mu.Lock()
			gotPath = r.URL.Path
			defer mu.Unlock()
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode member add body: %v", err)
			}
			writeManagementJSON(t, w, map[string]string{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	var stdout, stderr strings.Builder
	started := false
	code := cliWithInput(
		[]string{"--gateway", srv.URL, "member", "add", "dst", "c", "--credential-stdin", "--base-url", "https://new.example"},
		strings.NewReader(secret), &stdout, &stderr,
		func(string) error { started = true; return nil },
	)
	if code != 0 {
		t.Fatalf("member add exit=%d stderr=%q", code, stderr.String())
	}
	if started {
		t.Fatal("management command invoked server startup")
	}
	mu.Lock()
	defer mu.Unlock()
	if gotPath != "/_gateway/pool/dst/member/c" {
		t.Fatalf("request path=%q, want member path", gotPath)
	}
	if gotBody["credential"] != secret || gotBody["upsert"] != true {
		t.Fatalf("request body=%v, want credential and upsert", gotBody)
	}
	if gotBody["base_url"] != "https://new.example" {
		t.Fatalf("request base_url=%v, want https://new.example", gotBody["base_url"])
	}
	placement, ok := gotBody["placement"].([]any)
	if !ok || len(placement) != 3 || placement[0] != "a" || placement[1] != "b" || placement[2] != "c" {
		t.Fatalf("request placement=%v, want [a b c]", gotBody["placement"])
	}
	if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
		t.Fatalf("credential leaked in CLI output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestManagementCLI_existingMemberDoesNotSendReplacementPlacement(t *testing.T) {
	const secret = "rotated-secret"
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_gateway/config":
			writeManagementJSON(t, w, managementTestViews())
		case "/_gateway/pool/dst/member/a":
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode replacement body: %v", err)
			}
			writeManagementJSON(t, w, map[string]string{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	var stdout, stderr strings.Builder
	code := cliWithInput(
		[]string{"member", "add", "dst", "a", "--gateway", srv.URL, "--credential-stdin", "--base-url", "https://drift.example"},
		strings.NewReader(secret), &stdout, &stderr, func(string) error { t.Fatal("server start called"); return nil },
	)
	if code != 0 {
		t.Fatalf("existing member add exit=%d stderr=%q", code, stderr.String())
	}
	if body["credential"] != secret || body["upsert"] != true || body["base_url"] != "https://drift.example" {
		t.Fatalf("replacement body=%v, want credential/upsert/base_url", body)
	}
	if _, ok := body["placement"]; ok {
		t.Fatalf("existing-member replacement sent placement=%v", body["placement"])
	}
}

func TestManagementCLI_poolListJSONIsRedacted(t *testing.T) {
	const secret = "server-side-secret"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_gateway/config" {
			http.NotFound(w, r)
			return
		}
		writeManagementJSON(t, w, managementTestViews())
	}))
	t.Cleanup(srv.Close)

	var stdout, stderr strings.Builder
	code := cliWithInput([]string{"pool", "ls", "--json", "--gateway", srv.URL}, strings.NewReader(""), &stdout, &stderr, func(string) error {
		t.Fatal("server start called")
		return nil
	})
	if code != 0 {
		t.Fatalf("pool ls exit=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"pool":"dst"`) || strings.Contains(stdout.String(), secret) {
		t.Fatalf("pool ls output=%q, want redacted JSON", stdout.String())
	}
	var views []auto.PoolConfigView
	if err := json.Unmarshal([]byte(stdout.String()), &views); err != nil {
		t.Fatalf("decode pool ls JSON: %v", err)
	}
	if len(views) != 1 || views[0].Members[0].Nick != "a" {
		t.Fatalf("pool ls views=%+v", views)
	}
}

func TestManagementCLI_rejectsCredentialArgWithoutLeakingIt(t *testing.T) {
	const secret = "secret-in-unsupported-flag"
	var stdout, stderr strings.Builder
	code := cliWithInput([]string{"member", "add", "dst", "a", "--credential=" + secret}, strings.NewReader(""), &stdout, &stderr, func(string) error {
		t.Fatal("server start called")
		return nil
	})
	if code != 2 {
		t.Fatalf("unsupported credential flag exit=%d stderr=%q", code, stderr.String())
	}
	if strings.Contains(stderr.String(), secret) || strings.Contains(stdout.String(), secret) {
		t.Fatalf("credential leaked in usage output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestManagementCLI_emptyCredentialIsUsageError(t *testing.T) {
	var stdout, stderr strings.Builder
	code := cliWithInput([]string{"member", "add", "dst", "a", "--credential-stdin"}, strings.NewReader("\n"), &stdout, &stderr, func(string) error {
		t.Fatal("server start called")
		return nil
	})
	if code != 2 || !strings.Contains(stderr.String(), "empty") {
		t.Fatalf("empty stdin exit=%d stderr=%q, want usage error", code, stderr.String())
	}
}

func TestManagementCLI_missingMemberIsNoOp(t *testing.T) {
	deleted := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_gateway/config":
			writeManagementJSON(t, w, managementTestViews())
		case "/_gateway/pool/dst/member/missing":
			deleted = true
			http.Error(w, `{"error":"nick missing not found in pool"}`, http.StatusBadRequest)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	var stdout, stderr strings.Builder
	code := cliWithInput([]string{"member", "rm", "dst", "missing", "--gateway", srv.URL}, strings.NewReader(""), &stdout, &stderr, func(string) error {
		t.Fatal("server start called")
		return nil
	})
	if code != 0 || deleted {
		t.Fatalf("missing member exit=%d deleted=%v stderr=%q", code, deleted, stderr.String())
	}
}

func TestManagementCLI_poolAndMemberMutationsMapMethodsAndRefusals(t *testing.T) {
	var poolAdded, memberDeleted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/_gateway/pool":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["name"] != "byok" {
				t.Fatalf("pool add body=%v err=%v", body, err)
			}
			poolAdded = true
			writeManagementJSON(t, w, map[string]string{"pool": "byok"})
		case r.Method == http.MethodDelete && r.URL.Path == "/_gateway/pool/byok":
			http.Error(w, `{"error":"pool byok still has members"}`, http.StatusConflict)
		case r.Method == http.MethodGet && r.URL.Path == "/_gateway/config":
			writeManagementJSON(t, w, managementTestViews())
		case r.Method == http.MethodDelete && r.URL.Path == "/_gateway/pool/dst/member/a":
			memberDeleted = true
			writeManagementJSON(t, w, map[string]string{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	var stdout, stderr strings.Builder
	if code := cliWithInput([]string{"pool", "add", "byok", "--gateway", srv.URL}, strings.NewReader(""), &stdout, &stderr, func(string) error {
		t.Fatal("server start called")
		return nil
	}); code != 0 || !poolAdded {
		t.Fatalf("pool add exit=%d added=%v stdout=%q stderr=%q", code, poolAdded, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := cliWithInput([]string{"pool", "rm", "byok", "--gateway", srv.URL}, strings.NewReader(""), &stdout, &stderr, func(string) error {
		t.Fatal("server start called")
		return nil
	}); code != 1 || !strings.Contains(stderr.String(), "still has members") {
		t.Fatalf("pool refusal exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := cliWithInput([]string{"member", "rm", "dst", "a", "--gateway", srv.URL}, strings.NewReader(""), &stdout, &stderr, func(string) error {
		t.Fatal("server start called")
		return nil
	}); code != 0 || !memberDeleted {
		t.Fatalf("member remove exit=%d deleted=%v stdout=%q stderr=%q", code, memberDeleted, stdout.String(), stderr.String())
	}
}

func TestManagementCLI_unreachableNamesOrigin(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	origin := srv.URL
	srv.Close()

	var stdout, stderr strings.Builder
	code := cliWithInput([]string{"pool", "ls", "--gateway", origin}, strings.NewReader(""), &stdout, &stderr, func(string) error {
		t.Fatal("server start called")
		return nil
	})
	if code != 3 || !strings.Contains(stderr.String(), origin) {
		t.Fatalf("unreachable exit=%d stderr=%q, want code 3 and origin", code, stderr.String())
	}
}

func TestManagementCLI_defaultOriginUsesListenAddr(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_gateway/config" {
			http.NotFound(w, r)
			return
		}
		writeManagementJSON(t, w, []auto.PoolConfigView{})
	}))
	t.Cleanup(srv.Close)
	t.Setenv("LISTEN_ADDR", strings.TrimPrefix(srv.URL, "http://"))

	var stdout, stderr strings.Builder
	code := cliWithInput([]string{"pool", "ls"}, strings.NewReader(""), &stdout, &stderr, func(string) error {
		t.Fatal("server start called")
		return nil
	})
	if code != 0 {
		t.Fatalf("default-origin pool ls exit=%d stderr=%q", code, stderr.String())
	}
}
