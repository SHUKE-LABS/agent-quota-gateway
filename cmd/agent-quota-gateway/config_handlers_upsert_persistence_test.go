package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/configfile"
)

func issue365PersistedGateway(t *testing.T) (*auto.Pools, string, string) {
	t.Helper()
	scrubPoolEnv(t)
	unsetenv(t, "AQG_CONFIG")
	unsetenv(t, "AQG_STATE_FILE")

	dir := t.TempDir()
	cfgPath := dir + "/aqg.json"
	t.Setenv("AQG_CONFIG", cfgPath)
	t.Setenv("AQG_POOL_AUTO_BASE_URL", "https://pool.example")
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "cred-a|https://member.example")
	t.Setenv("AQG_POOL_AUTO_BACKEND_B", "cred-b")
	t.Setenv("AQG_POOL_AUTO_PRIORITY", "a,b")
	t.Setenv("AQG_POOL_OTHER_BACKEND_SHARED", "cred-shared")

	var log bytes.Buffer
	resolvedCfg, _, path, err := resolveConfig("", &log)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if path != cfgPath {
		t.Fatalf("config path=%q, want %q", path, cfgPath)
	}
	_, reg, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	pools := auto.NewPools(reg, nil, nil, io.Discard)
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
	return pools, cfgPath, srv.URL
}

func runIssue365MemberAdd(t *testing.T, origin, pool, nick, credential, baseURL string) int {
	t.Helper()
	args := []string{"--gateway", origin, "member", "add", pool, nick, "--credential-stdin"}
	if baseURL != "" {
		args = append(args, "--base-url", baseURL)
	}
	var stdout, stderr strings.Builder
	code := cliWithInput(args, strings.NewReader(credential+"\n"), &stdout, &stderr, func(string) error {
		t.Fatal("management command invoked server startup")
		return nil
	})
	if strings.Contains(stdout.String(), credential) || strings.Contains(stderr.String(), credential) {
		t.Fatalf("credential leaked in CLI output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	return code
}

func TestManagementCLI_memberUpsertPersistsThroughRestart(t *testing.T) {
	pools, cfgPath, origin := issue365PersistedGateway(t)

	if code := runIssue365MemberAdd(t, origin, "auto", "c", "cred-c", "https://c.example"); code != 0 {
		t.Fatalf("new member add exit=%d", code)
	}
	if got := poolPriority(t, origin, "auto"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("priority after CLI add=%v, want [a b c]", got)
	}

	// The listing is served by the real config handler and must stay redacted.
	var listing, listingErr strings.Builder
	if code := cliWithInput([]string{"pool", "ls", "--json", "--gateway", origin}, strings.NewReader(""), &listing, &listingErr, func(string) error {
		t.Fatal("management command invoked server startup")
		return nil
	}); code != 0 {
		t.Fatalf("pool ls exit=%d stderr=%q", code, listingErr.String())
	}
	if strings.Contains(listing.String(), "cred-") {
		t.Fatalf("pool listing leaked credential: %s", listing.String())
	}
	var views []auto.PoolConfigView
	if err := json.Unmarshal([]byte(listing.String()), &views); err != nil {
		t.Fatalf("decode pool listing: %v", err)
	}
	if len(views) != 2 || !managementHasMember(views[0], "c") && !managementHasMember(views[1], "c") {
		t.Fatalf("pool listing=%+v, want new member c", views)
	}

	waitForDiskContain(t, cfgPath, "cred-c")
	_, persisted, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadFile after CLI add: %v", err)
	}
	added, ok := persisted.ResolveIn("auto", "c")
	if !ok || added.Credential != "cred-c" || added.BaseURL != "https://c.example" {
		t.Fatalf("persisted c=%+v ok=%v, want credential/base URL", added, ok)
	}

	// Disable the first two entries through the real HTTP handlers. Each route
	// selection must advance through the ordered insertion to c.
	postJSON(t, origin+"/_gateway/pool/auto/member/a/disable", `{}`, http.StatusOK)
	if got, _, known, exhausted := pools.Route("auto"); !known || exhausted || got.Nick != "b" {
		t.Fatalf("route after disabling a=%q known=%v exhausted=%v, want b", got.Nick, known, exhausted)
	}
	postJSON(t, origin+"/_gateway/pool/auto/member/b/disable", `{}`, http.StatusOK)
	if got, _, known, exhausted := pools.Route("auto"); !known || exhausted || got.Nick != "c" {
		t.Fatalf("route after disabling a,b=%q known=%v exhausted=%v, want c", got.Nick, known, exhausted)
	}

	beforeRepeat := pools.CurrentRegistry().Spec()
	if code := runIssue365MemberAdd(t, origin, "auto", "c", "cred-c", "https://repeat-drift.example"); code != 0 {
		t.Fatalf("identical repeat exit=%d", code)
	}
	if after := pools.CurrentRegistry().Spec(); !reflect.DeepEqual(after, beforeRepeat) {
		t.Fatalf("identical repeat changed declaration:\nbefore=%+v\nafter=%+v", beforeRepeat, after)
	}

	if code := runIssue365MemberAdd(t, origin, "auto", "c", "cred-c-rotated", "https://rotation-drift.example"); code != 0 {
		t.Fatalf("credential rotation exit=%d", code)
	}
	rotated, ok := pools.CurrentRegistry().ResolveIn("auto", "c")
	if !ok || rotated.Credential != "cred-c-rotated" || rotated.BaseURL != "https://c.example" {
		t.Fatalf("rotated c=%+v ok=%v, want credential rotated and base URL retained", rotated, ok)
	}

	beforeRefusals := pools.CurrentRegistry().Spec()
	if code := runIssue365MemberAdd(t, origin, "auto", "c", "cred-b", "https://collision.example"); code != 1 {
		t.Fatalf("other-nick credential refusal exit=%d, want 1", code)
	}
	if after := pools.CurrentRegistry().Spec(); !reflect.DeepEqual(after, beforeRefusals) {
		t.Fatalf("credential refusal changed declaration:\nbefore=%+v\nafter=%+v", beforeRefusals, after)
	}

	// Seed the same nick in auto with the same credential as the other pool,
	// then prove a shared-nick rotation is refused without a disk drift.
	if code := runIssue365MemberAdd(t, origin, "auto", "shared", "cred-shared", "https://shared.example"); code != 0 {
		t.Fatalf("shared member add exit=%d", code)
	}
	beforeSharedRefusal := pools.CurrentRegistry().Spec()
	if code := runIssue365MemberAdd(t, origin, "auto", "shared", "cred-shared-rotated", "https://shared-drift.example"); code != 1 {
		t.Fatalf("shared-nick refusal exit=%d, want 1", code)
	}
	if after := pools.CurrentRegistry().Spec(); !reflect.DeepEqual(after, beforeSharedRefusal) {
		t.Fatalf("shared-nick refusal changed declaration:\nbefore=%+v\nafter=%+v", beforeSharedRefusal, after)
	}

	waitForDiskContain(t, cfgPath, "cred-c-rotated")
	waitForDiskContain(t, cfgPath, "https://shared.example")
	_, reloaded, err := configfile.LoadFile(cfgPath)
	if err != nil {
		t.Fatalf("LoadFile after refusals: %v", err)
	}
	if got, want := reloaded.Spec(), pools.CurrentRegistry().Spec(); !reflect.DeepEqual(got, want) {
		t.Fatalf("disk config drifted after repeat/refusals:\ndisk=%+v\nmemory=%+v", got, want)
	}
	if got := reloaded.PoolNames(); !reflect.DeepEqual(got, []string{"auto", "other"}) {
		t.Fatalf("reloaded pools=%v, want auto and other", got)
	}
	if got, ok := reloaded.ResolveIn("auto", "shared"); !ok || got.Credential != "cred-shared" {
		t.Fatalf("reloaded shared=%+v ok=%v, want unchanged shared credential", got, ok)
	}
	if got, _, known, exhausted := auto.NewPools(reloaded, nil, nil, io.Discard).Route("auto"); !known || exhausted || got.Nick != "c" {
		t.Fatalf("restarted route=%q known=%v exhausted=%v, want c", got.Nick, known, exhausted)
	}
}
