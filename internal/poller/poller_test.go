package poller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// fixedNow is a stable clock for deterministic AsOf stamping in tests.
var fixedNow = time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)

func wantFloatPtr(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: nil, want %v", name, want)
	}
	if *got != want {
		t.Errorf("%s = %v, want %v", name, *got, want)
	}
}

func wantTimePtr(t *testing.T, name string, got *time.Time, wantMs int64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: nil, want epoch-ms %d", name, wantMs)
	}
	if got.UnixMilli() != wantMs {
		t.Errorf("%s = %d ms, want %d ms", name, got.UnixMilli(), wantMs)
	}
}

func TestParseZhipu_usedPercentages(t *testing.T) {
	// Z.ai/Zhipu report *used* percentages in 0..100; TOKENS_LIMIT is the
	// short window, TIME_LIMIT the long one. Both carry epoch-ms resets.
	body := []byte(`{
		"data": {
			"limits": [
				{ "type": "TOKENS_LIMIT", "percentage": 1,  "nextResetTime": 1781418024826 },
				{ "type": "TIME_LIMIT",   "percentage": 16, "nextResetTime": 1783536777977, "usage": 100, "currentValue": 16, "remaining": 84 }
			],
			"level": "lite"
		}
	}`)

	snap, err := parseZhipu(body, fixedNow)
	if err != nil {
		t.Fatalf("parseZhipu: %v", err)
	}
	wantFloatPtr(t, "Unified5hUtilization", snap.Unified5hUtilization, 0.01)
	wantFloatPtr(t, "Unified7dUtilization", snap.Unified7dUtilization, 0.16)
	wantTimePtr(t, "Unified5hReset", snap.Unified5hReset, 1781418024826)
	wantTimePtr(t, "Unified7dReset", snap.Unified7dReset, 1783536777977)
	if !snap.AsOf.Equal(fixedNow) {
		t.Errorf("AsOf = %v, want %v", snap.AsOf, fixedNow)
	}
}

func TestParseZhipu_emptyLimitsIsError(t *testing.T) {
	// A response with no recognised limits carries no quota data; the
	// parser must report an error so the caller keeps the prior snapshot
	// rather than overwriting it with an empty one.
	if _, err := parseZhipu([]byte(`{"data":{"limits":[]}}`), fixedNow); err == nil {
		t.Fatal("parseZhipu: want error for empty limits, got nil")
	}
}

// TestParseZhipu_timeLimitOnlyProducesSnapshot covers issue #138: a Z.AI
// response that contains only TIME_LIMIT (no TOKENS_LIMIT) must still
// produce a usable snapshot — the monthly limit alone is enough data, and
// returning an error would make the poller hold the prior snapshot in a
// place where the user can no longer see when the monthly window resets.
func TestParseZhipu_timeLimitOnlyProducesSnapshot(t *testing.T) {
	body := []byte(`{
		"data": {
			"limits": [
				{ "type": "TIME_LIMIT", "percentage": 22, "nextResetTime": 1783536777977 }
			]
		}
	}`)

	snap, err := parseZhipu(body, fixedNow)
	if err != nil {
		t.Fatalf("parseZhipu: TIME_LIMIT-only response should not error, got %v", err)
	}
	if !snap.HasData() {
		t.Fatal("HasData() = false; want true (TIME_LIMIT populated Unified7d*)")
	}
	if snap.Unified5hUtilization != nil || snap.Unified5hReset != nil {
		t.Errorf("5h fields populated for TIME_LIMIT-only response: util=%v reset=%v",
			snap.Unified5hUtilization, snap.Unified5hReset)
	}
	wantFloatPtr(t, "Unified7dUtilization", snap.Unified7dUtilization, 0.22)
	wantTimePtr(t, "Unified7dReset", snap.Unified7dReset, 1783536777977)
}

// TestParseZhipu_exhausted5hWithHealthyMonthly covers the user-visible bug
// from issue #138: a real 5h-exhausted account with a healthy monthly
// limit must surface the monthly reset in Unified7dReset, not lose it. The
// upstream nextResetTime for an exhausted TOKENS_LIMIT can be in the past;
// the 5h reset field will already be non-nil but stale, and that's the
// parser's faithful representation of what Z.AI returned.
func TestParseZhipu_exhausted5hWithHealthyMonthly(t *testing.T) {
	body := []byte(`{
		"data": {
			"limits": [
				{ "type": "TOKENS_LIMIT", "percentage": 100, "nextResetTime": 1750000000000 },
				{ "type": "TIME_LIMIT",   "percentage": 16,  "nextResetTime": 1783536777977 }
			]
		}
	}`)

	snap, err := parseZhipu(body, fixedNow)
	if err != nil {
		t.Fatalf("parseZhipu: %v", err)
	}
	wantFloatPtr(t, "Unified5hUtilization", snap.Unified5hUtilization, 1.0)
	wantFloatPtr(t, "Unified7dUtilization", snap.Unified7dUtilization, 0.16)
	wantTimePtr(t, "Unified7dReset", snap.Unified7dReset, 1783536777977)
	if snap.Unified7dReset == nil {
		t.Fatal("Unified7dReset: nil; the monthly reset must survive even when 5h is exhausted")
	}
}

// TestParseZhipu_unknownLimitTypeFallsIntoLongWindow is the defensive
// fallback for issue #138: if Z.AI ever ships a new limit type string
// (e.g. MONTHLY_LIMIT, MONTH_LIMIT), it should land in the long-window
// snapshot slot rather than be silently dropped, so the table still
// shows monthly data when the upstream changes.
func TestParseZhipu_unknownLimitTypeFallsIntoLongWindow(t *testing.T) {
	body := []byte(`{
		"data": {
			"limits": [
				{ "type": "TOKENS_LIMIT", "percentage": 5, "nextResetTime": 1781418024826 },
				{ "type": "MONTHLY_LIMIT", "percentage": 30, "nextResetTime": 1783536777977 }
			]
		}
	}`)

	snap, err := parseZhipu(body, fixedNow)
	if err != nil {
		t.Fatalf("parseZhipu: %v", err)
	}
	wantFloatPtr(t, "Unified5hUtilization", snap.Unified5hUtilization, 0.05)
	wantFloatPtr(t, "Unified7dUtilization", snap.Unified7dUtilization, 0.30)
	wantTimePtr(t, "Unified7dReset", snap.Unified7dReset, 1783536777977)
}

func TestParseMinimaxi_remainingInvertedToUsed(t *testing.T) {
	// MiniMaxi reports *remaining* percentages (100 = full quota), so the
	// parser must invert them to utilization (used).
	body := []byte(`{
		"model_remains": [
			{
				"model_name": "general",
				"current_interval_remaining_percent": 91,
				"current_weekly_remaining_percent": 90,
				"end_time": 1781402400000,
				"weekly_end_time": 1781452800000
			}
		]
	}`)

	snap, err := parseMinimaxi(body, fixedNow)
	if err != nil {
		t.Fatalf("parseMinimaxi: %v", err)
	}
	// 100 - 91 = 9 -> 0.09; 100 - 90 = 10 -> 0.10.
	wantFloatPtr(t, "Unified5hUtilization", snap.Unified5hUtilization, 0.09)
	wantFloatPtr(t, "Unified7dUtilization", snap.Unified7dUtilization, 0.10)
	wantTimePtr(t, "Unified5hReset", snap.Unified5hReset, 1781402400000)
	wantTimePtr(t, "Unified7dReset", snap.Unified7dReset, 1781452800000)
}

func TestParseMinimaxi_emptyRemainsIsError(t *testing.T) {
	if _, err := parseMinimaxi([]byte(`{"model_remains":[]}`), fixedNow); err == nil {
		t.Fatal("parseMinimaxi: want error for empty model_remains, got nil")
	}
}

func TestParseMinimaxi_absentRemainingPercentNotFabricated(t *testing.T) {
	// An entry that carries the weekly window but omits
	// current_interval_remaining_percent must NOT fabricate a 5h
	// utilization=1.0 window (issue #207): a plain float64 would read the
	// absent field as 0 → (100-0)/100 = 1.0 and park a healthy member.
	body := []byte(`{
		"model_remains": [
			{
				"model_name": "general",
				"current_weekly_remaining_percent": 90,
				"weekly_end_time": 1781452800000
			}
		]
	}`)
	snap, err := parseMinimaxi(body, fixedNow)
	if err != nil {
		t.Fatalf("parseMinimaxi: %v", err)
	}
	if snap.Unified5hUtilization != nil {
		t.Fatalf("Unified5hUtilization = %v; want nil (interval field absent)", *snap.Unified5hUtilization)
	}
	wantFloatPtr(t, "Unified7dUtilization", snap.Unified7dUtilization, 0.10)

	// An entry with no remaining-percent fields at all is unreadable and must
	// error, so store.Merge keeps the prior good snapshot.
	if _, err := parseMinimaxi([]byte(`{"model_remains":[{"model_name":"general"}]}`), fixedNow); err == nil {
		t.Fatal("parseMinimaxi: want error when no remaining-percent fields present, got nil")
	}
}

func TestParseVolcengine_sessionWeeklyMapped(t *testing.T) {
	// session → 5h window, weekly → 7d window. ResetTimestamp is epoch
	// seconds (not milliseconds). monthly is ignored.
	// Use percentages that are exact in float64 (multiples of 0.25) to
	// avoid rounding noise in the /100 division.
	body := []byte(`{
		"Result": {
			"Status": "Running",
			"QuotaUsage": [
				{ "Level": "session", "Percent": 25,   "ResetTimestamp": 1781484774 },
				{ "Level": "weekly",  "Percent": 50,   "ResetTimestamp": 1782057600 },
				{ "Level": "monthly", "Percent": 61.2, "ResetTimestamp": 1783007999 }
			]
		}
	}`)

	snap, err := parseVolcengine(body, fixedNow)
	if err != nil {
		t.Fatalf("parseVolcengine: %v", err)
	}
	wantFloatPtr(t, "Unified5hUtilization", snap.Unified5hUtilization, 0.25)
	wantFloatPtr(t, "Unified7dUtilization", snap.Unified7dUtilization, 0.50)
	// ResetTimestamp is epoch seconds; verify by Unix(), not UnixMilli().
	if snap.Unified5hReset == nil || snap.Unified5hReset.Unix() != 1781484774 {
		t.Errorf("Unified5hReset = %v, want Unix %d", snap.Unified5hReset, int64(1781484774))
	}
	if snap.Unified7dReset == nil || snap.Unified7dReset.Unix() != 1782057600 {
		t.Errorf("Unified7dReset = %v, want Unix %d", snap.Unified7dReset, int64(1782057600))
	}
	if !snap.AsOf.Equal(fixedNow) {
		t.Errorf("AsOf = %v, want %v", snap.AsOf, fixedNow)
	}
}

func TestParseVolcengine_unknownLevelSkipped(t *testing.T) {
	// monthly-only response: no recognised levels → error, not empty snapshot.
	body := []byte(`{"Result":{"QuotaUsage":[{"Level":"monthly","Percent":50,"ResetTimestamp":1783007999}]}}`)
	_, err := parseVolcengine(body, fixedNow)
	if err == nil {
		t.Fatal("parseVolcengine: want error when no recognised levels, got nil")
	}
}

func TestParseVolcengine_emptyUsageIsError(t *testing.T) {
	if _, err := parseVolcengine([]byte(`{"Result":{"QuotaUsage":[]}}`), fixedNow); err == nil {
		t.Fatal("parseVolcengine: want error for empty QuotaUsage, got nil")
	}
}

func TestVolcengineSign_missingAccessKey(t *testing.T) {
	t.Setenv("VOLC_ACCESSKEY", "")
	t.Setenv("VOLC_SECRETKEY", "test-sk")
	req, _ := http.NewRequest(http.MethodPost, "https://open.volcengineapi.com/?Action=GetCodingPlanUsage&Version=2024-01-01", strings.NewReader("{}"))
	if err := volcengineSign(req, ""); err == nil {
		t.Fatal("volcengineSign: want error when VOLC_ACCESSKEY absent, got nil")
	}
}

func TestVolcengineSign_missingSecretKey(t *testing.T) {
	t.Setenv("VOLC_ACCESSKEY", "test-ak")
	t.Setenv("VOLC_SECRETKEY", "")
	req, _ := http.NewRequest(http.MethodPost, "https://open.volcengineapi.com/?Action=GetCodingPlanUsage&Version=2024-01-01", strings.NewReader("{}"))
	if err := volcengineSign(req, ""); err == nil {
		t.Fatal("volcengineSign: want error when VOLC_SECRETKEY absent, got nil")
	}
}

func TestVolcengineSign_setsRequiredHeaders(t *testing.T) {
	t.Setenv("VOLC_ACCESSKEY", "test-ak")
	t.Setenv("VOLC_SECRETKEY", "test-sk")
	req, _ := http.NewRequest(http.MethodPost, "https://open.volcengineapi.com/?Action=GetCodingPlanUsage&Version=2024-01-01", strings.NewReader("{}"))
	if err := volcengineSign(req, ""); err != nil {
		t.Fatalf("volcengineSign: %v", err)
	}
	if req.Header.Get("X-Date") == "" {
		t.Error("X-Date header absent after volcengineSign")
	}
	if req.Header.Get("Authorization") == "" {
		t.Error("Authorization header absent after volcengineSign")
	}
	if !strings.HasPrefix(req.Header.Get("Authorization"), "HMAC-SHA256 ") {
		t.Errorf("Authorization = %q, want HMAC-SHA256 prefix", req.Header.Get("Authorization"))
	}
	if req.Header.Get("X-Content-Sha256") == "" {
		t.Error("X-Content-Sha256 header absent after volcengineSign")
	}
}

func TestMsToTime_nonPositiveIsNil(t *testing.T) {
	if got := msToTime(0); got != nil {
		t.Errorf("msToTime(0) = %v, want nil", got)
	}
	if got := msToTime(-5); got != nil {
		t.Errorf("msToTime(-5) = %v, want nil", got)
	}
	if got := msToTime(1781402400000); got == nil {
		t.Error("msToTime(positive) = nil, want time")
	}
}

func TestSecToTime_nonPositiveIsNil(t *testing.T) {
	if got := secToTime(0); got != nil {
		t.Errorf("secToTime(0) = %v, want nil", got)
	}
	if got := secToTime(-1); got != nil {
		t.Errorf("secToTime(-1) = %v, want nil", got)
	}
	if got := secToTime(1781484774); got == nil || got.Unix() != 1781484774 {
		t.Errorf("secToTime(1781484774) = %v, want Unix 1781484774", got)
	}
}

func TestProviderFor(t *testing.T) {
	cases := []struct {
		baseURL  string
		wantName string
		wantOK   bool
	}{
		{"https://api.z.ai/api/anthropic", "z.ai/zhipu", true},
		{"https://open.bigmodel.cn/api/anthropic", "z.ai/zhipu", true},
		{"https://API.Z.AI/v1", "z.ai/zhipu", true}, // case-insensitive
		{"https://api.minimaxi.com/v1", "minimaxi", true},
		{"https://ark.cn-beijing.volces.com/api/v3", "volcengine-ark", true},
		{"https://api.anthropic.com", "", false},
	}
	for _, c := range cases {
		prov, ok := providerFor(c.baseURL)
		if ok != c.wantOK {
			t.Errorf("providerFor(%q) ok = %v, want %v", c.baseURL, ok, c.wantOK)
			continue
		}
		if ok && prov.name != c.wantName {
			t.Errorf("providerFor(%q) name = %q, want %q", c.baseURL, prov.name, c.wantName)
		}
	}
}

func TestHostURL_keepsHostReplacesPath(t *testing.T) {
	build := hostURL("/api/monitor/usage/quota/limit")
	got, err := build("https://api.z.ai/api/anthropic/v1/messages")
	if err != nil {
		t.Fatalf("hostURL: %v", err)
	}
	want := "https://api.z.ai/api/monitor/usage/quota/limit"
	if got != want {
		t.Errorf("hostURL = %q, want %q", got, want)
	}
}

func TestAuthSchemes(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if err := rawAuth(req, "zkey"); err != nil {
		t.Fatalf("rawAuth: %v", err)
	}
	if got := req.Header.Get("Authorization"); got != "zkey" {
		t.Errorf("rawAuth Authorization = %q, want %q", got, "zkey")
	}

	req2, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if err := bearerAuth(req2, "mkey"); err != nil {
		t.Fatalf("bearerAuth: %v", err)
	}
	if got := req2.Header.Get("Authorization"); got != "Bearer mkey" {
		t.Errorf("bearerAuth Authorization = %q, want %q", got, "Bearer mkey")
	}
}

// stubCurrent builds a CurrentFunc backed by a map of pool -> backend.
func stubCurrent(m map[string]backend.Backend) CurrentFunc {
	return func(pool string) (backend.Backend, bool) {
		b, ok := m[pool]
		return b, ok
	}
}

// TestPollAll_dynamicPoolSet proves NewDynamic re-reads its pool set on every
// tick: a pool absent when the poller was built (as a runtime-created pool is)
// is skipped, then polled on the next tick once the supplier reports it — the
// fix for the frozen poolNames snapshot in issue #202.
func TestPollAll_dynamicPoolSet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":25,"nextResetTime":1781418024826}
		]}}`)
	}))
	defer srv.Close()

	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	store := quota.NewStore()

	// Supplier starts empty, then gains "chn" — modelling AddPool after startup.
	var names []string
	p := NewDynamic(func() []string { return names },
		stubCurrent(map[string]backend.Backend{"chn": b}),
		nil, store, srv.Client(), time.Hour, func() time.Time { return fixedNow }, io.Discard)
	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)

	// First tick: the pool does not exist yet, so nothing is polled.
	p.pollAll(context.Background())
	if store.Get(b.QuotaKey()).HasData() {
		t.Fatal("store populated before the pool appeared in the supplier")
	}

	// The pool appears at runtime; the next tick must pick it up.
	names = []string{"chn"}
	p.pollAll(context.Background())
	wantFloatPtr(t, "Unified5hUtilization", store.Get(b.QuotaKey()).Unified5hUtilization, 0.25)
}

func TestPollAll_zaiPopulatesStoreWithCorrectAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/api/monitor/usage/quota/limit" {
			t.Errorf("path = %q, want the quota path", r.URL.Path)
		}
		fmt.Fprint(w, `{"data":{"limits":[
			{"type":"TOKENS_LIMIT","percentage":1,"nextResetTime":1781418024826},
			{"type":"TIME_LIMIT","percentage":16,"nextResetTime":1783536777977}
		]}}`)
	}))
	defer srv.Close()

	// The matcher keys on api.z.ai; the test server lives on 127.0.0.1, so
	// poll against the real server URL while registering a provider that
	// recognises it. We do that by appending a test-only provider that
	// matches the httptest host and reuses the Zhipu builders.
	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	store := quota.NewStore()
	p := New([]string{"chn"}, stubCurrent(map[string]backend.Backend{"chn": b}), nil, store, srv.Client(), time.Hour, func() time.Time { return fixedNow }, io.Discard)

	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)

	p.pollAll(context.Background())

	snap := store.Get(b.QuotaKey())
	wantFloatPtr(t, "Unified5hUtilization", snap.Unified5hUtilization, 0.01)
	wantFloatPtr(t, "Unified7dUtilization", snap.Unified7dUtilization, 0.16)
	if gotAuth != "zkey" {
		t.Errorf("upstream Authorization = %q, want raw %q", gotAuth, "zkey")
	}
}

func TestPollAll_skipsUntrackedBackend(t *testing.T) {
	var hit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		fmt.Fprint(w, `{}`)
	}))
	defer srv.Close()

	// Anthropic-shaped backend: no provider matches its BaseURL, so the
	// poller must not touch it. (No test provider registered.)
	b := backend.Backend{Pool: "us", Nick: "acct-a", Credential: "sk-ant", BaseURL: "https://api.anthropic.com"}
	store := quota.NewStore()
	p := New([]string{"us"}, stubCurrent(map[string]backend.Backend{"us": b}), nil, store, srv.Client(), time.Hour, func() time.Time { return fixedNow }, io.Discard)

	p.pollAll(context.Background())

	if hit {
		t.Error("poller hit an upstream for an untracked backend")
	}
	if store.Get(b.QuotaKey()).HasData() {
		t.Error("store populated for an untracked backend")
	}
}

func TestPollAll_non200LeavesPriorSnapshot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	store := quota.NewStore()
	// Seed a prior good snapshot.
	prior := 0.42
	store.Put(b.QuotaKey(), quota.Snapshot{Unified5hUtilization: &prior, AsOf: fixedNow})

	p := New([]string{"chn"}, stubCurrent(map[string]backend.Backend{"chn": b}), nil, store, srv.Client(), time.Hour, func() time.Time { return fixedNow }, io.Discard)
	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)

	p.pollAll(context.Background())

	wantFloatPtr(t, "Unified5hUtilization", store.Get(b.QuotaKey()).Unified5hUtilization, 0.42)
}

func TestPollAll_logsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	var logBuf strings.Builder
	p := New([]string{"chn"}, stubCurrent(map[string]backend.Backend{"chn": b}), nil, quota.NewStore(), srv.Client(), time.Hour, func() time.Time { return fixedNow }, &logBuf)
	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)

	p.pollAll(context.Background())

	if !strings.Contains(logBuf.String(), "poll failed") {
		t.Errorf("log = %q, want a 'poll failed' line", logBuf.String())
	}
}

func TestPollAll_volcenginePopulatesStore(t *testing.T) {
	var gotMethod, gotXDate, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotXDate = r.Header.Get("X-Date")
		gotAuth = r.Header.Get("Authorization")
		fmt.Fprint(w, `{"Result":{"Status":"Running","QuotaUsage":[
			{"Level":"session","Percent":25,"ResetTimestamp":1781484774},
			{"Level":"weekly","Percent":50,"ResetTimestamp":1782057600}
		]}}`)
	}))
	defer srv.Close()

	t.Setenv("VOLC_ACCESSKEY", "test-ak")
	t.Setenv("VOLC_SECRETKEY", "test-sk")

	b := backend.Backend{Pool: "ark", Nick: "key-a", Credential: "", BaseURL: srv.URL}
	store := quota.NewStore()
	p := New([]string{"ark"}, stubCurrent(map[string]backend.Backend{"ark": b}), nil, store, srv.Client(), time.Hour, func() time.Time { return fixedNow }, io.Discard)

	// Register a test provider matching the httptest server with POST + volcengine sign.
	orig := providers
	providers = append([]provider{{
		name:     "test-volc",
		matches:  func(u string) bool { return strings.Contains(u, srv.URL) },
		quotaURL: fixedURL(srv.URL),
		sign:     volcengineSign,
		method:   http.MethodPost,
		body:     []byte("{}"),
		parse:    parseVolcengine,
	}}, providers...)
	t.Cleanup(func() { providers = orig })

	p.pollAll(context.Background())

	snap := store.Get(b.QuotaKey())
	if !snap.HasData() {
		t.Fatal("store not populated for volcengine backend")
	}
	wantFloatPtr(t, "Unified5hUtilization", snap.Unified5hUtilization, 0.25)
	wantFloatPtr(t, "Unified7dUtilization", snap.Unified7dUtilization, 0.50)
	if gotMethod != http.MethodPost {
		t.Errorf("upstream method = %q, want POST", gotMethod)
	}
	if gotXDate == "" {
		t.Error("X-Date header absent on volcengine poll")
	}
	if !strings.HasPrefix(gotAuth, "HMAC-SHA256 ") {
		t.Errorf("Authorization = %q, want HMAC-SHA256 prefix", gotAuth)
	}
}

func TestRun_pollsImmediatelyThenStopsOnContextCancel(t *testing.T) {
	var mu sync.Mutex
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		fmt.Fprint(w, `{"data":{"limits":[{"type":"TOKENS_LIMIT","percentage":5,"nextResetTime":1781418024826}]}}`)
	}))
	defer srv.Close()

	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	store := quota.NewStore()
	// Long interval so only the immediate startup pass runs within the test.
	p := New([]string{"chn"}, stubCurrent(map[string]backend.Backend{"chn": b}), nil, store, srv.Client(), time.Hour, func() time.Time { return fixedNow }, io.Discard)
	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	// The immediate first pass should populate the store quickly.
	deadline := time.After(2 * time.Second)
	for {
		if store.Get(b.QuotaKey()).HasData() {
			break
		}
		select {
		case <-deadline:
			t.Fatal("store not populated by the immediate startup poll")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	if calls < 1 {
		t.Errorf("calls = %d, want at least the immediate startup poll", calls)
	}
}

// TestPoller_marksLocalSnapshot proves that a successful poll calls the
// MarkLocalSnapshot callback so the originating pool's controller can
// stop suppressing cross-pool snapshot sharing for that nick (issue
// #111). The callback records the (poolName, nick) pair; the test
// asserts both the invocation and the argument values.
func TestPoller_marksLocalSnapshot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"limits":[{"type":"TOKENS_LIMIT","percentage":5,"nextResetTime":1781418024826}]}}`)
	}))
	defer srv.Close()

	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	store := quota.NewStore()

	var (
		mu          sync.Mutex
		calls       int
		gotPool     string
		gotNick     string
		gotAfterPut bool
	)
	markLocal := func(poolName, nick string) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		gotPool = poolName
		gotNick = nick
		// markLocal must run AFTER store.Put: the contract is "the store
		// already has the snapshot, now lift the suppression". Probing
		// the store here is a fast white-box check of that ordering.
		gotAfterPut = store.Get(b.QuotaKey()).HasData()
	}

	p := New([]string{"chn"}, stubCurrent(map[string]backend.Backend{"chn": b}), markLocal, store, srv.Client(), time.Hour, func() time.Time { return fixedNow }, io.Discard)
	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	// Wait for the immediate startup poll to land and propagate the
	// markLocal callback.
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		count := calls
		mu.Unlock()
		if count >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("markLocal not called within deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	if gotPool != "chn" {
		t.Errorf("markLocal poolName=%q, want chn", gotPool)
	}
	if gotNick != "key-a" {
		t.Errorf("markLocal nick=%q, want key-a", gotNick)
	}
	if !gotAfterPut {
		t.Error("markLocal ran before store.Put had data (ordering regression)")
	}
}

// TestPoller_partialPollPreservesPriorSnapshot is the #167 writer-layer
// regression for #163: a poll that returns only one window (here TOKENS_LIMIT,
// the short/5h slot, with no TIME_LIMIT) must update that window while leaving
// the previously-learned long-window (7d/monthly) reset intact. This drives
// the real Run/pollAll path — if pollAll reverts p.store.Merge to
// p.store.Put, the seeded 7d reset blanks and this test fails. The Store-level
// tests in internal/quota only cover Store.Merge in isolation.
func TestPoller_partialPollPreservesPriorSnapshot(t *testing.T) {
	// Poll response carries ONLY TOKENS_LIMIT (5h), no TIME_LIMIT (7d).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"limits":[{"type":"TOKENS_LIMIT","percentage":7,"nextResetTime":1781418024826}]}}`)
	}))
	defer srv.Close()

	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	store := quota.NewStore()

	// Seed both windows under the nick's quota key, as a full earlier poll
	// would have. The 7d reset is the field that must survive the partial poll.
	priorReset5h := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	reset7d := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	util7d := 0.16
	store.Put(b.QuotaKey(), quota.Snapshot{
		Unified5hReset:       &priorReset5h,
		Unified7dReset:       &reset7d,
		Unified7dUtilization: &util7d,
		AsOf:                 fixedNow,
	})

	// markLocal runs after the merge (see TestPoller_marksLocalSnapshot), so
	// it is a reliable signal that the poll has landed in the store.
	var (
		mu     sync.Mutex
		polled bool
	)
	markLocal := func(poolName, nick string) {
		mu.Lock()
		defer mu.Unlock()
		polled = true
	}

	p := New([]string{"chn"}, stubCurrent(map[string]backend.Backend{"chn": b}), markLocal, store, srv.Client(), time.Hour, func() time.Time { return fixedNow }, io.Discard)
	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		ok := polled
		mu.Unlock()
		if ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("poll did not land within deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}

	got := store.Get(b.QuotaKey())
	// Present window updated to the polled reset.
	wantReset5h := msToTime(1781418024826)
	if got.Unified5hReset == nil || !got.Unified5hReset.Equal(*wantReset5h) {
		t.Errorf("after partial poll, Unified5hReset = %v, want %v (polled window must update)", got.Unified5hReset, wantReset5h)
	}
	// Absent window's learned reset must survive.
	if got.Unified7dReset == nil || !got.Unified7dReset.Equal(reset7d) {
		t.Errorf("after partial poll, Unified7dReset = %v, want %v (absent window's learned reset must survive)", got.Unified7dReset, reset7d)
	}
	if got.Unified7dUtilization == nil || *got.Unified7dUtilization != util7d {
		t.Errorf("after partial poll, Unified7dUtilization = %v, want %v (absent window's utilization must survive)", got.Unified7dUtilization, util7d)
	}
}

// withTestProvider appends a provider that matches a literal host fragment
// (the httptest server URL) and restores the registry when the test ends.
// httptest hosts are 127.0.0.1:port, which no real provider matcher would
// recognise, so this lets the poll-loop tests exercise the full path
// against a local server.
func withTestProvider(t *testing.T, matchFragment string, quotaURL func(string) (string, error), sign func(*http.Request, string) error, parse func([]byte, time.Time) (quota.Snapshot, error)) {
	t.Helper()
	orig := providers
	providers = append([]provider{{
		name:     "test",
		matches:  func(u string) bool { return strings.Contains(u, matchFragment) },
		quotaURL: quotaURL,
		sign:     sign,
		parse:    parse,
	}}, providers...)
	t.Cleanup(func() { providers = orig })
}

// recordingClock is a controllable clock for AC9's "advance now past the
// staleness threshold" tests. Set+advance it directly from each test;
// the poller reads it via its `now` field, so Status() / HealthSummary()
// derive staleness against the value at read time.
type recordingClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *recordingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *recordingClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// neverPolledFixture sets up a poller whose current() returns a tracked
// backend but pollAll has never run. The poller is wired against a
// controllable clock so the test can drive time.
func neverPolledFixture(t *testing.T, clk *recordingClock) *Poller {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{}`)
	}))
	t.Cleanup(srv.Close)
	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)
	return New([]string{"chn"}, stubCurrent(map[string]backend.Backend{"chn": b}), nil, quota.NewStore(), srv.Client(), time.Hour, clk.Now, io.Discard)
}

// TestPoller_liveness_neverPolled covers the boot-time / never-ticked case
// (issue #247): the pool is tracked, the backend matches a provider, but
// no poll has ever succeeded. Status reports the absence (no LastSuccess,
// stale=true). HealthSummary reports the pool as stale.
func TestPoller_liveness_neverPolled(t *testing.T) {
	clk := &recordingClock{now: fixedNow}
	p := neverPolledFixture(t, clk)

	if p.IsTracked("chn") {
		t.Error("IsTracked(chn) = true before any poll; want false (absent-from-state)")
	}

	statuses := p.Status()
	if _, ok := statuses["chn"]; ok {
		t.Errorf("Status() contains 'chn' before any poll; want absent entry")
	}

	// Even with the clock at boot-time, the pool reads stale — there is
	// no fresh signal. HealthSummary reports it as stale.
	polled, stale := p.HealthSummary()
	if polled != 0 || stale != 0 {
		t.Errorf("HealthSummary() = (polled=%d, stale=%d); want (0,0) — empty state is not 'polled'", polled, stale)
	}
}

// TestPoller_liveness_healthyAfterSuccess drives a successful poll and
// verifies the per-pool state: LastSuccess set, no error, no consecutive
// failures, Stale=false (the clock is at fixedNow so the threshold has
// not elapsed).
func TestPoller_liveness_healthyAfterSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"limits":[{"type":"TOKENS_LIMIT","percentage":10,"nextResetTime":1}]}}`)
	}))
	defer srv.Close()
	clk := &recordingClock{now: fixedNow}
	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)
	p := New([]string{"chn"}, stubCurrent(map[string]backend.Backend{"chn": b}), nil, quota.NewStore(), srv.Client(), time.Hour, clk.Now, io.Discard)

	p.pollAll(context.Background())

	if !p.IsTracked("chn") {
		t.Fatal("IsTracked(chn) = false after a successful poll; want true")
	}
	statuses := p.Status()
	ps, ok := statuses["chn"]
	if !ok {
		t.Fatal("Status() missing 'chn' after a successful poll")
	}
	if ps.LastSuccess == nil {
		t.Error("LastSuccess = nil; want set")
	} else if !ps.LastSuccess.Equal(fixedNow) {
		t.Errorf("LastSuccess = %v; want %v", *ps.LastSuccess, fixedNow)
	}
	if ps.LastError != "" {
		t.Errorf("LastError = %q; want empty", ps.LastError)
	}
	if ps.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d; want 0", ps.ConsecutiveFailures)
	}
	if ps.Stale {
		t.Error("Stale = true immediately after success; want false (threshold not elapsed)")
	}
	_, stale := p.HealthSummary()
	if stale != 0 {
		t.Errorf("HealthSummary() stale = %d; want 0", stale)
	}
}

// TestPoller_liveness_polledThenFailing covers the issue's central case:
// a poll succeeded once and is now failing. The per-pool record carries
// the error and the consecutive-failure count; advancing the clock past
// the threshold flips the aggregate to "stale".
func TestPoller_liveness_polledThenFailing(t *testing.T) {
	clk := &recordingClock{now: fixedNow}
	var failNext atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failNext.Load() {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, `{"data":{"limits":[{"type":"TOKENS_LIMIT","percentage":10,"nextResetTime":1}]}}`)
	}))
	defer srv.Close()
	b := backend.Backend{Pool: "chn", Nick: "key-a", Credential: "zkey", BaseURL: srv.URL}
	withTestProvider(t, srv.URL, hostURL("/api/monitor/usage/quota/limit"), rawAuth, parseZhipu)
	p := New([]string{"chn"}, stubCurrent(map[string]backend.Backend{"chn": b}), nil, quota.NewStore(), srv.Client(), time.Hour, clk.Now, io.Discard)

	// One successful poll to seed LastSuccess.
	p.pollAll(context.Background())
	if !p.IsTracked("chn") || p.Status()["chn"].Stale {
		t.Fatal("setup: expected healthy after one successful poll")
	}

	// Now flip the server to failing and drive one failure.
	clk.Set(clk.Now().Add(30 * time.Second))
	failNext.Store(true)
	p.pollAll(context.Background())

	ps := p.Status()["chn"]
	if ps.LastError == "" {
		t.Error("LastError = empty after a failure; want set")
	}
	if ps.LastErrorAt == nil {
		t.Error("LastErrorAt = nil after a failure; want set")
	}
	if ps.ConsecutiveFailures != 1 {
		t.Errorf("ConsecutiveFailures = %d; want 1", ps.ConsecutiveFailures)
	}
	if ps.LastSuccess == nil {
		t.Error("LastSuccess = nil after success-then-fail; want preserved")
	}
	// One failure inside the threshold does not yet flip to stale.
	if ps.Stale {
		t.Error("Stale = true one failure after threshold; want false (still inside threshold)")
	}

	// Advance the clock past the threshold and read again — without
	// driving more polls. The stale verdict is computed at read time.
	clk.Set(clk.Now().Add(3 * time.Hour)) // > StaleAfterIntervals × 1h
	ps = p.Status()["chn"]
	if !ps.Stale {
		t.Error("Stale = false after threshold elapsed since last success; want true")
	}
	polled, stale := p.HealthSummary()
	if polled != 1 || stale != 1 {
		t.Errorf("HealthSummary() = (polled=%d, stale=%d); want (1, 1)", polled, stale)
	}
}

// TestPoller_liveness_omittedForUntrackedBackend covers the AC2 "omitted
// entirely / no delta" requirement: a poller with no recorded state for
// a pool must report IsTracked=false so the admin handler omits the
// `poller` key for Anthropic / untracked backends.
func TestPoller_liveness_omittedForUntrackedBackend(t *testing.T) {
	clk := &recordingClock{now: fixedNow}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	// Anthropic-shaped backend: no provider matches its BaseURL, so
	// pollAll will skip it (no state entry will be created).
	b := backend.Backend{Pool: "us", Nick: "acct-a", Credential: "sk-ant", BaseURL: "https://api.anthropic.com"}
	p := New([]string{"us"}, stubCurrent(map[string]backend.Backend{"us": b}), nil, quota.NewStore(), srv.Client(), time.Hour, clk.Now, io.Discard)

	p.pollAll(context.Background())

	if p.IsTracked("us") {
		t.Error("IsTracked(us) = true for an untracked backend; want false (handler omits the field)")
	}
	if _, ok := p.Status()["us"]; ok {
		t.Error("Status() contains 'us' for an untracked backend; want absent")
	}
}
