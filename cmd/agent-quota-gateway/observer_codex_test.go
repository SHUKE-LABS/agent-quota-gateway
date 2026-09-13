package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// observerResp builds a synthetic upstream response that went through member
// b's resolver context, the way the proxy would hand it to the observer.
func observerResp(b backend.Backend, status int, headers map[string]string) *http.Response {
	ctx := backend.WithBackend(context.Background(), b)
	req := httptest.NewRequest(http.MethodPost, "/responses", nil).WithContext(ctx)
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{
		StatusCode: status,
		Header:     h,
		Request:    req,
		Body:       io.NopCloser(strings.NewReader("{}")),
	}
}

// codexTestPools builds a one-seat codex pool (plus the ambient default-pool
// machinery backend.Load provides) with its own store, ready for
// quotaObserver tests.
func codexTestPools(t *testing.T) (*quota.Store, *auto.Pools, backend.Backend) {
	t.Helper()
	scrubPoolEnv(t)
	t.Setenv("AQG_POOL_CODEX_BASE_URL", "https://chatgpt.com/backend-api/codex")
	t.Setenv("AQG_POOL_CODEX_BACKEND_SEAT1", "chatgpt-oat-seat1")
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	store := quota.NewStore()
	pools := auto.NewPools(registry, store, nil, io.Discard)
	seat1, ok := registry.ResolveIn("codex", "seat1")
	if !ok {
		t.Fatalf("ResolveIn(codex, seat1) not found")
	}
	return store, pools, seat1
}

// TestQuotaObserver_codexWindowsMergedUnderNick: a response from a
// chatgpt.com member files its x-codex-* windows under the nick (QuotaKey),
// poller-style (no status) — the store read behind
// /_gateway/quota?backend=codex (issue #304 AC2).
func TestQuotaObserver_codexWindowsMergedUnderNick(t *testing.T) {
	store, pools, seat1 := codexTestPools(t)

	reset5h := time.Now().Add(90 * time.Minute).UTC().Unix()
	reset7d := time.Now().Add(79 * time.Hour).UTC().Unix()
	obs := quotaObserver(store, pools)
	obs(observerResp(seat1, http.StatusOK, map[string]string{
		"x-codex-primary-used-percent":   "70",
		"x-codex-primary-window-minutes": "300",
		"x-codex-primary-reset-at":       strconv.FormatInt(reset5h, 10),
		"x-codex-secondary-used-percent": "25",
		"x-codex-secondary-reset-at":     strconv.FormatInt(reset7d, 10),
	}))

	snap := store.Get("seat1")
	if snap.Backend != "seat1" {
		t.Fatalf("snapshot keyed as %q, want the nick seat1", snap.Backend)
	}
	if snap.Unified5hUtilization == nil || *snap.Unified5hUtilization != 0.7 {
		t.Errorf("5h utilization=%v, want 0.7", snap.Unified5hUtilization)
	}
	if snap.Unified5hWindowMinutes == nil || *snap.Unified5hWindowMinutes != 300 {
		t.Errorf("5h window minutes=%v, want 300", snap.Unified5hWindowMinutes)
	}
	if snap.Unified5hReset == nil {
		t.Errorf("5h reset missing")
	}
	if snap.Unified7dUtilization == nil || *snap.Unified7dUtilization != 0.25 {
		t.Errorf("7d utilization=%v, want 0.25", snap.Unified7dUtilization)
	}
	if snap.Unified7dReset == nil {
		t.Errorf("7d reset missing")
	}
	if snap.Unified5hStatus != "" || snap.Unified7dStatus != "" {
		t.Errorf("codex snapshot carried a status: %+v", snap)
	}
}

// TestQuotaObserver_nonCodexHostIgnoresCodexHeaders: the same x-codex-*
// headers from a non-chatgpt.com member must not feed the store — the host
// gate the 429 classifier applies holds for the observer too (issue #304
// AC4).
func TestQuotaObserver_nonCodexHostIgnoresCodexHeaders(t *testing.T) {
	scrubPoolEnv(t)
	t.Setenv("AQG_POOL_AUTO_BACKEND_A", "sk-ant-oat-a")
	registry, err := backend.Load("https://api.anthropic.com")
	if err != nil {
		t.Fatalf("backend.Load: %v", err)
	}
	store := quota.NewStore()
	pools := auto.NewPools(registry, store, nil, io.Discard)
	member, ok := registry.ResolveIn("auto", "a")
	if !ok {
		t.Fatalf("ResolveIn(auto, a) not found")
	}

	obs := quotaObserver(store, pools)
	obs(observerResp(member, http.StatusOK, map[string]string{
		"x-codex-primary-used-percent": "70",
		"x-codex-primary-reset-at":     "1900000000",
	}))

	if snap := store.Get("a"); snap.HasData() {
		t.Errorf("non-codex member's snapshot admitted from x-codex-* headers: %+v", snap)
	}
}

// TestQuotaObserver_codexMinutesAloneNotAdmitted: window-minutes is duration
// metadata, not window state — a response carrying only that header files
// nothing (the same admission rule hasQuotaWindow/HasData apply).
func TestQuotaObserver_codexMinutesAloneNotAdmitted(t *testing.T) {
	store, pools, seat1 := codexTestPools(t)

	obs := quotaObserver(store, pools)
	obs(observerResp(seat1, http.StatusOK, map[string]string{
		"x-codex-primary-window-minutes": "300",
	}))

	if snap := store.Get("seat1"); snap.HasData() {
		t.Errorf("minutes-only snapshot admitted: %+v", snap)
	}
}
