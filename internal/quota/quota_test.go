package quota

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

// makeResp builds a minimal *http.Response carrying the given headers,
// suitable for handing to Extract. Body/status are irrelevant — only
// the header set is read.
func makeResp(headers map[string]string) *http.Response {
	h := make(http.Header, len(headers))
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{Header: h}
}

func TestExtract_fullHeaderSet(t *testing.T) {
	// A real subscription-token response: both rolling windows plus the
	// top-level decision and overage fields. Reset headers are Unix
	// seconds; utilization is a 0..1 fraction.
	resp := makeResp(map[string]string{
		HeaderUnifiedStatus:                "allowed",
		HeaderUnifiedReset:                 "1781352600",
		HeaderUnifiedRepresentativeClaim:   "five_hour",
		HeaderUnified5hStatus:              "allowed",
		HeaderUnified5hUtilization:         "0.25",
		HeaderUnified5hReset:               "1781352600",
		HeaderUnified7dStatus:              "allowed",
		HeaderUnified7dUtilization:         "0.07",
		HeaderUnified7dReset:               "1781445600",
		HeaderUnifiedFallbackPercentage:    "0.5",
		HeaderUnifiedOverageStatus:         "rejected",
		HeaderUnifiedOverageDisabledReason: "org_level_disabled",
		HeaderOrgID:                        "org_abc123",
	})

	s := Extract(resp)

	wantFloat := func(name string, got *float64, want float64) {
		t.Helper()
		if got == nil {
			t.Fatalf("%s: nil, want %v", name, want)
		}
		if *got != want {
			t.Errorf("%s = %v, want %v", name, *got, want)
		}
	}
	wantStr := func(name, got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}

	wantStr("UnifiedStatus", s.UnifiedStatus, "allowed")
	wantStr("UnifiedRepresentativeClaim", s.UnifiedRepresentativeClaim, "five_hour")
	wantStr("Unified5hStatus", s.Unified5hStatus, "allowed")
	wantStr("Unified7dStatus", s.Unified7dStatus, "allowed")
	wantStr("UnifiedOverageStatus", s.UnifiedOverageStatus, "rejected")
	wantStr("UnifiedOverageDisabledReason", s.UnifiedOverageDisabledReason, "org_level_disabled")

	wantFloat("Unified5hUtilization", s.Unified5hUtilization, 0.25)
	wantFloat("Unified7dUtilization", s.Unified7dUtilization, 0.07)
	wantFloat("UnifiedFallbackPercentage", s.UnifiedFallbackPercentage, 0.5)

	if s.UnifiedReset == nil || !s.UnifiedReset.Equal(time.Unix(1781352600, 0).UTC()) {
		t.Errorf("UnifiedReset = %v, want %v", s.UnifiedReset, time.Unix(1781352600, 0).UTC())
	}
	if s.Unified5hReset == nil || !s.Unified5hReset.Equal(time.Unix(1781352600, 0).UTC()) {
		t.Errorf("Unified5hReset = %v, want %v", s.Unified5hReset, time.Unix(1781352600, 0).UTC())
	}
	if s.Unified7dReset == nil || !s.Unified7dReset.Equal(time.Unix(1781445600, 0).UTC()) {
		t.Errorf("Unified7dReset = %v, want %v", s.Unified7dReset, time.Unix(1781445600, 0).UTC())
	}
	if s.OrgID != "org_abc123" {
		t.Errorf("OrgID = %q, want org_abc123", s.OrgID)
	}
	if s.AsOf.IsZero() {
		t.Error("AsOf was zero; expected gateway-side stamp")
	}
	if !s.HasData() {
		t.Error("HasData() = false on a fully populated snapshot")
	}
}

func TestExtract_zeroUtilizationIsNotMissing(t *testing.T) {
	// A window at 0.0 utilization is full quota, not absent data. The
	// *float64 must be non-nil and equal to 0, distinguishable from a
	// header that was never sent.
	resp := makeResp(map[string]string{
		HeaderUnified5hStatus:      "allowed",
		HeaderUnified5hUtilization: "0",
		HeaderUnified5hReset:       "1781352600",
	})

	s := Extract(resp)

	if s.Unified5hUtilization == nil {
		t.Fatal("Unified5hUtilization = nil, want a pointer to 0.0")
	}
	if *s.Unified5hUtilization != 0 {
		t.Errorf("Unified5hUtilization = %v, want 0", *s.Unified5hUtilization)
	}
	if s.Unified7dUtilization != nil {
		t.Errorf("Unified7dUtilization = %v, want nil (header absent)", *s.Unified7dUtilization)
	}
	if !s.HasData() {
		t.Error("HasData() = false despite a present 5h window")
	}
}

func TestExtract_partialWindows(t *testing.T) {
	// Only the 7d window is present. The extractor must surface it and
	// leave the 5h fields zero/nil — not invent values.
	resp := makeResp(map[string]string{
		HeaderUnified7dStatus:      "allowed",
		HeaderUnified7dUtilization: "0.42",
		HeaderUnified7dReset:       "1781445600",
	})

	s := Extract(resp)

	if s.Unified5hStatus != "" {
		t.Errorf("Unified5hStatus = %q, want empty", s.Unified5hStatus)
	}
	if s.Unified5hUtilization != nil {
		t.Errorf("Unified5hUtilization = %v, want nil", *s.Unified5hUtilization)
	}
	if s.Unified5hReset != nil {
		t.Errorf("Unified5hReset = %v, want nil", s.Unified5hReset)
	}
	if s.Unified7dUtilization == nil || *s.Unified7dUtilization != 0.42 {
		t.Errorf("Unified7dUtilization not preserved: %v", s.Unified7dUtilization)
	}
	if s.OrgID != "" {
		t.Errorf("OrgID = %q, want empty", s.OrgID)
	}
	if !s.HasData() {
		t.Error("HasData() = false despite 7d window present")
	}
}

func TestExtract_noHeaders(t *testing.T) {
	// A response with no Anthropic rate-limit headers at all (an
	// error page, a non-Messages endpoint someone proxied through,
	// etc.) must yield a snapshot with HasData() == false. The
	// gateway uses this to keep an "unknown" backend visible without
	// pretending it has quota information.
	s := Extract(makeResp(nil))

	if s.HasData() {
		t.Error("HasData() = true on an empty response")
	}
	if s.AsOf.IsZero() {
		t.Error("AsOf was zero on empty response")
	}
}

func TestExtract_malformedHeadersIgnored(t *testing.T) {
	// Defensive: a future upstream-bug or middlebox could rewrite a
	// numeric header to a non-number. parseFloat/parseUnixTime must
	// return nil rather than 0/epoch so downstream consumers see
	// "missing" not "untouched at the epoch". A string status field is
	// passed through verbatim — there is nothing to validate.
	resp := makeResp(map[string]string{
		HeaderUnified5hUtilization: "not-a-number",
		HeaderUnified5hReset:       "yesterday",
		HeaderUnified7dUtilization: "",
		HeaderUnifiedReset:         "3.14",
	})

	s := Extract(resp)
	if s.Unified5hUtilization != nil {
		t.Errorf("Unified5hUtilization should be nil on parse failure, got %v", *s.Unified5hUtilization)
	}
	if s.Unified5hReset != nil {
		t.Errorf("Unified5hReset should be nil on parse failure, got %v", s.Unified5hReset)
	}
	if s.Unified7dUtilization != nil {
		t.Errorf("Unified7dUtilization should be nil on empty header, got %v", *s.Unified7dUtilization)
	}
	if s.UnifiedReset != nil {
		t.Errorf("UnifiedReset should be nil on non-integer seconds, got %v", s.UnifiedReset)
	}
}

func TestExtract_nilResponse(t *testing.T) {
	s := Extract(nil)
	if s.HasData() {
		t.Error("HasData() = true on nil response")
	}
}

func TestStore_putGetBackendKeyEnforced(t *testing.T) {
	st := NewStore()
	util := 0.25
	// Caller passes a snapshot without the Backend field — Put must
	// stamp it. This is the contract main.go relies on so Get never
	// returns a snapshot mislabelled with a stale key.
	st.Put("anthropic-prod", Snapshot{Unified5hUtilization: &util, AsOf: time.Unix(1, 0).UTC()})

	got := st.Get("anthropic-prod")
	if got.Backend != "anthropic-prod" {
		t.Errorf("Backend = %q, want anthropic-prod", got.Backend)
	}
	if got.Unified5hUtilization == nil || *got.Unified5hUtilization != 0.25 {
		t.Errorf("Unified5hUtilization lost in round trip: %v", got.Unified5hUtilization)
	}
}

func TestStore_getUnknownReturnsEmptySnapshot(t *testing.T) {
	st := NewStore()
	got := st.Get("never-recorded")
	if got.Backend != "never-recorded" {
		t.Errorf("Backend = %q, want never-recorded", got.Backend)
	}
	if got.HasData() {
		t.Error("HasData() = true on an unrecorded key")
	}
	if got.AsOf.IsZero() {
		t.Error("AsOf was zero on an unrecorded key")
	}
}

func TestStore_concurrentReadWrite(t *testing.T) {
	// Race-detector smoke. With -race this catches an unguarded map
	// write; without -race it still asserts the store doesn't panic
	// under contention. Each writer overwrites a small set of keys
	// while readers hammer the same keys.
	st := NewStore()
	keys := []string{"a", "b", "c", "d"}
	const iterations = 500

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				u := float64(seed*1000+i) / 10000
				st.Put(keys[i%len(keys)], Snapshot{Unified5hUtilization: &u})
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = st.Get(keys[i%len(keys)])
			}
		}()
	}
	wg.Wait()

	// Sanity: after the dust settles, every key we wrote returns the
	// key we asked for. Concrete value is non-deterministic.
	for _, k := range keys {
		if got := st.Get(k); got.Backend != k {
			t.Errorf("Get(%q).Backend = %q", k, got.Backend)
		}
	}
}
