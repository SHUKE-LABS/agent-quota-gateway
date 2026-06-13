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
	resp := makeResp(map[string]string{
		HeaderRequestsLimit:     "1000",
		HeaderRequestsRemaining: "997",
		HeaderRequestsReset:     "2026-06-13T13:45:00Z",
		HeaderTokensLimit:       "80000",
		HeaderTokensRemaining:   "79412",
		HeaderTokensReset:       "2026-06-13T13:45:30Z",
		HeaderOrgID:             "org_abc123",
	})

	s := Extract(resp)

	wantInt := func(t *testing.T, name string, got *int64, want int64) {
		t.Helper()
		if got == nil {
			t.Fatalf("%s: nil, want %d", name, want)
		}
		if *got != want {
			t.Errorf("%s = %d, want %d", name, *got, want)
		}
	}
	wantInt(t, "RequestsLimit", s.RequestsLimit, 1000)
	wantInt(t, "RequestsRemaining", s.RequestsRemaining, 997)
	wantInt(t, "TokensLimit", s.TokensLimit, 80000)
	wantInt(t, "TokensRemaining", s.TokensRemaining, 79412)

	if s.RequestsReset == nil || !s.RequestsReset.Equal(time.Date(2026, 6, 13, 13, 45, 0, 0, time.UTC)) {
		t.Errorf("RequestsReset = %v, want 2026-06-13T13:45:00Z", s.RequestsReset)
	}
	if s.TokensReset == nil || !s.TokensReset.Equal(time.Date(2026, 6, 13, 13, 45, 30, 0, time.UTC)) {
		t.Errorf("TokensReset = %v, want 2026-06-13T13:45:30Z", s.TokensReset)
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

func TestExtract_partialHeaders(t *testing.T) {
	// Tier 1 / probe responses sometimes carry only the token bucket.
	// The extractor must surface what is present and leave the missing
	// fields nil — not invent zeros that would look like "exhausted".
	resp := makeResp(map[string]string{
		HeaderTokensLimit:     "40000",
		HeaderTokensRemaining: "40000",
		HeaderTokensReset:     "2026-06-13T14:00:00Z",
	})

	s := Extract(resp)

	if s.RequestsLimit != nil {
		t.Errorf("RequestsLimit = %d, want nil", *s.RequestsLimit)
	}
	if s.RequestsRemaining != nil {
		t.Errorf("RequestsRemaining = %d, want nil", *s.RequestsRemaining)
	}
	if s.RequestsReset != nil {
		t.Errorf("RequestsReset = %v, want nil", s.RequestsReset)
	}
	if s.TokensLimit == nil || *s.TokensLimit != 40000 {
		t.Errorf("TokensLimit not preserved: %v", s.TokensLimit)
	}
	if s.OrgID != "" {
		t.Errorf("OrgID = %q, want empty", s.OrgID)
	}
	if !s.HasData() {
		t.Error("HasData() = false despite token headers present")
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
	// numeric header to a non-number. parseInt/parseTime must return
	// nil rather than 0/epoch so downstream consumers see "missing"
	// not "exhausted at the epoch".
	resp := makeResp(map[string]string{
		HeaderRequestsLimit:     "not-a-number",
		HeaderRequestsRemaining: "",
		HeaderRequestsReset:     "yesterday",
		HeaderTokensReset:       "2026-13-99T99:99:99Z",
	})

	s := Extract(resp)
	if s.RequestsLimit != nil {
		t.Errorf("RequestsLimit should be nil on parse failure, got %d", *s.RequestsLimit)
	}
	if s.RequestsReset != nil {
		t.Errorf("RequestsReset should be nil on parse failure, got %v", *s.RequestsReset)
	}
	if s.TokensReset != nil {
		t.Errorf("TokensReset should be nil on parse failure, got %v", *s.TokensReset)
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
	one := int64(99)
	// Caller passes a snapshot without the Backend field — Put must
	// stamp it. This is the contract main.go relies on so Get never
	// returns a snapshot mislabelled with a stale key.
	st.Put("anthropic-prod", Snapshot{RequestsRemaining: &one, AsOf: time.Unix(1, 0).UTC()})

	got := st.Get("anthropic-prod")
	if got.Backend != "anthropic-prod" {
		t.Errorf("Backend = %q, want anthropic-prod", got.Backend)
	}
	if got.RequestsRemaining == nil || *got.RequestsRemaining != 99 {
		t.Errorf("RequestsRemaining lost in round trip: %v", got.RequestsRemaining)
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
				n := int64(seed*1000 + i)
				st.Put(keys[i%len(keys)], Snapshot{RequestsRemaining: &n})
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
