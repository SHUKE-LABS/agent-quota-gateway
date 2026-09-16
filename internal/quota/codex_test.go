package quota

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestExtractCodex_fullFamily(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	h := http.Header{}
	h.Set(HeaderCodexPrimaryUsedPercent, "80")
	h.Set(HeaderCodexPrimaryWindowMinutes, "300")
	h.Set(HeaderCodexPrimaryResetAt, "1700003600") // now + 1h
	h.Set(HeaderCodexSecondaryUsedPercent, "100")
	h.Set(HeaderCodexSecondaryWindowMinutes, "10080")
	h.Set(HeaderCodexSecondaryResetAt, "1700288000") // now + 80h

	s := ExtractCodex(h, now)

	if s.Unified5hUtilization == nil || *s.Unified5hUtilization != 0.8 {
		t.Errorf("5h utilization=%v, want 0.8", s.Unified5hUtilization)
	}
	if s.Unified5hReset == nil || !s.Unified5hReset.Equal(now.Add(time.Hour)) {
		t.Errorf("5h reset=%v, want %v", s.Unified5hReset, now.Add(time.Hour))
	}
	if s.Unified5hWindowMinutes == nil || *s.Unified5hWindowMinutes != 300 {
		t.Errorf("5h window minutes=%v, want 300", s.Unified5hWindowMinutes)
	}
	if s.Unified7dUtilization == nil || *s.Unified7dUtilization != 1.0 {
		t.Errorf("7d utilization=%v, want 1.0", s.Unified7dUtilization)
	}
	if s.Unified7dReset == nil || !s.Unified7dReset.Equal(time.Unix(1700288000, 0).UTC()) {
		t.Errorf("7d reset=%v, want 1700288000", s.Unified7dReset)
	}
	if s.Unified7dWindowMinutes == nil || *s.Unified7dWindowMinutes != 10080 {
		t.Errorf("7d window minutes=%v, want 10080", s.Unified7dWindowMinutes)
	}
	// Poller-style: no status anywhere.
	if s.Unified5hStatus != "" || s.Unified7dStatus != "" || s.UnifiedStatus != "" {
		t.Errorf("codex snapshot carried a status: %+v", s)
	}
}

func TestExtractCodex_resetAtPreferredOverRelative(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	h := http.Header{}
	h.Set(HeaderCodexPrimaryResetAt, "1700003600")        // now + 1h
	h.Set(HeaderCodexPrimaryResetAfterSeconds, "9999999") // ignored

	s := ExtractCodex(h, now)
	if s.Unified5hReset == nil || !s.Unified5hReset.Equal(now.Add(time.Hour)) {
		t.Errorf("5h reset=%v, want reset-at value (now+1h), not the relative one", s.Unified5hReset)
	}
}

func TestExtractCodex_relativeResetFallback(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	h := http.Header{}
	h.Set(HeaderCodexSecondaryUsedPercent, "50")
	h.Set(HeaderCodexSecondaryResetAfterSeconds, "5400") // 90 min

	s := ExtractCodex(h, now)
	if s.Unified7dReset == nil || !s.Unified7dReset.Equal(now.Add(90*time.Minute)) {
		t.Errorf("7d reset=%v, want now+90m from reset-after-seconds", s.Unified7dReset)
	}
	if s.Unified5hReset != nil {
		t.Errorf("5h reset=%v, want nil when both headers absent", s.Unified5hReset)
	}
}

func TestExtractCodex_malformedAndAbsent(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	h := http.Header{}
	h.Set(HeaderCodexPrimaryUsedPercent, "not-a-number")
	h.Set(HeaderCodexPrimaryResetAt, "yesterday")
	h.Set(HeaderCodexPrimaryWindowMinutes, "0")
	h.Set(HeaderCodexSecondaryWindowMinutes, "-5")
	h.Set(HeaderCodexSecondaryResetAfterSeconds, "-1")

	s := ExtractCodex(h, now)
	if s.Unified5hUtilization != nil || s.Unified5hReset != nil || s.Unified5hWindowMinutes != nil {
		t.Errorf("malformed primary headers yielded values: %+v", s)
	}
	if s.Unified7dWindowMinutes != nil || s.Unified7dReset != nil {
		t.Errorf("non-positive secondary headers yielded values: %+v", s)
	}
}

func TestOverlayCodex_setsOnlyCarriedFields(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	h := http.Header{}
	h.Set(HeaderCodexPrimaryUsedPercent, "40")
	h.Set(HeaderCodexPrimaryResetAt, "1700003600")

	codex := ExtractCodex(h, now)
	s := Snapshot{AsOf: now, Unified5hStatus: "allowed", Unified7dUtilization: ptrFloat(0.9)}
	// Fresh key: the zero prev must keep today's behavior (issue #311).
	s.OverlayCodex(Snapshot{}, codex)

	if s.Unified5hStatus != "allowed" {
		t.Errorf("overlay clobbered a non-codex field: 5h status=%q", s.Unified5hStatus)
	}
	if s.Unified5hUtilization == nil || *s.Unified5hUtilization != 0.4 {
		t.Errorf("overlay did not set 5h utilization: %v", s.Unified5hUtilization)
	}
	if s.Unified5hReset == nil {
		t.Errorf("overlay did not set 5h reset")
	}
	if s.Unified7dUtilization == nil || *s.Unified7dUtilization != 0.9 {
		t.Errorf("overlay clobbered pre-set 7d utilization: %v", s.Unified7dUtilization)
	}
	if s.Unified5hWindowMinutes != nil || s.Unified7dWindowMinutes != nil {
		t.Errorf("overlay invented window minutes: %+v", s)
	}
}

// TestOverlayCodex_prevCarryAfterFirstResponseGap reproduces defect 1 of
// issue #311: the first observed response omits the 5h primary window, and
// the pre-#311 prev-blind overlay left 5h utilization nil. The second
// response carries it; the overlay against the accumulated prev must yield a
// snapshot with BOTH windows (AC1), and window-minutes learned on the first
// response must survive a later response that omits them (AC5).
func TestOverlayCodex_prevCarryAfterFirstResponseGap(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()

	// Response 1: secondary window only, plus the 5h window length.
	h1 := http.Header{}
	h1.Set(HeaderCodexSecondaryUsedPercent, "30")
	h1.Set(HeaderCodexSecondaryResetAt, "1700288000")
	h1.Set(HeaderCodexPrimaryWindowMinutes, "300")
	first := ExtractCodex(h1, now)
	s := Snapshot{AsOf: now}
	s.OverlayCodex(Snapshot{}, first)
	if s.Unified5hUtilization != nil {
		t.Fatalf("setup: first response must not carry 5h utilization: %v", s.Unified5hUtilization)
	}

	// Response 2 (the "shared prev" is what response 1 taught us): carries
	// only the 5h utilization the first response omitted.
	h2 := http.Header{}
	h2.Set(HeaderCodexPrimaryUsedPercent, "42")
	second := ExtractCodex(h2, now.Add(time.Second))
	s.OverlayCodex(s, second)

	if s.Unified5hUtilization == nil || *s.Unified5hUtilization != 0.42 {
		t.Errorf("AC1: 5h utilization=%v, want 0.42 after the second overlay", s.Unified5hUtilization)
	}
	if s.Unified7dUtilization == nil || *s.Unified7dUtilization != 0.3 {
		t.Errorf("7d utilization=%v, want 0.3 carried forward from the first response", s.Unified7dUtilization)
	}
	if s.Unified7dReset == nil || !s.Unified7dReset.Equal(time.Unix(1700288000, 0).UTC()) {
		t.Errorf("7d reset=%v, want carried forward", s.Unified7dReset)
	}
	if s.Unified5hWindowMinutes == nil || *s.Unified5hWindowMinutes != 300 {
		t.Errorf("AC5: 5h window minutes=%v, want 300 carried forward", s.Unified5hWindowMinutes)
	}
}

// TestOverlayCodex_fallbackResetDoesNotDrift reproduces defect 2 of issue
// #311: consecutive reset-after-seconds responses each resolve a fresh
// now+secs, so a prev-blind overlay drifted the stored reset by the
// inter-arrival gap on every response (AC2).
func TestOverlayCodex_fallbackResetDoesNotDrift(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0).UTC()

	r1, fb1 := parseCodexReset("", "18000", t0)
	if r1 == nil || !fb1 {
		t.Fatalf("setup: parseCodexReset fallback = (%v, %v), want (non-nil, true)", r1, fb1)
	}
	prev := Snapshot{AsOf: t0, Unified5hReset: r1}
	prev.unified5hResetIsFallback = fb1

	// Response 2 lands 3s later and reports 3s less remaining.
	r2, fb2 := parseCodexReset("", "17997", t0.Add(3*time.Second))
	if r2 == nil || !fb2 {
		t.Fatalf("setup: parseCodexReset fallback = (%v, %v), want (non-nil, true)", r2, fb2)
	}
	c := Snapshot{Unified5hReset: r2}
	c.unified5hResetIsFallback = fb2

	s := Snapshot{AsOf: t0.Add(3 * time.Second)}
	s.OverlayCodex(prev, c)

	if s.Unified5hReset == nil || !s.Unified5hReset.Equal(*prev.Unified5hReset) {
		t.Errorf("AC2: 5h reset=%v, want prev's %v (no drift)", s.Unified5hReset, prev.Unified5hReset)
	}
	if !s.unified5hResetIsFallback {
		t.Errorf("AC2: fallback flag cleared by a fallback carry: %+v", s)
	}
}

// TestOverlayCodex_preciseResetBeatsFallbackPrev: an absolute reset-at is
// authoritative regardless of what prev knows (AC3), and a response carrying
// both headers records reset-at (AC4 — defensive; parseCodexReset prefers
// reset-at).
func TestOverlayCodex_preciseResetBeatsFallbackPrev(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()

	t.Run("precise over fallback prev", func(t *testing.T) {
		fbReset := now.Add(18000 * time.Second)
		prev := Snapshot{Unified5hReset: &fbReset}
		prev.unified5hResetIsFallback = true

		r, f := parseCodexReset("1781352600", "", now)
		if r == nil || f {
			t.Fatalf("setup: parseCodexReset(reset-at) = (%v, %v), want (non-nil, false)", r, f)
		}
		c := Snapshot{Unified5hReset: r}
		c.unified5hResetIsFallback = f

		s := Snapshot{}
		s.OverlayCodex(prev, c)

		if s.Unified5hReset == nil || !s.Unified5hReset.Equal(time.Unix(1781352600, 0).UTC()) {
			t.Errorf("AC3: 5h reset=%v, want the precise 1781352600", s.Unified5hReset)
		}
		if s.unified5hResetIsFallback {
			t.Errorf("AC3: fallback flag still set after a precise overlay")
		}
	})

	t.Run("both headers present", func(t *testing.T) {
		r, f := parseCodexReset("1781352600", "9999999", now)
		if r == nil || f {
			t.Fatalf("setup: parseCodexReset(both) = (%v, %v), want reset-at (non-nil, false)", r, f)
		}
		c := Snapshot{Unified5hReset: r}
		c.unified5hResetIsFallback = f

		s := Snapshot{}
		s.OverlayCodex(Snapshot{}, c)

		if s.Unified5hReset == nil || !s.Unified5hReset.Equal(time.Unix(1781352600, 0).UTC()) {
			t.Errorf("AC4: 5h reset=%v, want reset-at 1781352600", s.Unified5hReset)
		}
		if s.unified5hResetIsFallback {
			t.Errorf("AC4: fallback flag set when reset-at was present")
		}
	})
}

// TestMerge_fallbackResetNeverOverwritesKnownReset is the store-level
// serialization test (AC10): response A overlays a fallback reset against a
// stale (empty) prev and its Merge lands AFTER response B's precise merge.
// mergeSnapshot's fallback-precision gate runs under the write lock, so the
// precise value must survive — a deterministic stand-in for the interleaving
// that -race cannot prove by itself.
func TestMerge_fallbackResetNeverOverwritesKnownReset(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0).UTC()
	store := NewStore()

	// B's precise merge lands first (5h), alongside a fallback 7d reset
	// adopted first-time (prev was nil).
	precise := Snapshot{AsOf: t0}
	p := time.Unix(1781352600, 0).UTC()
	precise.Unified5hReset = &p
	fb7d := t0.Add(10080 * time.Minute)
	precise.Unified7dReset = &fb7d
	precise.unified7dResetIsFallback = true
	store.Merge("seat", precise)

	// A's stale-prev fallback merge lands second: 5h fallback vs B's
	// precise, 7d fresh fallback vs the adopted one — both must keep prev.
	stale := Snapshot{AsOf: t0.Add(2 * time.Second)}
	fb5h := t0.Add(18000 * time.Second)
	stale.Unified5hReset = &fb5h
	stale.unified5hResetIsFallback = true
	fb7d2 := t0.Add(2*time.Second + 10080*time.Minute)
	stale.Unified7dReset = &fb7d2
	stale.unified7dResetIsFallback = true
	store.Merge("seat", stale)

	got := store.Get("seat")
	if got.Unified5hReset == nil || !got.Unified5hReset.Equal(p) {
		t.Errorf("AC10: 5h reset=%v, want the precise %v kept over the late fallback", got.Unified5hReset, p)
	}
	if got.unified5hResetIsFallback {
		t.Errorf("AC10: 5h fallback flag set while holding a precise value")
	}
	if got.Unified7dReset == nil || !got.Unified7dReset.Equal(fb7d) {
		t.Errorf("AC10: 7d reset=%v, want the first fallback %v kept (no drift)", got.Unified7dReset, fb7d)
	}
	if !got.unified7dResetIsFallback {
		t.Errorf("AC10: 7d fallback flag lost; a later precise reset-at must still be able to supersede it")
	}

	// A precise reset-at still supersedes the retained fallback afterwards.
	next := Snapshot{AsOf: t0.Add(time.Hour)}
	p7d := time.Unix(1781500000, 0).UTC()
	next.Unified7dReset = &p7d
	store.Merge("seat", next)
	got = store.Get("seat")
	if got.Unified7dReset == nil || !got.Unified7dReset.Equal(p7d) {
		t.Errorf("AC10: 7d reset=%v, want the later precise %v", got.Unified7dReset, p7d)
	}
	if got.unified7dResetIsFallback {
		t.Errorf("AC10: 7d fallback flag survived a precise overwrite")
	}
}

// TestOverlayCodex_jsonWireShapeUnchanged pins AC7/#311: the fallback flags
// are unexported implementation details and must never surface in the JSON
// wire shape (the persister and /_gateway/quota marshal Snapshots).
func TestOverlayCodex_jsonWireShapeUnchanged(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	h := http.Header{}
	h.Set(HeaderCodexPrimaryUsedPercent, "42")
	h.Set(HeaderCodexPrimaryResetAfterSeconds, "18000")
	h.Set(HeaderCodexPrimaryWindowMinutes, "300")
	h.Set(HeaderCodexSecondaryUsedPercent, "30")
	h.Set(HeaderCodexSecondaryResetAt, "1700288000")
	h.Set(HeaderCodexSecondaryWindowMinutes, "10080")

	s := Snapshot{AsOf: now, Backend: "seat"}
	s.OverlayCodex(Snapshot{}, ExtractCodex(h, now))
	if !s.unified5hResetIsFallback || s.unified7dResetIsFallback {
		t.Fatalf("setup: flags = (%v, %v), want (true, false)", s.unified5hResetIsFallback, s.unified7dResetIsFallback)
	}

	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := map[string]bool{
		"backend":                   true,
		"as_of":                     true,
		"unified_5h_utilization":    true,
		"unified_5h_reset":          true,
		"unified_5h_window_minutes": true,
		"unified_7d_utilization":    true,
		"unified_7d_reset":          true,
		"unified_7d_window_minutes": true,
	}
	if len(m) != len(want) {
		t.Errorf("AC7: key set drifted: got %v, want exactly %v", m, want)
	}
	for k := range want {
		if _, ok := m[k]; !ok {
			t.Errorf("AC7: expected key %q missing from %v", k, m)
		}
	}
	for k := range m {
		if strings.Contains(k, "fallback") && k != "unified_fallback_percentage" {
			t.Errorf("AC7: fallback flag leaked into JSON as %q", k)
		}
	}
}

func TestMerge_carriesCodexWindowMinutesForward(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	store := NewStore()

	full := Snapshot{AsOf: now}
	util := 0.5
	full.Unified5hUtilization = &util
	mins := 120
	full.Unified5hWindowMinutes = &mins
	reset := now.Add(time.Hour)
	full.Unified5hReset = &reset
	store.Merge("seat", full)

	// A later response reporting only the window it touched (percent+reset,
	// no minutes header) must not blank the learned length (issue #163
	// semantics extended to the codex minutes fields).
	partial := Snapshot{AsOf: now.Add(time.Minute)}
	util2 := 0.6
	partial.Unified5hUtilization = &util2
	store.Merge("seat", partial)

	got := store.Get("seat")
	if got.Unified5hWindowMinutes == nil || *got.Unified5hWindowMinutes != 120 {
		t.Errorf("5h window minutes=%v, want 120 carried forward", got.Unified5hWindowMinutes)
	}
	if got.Unified5hUtilization == nil || *got.Unified5hUtilization != 0.6 {
		t.Errorf("5h utilization=%v, want the fresh 0.6", got.Unified5hUtilization)
	}
}

func TestHasData_windowMinutesAloneIsNotData(t *testing.T) {
	mins := 300
	s := Snapshot{Unified5hWindowMinutes: &mins}
	if s.HasData() {
		t.Errorf("HasData=true for a minutes-only snapshot; duration metadata must not admit")
	}
}

func ptrFloat(v float64) *float64 { return &v }
