package quota

import (
	"net/http"
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
	s.OverlayCodex(codex)

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
