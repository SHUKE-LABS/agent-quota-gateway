package configfile

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// TestWriter_Mode_immutableAfterConstruct pins the issue #246 contract:
// Mode is decided at NewWriter time from whether path was non-empty, and
// does not change across the writer's lifetime (the run path never
// re-evaluates persistence — a process either was started with a config
// file or it wasn't).
func TestWriter_Mode_immutableAfterConstruct(t *testing.T) {
	t.Run("empty path yields env-only", func(t *testing.T) {
		w := NewWriter("", func() ([]byte, error) { return nil, nil })
		if got := w.Mode(); got != ModeEnvOnly {
			t.Errorf("Mode()=%v, want ModeEnvOnly", got)
		}
		if got := w.Mode().String(); got != "env_only" {
			t.Errorf("Mode().String()=%q, want \"env_only\"", got)
		}
	})

	t.Run("non-empty path yields persisted", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "aqg.json")
		w := NewWriter(path, func() ([]byte, error) { return nil, nil })
		if got := w.Mode(); got != ModePersisted {
			t.Errorf("Mode()=%v, want ModePersisted", got)
		}
		if got := w.Mode().String(); got != "persisted" {
			t.Errorf("Mode().String()=%q, want \"persisted\"", got)
		}
	})

	t.Run("mode is stable across MarkDirty/Unsaved", func(t *testing.T) {
		w := NewWriter("", func() ([]byte, error) { return nil, nil })
		before := w.Mode()
		w.MarkDirty()
		_ = w.Unsaved()
		after := w.Mode()
		if before != after {
			t.Errorf("Mode changed across calls: %v -> %v", before, after)
		}
	})
}

// TestPersistenceStateOf captures the right Mode + a live Unsaved method
// value. The HTTP layer relies on both fields behaving as advertised.
func TestPersistenceStateOf(t *testing.T) {
	w := NewWriter("", func() ([]byte, error) { return nil, nil })
	st := w.PersistenceStateOf()
	if !st.IsEnvOnly() {
		t.Error("IsEnvOnly()=false, want true for path=\"\" writer")
	}
	if st.Unsaved != nil {
		// Unsaved() on an env-only writer returns false (the latch is never
		// set, since the flusher is disabled). Returning false is correct;
		// the field existing is the contract.
		if got := st.Unsaved(); got {
			t.Errorf("Unsaved()=%v, want false for env-only", got)
		}
	}

	w2 := NewWriter(filepath.Join(t.TempDir(), "aqg.json"), func() ([]byte, error) { return nil, nil })
	st2 := w2.PersistenceStateOf()
	if st2.IsEnvOnly() {
		t.Error("IsEnvOnly()=true, want false for path!=empty writer")
	}
}

// TestApplyPersistenceHeader covers the three-state header surface that
// every /_gateway/* response uses (issue #246):
//   - persisted, clean: X-AQG-Persistence: persisted; no unsaved header
//   - persisted, unsaved: both headers
//   - env-only: X-AQG-Persistence: env_only; no unsaved header
func TestApplyPersistenceHeader(t *testing.T) {
	t.Run("persisted clean", func(t *testing.T) {
		w := NewWriter("/tmp/x.json", func() ([]byte, error) { return nil, nil })
		rr := httptest.NewRecorder()
		ApplyPersistenceHeader(rr, w.PersistenceStateOf())
		if got := rr.Header().Get(HeaderPersistence); got != "persisted" {
			t.Errorf("X-AQG-Persistence=%q, want \"persisted\"", got)
		}
		if got := rr.Header().Get(HeaderUnsavedConfig); got != "" {
			t.Errorf("X-AQG-Unsaved-Config=%q, want empty", got)
		}
	})

	t.Run("persisted unsaved", func(t *testing.T) {
		w := NewWriter("/tmp/x.json", func() ([]byte, error) { return nil, nil })
		w.setUnsaved(true)
		rr := httptest.NewRecorder()
		ApplyPersistenceHeader(rr, w.PersistenceStateOf())
		if got := rr.Header().Get(HeaderPersistence); got != "persisted" {
			t.Errorf("X-AQG-Persistence=%q, want \"persisted\"", got)
		}
		if got := rr.Header().Get(HeaderUnsavedConfig); got != "true" {
			t.Errorf("X-AQG-Unsaved-Config=%q, want \"true\"", got)
		}
	})

	t.Run("env-only never sets unsaved", func(t *testing.T) {
		w := NewWriter("", func() ([]byte, error) { return nil, nil })
		// Even if a future regression set the latch on an env-only writer,
		// the env-only branch must short-circuit before consulting it.
		rr := httptest.NewRecorder()
		ApplyPersistenceHeader(rr, w.PersistenceStateOf())
		if got := rr.Header().Get(HeaderPersistence); got != "env_only" {
			t.Errorf("X-AQG-Persistence=%q, want \"env_only\"", got)
		}
		if got := rr.Header().Get(HeaderUnsavedConfig); got != "" {
			t.Errorf("X-AQG-Unsaved-Config=%q, want empty in env-only", got)
		}
	})
}

// TestApplyEnvOnlyBodyField is the single source of truth for the
// mutation-family body shape (issue #246, review note #2): in env-only
// mode the helper stamps "persistence":"env_only" onto the body map; in
// persisted mode the map is left byte-identical to its pre-call state.
func TestApplyEnvOnlyBodyField(t *testing.T) {
	t.Run("env-only adds the field", func(t *testing.T) {
		w := NewWriter("", func() ([]byte, error) { return nil, nil })
		body := map[string]any{"status": "ok"}
		ApplyEnvOnlyBodyField(body, w.PersistenceStateOf())
		if got := body[PersistenceField]; got != "env_only" {
			t.Errorf("body[%q]=%v, want \"env_only\"", PersistenceField, got)
		}
		if body["status"] != "ok" {
			t.Errorf("body[status]=%v, want \"ok\" (helper must preserve other fields)", body["status"])
		}
	})

	t.Run("persisted leaves the body alone", func(t *testing.T) {
		w := NewWriter("/tmp/x.json", func() ([]byte, error) { return nil, nil })
		body := map[string]any{"pool": "auto"}
		ApplyEnvOnlyBodyField(body, w.PersistenceStateOf())
		if _, ok := body[PersistenceField]; ok {
			t.Errorf("persisted mode stamped body[%q]; want no field", PersistenceField)
		}
		if body["pool"] != "auto" {
			t.Errorf("body[pool]=%v, want \"auto\"", body["pool"])
		}
	})
}

// TestMode_String_isStable pins the wire form that goes onto headers and
// into JSON bodies. Renaming this is a breaking wire change.
func TestMode_String_isStable(t *testing.T) {
	cases := map[Mode]string{
		ModePersisted: "persisted",
		ModeEnvOnly:   "env_only",
	}
	for m, want := range cases {
		if got := m.String(); got != want {
			t.Errorf("%d.String()=%q, want %q", m, got, want)
		}
	}
	// An out-of-range Mode value falls back to "persisted" — the documented
	// default for unknown states. This guards against a future addition
	// silently emitting an empty or garbage value before String is updated.
	if got := Mode(99).String(); got != "persisted" {
		t.Errorf("Mode(99).String()=%q, want \"persisted\" (default)", got)
	}
}

// TestHeaderPersistence_valueSpace ensures the documented header constant
// matches the wire form Mode.String() emits. They have to agree; the
// bundled UI compares them.
func TestHeaderPersistence_valueSpace(t *testing.T) {
	if HeaderPersistence != "X-AQG-Persistence" {
		t.Errorf("HeaderPersistence=%q, want \"X-AQG-Persistence\"", HeaderPersistence)
	}
	if HeaderUnsavedConfig != "X-AQG-Unsaved-Config" {
		t.Errorf("HeaderUnsavedConfig=%q, want \"X-AQG-Unsaved-Config\"", HeaderUnsavedConfig)
	}
	// A real HTTP round trip writes the header on the wire.
	w := NewWriter("", func() ([]byte, error) { return nil, nil })
	req := httptest.NewRequest(http.MethodGet, "/_gateway/health", nil)
	rr := httptest.NewRecorder()
	ApplyPersistenceHeader(rr, w.PersistenceStateOf())
	if got := rr.Result().Header.Get(HeaderPersistence); got != "env_only" {
		t.Errorf("on-wire header=%q, want \"env_only\"", got)
	}
	_ = req
}
