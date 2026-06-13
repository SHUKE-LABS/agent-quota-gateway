package backend

import (
	"context"
	"reflect"
	"testing"
)

func TestLoadFrom_collectsAndNormalizes(t *testing.T) {
	reg, err := loadFrom([]string{
		"AQG_BACKEND_CLAUDE_A=cred-a",
		"AQG_BACKEND_CLAUDE_B=cred-b",
		"PATH=/usr/bin",              // unrelated, ignored
		"AQG_BACKENDISH=not-a-match", // wrong prefix shape, ignored
	})
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}

	if got := reg.Nicks(); !reflect.DeepEqual(got, []string{"claude-a", "claude-b"}) {
		t.Fatalf("Nicks() = %v, want [claude-a claude-b]", got)
	}

	b, ok := reg.Resolve("claude-a")
	if !ok {
		t.Fatal("Resolve(claude-a) not found")
	}
	if b.Nick != "claude-a" || b.Credential != "cred-a" {
		t.Errorf("Resolve(claude-a) = %+v, want {claude-a cred-a}", b)
	}
}

func TestResolve_caseInsensitive(t *testing.T) {
	reg, err := loadFrom([]string{"AQG_BACKEND_CLAUDE_A=cred-a"})
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	for _, sel := range []string{"claude-a", "CLAUDE-A", "Claude-A", "  claude-a  ", "claude_a"} {
		if b, ok := reg.Resolve(sel); !ok || b.Nick != "claude-a" {
			t.Errorf("Resolve(%q) = (%+v, %v), want claude-a backend", sel, b, ok)
		}
	}
}

func TestResolve_unknownFailsClosed(t *testing.T) {
	reg, err := loadFrom([]string{"AQG_BACKEND_CLAUDE_A=cred-a"})
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	for _, sel := range []string{"claude-b", "", "unknown"} {
		if _, ok := reg.Resolve(sel); ok {
			t.Errorf("Resolve(%q) resolved; want not found (fail closed)", sel)
		}
	}
}

func TestLoadFrom_emptyCredentialRejected(t *testing.T) {
	if _, err := loadFrom([]string{"AQG_BACKEND_CLAUDE_A="}); err == nil {
		t.Fatal("expected error for empty credential")
	}
}

func TestLoadFrom_emptyNickRejected(t *testing.T) {
	// "AQG_BACKEND_" with nothing after, or only separators, has no nick.
	for _, kv := range []string{"AQG_BACKEND_=cred", "AQG_BACKEND_-=cred", "AQG_BACKEND___=cred"} {
		if _, err := loadFrom([]string{kv}); err == nil {
			t.Errorf("loadFrom(%q): expected empty-nick error", kv)
		}
	}
}

func TestLoadFrom_collisionRejected(t *testing.T) {
	// Two distinct env keys normalize to the same nick.
	_, err := loadFrom([]string{
		"AQG_BACKEND_CLAUDE_A=cred-1",
		"AQG_BACKEND_claude-a=cred-2",
	})
	if err == nil {
		t.Fatal("expected collision error for two keys mapping to claude-a")
	}
}

func TestLoadFrom_noBackendsRejected(t *testing.T) {
	if _, err := loadFrom([]string{"PATH=/usr/bin", "HOME=/root"}); err == nil {
		t.Fatal("expected error when no AQG_BACKEND_* are set")
	}
}

func TestLoadFrom_reservedAutoNickRejected(t *testing.T) {
	// "auto" is the reserved selector; a backend that normalizes to it
	// must be rejected so the word stays unambiguous.
	for _, kv := range []string{"AQG_BACKEND_AUTO=cred", "AQG_BACKEND_auto=cred", "AQG_BACKEND_Auto=cred"} {
		if _, err := loadFrom([]string{kv}); err == nil {
			t.Errorf("loadFrom(%q): expected reserved-nick error", kv)
		}
	}
}

func TestIsAutoSelector(t *testing.T) {
	for _, sel := range []string{"auto", "AUTO", "  Auto  "} {
		if !IsAutoSelector(sel) {
			t.Errorf("IsAutoSelector(%q)=false, want true", sel)
		}
	}
	for _, sel := range []string{"", "autox", "claude-a"} {
		if IsAutoSelector(sel) {
			t.Errorf("IsAutoSelector(%q)=true, want false", sel)
		}
	}
}

func TestAutoFlag_roundTrip(t *testing.T) {
	if IsAutoRequest(context.Background()) {
		t.Error("bare context reported as auto-routed")
	}
	if !IsAutoRequest(MarkAuto(context.Background())) {
		t.Error("MarkAuto context not reported as auto-routed")
	}
}

func TestContext_roundTrip(t *testing.T) {
	b := Backend{Nick: "claude-a", Credential: "cred-a"}
	ctx := WithBackend(context.Background(), b)
	got, ok := FromContext(ctx)
	if !ok {
		t.Fatal("FromContext: not found after WithBackend")
	}
	if got != b {
		t.Errorf("FromContext = %+v, want %+v", got, b)
	}
}

func TestContext_absent(t *testing.T) {
	if _, ok := FromContext(context.Background()); ok {
		t.Error("FromContext on bare context returned ok=true")
	}
}
