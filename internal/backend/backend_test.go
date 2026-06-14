package backend

import (
	"context"
	"reflect"
	"testing"
)

const testDefaultBaseURL = "https://api.anthropic.com"

func TestLoadFrom_collectsAndNormalizes(t *testing.T) {
	reg, err := loadFrom([]string{
		"AQG_POOL_AUTO_BACKEND_CLAUDE_A=cred-a",
		"AQG_POOL_AUTO_BACKEND_CLAUDE_B=cred-b",
		"PATH=/usr/bin",           // unrelated, ignored
		"AQG_POOLISH=not-a-match", // wrong prefix shape, ignored
	}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}

	if got := reg.PoolNames(); !reflect.DeepEqual(got, []string{"auto"}) {
		t.Fatalf("PoolNames() = %v, want [auto]", got)
	}
	if got := reg.PoolNicks("auto"); !reflect.DeepEqual(got, []string{"claude-a", "claude-b"}) {
		t.Fatalf("PoolNicks(auto) = %v, want [claude-a claude-b]", got)
	}

	b, ok := reg.ResolveIn("auto", "claude-a")
	if !ok {
		t.Fatal("ResolveIn(auto, claude-a) not found")
	}
	want := Backend{Pool: "auto", Nick: "claude-a", Credential: "cred-a", BaseURL: testDefaultBaseURL}
	if b != want {
		t.Errorf("ResolveIn(auto, claude-a) = %+v, want %+v", b, want)
	}
	if b.QuotaKey() != "auto/claude-a" {
		t.Errorf("QuotaKey() = %q, want auto/claude-a", b.QuotaKey())
	}
}

func TestLoadFrom_multiplePools(t *testing.T) {
	reg, err := loadFrom([]string{
		"AQG_POOL_AUTO_BACKEND_A=sk-ant-oat-a",
		"AQG_POOL_API_BACKEND_K=sk-ant-api-k",
		"AQG_POOL_Z_AI_BASE_URL=https://open.example/anthropic",
		"AQG_POOL_Z_AI_BACKEND_X=zcred",
	}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if got := reg.PoolNames(); !reflect.DeepEqual(got, []string{"api", "auto", "z-ai"}) {
		t.Fatalf("PoolNames() = %v, want [api auto z-ai]", got)
	}
	// The z-ai pool's declared base URL applies to its members; the auto
	// and api pools inherit the gateway default.
	if b, _ := reg.ResolveIn("z-ai", "x"); b.BaseURL != "https://open.example/anthropic" {
		t.Errorf("z-ai/x BaseURL = %q, want the pool default", b.BaseURL)
	}
	if b, _ := reg.ResolveIn("auto", "a"); b.BaseURL != testDefaultBaseURL {
		t.Errorf("auto/a BaseURL = %q, want the gateway default", b.BaseURL)
	}
}

func TestLoadFrom_perMemberURLOverride(t *testing.T) {
	reg, err := loadFrom([]string{
		"AQG_POOL_Z_AI_BASE_URL=https://primary.example/anthropic",
		"AQG_POOL_Z_AI_BACKEND_X=cred-x",
		"AQG_POOL_Z_AI_BACKEND_Y=cred-y|https://mirror.example/anthropic",
	}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	x, _ := reg.ResolveIn("z-ai", "x")
	if x.Credential != "cred-x" || x.BaseURL != "https://primary.example/anthropic" {
		t.Errorf("x = %+v, want cred-x at the pool default", x)
	}
	y, _ := reg.ResolveIn("z-ai", "y")
	if y.Credential != "cred-y" || y.BaseURL != "https://mirror.example/anthropic" {
		t.Errorf("y = %+v, want cred-y at the per-member override", y)
	}
}

func TestResolveIn_caseInsensitive(t *testing.T) {
	reg, err := loadFrom([]string{"AQG_POOL_Z_AI_BACKEND_CLAUDE_A=cred-a"}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	for _, pool := range []string{"z-ai", "Z-AI", "  z-ai  ", "z_ai"} {
		for _, nick := range []string{"claude-a", "CLAUDE-A", "claude_a"} {
			if b, ok := reg.ResolveIn(pool, nick); !ok || b.Nick != "claude-a" || b.Pool != "z-ai" {
				t.Errorf("ResolveIn(%q,%q) = (%+v,%v), want z-ai/claude-a", pool, nick, b, ok)
			}
		}
	}
}

func TestResolveIn_unknownFailsClosed(t *testing.T) {
	reg, err := loadFrom([]string{"AQG_POOL_AUTO_BACKEND_CLAUDE_A=cred-a"}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	cases := []struct{ pool, nick string }{
		{"auto", "claude-b"}, // unknown nick in a known pool
		{"auto", ""},
		{"nope", "claude-a"}, // unknown pool
		{"", "claude-a"},
	}
	for _, tc := range cases {
		if _, ok := reg.ResolveIn(tc.pool, tc.nick); ok {
			t.Errorf("ResolveIn(%q,%q) resolved; want not found (fail closed)", tc.pool, tc.nick)
		}
	}
	if reg.HasPool("nope") {
		t.Error("HasPool(nope) = true, want false")
	}
	if !reg.HasPool("AUTO") {
		t.Error("HasPool(AUTO) = false, want true (case-insensitive)")
	}
}

func TestLoadFrom_emptyCredentialRejected(t *testing.T) {
	for _, kv := range []string{"AQG_POOL_AUTO_BACKEND_A=", "AQG_POOL_AUTO_BACKEND_A=|https://x.example"} {
		if _, err := loadFrom([]string{kv}, testDefaultBaseURL); err == nil {
			t.Errorf("loadFrom(%q): expected empty-credential error", kv)
		}
	}
}

func TestLoadFrom_emptyNickOrPoolRejected(t *testing.T) {
	for _, kv := range []string{
		"AQG_POOL_AUTO_BACKEND_=cred", // empty nick
		"AQG_POOL__BACKEND_A=cred",    // empty pool
		"AQG_POOL_-_BACKEND_A=cred",   // pool normalizes to empty
	} {
		if _, err := loadFrom([]string{kv}, testDefaultBaseURL); err == nil {
			t.Errorf("loadFrom(%q): expected empty-nick/pool error", kv)
		}
	}
}

func TestLoadFrom_collisionRejected(t *testing.T) {
	// Two distinct env keys normalize to the same pool/nick.
	_, err := loadFrom([]string{
		"AQG_POOL_AUTO_BACKEND_CLAUDE_A=cred-1",
		"AQG_POOL_auto_BACKEND_claude-a=cred-2",
	}, testDefaultBaseURL)
	if err == nil {
		t.Fatal("expected collision error for two keys mapping to auto/claude-a")
	}
	// Same nick in *different* pools is fine — they are distinct identities.
	if _, err := loadFrom([]string{
		"AQG_POOL_AUTO_BACKEND_A=cred-1",
		"AQG_POOL_API_BACKEND_A=cred-2",
	}, testDefaultBaseURL); err != nil {
		t.Fatalf("same nick in different pools should be allowed: %v", err)
	}
}

func TestLoadFrom_noBackendsRejected(t *testing.T) {
	// Unrelated env, and a base URL with no members, both leave zero pools.
	for _, env := range [][]string{
		{"PATH=/usr/bin", "HOME=/root"},
		{"AQG_POOL_AUTO_BASE_URL=https://api.anthropic.com"},
	} {
		if _, err := loadFrom(env, testDefaultBaseURL); err == nil {
			t.Errorf("loadFrom(%v): expected no-backends error", env)
		}
	}
}

func TestLoadFrom_baseURLForMemberlessPoolRejected(t *testing.T) {
	// A base URL for a pool that has no members of its own is a typo'd
	// nick; fail closed even when other pools are well-formed.
	_, err := loadFrom([]string{
		"AQG_POOL_AUTO_BACKEND_A=cred-a",
		"AQG_POOL_GHOST_BASE_URL=https://ghost.example",
	}, testDefaultBaseURL)
	if err == nil {
		t.Fatal("expected error for base URL on a pool with no members")
	}
}

func TestLoadFrom_malformedBaseURLRejected(t *testing.T) {
	cases := [][]string{
		{"AQG_POOL_Z_AI_BASE_URL=not-a-url", "AQG_POOL_Z_AI_BACKEND_X=cred"},
		{"AQG_POOL_Z_AI_BACKEND_X=cred|not-a-url"}, // bad per-member override
		{"AQG_POOL_Z_AI_BACKEND_X=ab|cd"},          // a `|` in the credential -> bogus URL tail
	}
	for _, env := range cases {
		if _, err := loadFrom(env, testDefaultBaseURL); err == nil {
			t.Errorf("loadFrom(%v): expected malformed base URL error", env)
		}
	}
	// And a malformed gateway default is caught when a pool relies on it.
	if _, err := loadFrom([]string{"AQG_POOL_AUTO_BACKEND_A=cred"}, "not-a-url"); err == nil {
		t.Error("expected error when the inherited default base URL is malformed")
	}
}

func TestLoadFrom_unrecognisedShapeRejected(t *testing.T) {
	for _, kv := range []string{"AQG_POOL_AUTO=cred", "AQG_POOL_AUTO_BACKED_A=cred"} {
		if _, err := loadFrom([]string{kv}, testDefaultBaseURL); err == nil {
			t.Errorf("loadFrom(%q): expected unrecognised-shape error", kv)
		}
	}
}

func TestLoadFrom_priorityParsed(t *testing.T) {
	// PRIORITY may appear before the members it names, and is normalized
	// the same way nicks are.
	reg, err := loadFrom([]string{
		"AQG_POOL_CHN_PRIORITY=ZAI,M3",
		"AQG_POOL_CHN_BACKEND_ZAI=cred-zai",
		"AQG_POOL_CHN_BACKEND_M3=cred-m3",
	}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if got := reg.PoolPriority("chn"); !reflect.DeepEqual(got, []string{"zai", "m3"}) {
		t.Errorf("PoolPriority(chn) = %v, want [zai m3]", got)
	}
}

func TestLoadFrom_priorityAbsentIsNil(t *testing.T) {
	reg, err := loadFrom([]string{"AQG_POOL_AUTO_BACKEND_A=cred-a"}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if got := reg.PoolPriority("auto"); got != nil {
		t.Errorf("PoolPriority(auto) = %v, want nil for a pool with no PRIORITY", got)
	}
	if got := reg.PoolPriority("nope"); got != nil {
		t.Errorf("PoolPriority(unknown) = %v, want nil", got)
	}
}

func TestLoadFrom_prioritySubsetAllowed(t *testing.T) {
	// Listing only some members is valid; the controller ranks the rest
	// after the listed ones.
	reg, err := loadFrom([]string{
		"AQG_POOL_CHN_PRIORITY=zai",
		"AQG_POOL_CHN_BACKEND_ZAI=cred-zai",
		"AQG_POOL_CHN_BACKEND_M3=cred-m3",
	}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if got := reg.PoolPriority("chn"); !reflect.DeepEqual(got, []string{"zai"}) {
		t.Errorf("PoolPriority(chn) = %v, want [zai]", got)
	}
}

func TestLoadFrom_priorityRejectsBadInput(t *testing.T) {
	cases := map[string][]string{
		"unknown nick": {
			"AQG_POOL_CHN_PRIORITY=zai,ghost",
			"AQG_POOL_CHN_BACKEND_ZAI=cred-zai",
		},
		"pool with no members": {
			"AQG_POOL_CHN_PRIORITY=zai",
			"AQG_POOL_OTHER_BACKEND_A=cred-a",
		},
		"empty list": {
			"AQG_POOL_CHN_PRIORITY=",
			"AQG_POOL_CHN_BACKEND_ZAI=cred-zai",
		},
		"empty entry": {
			"AQG_POOL_CHN_PRIORITY=zai,,m3",
			"AQG_POOL_CHN_BACKEND_ZAI=cred-zai",
			"AQG_POOL_CHN_BACKEND_M3=cred-m3",
		},
		"duplicate nick": {
			"AQG_POOL_CHN_PRIORITY=zai,zai",
			"AQG_POOL_CHN_BACKEND_ZAI=cred-zai",
		},
		"duplicate priority var": {
			"AQG_POOL_CHN_PRIORITY=zai",
			"AQG_POOL_chn_PRIORITY=zai",
			"AQG_POOL_CHN_BACKEND_ZAI=cred-zai",
		},
	}
	for name, env := range cases {
		if _, err := loadFrom(env, testDefaultBaseURL); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}

func TestContext_roundTrip(t *testing.T) {
	b := Backend{Pool: "auto", Nick: "claude-a", Credential: "cred-a", BaseURL: testDefaultBaseURL}
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
