package backend

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
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
	if b.QuotaKey() != "claude-a" {
		t.Errorf("QuotaKey() = %q, want claude-a", b.QuotaKey())
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
	// Same nick across two pools with the *identical* credential is the
	// sharing shape (one physical account present in two routing contexts);
	// nick is the global quota identity, so this is allowed.
	if _, err := loadFrom([]string{
		"AQG_POOL_AUTO_BACKEND_A=cred-shared",
		"AQG_POOL_API_BACKEND_A=cred-shared",
	}, testDefaultBaseURL); err != nil {
		t.Fatalf("same nick + identical credential across pools should be allowed: %v", err)
	}
	// Same nick across two pools with *different* credentials is rejected:
	// the two declarations would point the same quota key at two distinct
	// credentials, reintroducing the cross-pool staleness bug.
	if _, err := loadFrom([]string{
		"AQG_POOL_AUTO_BACKEND_A=cred-1",
		"AQG_POOL_API_BACKEND_A=cred-2",
	}, testDefaultBaseURL); err == nil {
		t.Fatal("expected error for the same nick with different credentials across pools")
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

func TestLoadFrom_balanceParsed(t *testing.T) {
	reg, err := loadFrom([]string{
		"AQG_POOL_SUB_BALANCE=lead",
		"AQG_POOL_SUB_BACKEND_A=cred-a",
		"AQG_POOL_SUB_BACKEND_B=cred-b",
	}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if got := reg.PoolBalanceGap("sub"); got != defaultBalanceGap {
		t.Errorf("PoolBalanceGap(sub) = %v, want default %v", got, defaultBalanceGap)
	}
	if got := reg.PoolBalanceDwell("sub"); got != defaultBalanceDwell {
		t.Errorf("PoolBalanceDwell(sub) = %v, want default %v", got, defaultBalanceDwell)
	}
}

func TestLoadFrom_balanceWithCustomTuning(t *testing.T) {
	reg, err := loadFrom([]string{
		"AQG_POOL_SUB_BALANCE=lead",
		"AQG_POOL_SUB_BALANCE_GAP=0.20",
		"AQG_POOL_SUB_BALANCE_DWELL=10m",
		"AQG_POOL_SUB_BACKEND_A=cred-a",
		"AQG_POOL_SUB_BACKEND_B=cred-b",
	}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if got := reg.PoolBalanceGap("sub"); got != 0.20 {
		t.Errorf("PoolBalanceGap(sub) = %v, want 0.20", got)
	}
	if got := reg.PoolBalanceDwell("sub"); got != 10*time.Minute {
		t.Errorf("PoolBalanceDwell(sub) = %v, want 10m", got)
	}
}

func TestLoadFrom_balanceAbsentReturnsZero(t *testing.T) {
	reg, err := loadFrom([]string{"AQG_POOL_AUTO_BACKEND_A=cred-a"}, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if got := reg.PoolBalanceGap("auto"); got != 0 {
		t.Errorf("PoolBalanceGap(non-balance pool) = %v, want 0", got)
	}
	if got := reg.PoolBalanceDwell("auto"); got != 0 {
		t.Errorf("PoolBalanceDwell(non-balance pool) = %v, want 0", got)
	}
}

func TestLoadFrom_balanceRejectsBadInput(t *testing.T) {
	cases := map[string][]string{
		"unsupported mode": {
			"AQG_POOL_SUB_BALANCE=round-robin",
			"AQG_POOL_SUB_BACKEND_A=cred-a",
		},
		"conflict with priority": {
			"AQG_POOL_SUB_BALANCE=lead",
			"AQG_POOL_SUB_PRIORITY=a",
			"AQG_POOL_SUB_BACKEND_A=cred-a",
			"AQG_POOL_SUB_BACKEND_B=cred-b",
		},
		"balance for pool with no backends": {
			"AQG_POOL_SUB_BALANCE=lead",
			"AQG_POOL_OTHER_BACKEND_A=cred-a",
		},
		"gap without balance": {
			"AQG_POOL_SUB_BALANCE_GAP=0.15",
			"AQG_POOL_SUB_BACKEND_A=cred-a",
		},
		"dwell without balance": {
			"AQG_POOL_SUB_BALANCE_DWELL=5m",
			"AQG_POOL_SUB_BACKEND_A=cred-a",
		},
		"invalid gap (zero)": {
			"AQG_POOL_SUB_BALANCE=lead",
			"AQG_POOL_SUB_BALANCE_GAP=0",
			"AQG_POOL_SUB_BACKEND_A=cred-a",
		},
		"invalid gap (non-numeric)": {
			"AQG_POOL_SUB_BALANCE=lead",
			"AQG_POOL_SUB_BALANCE_GAP=high",
			"AQG_POOL_SUB_BACKEND_A=cred-a",
		},
		"invalid dwell (zero)": {
			"AQG_POOL_SUB_BALANCE=lead",
			"AQG_POOL_SUB_BALANCE_DWELL=0s",
			"AQG_POOL_SUB_BACKEND_A=cred-a",
		},
		"invalid dwell (non-duration)": {
			"AQG_POOL_SUB_BALANCE=lead",
			"AQG_POOL_SUB_BALANCE_DWELL=fast",
			"AQG_POOL_SUB_BACKEND_A=cred-a",
		},
		"duplicate balance var": {
			"AQG_POOL_SUB_BALANCE=lead",
			"AQG_POOL_sub_BALANCE=lead",
			"AQG_POOL_SUB_BACKEND_A=cred-a",
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

func TestBuildFromSpec_parityWithEnv(t *testing.T) {
	// A spec that matches the env config from TestLoadFrom_collectsAndNormalizes
	spec := Spec{
		Pools: map[string]PoolSpec{
			"AUTO": {
				Members: map[string]MemberSpec{
					"CLAUDE_A": {Credential: "cred-a"},
					"CLAUDE_B": {Credential: "cred-b"},
				},
			},
		},
	}
	reg, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
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
	if b.QuotaKey() != "claude-a" {
		t.Errorf("QuotaKey() = %q, want claude-a", b.QuotaKey())
	}
}

func TestBuildFromSpec_multiplePools(t *testing.T) {
	// Parity with TestLoadFrom_multiplePools
	spec := Spec{
		Pools: map[string]PoolSpec{
			"AUTO": {
				Members: map[string]MemberSpec{
					"A": {Credential: "sk-ant-oat-a"},
				},
			},
			"API": {
				Members: map[string]MemberSpec{
					"K": {Credential: "sk-ant-api-k"},
				},
			},
			"Z_AI": {
				BaseURL: "https://open.example/anthropic",
				Members: map[string]MemberSpec{
					"X": {Credential: "zcred"},
				},
			},
		},
	}
	reg, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	if got := reg.PoolNames(); !reflect.DeepEqual(got, []string{"api", "auto", "z-ai"}) {
		t.Fatalf("PoolNames() = %v, want [api auto z-ai]", got)
	}
	if b, _ := reg.ResolveIn("z-ai", "x"); b.BaseURL != "https://open.example/anthropic" {
		t.Errorf("z-ai/x BaseURL = %q, want the pool default", b.BaseURL)
	}
	if b, _ := reg.ResolveIn("auto", "a"); b.BaseURL != testDefaultBaseURL {
		t.Errorf("auto/a BaseURL = %q, want the gateway default", b.BaseURL)
	}
}

func TestBuildFromSpec_perMemberURLOverride(t *testing.T) {
	// Parity with TestLoadFrom_perMemberURLOverride
	spec := Spec{
		Pools: map[string]PoolSpec{
			"Z_AI": {
				BaseURL: "https://primary.example/anthropic",
				Members: map[string]MemberSpec{
					"X": {Credential: "cred-x"},
					"Y": {Credential: "cred-y", BaseURL: "https://mirror.example/anthropic"},
				},
			},
		},
	}
	reg, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
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

func TestBuildFromSpec_priorityParsed(t *testing.T) {
	// Parity with TestLoadFrom_priorityParsed
	spec := Spec{
		Pools: map[string]PoolSpec{
			"CHN": {
				Priority: []string{"ZAI", "M3"},
				Members: map[string]MemberSpec{
					"ZAI": {Credential: "cred-zai"},
					"M3":  {Credential: "cred-m3"},
				},
			},
		},
	}
	reg, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	if got := reg.PoolPriority("chn"); !reflect.DeepEqual(got, []string{"zai", "m3"}) {
		t.Errorf("PoolPriority(chn) = %v, want [zai m3]", got)
	}
}

func TestBuildFromSpec_balanceParsed(t *testing.T) {
	// Parity with TestLoadFrom_balanceParsed
	spec := Spec{
		Pools: map[string]PoolSpec{
			"SUB": {
				Balance: "lead",
				Members: map[string]MemberSpec{
					"A": {Credential: "cred-a"},
					"B": {Credential: "cred-b"},
				},
			},
		},
	}
	reg, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	if got := reg.PoolBalanceGap("sub"); got != defaultBalanceGap {
		t.Errorf("PoolBalanceGap(sub) = %v, want default %v", got, defaultBalanceGap)
	}
	if got := reg.PoolBalanceDwell("sub"); got != defaultBalanceDwell {
		t.Errorf("PoolBalanceDwell(sub) = %v, want default %v", got, defaultBalanceDwell)
	}
}

func TestBuildFromSpec_balanceWithCustomTuning(t *testing.T) {
	// Parity with TestLoadFrom_balanceWithCustomTuning
	spec := Spec{
		Pools: map[string]PoolSpec{
			"SUB": {
				Balance:      "lead",
				BalanceGap:   0.20,
				BalanceDwell: Duration{D: 10 * time.Minute},
				Members: map[string]MemberSpec{
					"A": {Credential: "cred-a"},
					"B": {Credential: "cred-b"},
				},
			},
		},
	}
	reg, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	if got := reg.PoolBalanceGap("sub"); got != 0.20 {
		t.Errorf("PoolBalanceGap(sub) = %v, want 0.20", got)
	}
	if got := reg.PoolBalanceDwell("sub"); got != 10*time.Minute {
		t.Errorf("PoolBalanceDwell(sub) = %v, want 10m", got)
	}
}

func TestBuildFromSpec_caseInsensitive(t *testing.T) {
	// Parity with TestResolveIn_caseInsensitive
	spec := Spec{
		Pools: map[string]PoolSpec{
			"Z_AI": {
				Members: map[string]MemberSpec{
					"CLAUDE_A": {Credential: "cred-a"},
				},
			},
		},
	}
	reg, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	for _, pool := range []string{"z-ai", "Z-AI", "  z-ai  ", "z_ai"} {
		for _, nick := range []string{"claude-a", "CLAUDE-A", "claude_a"} {
			if b, ok := reg.ResolveIn(pool, nick); !ok || b.Nick != "claude-a" || b.Pool != "z-ai" {
				t.Errorf("ResolveIn(%q,%q) = (%+v,%v), want z-ai/claude-a", pool, nick, b, ok)
			}
		}
	}
}

func TestBuildFromSpec_validatorTable(t *testing.T) {
	cases := map[string]Spec{
		"empty credential": {
			Pools: map[string]PoolSpec{
				"A": {Members: map[string]MemberSpec{"X": {Credential: ""}}},
			},
		},
		"invalid balance mode": {
			Pools: map[string]PoolSpec{
				"SUB": {
					Balance: "round-robin",
					Members: map[string]MemberSpec{"A": {Credential: "cred-a"}},
				},
			},
		},
		"non-positive gap": {
			Pools: map[string]PoolSpec{
				"SUB": {
					Balance:    "lead",
					BalanceGap: -0.1,
					Members:    map[string]MemberSpec{"A": {Credential: "cred-a"}},
				},
			},
		},
		"non-positive dwell": {
			Pools: map[string]PoolSpec{
				"SUB": {
					Balance:      "lead",
					BalanceDwell: Duration{D: -1 * time.Second},
					Members:      map[string]MemberSpec{"A": {Credential: "cred-a"}},
				},
			},
		},
		"gap without balance": {
			Pools: map[string]PoolSpec{
				"SUB": {
					BalanceGap: 0.15,
					Members:    map[string]MemberSpec{"A": {Credential: "cred-a"}},
				},
			},
		},
		"dwell without balance": {
			Pools: map[string]PoolSpec{
				"SUB": {
					BalanceDwell: Duration{D: 5 * time.Minute},
					Members:      map[string]MemberSpec{"A": {Credential: "cred-a"}},
				},
			},
		},
		"priority names non-member": {
			Pools: map[string]PoolSpec{
				"SUB": {
					Priority: []string{"ghost"},
					Members: map[string]MemberSpec{
						"A": {Credential: "cred-a"},
						"B": {Credential: "cred-b"},
					},
				},
			},
		},
		"priority + balance together": {
			Pools: map[string]PoolSpec{
				"SUB": {
					Priority: []string{"a"},
					Balance:  "lead",
					Members: map[string]MemberSpec{
						"A": {Credential: "cred-a"},
						"B": {Credential: "cred-b"},
					},
				},
			},
		},
		"base URL on memberless pool": {
			Pools: map[string]PoolSpec{
				"GHOST": {
					BaseURL: "https://ghost.example",
				},
				"OK": {
					Members: map[string]MemberSpec{"A": {Credential: "cred-a"}},
				},
			},
		},
		"malformed base URL": {
			Pools: map[string]PoolSpec{
				"Z_AI": {
					BaseURL: "not-a-url",
					Members: map[string]MemberSpec{"X": {Credential: "cred"}},
				},
			},
		},
		"malformed per-member URL": {
			Pools: map[string]PoolSpec{
				"Z_AI": {
					Members: map[string]MemberSpec{
						"X": {Credential: "cred", BaseURL: "not-a-url"},
					},
				},
			},
		},
		"no backends": {
			Pools: map[string]PoolSpec{},
		},
		"empty pool name": {
			Pools: map[string]PoolSpec{
				"": {Members: map[string]MemberSpec{"X": {Credential: "cred"}}},
			},
		},
		"empty member name": {
			Pools: map[string]PoolSpec{
				"A": {Members: map[string]MemberSpec{"": {Credential: "cred"}}},
			},
		},
		"collision after normalization (same pool, different nick case)": {
			Pools: map[string]PoolSpec{
				"SUB": {
					Members: map[string]MemberSpec{
						"CLAUDA": {Credential: "cred-1"},
						"clauda": {Credential: "cred-2"},
					},
				},
			},
		},
		"collision after normalization (different pool case)": {
			Pools: map[string]PoolSpec{
				"SUB":  {Members: map[string]MemberSpec{"A": {Credential: "cred-1"}}},
				"sub_": {Members: map[string]MemberSpec{"A": {Credential: "cred-2"}}},
			},
		},
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := BuildFromSpec(spec, testDefaultBaseURL)
			if err == nil {
				t.Errorf("expected error for case: %s", name)
			}
		})
	}
}

func TestBuildFromSpec_duplicatePriorityNick(t *testing.T) {
	spec := Spec{
		Pools: map[string]PoolSpec{
			"SUB": {
				Priority: []string{"a", "a"},
				Members: map[string]MemberSpec{
					"A": {Credential: "cred-a"},
				},
			},
		},
	}
	_, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err == nil {
		t.Error("expected error for duplicate priority nick")
	}
}

func TestBuildFromSpec_emptyPriorityEntry(t *testing.T) {
	spec := Spec{
		Pools: map[string]PoolSpec{
			"SUB": {
				Priority: []string{"a", ""},
				Members: map[string]MemberSpec{
					"A": {Credential: "cred-a"},
				},
			},
		},
	}
	_, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err == nil {
		t.Error("expected error for empty priority entry")
	}
}

// TestBuildFromSpec_sameNickAcrossPoolsSharesQuotaKey proves the intended
// sharing shape: a high-volume subscription declared by the same nick in two
// pools, with the identical credential, builds, and both pool members resolve
// to the same QuotaKey() — so the shared quota store becomes the cross-pool
// sharing path by construction.
func TestBuildFromSpec_sameNickAcrossPoolsSharesQuotaKey(t *testing.T) {
	spec := Spec{
		Pools: map[string]PoolSpec{
			"P1": {Members: map[string]MemberSpec{"shared": {Credential: "cred-1"}}},
			"P2": {Members: map[string]MemberSpec{"shared": {Credential: "cred-1"}}},
		},
	}
	reg, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	b1, ok := reg.ResolveIn("p1", "shared")
	if !ok {
		t.Fatal("shared not found in p1")
	}
	b2, ok := reg.ResolveIn("p2", "shared")
	if !ok {
		t.Fatal("shared not found in p2")
	}
	if b1.QuotaKey() != b2.QuotaKey() {
		t.Errorf("QuotaKey() differs across pools: %q vs %q (want identical)", b1.QuotaKey(), b2.QuotaKey())
	}
	if b1.QuotaKey() != "shared" {
		t.Errorf("QuotaKey() = %q, want shared (nick alone)", b1.QuotaKey())
	}
}

// TestBuildFromSpec_sameNickDifferentCredentialsRejected proves the
// nick⇒one-credential half of the bijection: a nick reappearing across pools
// must carry the identical credential, otherwise the same quota key would
// alias two distinct credentials and the per-nick snapshot becomes ambiguous.
func TestBuildFromSpec_sameNickDifferentCredentialsRejected(t *testing.T) {
	spec := Spec{
		Pools: map[string]PoolSpec{
			"P1": {Members: map[string]MemberSpec{"shared": {Credential: "cred-1"}}},
			"P2": {Members: map[string]MemberSpec{"shared": {Credential: "cred-2"}}},
		},
	}
	_, err := BuildFromSpec(spec, testDefaultBaseURL)
	if err == nil {
		t.Fatal("expected error for the same nick with different credentials")
	}
	msg := err.Error()
	if !strings.Contains(msg, "redeclares nick") || !strings.Contains(msg, "different credential") {
		t.Errorf("error should mention the redeclared nick and credential mismatch: %q", msg)
	}
	// Both occurrences must be named.
	if !strings.Contains(msg, "pools.P1.members.shared") || !strings.Contains(msg, "pools.P2.members.shared") {
		t.Errorf("error should name both origins: %q", msg)
	}
	// The credential values must never leak into the error.
	if strings.Contains(msg, "cred-1") || strings.Contains(msg, "cred-2") {
		t.Errorf("error must not contain a credential value: %q", msg)
	}
}

// TestBuildFromSpec_sameCredentialDifferentNicksRejected proves the
// credential⇒one-nick half of the bijection: a credential bound under two
// different nicks would still alias one physical account across two quota
// keys, reproducing the cross-pool staleness under a different shape.
func TestBuildFromSpec_sameCredentialDifferentNicksRejected(t *testing.T) {
	cases := map[string]Spec{
		"across pools": {
			Pools: map[string]PoolSpec{
				"P1": {Members: map[string]MemberSpec{"a": {Credential: "shared"}}},
				"P2": {Members: map[string]MemberSpec{"b": {Credential: "shared"}}},
			},
		},
		"within one pool": {
			Pools: map[string]PoolSpec{
				"P1": {Members: map[string]MemberSpec{
					"a": {Credential: "shared"},
					"b": {Credential: "shared"},
				}},
			},
		},
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := BuildFromSpec(spec, testDefaultBaseURL)
			if err == nil {
				t.Fatal("expected error for a credential bound under two nicks")
			}
			msg := err.Error()
			if !strings.Contains(msg, "redeclares credential") {
				t.Errorf("error should mention the redeclared credential: %q", msg)
			}
			// The credential value must never leak into the error.
			if strings.Contains(msg, "shared") {
				t.Errorf("error must not contain a credential value: %q", msg)
			}
		})
	}
}
