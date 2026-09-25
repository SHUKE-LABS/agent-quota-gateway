package configfile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
	"github.com/shukebeta/agent-quota-gateway/internal/debounce"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Build(config.Inputs{AnthropicBaseURL: "https://api.anthropic.com"})
	if err != nil {
		t.Fatalf("config.Build: %v", err)
	}
	return cfg
}

func testRegistry(t *testing.T) *backend.Registry {
	t.Helper()
	reg, err := backend.BuildFromSpec(backend.Spec{Pools: map[string]backend.PoolSpec{
		"auto": {
			Members: map[string]backend.MemberSpec{
				"a": {Credential: "sk-ant-oat-a"},
				"b": {Credential: "sk-ant-oat-b", Disabled: true},
			},
			Priority: []string{"a", "b"},
		},
		"z-ai": {
			BaseURL: "https://open.example/anthropic",
			Members: map[string]backend.MemberSpec{
				"x": {Credential: "vendor-x", BaseURL: "https://mirror.example/anthropic"},
			},
			Balance:      "lead",
			BalanceGap:   0.2,
			BalanceDwell: backend.Duration{D: 10 * time.Minute},
		},
	}}, "https://api.anthropic.com")
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	return reg
}

func TestMarshal_roundTripsThroughLoadFile(t *testing.T) {
	cfg := testConfig(t)
	reg := testRegistry(t)

	data, err := Marshal(cfg, reg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	path := filepath.Join(t.TempDir(), "aqg.json")
	if err := WriteAtomic(path, data); err != nil {
		t.Fatalf("WriteAtomic: %v", err)
	}
	// WriteAtomic must land at 0600.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("written config mode = %#o, want 0600", fi.Mode().Perm())
	}

	_, reg2, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile round-trip: %v", err)
	}
	for _, p := range []string{"auto", "z-ai"} {
		for _, n := range reg.PoolNicks(p) {
			b1, _ := reg.ResolveIn(p, n)
			b2, ok := reg2.ResolveIn(p, n)
			if !ok || b1 != b2 {
				t.Errorf("%s/%s mismatch after round-trip: %+v vs %+v (ok=%v)", p, n, b1, b2, ok)
			}
		}
	}
	// Disabled flag survives.
	if b, _ := reg2.ResolveIn("auto", "b"); !b.Disabled {
		t.Error("disabled flag lost through Marshal→LoadFile")
	}
	// Balance config survives.
	if reg2.PoolBalanceGap("z-ai") != 0.2 || reg2.PoolBalanceDwell("z-ai") != 10*time.Minute {
		t.Errorf("balance config lost: gap=%v dwell=%v", reg2.PoolBalanceGap("z-ai"), reg2.PoolBalanceDwell("z-ai"))
	}
}

func TestMarshal_preservesNonDefaultConcurrencyAndOmitsDefault(t *testing.T) {
	cfg := testConfig(t)
	concurrency := 2
	reg, err := backend.BuildFromSpec(backend.Spec{Pools: map[string]backend.PoolSpec{
		"auto": {
			Concurrency: &concurrency,
			Members: map[string]backend.MemberSpec{
				"a": {Credential: "cred-a"},
				"b": {Credential: "cred-b"},
			},
		},
	}}, cfg.AnthropicBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec: %v", err)
	}
	reg, err = reg.WithMemberSet("auto", "c", "cred-c", "", false)
	if err != nil {
		t.Fatalf("WithMemberSet: %v", err)
	}
	data, err := Marshal(cfg, reg)
	if err != nil {
		t.Fatalf("Marshal non-default: %v", err)
	}
	if !strings.Contains(string(data), `"concurrency": 2`) {
		t.Fatalf("Marshal omitted non-default concurrency: %s", data)
	}
	path := filepath.Join(t.TempDir(), "aqg.json")
	if err := WriteAtomic(path, data); err != nil {
		t.Fatalf("WriteAtomic: %v", err)
	}
	_, restored, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile after copy-on-write: %v", err)
	}
	if got := restored.PoolConcurrency("auto"); got != 2 {
		t.Errorf("restored concurrency = %d, want 2", got)
	}

	defaultData, err := Marshal(cfg, testRegistry(t))
	if err != nil {
		t.Fatalf("Marshal default: %v", err)
	}
	if strings.Contains(string(defaultData), `"concurrency"`) {
		t.Errorf("Marshal should omit default concurrency: %s", defaultData)
	}
	one := 1
	oneRegistry, err := backend.BuildFromSpec(backend.Spec{Pools: map[string]backend.PoolSpec{
		"auto": {
			Concurrency: &one,
			Members:     map[string]backend.MemberSpec{"a": {Credential: "cred-a"}},
		},
	}}, cfg.AnthropicBaseURL)
	if err != nil {
		t.Fatalf("BuildFromSpec explicit default: %v", err)
	}
	oneData, err := Marshal(cfg, oneRegistry)
	if err != nil {
		t.Fatalf("Marshal explicit default: %v", err)
	}
	if strings.Contains(string(oneData), `"concurrency"`) {
		t.Errorf("Marshal should omit explicit concurrency 1: %s", oneData)
	}
}

func TestWriter_debouncedFlushAndUnsaved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aqg.json")
	cfg := testConfig(t)
	reg := testRegistry(t)

	w := NewWriter(path, func() ([]byte, error) { return Marshal(cfg, reg) })
	w.flusher = debounce.New(true, 5*time.Millisecond, w.flush)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	w.MarkDirty()
	// Poll for the flush to land.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("config file not written after MarkDirty")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if w.Unsaved() {
		t.Error("Unsaved should be false after a successful flush")
	}

	cancel()
	<-done
}

// TestWriter_flushesBufferedDirtyOnShutdown covers the shutdown select race
// (issue #201): a dirty signal buffered but not yet promoted to pending must
// still be flushed when ctx.Done() and dirty are simultaneously ready. Losing
// it drops an operator mutation — including a runtime-added credential — from
// aqg.json on the next boot. Go's select is uniform-random, so without the
// drain in the ctx.Done() branch this fails ~50% per iteration; the loop makes
// a without-fix failure near-certain while the fix always flushes.
func TestWriter_flushesBufferedDirtyOnShutdown(t *testing.T) {
	cfg := testConfig(t)
	reg := testRegistry(t)
	for i := 0; i < 64; i++ {
		path := filepath.Join(t.TempDir(), "aqg.json")
		w := NewWriter(path, func() ([]byte, error) { return Marshal(cfg, reg) })
		w.flusher = debounce.New(true, 30*time.Second, w.flush) // never fires; only the shutdown path can flush

		ctx, cancel := context.WithCancel(context.Background())
		// Buffer a dirty signal and cancel BEFORE Run starts, so Run's first
		// select sees both ctx.Done() and dirty ready at once.
		w.MarkDirty()
		cancel()

		done := make(chan struct{})
		go func() { w.Run(ctx); close(done) }()
		<-done

		if _, err := os.Stat(path); err != nil {
			t.Fatalf("iteration %d: buffered dirty dropped at shutdown, config not written: %v", i, err)
		}
	}
}

func TestWriter_emptyPathIsNoOp(t *testing.T) {
	w := NewWriter("", func() ([]byte, error) { return []byte("{}"), nil })
	w.MarkDirty() // must not panic or block
	if w.Unsaved() {
		t.Error("no-op writer should never report unsaved")
	}
}
