package persist

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
)

// TestLoad_legacyOverlayKeysIgnored proves a pre-#198 state file carrying the
// old operator-intent overlay keys (config / added_pools) loads cleanly — the
// state file is observation-only now, and Go's decoder ignores the unknown
// keys. The bootstrap migration reads them separately, once (issue #198).
func TestLoad_legacyOverlayKeysIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	legacy := `{"pools":{"auto":{"sticky":"a","exhausted":{}}},"snapshots":{},` +
		`"config":{"auto":{"disabled":["a"],"added_members":{"b":{"credential":"x","base_url":"https://e"}}}},` +
		`"added_pools":{"rt":{}}}`
	if err := os.WriteFile(path, []byte(legacy), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	state, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := state.Pools["auto"]; !ok {
		t.Errorf("observation pools not loaded: %+v", state.Pools)
	}
}

// TestLoad_missingFileStartsFresh proves a non-existent path is treated as a
// first start: empty state, no error.
func TestLoad_missingFileStartsFresh(t *testing.T) {
	state, err := Load(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Pools != nil || state.Snapshots != nil {
		t.Errorf("missing file should yield empty state, got %+v", state)
	}
}

// TestLoad_emptyPathStartsFresh proves the disabled-persistence case (empty
// path) returns empty state without touching the filesystem.
func TestLoad_emptyPathStartsFresh(t *testing.T) {
	state, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Pools != nil || state.Snapshots != nil {
		t.Errorf("empty path should yield empty state, got %+v", state)
	}
}

// TestLoad_unparseableStartsFresh proves a corrupt state file logs and starts
// fresh rather than failing startup (the package contract).
func TestLoad_unparseableStartsFresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	state, err := Load(path)
	if err != nil {
		t.Fatalf("Load should not error on unparseable file: %v", err)
	}
	if state.Pools != nil || state.Snapshots != nil {
		t.Errorf("unparseable file should yield empty state, got %+v", state)
	}
}

// TestLoad_ioErrorIsReturned proves a real read error (path is a directory) is
// surfaced to the caller rather than swallowed.
func TestLoad_ioErrorIsReturned(t *testing.T) {
	if _, err := Load(t.TempDir()); err == nil {
		t.Fatal("Load on a directory should return an error")
	}
}

// TestNewPersister_emptyPathIsNoOp proves that with persistence disabled the
// Persister never writes: MarkDirty, Run, and a final flush all do nothing and
// no file is created in the working directory.
func TestNewPersister_emptyPathIsNoOp(t *testing.T) {
	called := false
	p := NewPersister("", func() GatewayState { called = true; return GatewayState{} })
	p.MarkDirty()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Run must return immediately on a no-op persister.
	p.Run(ctx)

	if called {
		t.Error("snapFn must not be called when path is empty")
	}
	if _, err := os.Stat(".tmp"); err == nil {
		t.Error("no-op persister wrote a stray .tmp file")
	}
}

// TestMarkDirty_coalesces proves repeated MarkDirty calls never block and
// collapse to a single pending signal (the buffered cap-1 dirty channel).
func TestMarkDirty_coalesces(t *testing.T) {
	p := NewPersister(filepath.Join(t.TempDir(), "state.json"), func() GatewayState { return GatewayState{} })
	for i := 0; i < 100; i++ {
		p.MarkDirty() // must not block even with no Run draining.
	}
	if got := len(p.dirty); got != 1 {
		t.Errorf("pending signals = %d, want 1 (coalesced)", got)
	}
}

// stateWith returns a GatewayState carrying an identifiable observation entry
// so a flushed file can be distinguished from empty. The marker is a "rt" pool
// with a sticky pointer.
func stateWith(_ string) GatewayState {
	return GatewayState{Pools: map[string]auto.PoolPersistState{"rt": {Sticky: "a"}}}
}

// waitForFile polls for path to appear, failing the test if it never does.
// Polling (rather than an exact debounce-window Sleep) keeps the test stable
// under CI load.
func waitForFile(t *testing.T, path string) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("state file %q never appeared", path)
}

// TestRun_flushesAfterDebounce proves the debounce loop writes a marked-dirty
// state to disk once the window elapses.
func TestRun_flushesAfterDebounce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	p := NewPersister(path, func() GatewayState { return stateWith("https://debounce.example") })
	p.debounce = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	p.MarkDirty()
	waitForFile(t, path)

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := got.Pools["rt"]; !ok {
		t.Errorf("flushed state missing rt pool: %+v", got.Pools)
	}
}

// TestRun_finalFlushOnShutdown proves a pending mutation is persisted when the
// context is cancelled, even though the debounce timer has not fired. A long
// debounce ensures the only path that can write is the shutdown flush.
func TestRun_finalFlushOnShutdown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	p := NewPersister(path, func() GatewayState { return stateWith("https://shutdown.example") })
	p.debounce = 30 * time.Second // long enough that the timer never fires in this test

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	p.MarkDirty()
	// Wait until Run has consumed the dirty signal (and thus set pending),
	// so the subsequent cancel deterministically takes the final-flush path.
	for i := 0; i < 400 && len(p.dirty) != 0; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	waitForFile(t, path)

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := got.Pools["rt"]; !ok {
		t.Errorf("final-flush state missing rt pool: %+v", got.Pools)
	}
}

// TestRun_flushesBufferedDirtyOnShutdown covers the shutdown select race
// (issue #201): a dirty signal that is buffered but not yet promoted to
// pending must still be flushed when ctx.Done() and dirty are simultaneously
// ready. Go's select is uniform-random, so without the drain in the
// ctx.Done() branch this drops ~50% of the time; the loop amplifies that to a
// near-certain failure without the fix, while the fix makes it always flush.
func TestRun_flushesBufferedDirtyOnShutdown(t *testing.T) {
	for i := 0; i < 64; i++ {
		path := filepath.Join(t.TempDir(), "state.json")
		p := NewPersister(path, func() GatewayState { return stateWith("") })
		p.debounce = 30 * time.Second // never fires; only the shutdown path can flush

		ctx, cancel := context.WithCancel(context.Background())
		// Buffer a dirty signal and cancel BEFORE Run starts, so Run's first
		// select sees both ctx.Done() and dirty ready at once.
		p.MarkDirty()
		cancel()

		done := make(chan struct{})
		go func() { p.Run(ctx); close(done) }()
		<-done

		if _, err := os.Stat(path); err != nil {
			t.Fatalf("iteration %d: buffered dirty dropped at shutdown, file not written: %v", i, err)
		}
	}
}

// TestFlush_atomicAnd0600 proves flush writes at mode 0600 and leaves no
// leftover temp file after the rename.
func TestFlush_atomicAnd0600(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	p := NewPersister(path, func() GatewayState { return stateWith("https://flush.example") })
	p.flush()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("state file mode = %o, want 600", perm)
	}
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Error("flush left a stray .tmp file behind")
	}
}
