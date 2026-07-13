package debounce

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runInBackground starts f.Run under a fresh cancellable context and returns
// the cancel func plus a channel closed when Run returns.
func runInBackground(f *Flusher) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.Run(ctx)
		close(done)
	}()
	return cancel, done
}

func waitClosed(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

// A disabled Flusher never flushes and Run returns immediately.
func TestFlusher_disabledIsNoOp(t *testing.T) {
	var calls int32
	f := New(false, DefaultDebounce, func() { atomic.AddInt32(&calls, 1) })

	f.MarkDirty() // must not panic and must not enqueue anything

	done := make(chan struct{})
	go func() { f.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run on a disabled Flusher should return immediately")
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("disabled Flusher flushed %d times, want 0", got)
	}
}

// MarkDirty never blocks even with no Run draining: the size-1 buffered
// channel absorbs a burst, coalescing it to a single pending signal.
func TestFlusher_markDirtyNeverBlocks(t *testing.T) {
	f := New(true, DefaultDebounce, func() {})
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			f.MarkDirty() // no Run is draining f.dirty
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MarkDirty blocked with no drainer")
	}
	if got := len(f.dirty); got != 1 {
		t.Fatalf("buffered signals = %d, want 1 (coalesced)", got)
	}
}

// Rapid MarkDirty calls within one window coalesce into a single flush; a
// later dirty after the window produces a second.
func TestFlusher_coalesces(t *testing.T) {
	var calls int32
	f := New(true, 30*time.Millisecond, func() { atomic.AddInt32(&calls, 1) })
	cancel, done := runInBackground(f)
	defer waitClosed(t, done)
	defer cancel()

	for i := 0; i < 5; i++ {
		f.MarkDirty()
	}
	time.Sleep(120 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("after a burst within one window: %d flushes, want 1", got)
	}

	f.MarkDirty()
	time.Sleep(120 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("after a second window: %d flushes, want 2", got)
	}
}

// A dirty buffered when the context is already cancelled is still flushed on
// shutdown — the issue #201 / PR #204 select-race drain. Using an
// already-cancelled context makes the ctx.Done() branch win deterministically,
// so the buffered dirty must be drained and flushed exactly once.
func TestFlusher_drainsBufferedDirtyOnCancel(t *testing.T) {
	var calls int32
	// A long debounce guarantees the waitCh timer never fires, so the only
	// path to a flush is the shutdown drain.
	f := New(true, time.Hour, func() { atomic.AddInt32(&calls, 1) })

	f.MarkDirty() // buffer one dirty before Run ever observes it

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Done() already closed when Run starts
	f.Run(ctx)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("buffered dirty on cancel flushed %d times, want 1", got)
	}
}

// With nothing pending and no buffered dirty, cancel triggers no final flush.
func TestFlusher_noFlushWhenClean(t *testing.T) {
	var calls int32
	f := New(true, time.Hour, func() { atomic.AddInt32(&calls, 1) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.Run(ctx)

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("clean shutdown flushed %d times, want 0", got)
	}
}

// flush is invoked only from the Run goroutine, never from MarkDirty, so a
// caller may hold an unrelated lock across MarkDirty without deadlocking.
func TestFlusher_markDirtyDoesNotCallFlush(t *testing.T) {
	var mu sync.Mutex
	var calls int32
	f := New(true, time.Hour, func() {
		mu.Lock() // would deadlock if MarkDirty called flush inline under the lock
		calls++
		mu.Unlock()
	})
	cancel, done := runInBackground(f)
	defer waitClosed(t, done)
	defer cancel()

	mu.Lock()
	f.MarkDirty()
	mu.Unlock()

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("MarkDirty invoked flush synchronously (%d calls)", got)
	}
}
