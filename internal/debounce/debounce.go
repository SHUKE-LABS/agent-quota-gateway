// Package debounce provides a single shared debounced-flush loop used by both
// the config-file writer (internal/configfile) and the state persister
// (internal/persist). Both coalesce disk writes at a fixed rate and flush once
// more on shutdown; before this package each carried a line-for-line identical
// copy of that loop, so a correctness fix (e.g. the issue #201 shutdown drain,
// PR #204) had to be written twice and the copies could silently diverge
// (issue #210).
//
// The owner supplies only its flush() payload; this type owns the debounce
// timing, the dirty signal, and the shutdown-drain semantics.
package debounce

import "time"

// DefaultDebounce is the maximum write rate when callers are continuously
// marking dirty. A chatty proxy path (many requests/s) produces at most one
// flush per DefaultDebounce interval.
const DefaultDebounce = 200 * time.Millisecond

// Flusher coalesces flush requests with a debounce window. Build with New;
// call Run in a goroutine sharing the shutdown context and MarkDirty whenever
// the underlying state changes. When disabled (enabled == false, e.g. an empty
// output path) MarkDirty is a no-op and Run returns immediately, so the owner
// is a complete no-op with nothing written to disk.
type Flusher struct {
	flush    func()
	dirty    chan struct{}
	debounce time.Duration
	enabled  bool
}

// New returns a Flusher that invokes flush after the debounce window elapses.
// flush is called only from the Run goroutine (never from MarkDirty), so it
// must be safe for concurrent use with the objects it reads. When enabled is
// false the Flusher is inert.
func New(enabled bool, debounce time.Duration, flush func()) *Flusher {
	return &Flusher{
		flush:    flush,
		dirty:    make(chan struct{}, 1),
		debounce: debounce,
		enabled:  enabled,
	}
}

// MarkDirty signals that the underlying state changed. The call is
// non-blocking: if a flush is already pending it is absorbed into the size-1
// buffered channel. Safe to call while holding any unrelated lock. No-op when
// disabled.
func (f *Flusher) MarkDirty() {
	if !f.enabled {
		return
	}
	select {
	case f.dirty <- struct{}{}:
	default:
	}
}

// Run drives the debounced flush loop until ctx is done, then performs one
// final flush so any change observed up to context cancellation is persisted.
// Callers cancel this context only after the HTTP server has drained, so a
// change acked during the shutdown grace window is still captured (issue #201).
func (f *Flusher) Run(ctx interface{ Done() <-chan struct{} }) {
	if !f.enabled {
		return
	}
	var pending bool
	var deadline time.Time

	for {
		var waitCh <-chan time.Time
		if pending {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				f.flush()
				pending = false
				continue
			}
			waitCh = time.After(remaining)
		}

		select {
		case <-ctx.Done():
			// A dirty signal may be buffered but not yet promoted to pending
			// (Go's select is uniform-random, so ctx.Done() can win over a
			// simultaneously-ready dirty). Drain it so the final flush is not
			// skipped, dropping the last mutation before shutdown (issue #201).
			select {
			case <-f.dirty:
				pending = true
			default:
			}
			if pending {
				f.flush()
			}
			return
		case <-f.dirty:
			if !pending {
				deadline = time.Now().Add(f.debounce)
				pending = true
			}
		case <-waitCh:
			f.flush()
			pending = false
		}
	}
}
