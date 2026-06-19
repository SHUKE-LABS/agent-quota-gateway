// Package persist handles atomic read/write of gateway state across restarts.
//
// GatewayState is the on-disk JSON record: per-pool routing state (sticky
// nick + exhausted map) and per-backend quota snapshots. A single
// Persister goroutine debounces writes so the proxy hot path is never
// blocked on I/O — callers just call MarkDirty() (non-blocking channel
// send) and the persister coalesces flushes at most once per 200ms.
//
// Atomic write is temp-file + rename so a crash mid-write never leaves a
// torn JSON. A missing or unparseable state file logs and starts fresh
// rather than failing startup.
package persist

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// defaultDebounce is the maximum write rate when callers are continuously
// marking dirty. A chatty proxy path (many requests/s) produces at most
// one write per defaultDebounce interval.
const defaultDebounce = 200 * time.Millisecond

// GatewayState is the complete on-disk JSON record.
type GatewayState struct {
	Pools     map[string]auto.PoolPersistState `json:"pools"`
	Snapshots map[string]quota.Snapshot        `json:"snapshots"`
	// Config is the runtime configuration overlay: per-pool priority
	// overrides and disabled members. nil when no runtime config is set.
	// Stored separately from Pools because it's mutable at runtime via
	// the /_gateway/config API, while Pools carries the sticky/exhausted
	// routing state.
	Config map[string]auto.PoolRuntimeConfig `json:"config,omitempty"`
}

// Load reads the state file at path. A missing file returns an empty
// GatewayState (first start). An unparseable file logs and also returns
// empty. Any other I/O error is returned to the caller.
func Load(path string) (GatewayState, error) {
	if path == "" {
		return GatewayState{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return GatewayState{}, nil
		}
		return GatewayState{}, err
	}
	var s GatewayState
	if err := json.Unmarshal(data, &s); err != nil {
		fmt.Fprintf(os.Stderr, "persist: ignoring unparseable state file %q: %v\n", path, err)
		return GatewayState{}, nil
	}
	return s, nil
}

// Persister coalesces state writes with a debounce window. Build with
// NewPersister; call Run in a goroutine sharing the shutdown context.
type Persister struct {
	path     string
	snapFn   func() GatewayState
	dirty    chan struct{}
	debounce time.Duration
}

// NewPersister returns a Persister that writes to path using snapFn to
// capture the current state. snapFn is called from the Run goroutine
// (not from MarkDirty), so it must be safe for concurrent use with the
// objects it reads. When path is empty the persister is a no-op.
func NewPersister(path string, snapFn func() GatewayState) *Persister {
	return &Persister{
		path:     path,
		snapFn:   snapFn,
		dirty:    make(chan struct{}, 1),
		debounce: defaultDebounce,
	}
}

// MarkDirty signals that state has changed. The call is non-blocking: if
// a flush is already pending it is absorbed. Safe to call while holding
// any unrelated lock.
func (p *Persister) MarkDirty() {
	if p.path == "" {
		return
	}
	select {
	case p.dirty <- struct{}{}:
	default:
	}
}

// Run drives the debounced flush loop until ctx is done, then performs
// one final flush so the last mutation before shutdown is persisted.
// Callers start it in a goroutine that shares the process shutdown context.
func (p *Persister) Run(ctx interface{ Done() <-chan struct{} }) {
	if p.path == "" {
		return
	}
	var pending bool
	var deadline time.Time

	for {
		var waitCh <-chan time.Time
		if pending {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				p.flush()
				pending = false
				continue
			}
			waitCh = time.After(remaining)
		}

		select {
		case <-ctx.Done():
			if pending {
				p.flush()
			}
			return
		case <-p.dirty:
			if !pending {
				deadline = time.Now().Add(p.debounce)
				pending = true
			}
		case <-waitCh:
			p.flush()
			pending = false
		}
	}
}

// flush atomically writes the current state to disk.
func (p *Persister) flush() {
	state := p.snapFn()
	data, err := json.Marshal(state)
	if err != nil {
		fmt.Fprintf(os.Stderr, "persist: marshal: %v\n", err)
		return
	}
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		fmt.Fprintf(os.Stderr, "persist: write %q: %v\n", tmp, err)
		return
	}
	if err := os.Rename(tmp, p.path); err != nil {
		fmt.Fprintf(os.Stderr, "persist: rename %q -> %q: %v\n", tmp, p.path, err)
		_ = os.Remove(tmp)
	}
}
