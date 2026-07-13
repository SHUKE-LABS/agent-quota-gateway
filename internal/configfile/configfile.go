// Package configfile loads gateway and backend configuration from a JSON
// file. It provides precedence resolution (flag > env > default path),
// permission checks (0600), and decoding into the config.Inputs and
// backend.Spec types that both paths share.
package configfile

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
	"github.com/shukebeta/agent-quota-gateway/internal/debounce"
)

const (
	// EnvConfigPath is the environment variable that holds the path to the
	// config file. The --config flag takes precedence.
	EnvConfigPath = "AQG_CONFIG"

	// DefaultConfigPath is the file name looked for in the current working
	// directory when neither the flag nor AQG_CONFIG are set.
	DefaultConfigPath = "aqg.json"
)

// Resolve implements the precedence order for config file discovery:
// (1) the --config flag value if non-empty; (2) AQG_CONFIG env if set;
// (3) ./aqg.json if it exists; (4) otherwise use env vars (useFile=false).
func Resolve(flagVal string) (path string, useFile bool) {
	// Flag takes highest precedence.
	if flagVal != "" {
		return flagVal, true
	}

	// Next, the env var.
	if envPath, ok := os.LookupEnv(EnvConfigPath); ok && envPath != "" {
		return envPath, true
	}

	// Finally, the default path in the current directory.
	if fi, err := os.Stat(DefaultConfigPath); err == nil && !fi.IsDir() {
		return DefaultConfigPath, true
	}

	// No file found; fall back to env.
	return "", false
}

// LoadFile reads and decodes a JSON config file from path, returning the
// resolved config.Config and backend.Registry. It fails closed on any
// error (unreadable file, wrong permissions, malformed JSON, validation
// failure). No env fallback — the caller must have already decided to
// use the file path via Resolve.
func LoadFile(path string) (config.Config, *backend.Registry, error) {
	// Check file permissions: must be 0600 (no group/other access).
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return config.Config{}, nil, fmt.Errorf("config file %q does not exist", path)
		}
		return config.Config{}, nil, fmt.Errorf("config file %q: %w", path, err)
	}
	mode := fi.Mode().Perm()
	if mode&0o077 != 0 {
		return config.Config{}, nil, fmt.Errorf("config file %q must be 0600 (no group/other access); has mode %#o", path, mode)
	}

	// Read and decode JSON.
	f, err := os.Open(path)
	if err != nil {
		return config.Config{}, nil, fmt.Errorf("config file %q: %w", path, err)
	}
	defer f.Close()

	var dto fileDTO
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields() // fail closed on typos
	if err := dec.Decode(&dto); err != nil {
		return config.Config{}, nil, fmt.Errorf("config file %q: %w", path, err)
	}

	// Map DTO to config.Inputs.
	cfgInputs := config.Inputs{
		AnthropicBaseURL: dto.BaseURL,
		ListenAddr:       dto.ListenAddr,
		SharedListenAddr: dto.SharedListenAddr,
		StateFile:        dto.StateFile,
	}
	cfg, err := config.Build(cfgInputs)
	if err != nil {
		return config.Config{}, nil, fmt.Errorf("config: %w", err)
	}

	// Map DTO to backend.Spec.
	spec := backend.Spec{Pools: make(map[string]backend.PoolSpec, len(dto.Pools))}
	for poolKey, poolDTO := range dto.Pools {
		poolSpec := backend.PoolSpec{
			BaseURL:      poolDTO.BaseURL,
			Members:      make(map[string]backend.MemberSpec, len(poolDTO.Members)),
			Priority:     poolDTO.Priority,
			Balance:      poolDTO.Balance,
			BalanceGap:   poolDTO.BalanceGap,
			BalanceDwell: backend.Duration{D: poolDTO.BalanceDwell.D},
		}
		for nickKey, memberDTO := range poolDTO.Members {
			poolSpec.Members[nickKey] = backend.MemberSpec{
				Credential: memberDTO.Credential,
				BaseURL:    memberDTO.BaseURL,
				Disabled:   memberDTO.Disabled,
			}
		}
		spec.Pools[poolKey] = poolSpec
	}

	registry, err := backend.BuildFromSpec(spec, cfg.AnthropicBaseURL)
	if err != nil {
		return config.Config{}, nil, fmt.Errorf("backend spec: %w", err)
	}

	return cfg, registry, nil
}

// Marshal serializes the effective gateway config plus registry back to the
// aqg.json wire shape. Config is the single source of truth for operator
// intent (issue #198), so every UI mutation round-trips to disk through this.
//
// Credentials are written in full: the config file IS the credential store,
// protected at 0600 (see the Writer). View redaction on /_gateway/config is a
// separate concern. A pool whose effective base_url equals the gateway default
// is written with an empty base_url (inherits), and a member that inherited
// its pool's default carries an empty per-member base_url, keeping the file
// shape clean.
func Marshal(cfg config.Config, reg *backend.Registry) ([]byte, error) {
	dto := fileDTO{
		BaseURL:   cfg.AnthropicBaseURL,
		StateFile: cfg.StateFile,
		Pools:     make(map[string]poolDTO),
	}
	// cfg.ListenAddr holds the Tailscale address when shared mode is active,
	// otherwise the loopback bind. Route it to the matching field so the file
	// round-trips through config.Build's mutual-exclusivity check.
	if cfg.Shared {
		dto.SharedListenAddr = cfg.ListenAddr
	} else {
		dto.ListenAddr = cfg.ListenAddr
	}

	spec := reg.Spec()
	for name, ps := range spec.Pools {
		pd := poolDTO{
			Members:      make(map[string]memberDTO, len(ps.Members)),
			Priority:     ps.Priority,
			Balance:      ps.Balance,
			BalanceGap:   ps.BalanceGap,
			BalanceDwell: backend.Duration{D: ps.BalanceDwell.D},
		}
		if ps.BaseURL != cfg.AnthropicBaseURL {
			pd.BaseURL = ps.BaseURL
		}
		for nick, m := range ps.Members {
			pd.Members[nick] = memberDTO{
				Credential: m.Credential,
				BaseURL:    m.BaseURL,
				Disabled:   m.Disabled,
			}
		}
		dto.Pools[name] = pd
	}
	return json.MarshalIndent(&dto, "", "  ")
}

// Writer coalesces config-file writes with a debounce window and writes
// atomically (temp-file + rename at 0600). An operator mutation calls
// MarkDirty; the shared debounce loop (internal/debounce, issue #210)
// re-serializes the whole config from snapFn and flushes. When path is empty
// (pure env-mode local dev, no config file) the Writer is a no-op — nothing is
// written to disk.
//
// Write-failure semantics (issue #198, decision 3): a failed flush does not
// roll back the in-memory mutation (which already took effect). It logs
// loudly and sets Unsaved so /_gateway/health and /_gateway/config can
// surface that on-disk config lags memory until the next successful flush.
type Writer struct {
	path    string
	snapFn  func() ([]byte, error)
	flusher *debounce.Flusher

	mu      sync.Mutex
	unsaved bool
}

// NewWriter returns a Writer that serializes to path via snapFn. snapFn is
// called from the Run goroutine, so it must be safe for concurrent use with
// the registry it reads. An empty path makes the Writer a no-op.
func NewWriter(path string, snapFn func() ([]byte, error)) *Writer {
	w := &Writer{
		path:   path,
		snapFn: snapFn,
	}
	w.flusher = debounce.New(path != "", debounce.DefaultDebounce, w.flush)
	return w
}

// MarkDirty signals that operator intent changed. Non-blocking: a pending
// flush absorbs it. No-op when path is empty.
func (w *Writer) MarkDirty() { w.flusher.MarkDirty() }

// Unsaved reports whether the last flush failed and on-disk config is behind
// the in-memory registry.
func (w *Writer) Unsaved() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.unsaved
}

// Run drives the debounced flush loop until ctx is done, then performs one
// final flush so any mutation observed up to context cancellation is
// persisted. The caller (main.run) cancels this context only after the HTTP
// server has drained, so a mutation acked 200 during the shutdown grace
// window is still captured by the final flush (issue #201).
func (w *Writer) Run(ctx interface{ Done() <-chan struct{} }) { w.flusher.Run(ctx) }

// flush atomically writes the current config to disk at 0600.
func (w *Writer) flush() {
	data, err := w.snapFn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configfile: marshal: %v\n", err)
		w.setUnsaved(true)
		return
	}
	tmp := w.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		fmt.Fprintf(os.Stderr, "configfile: write %q: %v\n", tmp, err)
		w.setUnsaved(true)
		return
	}
	if err := os.Rename(tmp, w.path); err != nil {
		fmt.Fprintf(os.Stderr, "configfile: rename %q -> %q: %v\n", tmp, w.path, err)
		_ = os.Remove(tmp)
		w.setUnsaved(true)
		return
	}
	w.setUnsaved(false)
}

func (w *Writer) setUnsaved(v bool) {
	w.mu.Lock()
	w.unsaved = v
	w.mu.Unlock()
}

// WriteAtomic writes data to path atomically at 0600 (temp-file + rename).
// Used by the one-time bootstrap that generates aqg.json from env + state on
// first deploy (issue #198), before the Writer/Run loop exists.
func WriteAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// fileDTO is the JSON shape of a config file. All fields are optional
// except the required structure (at least one pool with at least one member).
// Empty strings for BaseURL/Members[].BaseURL mean "use default".
type fileDTO struct {
	// BaseURL is the upstream URL. Empty string uses the gateway default.
	BaseURL string `json:"base_url"`

	// ListenAddr is the loopback bind address. Empty string uses the default.
	ListenAddr string `json:"listen_addr"`

	// SharedListenAddr opts into shared mode (Tailscale binding).
	SharedListenAddr string `json:"shared_listen_addr"`

	// StateFile is the path for the persistent state file. Empty disables it.
	StateFile string `json:"state_file"`

	// Pools maps pool names to their specs.
	Pools map[string]poolDTO `json:"pools"`
}

// poolDTO is one pool's configuration from the file.
type poolDTO struct {
	BaseURL      string               `json:"base_url"`
	Members      map[string]memberDTO `json:"members"`
	Priority     []string             `json:"priority"`
	Balance      string               `json:"balance"`
	BalanceGap   float64              `json:"balance_gap"`
	BalanceDwell backend.Duration     `json:"balance_dwell"`
}

// memberDTO is one backend's credential and optional base URL override.
type memberDTO struct {
	Credential string `json:"credential"`
	BaseURL    string `json:"base_url,omitempty"`
	// Disabled is operator intent (issue #198): a disabled member stays in
	// the config but is never selected until re-enabled. Persisted here, not
	// in a state-file overlay.
	Disabled bool `json:"disabled,omitempty"`
}
