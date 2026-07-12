package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
	"github.com/shukebeta/agent-quota-gateway/internal/configfile"
)

// resolveConfig implements the issue #198 config-source resolution.
//
//   - No config path resolves (no --config, no AQG_CONFIG, no ./aqg.json):
//     pure env-mode local dev — build from env, keep zero credentials on disk,
//     no config writer (returned configPath is "").
//   - A config path resolves and the file exists: read it, ignore AQG_POOL_*
//     env entirely.
//   - A config path resolves but the file is absent (first deploy): generate it
//     once by merging env (backend.Load) with the legacy state-file overlay
//     using state-wins precedence, write it 0600, then read it back. Env is
//     never consulted again on subsequent starts.
//
// It returns the effective config, the registry, and the config path the
// writer should flush to ("" disables write-through).
func resolveConfig(configFlag string, logOut io.Writer) (config.Config, *backend.Registry, string, error) {
	path, useFile := configfile.Resolve(configFlag)
	if !useFile {
		cfg, err := config.Load()
		if err != nil {
			return config.Config{}, nil, "", fmt.Errorf("config: %w", err)
		}
		reg, err := backend.Load(cfg.AnthropicBaseURL)
		if err != nil {
			return config.Config{}, nil, "", fmt.Errorf("backend: %w", err)
		}
		return cfg, reg, "", nil
	}

	if _, err := os.Stat(path); err == nil {
		cfg, reg, err := configfile.LoadFile(path)
		return cfg, reg, path, err
	} else if !errors.Is(err, fs.ErrNotExist) {
		return config.Config{}, nil, "", fmt.Errorf("config file %q: %w", path, err)
	}

	// First deploy: the resolved config path does not exist yet. Bootstrap it
	// from env + the legacy state overlay, then read it back.
	if err := bootstrapConfigFile(path, logOut); err != nil {
		return config.Config{}, nil, "", err
	}
	cfg, reg, err := configfile.LoadFile(path)
	return cfg, reg, path, err
}

// bootstrapConfigFile generates aqg.json at path by merging the env-declared
// backends with any operator mutations recorded in the legacy state file
// (state-wins precedence, per issue #198 decision 1), then writes it 0600.
func bootstrapConfigFile(path string, logOut io.Writer) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	reg, err := backend.Load(cfg.AnthropicBaseURL)
	if err != nil {
		return fmt.Errorf("backend: %w", err)
	}
	if legacy, ok := loadLegacyOverlay(cfg.StateFile); ok {
		reg = applyLegacyOverlay(reg, legacy, logOut)
	}
	data, err := configfile.Marshal(cfg, reg)
	if err != nil {
		return fmt.Errorf("bootstrap config marshal: %w", err)
	}
	if err := configfile.WriteAtomic(path, data); err != nil {
		return fmt.Errorf("bootstrap config write %q: %w", path, err)
	}
	fmt.Fprintf(logOut, "agent-quota-gateway: bootstrapped %s from env + state file (env is no longer consulted after this)\n", path)
	return nil
}

// legacyState decodes only the operator-intent overlay from a pre-#198 state
// file. The live persist.GatewayState no longer carries these keys; this
// struct exists solely for the one-time bootstrap migration. Unknown fields
// (pools, snapshots, ...) are ignored.
type legacyState struct {
	Config     map[string]legacyPoolConfig `json:"config"`
	AddedPools map[string]json.RawMessage  `json:"added_pools"`
}

type legacyPoolConfig struct {
	PriorityOverride []string                `json:"priority_override"`
	Disabled         []string                `json:"disabled"`
	AddedMembers     map[string]legacyMember `json:"added_members"`
	RemovedMembers   []string                `json:"removed_members"`
}

type legacyMember struct {
	Credential string `json:"credential"`
	BaseURL    string `json:"base_url"`
}

// loadLegacyOverlay reads the operator-intent overlay from the state file at
// path. ok is false when there is nothing to migrate (no path, missing file,
// or no overlay keys). A parse error is treated as "nothing to migrate" — the
// bootstrap falls back to env-only, matching the state file's own
// tolerate-and-continue policy.
func loadLegacyOverlay(path string) (legacyState, bool) {
	if path == "" {
		return legacyState{}, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return legacyState{}, false
	}
	var ls legacyState
	if err := json.Unmarshal(data, &ls); err != nil {
		return legacyState{}, false
	}
	if len(ls.Config) == 0 && len(ls.AddedPools) == 0 {
		return legacyState{}, false
	}
	return ls, true
}

// applyLegacyOverlay layers the legacy overlay onto the env registry with
// state-wins precedence, using the same copy-on-write mutators the runtime
// uses. Best-effort: a mutation that fails validation (e.g. a corrupt entry)
// is logged and skipped rather than aborting the bootstrap.
func applyLegacyOverlay(reg *backend.Registry, ls legacyState, logOut io.Writer) *backend.Registry {
	// mut applies a copy-on-write result, logging+skipping on validation error.
	mut := func(what string, next *backend.Registry, err error) *backend.Registry {
		if err != nil {
			fmt.Fprintf(logOut, "agent-quota-gateway: bootstrap: skipping %s: %v\n", what, err)
			return reg
		}
		return next
	}

	// Runtime-created pools first, so member/priority mutations can target them.
	for rawName := range ls.AddedPools {
		name := backend.NormalizeName(rawName)
		if name == "" || reg.HasPool(name) {
			continue
		}
		next, err := reg.WithPoolCreated(name)
		reg = mut("create pool "+name, next, err)
	}

	for rawPool, pc := range ls.Config {
		pool := backend.NormalizeName(rawPool)
		if !reg.HasPool(pool) {
			continue // overlay references a pool no longer in env and not runtime-created
		}
		disabled := make(map[string]bool, len(pc.Disabled))
		for _, n := range pc.Disabled {
			disabled[backend.NormalizeName(n)] = true
		}
		// Members: state credential/base_url win; bring in members absent from env.
		for rawNick, m := range pc.AddedMembers {
			nick := backend.NormalizeName(rawNick)
			base := m.BaseURL
			if base == "" {
				if b, ok := reg.ResolveIn(pool, nick); ok {
					base = b.BaseURL
				} else {
					fmt.Fprintf(logOut, "agent-quota-gateway: bootstrap: skipping member %s/%s: no base_url\n", pool, nick)
					continue
				}
			}
			next, err := reg.WithMemberSet(pool, nick, m.Credential, base, disabled[nick])
			reg = mut("set member "+pool+"/"+nick, next, err)
		}
		// Removals.
		for _, rawNick := range pc.RemovedMembers {
			nick := backend.NormalizeName(rawNick)
			if _, ok := reg.ResolveIn(pool, nick); ok {
				next, err := reg.WithMemberRemoved(pool, nick)
				reg = mut("remove member "+pool+"/"+nick, next, err)
			}
		}
		// Priority (filtered to surviving members).
		if len(pc.PriorityOverride) > 0 {
			order := make([]string, 0, len(pc.PriorityOverride))
			for _, rawNick := range pc.PriorityOverride {
				nick := backend.NormalizeName(rawNick)
				if _, ok := reg.ResolveIn(pool, nick); ok {
					order = append(order, nick)
				}
			}
			if len(order) > 0 {
				next, err := reg.WithPriority(pool, order)
				reg = mut("priority for "+pool, next, err)
			}
		}
		// Disabled (idempotent; covers members not carried in AddedMembers).
		for nick := range disabled {
			if _, ok := reg.ResolveIn(pool, nick); ok {
				next, err := reg.WithMemberDisabled(pool, nick, true)
				reg = mut("disable "+pool+"/"+nick, next, err)
			}
		}
	}
	return reg
}
