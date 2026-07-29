package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"

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
		if err != nil {
			return config.Config{}, nil, "", err
		}
		// Issue #241: a pre-existing aqg.json no longer suppresses a
		// legacy state-file priority_override. The deploy path pins
		// AQG_CONFIG to the StateDirectory and never overwrites aqg.json, so
		// every redeploy/upgrade hits this branch — and an operator who
		// adjusted a pool's priority before #198 made aqg.json the source of
		// truth would otherwise lose that adjustment on the next start.
		// reconcileLegacyPriority folds any unmigrated operator intent from
		// the legacy state file into the loaded registry and writes the
		// reconciled aqg.json back. After it runs, aqg.json is once again
		// the single source of truth and the legacy overlay becomes inert.
		reg, err = reconcileLegacyPriority(cfg, reg, path, logOut)
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

// reconcileLegacyPriority folds a still-unmigrated legacy state-file
// priority_override into the loaded config registry and persists the result
// to path (issue #241). It is the second-chance twin of bootstrapConfigFile:
// bootstrapConfigFile runs the same overlay against the env registry when
// aqg.json does not yet exist; reconcileLegacyPriority runs it against the
// loaded aqg.json when the file does exist.
//
// Policy (mirrors applyLegacyOverlay):
//   - a legacy priority_override that references a pool not in the loaded
//     registry AND not brought in via the legacy added_pools overlay is
//     logged and skipped — operator intent that names a now-gone pool is
//     outside the reconciler's scope;
//   - priority nicks that no longer name a current member are filtered out
//     (matching the bootstrap path's policy);
//   - if filtering empties the priority for a pool the overlay did target,
//     the reconciliation aborts loud: a zero-member priority in aqg.json
//     would silently discard operator intent, so startup must fail instead.
//
// reconcileLegacyPriority returns the in-memory registry (rebuilt on any
// change so the runtime sees the migrated order immediately) and writes a
// fresh aqg.json only when the legacy overlay actually changed something.
// When nothing needs migrating the on-disk file is left untouched, so a
// steady-state post-migration start incurs no write at all.
func reconcileLegacyPriority(cfg config.Config, reg *backend.Registry, path string, logOut io.Writer) (*backend.Registry, error) {
	legacy, ok := loadLegacyOverlay(cfg.StateFile)
	if !ok {
		return reg, nil
	}

	// First pass: scope the overlay to pools that exist in the loaded
	// registry (or that the legacy added_pools overlay will resurrect).
	// Anything else is outside the reconciler's scope and is skipped with
	// a log line, not an abort — matching applyLegacyOverlay.
	for rawName := range legacy.AddedPools {
		name := backend.NormalizeName(rawName)
		if name == "" || reg.HasPool(name) {
			continue
		}
		fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: skipping legacy added_pool %q (no matching pool in config)\n", name)
	}

	// Second pass: walk every legacy priority_override. Determine what
	// would change, apply the copy-on-write mutation when it does, and
	// collect any pool whose priority collapses to zero surviving members
	// (the only condition that aborts startup).
	var irreconcilable []string
	updated := reg
	for rawPool, pc := range legacy.Config {
		pool := backend.NormalizeName(rawPool)
		if pool == "" {
			continue
		}
		// Pool outside the reconciler's scope — log and skip, no abort.
		// This is the legacy-state-only entry the reviewer's note called
		// out: a state file that mentions a pool the loaded config does not
		// have is best-effort skipped.
		if !reg.HasPool(pool) {
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: skipping legacy priority for pool %q (not in config)\n", pool)
			continue
		}
		if len(pc.PriorityOverride) == 0 {
			continue // no priority to migrate for this pool
		}
		// Filter to nicks that still name a current member of the pool.
		order := make([]string, 0, len(pc.PriorityOverride))
		for _, rawNick := range pc.PriorityOverride {
			nick := backend.NormalizeName(rawNick)
			if nick == "" {
				continue
			}
			if _, ok := reg.ResolveIn(pool, nick); !ok {
				fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: dropping legacy priority nick %q for pool %q (not a current member)\n", nick, pool)
				continue
			}
			order = append(order, nick)
		}
		if len(order) == 0 {
			// The overlay targeted a pool we know about, but every nick in
			// its priority_override names a now-gone member. Silent fallback
			// would drop operator intent; abort so the operator sees a
			// startup error.
			irreconcilable = append(irreconcilable, pool)
			continue
		}
		// No-op when the loaded priority is already an exact match for the
		// legacy order — keeps the steady-state no-write property for
		// installs that already migrated their order verbatim. Set-equal
		// is not enough: the legacy state wins precedence (issue #197
		// pattern), so a different ordering with the same nicks must still
		// be migrated.
		if equalStringSlices(reg.PoolPriority(pool), order) {
			continue
		}
		next, err := reg.WithPriority(pool, order)
		if err != nil {
			return reg, fmt.Errorf("reconcile: pool %q priority: %w", pool, err)
		}
		updated = next
	}

	if len(irreconcilable) > 0 {
		sort.Strings(irreconcilable)
		return reg, fmt.Errorf("reconcile: legacy priority_override has no surviving members for pool(s) %s; resolve the mismatch in the state file or aqg.json before restarting", strings.Join(irreconcilable, ", "))
	}

	if updated == reg {
		// No priority override actually changed anything; the file on disk
		// is already the reconciled source of truth. Leave it untouched.
		return reg, nil
	}

	data, err := configfile.Marshal(cfg, updated)
	if err != nil {
		return reg, fmt.Errorf("reconcile: marshal %q: %w", path, err)
	}
	if err := configfile.WriteAtomic(path, data); err != nil {
		return reg, fmt.Errorf("reconcile: write %q: %w", path, err)
	}
	fmt.Fprintf(logOut, "agent-quota-gateway: reconciled legacy state-file priority into %s\n", path)
	return updated, nil
}

// equalStringSlices reports whether a and b contain the same elements in the
// same order (element-wise equality, len-checked first).
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
