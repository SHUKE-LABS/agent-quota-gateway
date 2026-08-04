package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
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
//     env entirely. A pre-#198 state-file priority_override is consumed into
//     aqg.json once before the server starts (issue #241).
//   - A config path resolves but the file is absent (first deploy): generate it
//     once by merging env (backend.Load) with the legacy state-file overlay
//     using state-wins precedence, write it 0600, then read it back. Env is
//     never consulted again on subsequent starts, except for the legacy state
//     file location probe described by reconcileLegacyPriority.
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
		reg, err = reconcileLegacy(cfg, reg, path, logOut)
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

// warnIfPersistenceDisabled tells config-file operators when an empty
// state_file leaves runtime state in memory only. Env-only mode intentionally
// stays quiet because an empty state path is an ordinary local-dev setup.
func warnIfPersistenceDisabled(configPath, statePath string, logOut io.Writer) {
	if configPath == "" || statePath != "" {
		return
	}
	fmt.Fprintf(logOut, "agent-quota-gateway: WARNING: config file %q has an empty state_file; runtime persistence is disabled, so sticky pointers, exhausted maps, balance sequence and quota snapshots do not survive a restart. Set state_file in %q and restart.\n", configPath, configPath)
}

type legacyPriorityVerdict struct {
	pool     string
	previous []string
	order    []string
	migrate  bool
	balanced bool
}

// reconcileLegacy is the unified orchestrator for the existing-config-file
// path (issues #241 and #259). It resolves the legacy state-file overlay
// exactly once and runs every per-key migration against the same triple, so a
// `priority_override` in $AQG_STATE_FILE and `disabled` in the same overlay
// apply from one read; lower-precedence candidates are not silently merged in.
//
// Per-key migrations each follow the same shape as the original issue #241
// reconciler: aggregate-validation pass → registry update via copy-on-write
// → Marshal+WriteAtomic of aqg.json if any update landed → log per applied
// change → final atomic state-file cleanup removing every consumed key in
// one write. A state-file cleanup failure is logged and startup continues; the
// cleanup is retried on the next start.
func reconcileLegacy(cfg config.Config, reg *backend.Registry, configPath string, logOut io.Writer) (*backend.Registry, error) {
	statePath, stateData, legacy, ok := findLegacyOverlay(cfg, logOut)
	if !ok {
		return reg, nil
	}

	updated, err := reconcileLegacyPriority(cfg, reg, legacy, statePath, configPath, logOut)
	if err != nil {
		return reg, err
	}
	updated, _, err = reconcileLegacyDisabled(cfg, updated, legacy, configPath, logOut)
	if err != nil {
		return reg, err
	}
	reconcileLegacyReportOnly(legacy, statePath, logOut)

	// Aggregate key cleanup: combine the handled pools for priority and
	// disabled into one atomic state-file write so a crash between the two
	// half-deletes does not leave a stale legacy key behind.
	consumedPools := legacyKeyCleanup(updated, legacy)
	if len(consumedPools) > 0 {
		if err := consumeLegacyKeys(statePath, stateData, consumedPools); err != nil {
			poolList := make([]string, 0, len(consumedPools))
			for p := range consumedPools {
				poolList = append(poolList, p)
			}
			sort.Strings(poolList)
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: aqg.json is current, but could not consume legacy config keys for pool(s) %s from %q: %v; deletion will be retried on the next start\n", strings.Join(poolList, ", "), statePath, err)
		}
	}
	return updated, nil
}

// reconcileLegacyPriority consumes the pre-#198 state-file priority_override
// into an existing aqg.json (issue #241). aqg.json is written first; the
// consumed key is removed by the orchestrator's final cleanup pass. That write
// order makes a crash between the two idempotent: the next start observes
// exact equality, leaves aqg.json untouched, and retries only the state-file
// cleanup.
//
// This is intentionally priority-only. applyLegacyOverlay also replays legacy
// members, credentials, removals, and disabled flags, which would overwrite
// newer operator intent already stored in aqg.json. It remains confined to the
// first-deploy bootstrap path.
func reconcileLegacyPriority(cfg config.Config, reg *backend.Registry, legacy legacyState, statePath, configPath string, logOut io.Writer) (*backend.Registry, error) {
	spec := reg.Spec()
	verdicts := make([]legacyPriorityVerdict, 0, len(legacy.Config))
	var irreconcilable []string
	for rawPool, pc := range legacy.Config {
		if len(pc.PriorityOverride) == 0 {
			continue
		}
		pool := backend.NormalizeName(rawPool)
		if pool == "" || !reg.HasPool(pool) {
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: skipping legacy priority for pool %q (not in config)\n", pool)
			continue
		}

		order := filteredLegacyPriority(reg, pool, pc.PriorityOverride, logOut)
		if len(order) == 0 {
			irreconcilable = append(irreconcilable, pool)
			continue
		}

		previous := reg.PoolPriority(pool)
		v := legacyPriorityVerdict{
			pool:     pool,
			previous: previous,
			order:    order,
		}
		if spec.Pools[pool].Balance != "" {
			v.balanced = true
			verdicts = append(verdicts, v)
			continue
		}
		v.migrate = !equalStringSlices(previous, order)
		verdicts = append(verdicts, v)
	}

	if len(irreconcilable) > 0 {
		sort.Strings(irreconcilable)
		return reg, fmt.Errorf("reconcile: legacy priority_override has no surviving members for pool(s) %s; state file %q and config file %q were not changed; remove those priority_override key(s) from the state file and restart", strings.Join(irreconcilable, ", "), statePath, configPath)
	}

	updated := reg
	var migrated []legacyPriorityVerdict
	for _, v := range verdicts {
		if v.balanced {
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: pool %q declares balance mode in aqg.json; legacy priority mode is superseded and will be consumed\n", v.pool)
			continue
		}
		if !v.migrate {
			continue
		}
		next, err := updated.WithPriority(v.pool, v.order)
		if err != nil {
			return reg, fmt.Errorf("reconcile: pool %q priority: %w", v.pool, err)
		}
		updated = next
		migrated = append(migrated, v)
	}

	if updated != reg {
		data, err := configfile.Marshal(cfg, updated)
		if err != nil {
			return reg, fmt.Errorf("reconcile: marshal %q: %w", configPath, err)
		}
		if err := configfile.WriteAtomic(configPath, data); err != nil {
			return reg, fmt.Errorf("reconcile: write %q: %w", configPath, err)
		}
		for _, v := range migrated {
			if len(v.previous) == 0 {
				fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: pool %q priority %v -> %v migrated into %s; nothing was overridden\n", v.pool, v.previous, v.order, configPath)
			} else {
				fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: pool %q priority %v -> %v migrated into %s; re-setting the order in the UI will not recur\n", v.pool, v.previous, v.order, configPath)
			}
		}
	}
	return updated, nil
}

func findLegacyOverlay(cfg config.Config, logOut io.Writer) (string, []byte, legacyState, bool) {
	if cfg.StateFile != "" {
		data, legacy, ok := readLegacyOverlay(cfg.StateFile)
		return cfg.StateFile, data, legacy, ok
	}

	var candidates []string
	if path := os.Getenv(config.EnvStateFile); path != "" {
		candidates = append(candidates, path)
	}
	if dir := os.Getenv("STATE_DIRECTORY"); dir != "" {
		path := filepath.Join(dir, "state.json")
		if len(candidates) == 0 || path != candidates[0] {
			candidates = append(candidates, path)
		}
	}
	for _, path := range candidates {
		if data, legacy, ok := readLegacyOverlay(path); ok {
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: discovered legacy state file %q from the environment because aqg.json declares no state_file\n", path)
			return path, data, legacy, true
		}
	}
	return "", nil, legacyState{}, false
}

func filteredLegacyPriority(reg *backend.Registry, pool string, rawOrder []string, logOut io.Writer) []string {
	order := make([]string, 0, len(rawOrder))
	seen := make(map[string]bool, len(rawOrder))
	for _, rawNick := range rawOrder {
		nick := backend.NormalizeName(rawNick)
		if _, ok := reg.ResolveIn(pool, nick); !ok {
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: dropping legacy priority nick %q for pool %q (not a current member)\n", nick, pool)
			continue
		}
		if seen[nick] {
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: dropping duplicate legacy priority nick %q for pool %q\n", nick, pool)
			continue
		}
		seen[nick] = true
		order = append(order, nick)
	}
	return order
}

type legacyDisabledVerdict struct {
	pool   string
	nick   string
	already bool // true if the member was already disabled in aqg.json; no rewrite
}

// reconcileLegacyDisabled consumes the pre-#198 state-file `disabled` list
// into aqg.json (issue #259). Each listed nick that is a configured,
// currently-enabled member of the pool is disabled; non-member nicks are
// logged and skipped; already-disabled members are recorded but produce no
// `aqg.json` rewrite. The `disabled` key is removed by the orchestrator's
// final cleanup pass over legacyKeyCleanup's output.
//
// The two-return form (handledDisabled slice) is a future hook for symmetry
// with the priority reconciler; it is currently the slice of pools whose
// legacy `disabled` list was processed (had at least one entry), independent
// of whether any disable was actually applied.
func reconcileLegacyDisabled(cfg config.Config, reg *backend.Registry, legacy legacyState, configPath string, logOut io.Writer) (*backend.Registry, []string, error) {
	verdicts := make([]legacyDisabledVerdict, 0)
	var handledPools []string
	for rawPool, pc := range legacy.Config {
		if len(pc.Disabled) == 0 {
			continue
		}
		pool := backend.NormalizeName(rawPool)
		if pool == "" {
			continue
		}
		handledPools = append(handledPools, pool)
		if !reg.HasPool(pool) {
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: skipping legacy disabled for pool %q (not in config)\n", pool)
			continue
		}
		seen := make(map[string]bool, len(pc.Disabled))
		for _, rawNick := range pc.Disabled {
			nick := backend.NormalizeName(rawNick)
			if nick == "" || seen[nick] {
				continue
			}
			seen[nick] = true
			b, ok := reg.ResolveIn(pool, nick)
			if !ok {
				fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: skipping legacy disabled nick %q for pool %q (not a current member)\n", nick, pool)
				continue
			}
			verdicts = append(verdicts, legacyDisabledVerdict{pool: pool, nick: nick, already: b.Disabled})
		}
	}

	updated := reg
	for _, v := range verdicts {
		if v.already {
			continue
		}
		next, err := updated.WithMemberDisabled(v.pool, v.nick, true)
		if err != nil {
			return reg, nil, fmt.Errorf("reconcile: pool %q disable %q: %w", v.pool, v.nick, err)
		}
		updated = next
		fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: pool %q member %q disabled and migrated into %s; re-enabling in the UI will not recur\n", v.pool, v.nick, configPath)
	}
	if updated == reg {
		return updated, handledPools, nil
	}
	data, err := configfile.Marshal(cfg, updated)
	if err != nil {
		return reg, nil, fmt.Errorf("reconcile: marshal %q: %w", configPath, err)
	}
	if err := configfile.WriteAtomic(configPath, data); err != nil {
		return reg, nil, fmt.Errorf("reconcile: write %q: %w", configPath, err)
	}
	return updated, handledPools, nil
}

// reconcileLegacyReportOnly logs the presence of legacy `removed_members`
// and `added_members` without applying them (issue #259). Both keys are
// credential-bearing and asymmetric-recoverable: applying a legacy removal
// can lose a credential the operator still needs, applying a legacy addition
// can re-inject a rotated credential. The first `persist.flush` is allowed to
// erase them on its own schedule — the startup log is the durable record of
// what was in the file.
func reconcileLegacyReportOnly(legacy legacyState, statePath string, logOut io.Writer) {
	for rawPool, pc := range legacy.Config {
		pool := backend.NormalizeName(rawPool)
		if pool == "" {
			continue
		}
		if len(pc.RemovedMembers) > 0 {
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: state file %q contains legacy removed_members for pool %q (nicks: %s); not applied — clearing them via the UI is the supported path; these keys will be removed by the next state-file flush\n", statePath, pool, strings.Join(pc.RemovedMembers, ", "))
		}
		if len(pc.AddedMembers) > 0 {
			nicks := make([]string, 0, len(pc.AddedMembers))
			for rawNick := range pc.AddedMembers {
				nicks = append(nicks, backend.NormalizeName(rawNick))
			}
			sort.Strings(nicks)
			fmt.Fprintf(logOut, "agent-quota-gateway: reconcile: state file %q contains legacy added_members for pool %q (nicks: %s); not applied — re-adding via the UI/API is the supported path; these keys will be removed by the next state-file flush\n", statePath, pool, strings.Join(nicks, ", "))
		}
	}
}

// consumeLegacyKeys removes the listed legacy keys from each handled pool's
// legacy config object. RawMessage keeps every unrelated JSON value
// semantically unchanged; no live GatewayState decode is used because that
// observation-only type deliberately omits legacy operator-intent fields.
//
// legacyKeys is {pool: [key1, key2, ...]} — per-pool key lists from
// legacyKeyCleanup. A pool is skipped entirely if none of its keys are
// actually present in the on-disk JSON (no rewritten empty object is written
// for it). One atomic write removes every consumed key.
func consumeLegacyKeys(path string, data []byte, legacyKeys map[string][]string) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return err
	}
	var configs map[string]json.RawMessage
	if err := json.Unmarshal(root["config"], &configs); err != nil {
		return err
	}

	changed := false
	for rawPool, keys := range legacyKeys {
		raw, ok := configs[rawPool]
		if !ok {
			continue
		}
		var poolConfig map[string]json.RawMessage
		if err := json.Unmarshal(raw, &poolConfig); err != nil {
			return fmt.Errorf("config.%s: %w", rawPool, err)
		}
		poolChanged := false
		for _, key := range keys {
			if _, ok := poolConfig[key]; !ok {
				continue
			}
			poolConfig = withoutRawMessage(poolConfig, key)
			poolChanged = true
		}
		if !poolChanged {
			continue
		}
		updated, err := json.Marshal(poolConfig)
		if err != nil {
			return err
		}
		configs[rawPool] = updated
		changed = true
	}
	if !changed {
		return nil
	}
	updatedConfigs, err := json.Marshal(configs)
	if err != nil {
		return err
	}
	root["config"] = updatedConfigs
	updated, err := json.Marshal(root)
	if err != nil {
		return err
	}
	return configfile.WriteAtomic(path, updated)
}

// legacyKeyCleanup returns the legacy overlay's pool-by-key cleanup map for
// pools that exist in the loaded registry. Every entry that *can* be consumed
// (priority_override always; disabled when the per-pool legacy entry was
// non-empty) shows up here, restricted to in-scope pools — out-of-scope pools
// (not in the loaded registry and not runtime-created) are left entirely
// alone so an operator who hand-edits a state file isn't quietly rewritten.
//
// The orchestrator uses this to drive a single atomic state-file rewrite
// covering both migrations from one read.
func legacyKeyCleanup(reg *backend.Registry, legacy legacyState) map[string][]string {
	out := map[string][]string{}
	for rawPool, pc := range legacy.Config {
		pool := backend.NormalizeName(rawPool)
		if pool == "" || !reg.HasPool(pool) {
			continue
		}
		var keys []string
		if len(pc.PriorityOverride) > 0 {
			keys = append(keys, "priority_override")
		}
		if len(pc.Disabled) > 0 {
			keys = append(keys, "disabled")
		}
		if len(keys) > 0 {
			out[rawPool] = keys
		}
	}
	return out
}

func withoutRawMessage(m map[string]json.RawMessage, key string) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(m)-1)
	for k, v := range m {
		if k != key {
			out[k] = v
		}
	}
	return out
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
	data, ls, ok := readLegacyOverlay(path)
	_ = data
	return ls, ok
}

func readLegacyOverlay(path string) ([]byte, legacyState, bool) {
	if path == "" {
		return nil, legacyState{}, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, legacyState{}, false
	}
	var ls legacyState
	if err := json.Unmarshal(data, &ls); err != nil {
		return nil, legacyState{}, false
	}
	if len(ls.Config) == 0 && len(ls.AddedPools) == 0 {
		return nil, legacyState{}, false
	}
	return data, ls, true
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
