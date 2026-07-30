// Command agent-quota-gateway is a loopback-only reverse proxy for the
// Anthropic Messages API. See the README for usage.
package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/activity"
	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/configfile"
)

// activityHandler serves GET /_gateway/activity — the rolling per-endpoint
// activity series (volume, error rate, latency percentiles) over the last 60
// one-minute buckets. In-memory only; empty until the first non-gateway
// request lands. Non-GET returns 405, matching the sibling read endpoints.
func activityHandler(store *activity.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(store.Snapshot(time.Now()))
	}
}

// configHandler serves GET /_gateway/config — the effective configuration
// for all pools, with credentials fully redacted. Non-GET returns 405.
//
// Config-durability headers (issue #246):
//   - X-AQG-Persistence: <persisted|env_only> is set in every successful
//     response (env-only advertises itself explicitly instead of being
//     indistinguishable from a clean persisted state).
//   - X-AQG-Unsaved-Config: true is set in addition when the flush latch
//     is tripped (persisted but lagging memory; issue #198 decision 3).
//   - Env-only mode never sets the unsaved header — there is nothing to
//     flush and therefore nothing to lag.
func configHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		configfile.ApplyPersistenceHeader(w, persistence)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(pools.EffectiveConfig())
	}
}

// createPoolRequest is the JSON request body for creating a runtime pool.
// base_url is intentionally absent: a runtime pool is a pure named container
// with no base_url property; each member resolves its own base_url via
// AddMember's fallback chain. An extra `base_url` field in the request body is
// silently ignored by the decoder, keeping pre-issue-#172 clients working.
type createPoolRequest struct {
	Name       string   `json:"name"` // required; normalized server-side
	Mode       string   `json:"mode"`
	Nick       string   `json:"nick"`
	Credential string   `json:"credential"`
	BaseURL    string   `json:"base_url"`
	Placement  []string `json:"placement"`
}

// createPoolHandler serves POST /_gateway/pool — creates a plain pool at
// runtime. On success it returns 201 with {"pool": "<normalized-name>"}.
// In env-only mode (issue #246) the response body gains a
// "persistence":"env_only" field and the X-AQG-Persistence header is set
// so an operator hitting the API directly sees the change is in-memory
// only and is lost on restart.
func createPoolHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createPoolRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body"})
			return
		}

		status, err := pools.CreatePoolWithMember(req.Name, req.Mode, req.Nick, req.Credential, req.BaseURL, req.Placement)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(status)
		body := map[string]any{"pool": backend.NormalizeName(req.Name)}
		configfile.ApplyEnvOnlyBodyField(body, persistence)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// deletePoolHandler serves DELETE /_gateway/pool/{name} — removes an empty
// runtime pool (issue #232). On success it returns 200 {"status":"ok"},
// matching removeMemberHandler. A pool that still has members returns 409
// (drain members first); an unknown pool returns 404.
func deletePoolHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolName := backend.NormalizeName(r.PathValue("name"))
		if poolName == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool name is required"})
			return
		}

		status, err := pools.RemovePool(poolName)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(http.StatusOK)
		body := map[string]any{"status": "ok"}
		configfile.ApplyEnvOnlyBodyField(body, persistence)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// renamePoolRequest is the JSON body for POST /_gateway/pool/{name}/rename.
type renamePoolRequest struct {
	Name string `json:"name"` // required; normalized server-side
}

// renamePoolHandler serves POST /_gateway/pool/{name}/rename — atomically
// renames a pool in place (issue #238). On success it returns 200 with the
// normalized new name, matching createPoolHandler's response shape. Mapping:
// unknown old pool → 404; empty / identical-after-normalize new name → 400;
// new name collides with a different existing pool → 409.
func renamePoolHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolName := backend.NormalizeName(r.PathValue("name"))
		if poolName == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool name is required"})
			return
		}

		var req renamePoolRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body"})
			return
		}

		status, err := pools.RenamePool(poolName, req.Name)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(http.StatusOK)
		body := map[string]any{"pool": backend.NormalizeName(req.Name)}
		configfile.ApplyEnvOnlyBodyField(body, persistence)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// priorityHandler serves POST /_gateway/pool/{name}/priority — sets a
// runtime priority override for the pool. The request body must be a JSON
// array of nicks (highest priority first). The override is expanded to a
// total order (unlisted members rank last in sorted order).
func priorityHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolName := backend.NormalizeName(r.PathValue("name"))
		if poolName == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool name is required"})
			return
		}

		var order []string
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body"})
			return
		}

		status, err := pools.SetPriority(poolName, order)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(http.StatusOK)
		body := map[string]any{"status": "ok"}
		configfile.ApplyEnvOnlyBodyField(body, persistence)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// disableMemberHandler serves POST /_gateway/pool/{name}/member/{nick}/disable —
// disables a pool member, making it unselectable until re-enabled.
func disableMemberHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolName := backend.NormalizeName(r.PathValue("name"))
		nick := backend.NormalizeName(r.PathValue("nick"))
		if poolName == "" || nick == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool name and nick are required"})
			return
		}

		status, err := pools.SetMemberDisabled(poolName, nick, true)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(http.StatusOK)
		body := map[string]any{"status": "ok"}
		configfile.ApplyEnvOnlyBodyField(body, persistence)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// enableMemberHandler serves POST /_gateway/pool/{name}/member/{nick}/enable —
// re-enables a previously disabled pool member.
func enableMemberHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolName := backend.NormalizeName(r.PathValue("name"))
		nick := backend.NormalizeName(r.PathValue("nick"))
		if poolName == "" || nick == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool name and nick are required"})
			return
		}

		status, err := pools.SetMemberDisabled(poolName, nick, false)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(http.StatusOK)
		body := map[string]any{"status": "ok"}
		configfile.ApplyEnvOnlyBodyField(body, persistence)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// addMemberRequest is the JSON request body for adding a pool member.
type addMemberRequest struct {
	Credential string   `json:"credential"` // optional; resolved from a known nick when omitted
	BaseURL    string   `json:"base_url"`   // optional; resolved (known nick) or pool default (new nick) when omitted
	Placement  []string `json:"placement"`  // required for a priority target with no existing slot
}

// addMemberHandler serves POST /_gateway/pool/{name}/member/{nick} —
// adds a runtime member to a pool. Credential and base_url are optional for a
// known subscription; a priority target requires an explicit placement.
func addMemberHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolName := backend.NormalizeName(r.PathValue("name"))
		nick := backend.NormalizeName(r.PathValue("nick"))
		if poolName == "" || nick == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool name and nick are required"})
			return
		}

		var req addMemberRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body"})
			return
		}

		status, err := pools.AddMember(poolName, nick, req.Credential, req.BaseURL, req.Placement)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(http.StatusOK)
		body := map[string]any{"status": "ok"}
		configfile.ApplyEnvOnlyBodyField(body, persistence)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// moveMemberRequest is the JSON request body for moving a pool member.
type moveMemberRequest struct {
	To        string   `json:"to"`        // required target pool
	Placement []string `json:"placement"` // required for priority target with no existing slot
	Force     bool     `json:"force"`     // confirm overwrite of a conflicting same-nick target
}

// moveMemberHandler serves POST /_gateway/pool/{name}/member/{nick}/move —
// relocates a subscription from {name} to the target pool named in the body.
func moveMemberHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fromPool := backend.NormalizeName(r.PathValue("name"))
		nick := backend.NormalizeName(r.PathValue("nick"))
		if fromPool == "" || nick == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool name and nick are required"})
			return
		}

		var req moveMemberRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body"})
			return
		}
		toPool := backend.NormalizeName(req.To)
		if toPool == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "target pool (to) is required"})
			return
		}

		status, err := pools.MoveMember(fromPool, nick, toPool, req.Placement, req.Force)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(http.StatusOK)
		body := map[string]any{"status": "ok"}
		configfile.ApplyEnvOnlyBodyField(body, persistence)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// removeMemberHandler serves DELETE /_gateway/pool/{name}/member/{nick} —
// removes a member (static or runtime-added) from pool selection.
func removeMemberHandler(pools *auto.Pools, persistence configfile.PersistenceState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolName := backend.NormalizeName(r.PathValue("name"))
		nick := backend.NormalizeName(r.PathValue("nick"))
		if poolName == "" || nick == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool name and nick are required"})
			return
		}

		status, err := pools.RemoveMember(poolName, nick)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(http.StatusOK)
		body := map[string]any{"status": "ok"}
		configfile.ApplyEnvOnlyBodyField(body, persistence)
		_ = json.NewEncoder(w).Encode(body)
	}
}
