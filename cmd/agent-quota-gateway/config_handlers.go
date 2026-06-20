// Command agent-quota-gateway is a loopback-only reverse proxy for the
// Anthropic Messages API. See the README for usage.
package main

import (
	"encoding/json"
	"net/http"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// configHandler serves GET /_gateway/config — the effective configuration
// for all pools, with credentials fully redacted. Non-GET returns 405.
func configHandler(pools *auto.Pools) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(pools.EffectiveConfig())
	}
}

// priorityHandler serves POST /_gateway/pool/{name}/priority — sets a
// runtime priority override for the pool. The request body must be a JSON
// array of nicks (highest priority first). The override is expanded to a
// total order (unlisted members rank last in sorted order).
func priorityHandler(pools *auto.Pools) http.HandlerFunc {
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

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	}
}

// disableMemberHandler serves POST /_gateway/pool/{name}/member/{nick}/disable —
// disables a pool member, making it unselectable until re-enabled.
func disableMemberHandler(pools *auto.Pools) http.HandlerFunc {
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

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	}
}

// enableMemberHandler serves POST /_gateway/pool/{name}/member/{nick}/enable —
// re-enables a previously disabled pool member.
func enableMemberHandler(pools *auto.Pools) http.HandlerFunc {
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

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	}
}

// addMemberRequest is the JSON request body for adding a pool member.
type addMemberRequest struct {
	Credential string `json:"credential"` // required
	BaseURL    string `json:"base_url"`   // optional
}

// addMemberHandler serves POST /_gateway/pool/{name}/member/{nick} —
// adds a runtime member to a pool with a credential.
func addMemberHandler(pools *auto.Pools) http.HandlerFunc {
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

		status, err := pools.AddMember(poolName, nick, req.Credential, req.BaseURL)
		if err != nil {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
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
func moveMemberHandler(pools *auto.Pools) http.HandlerFunc {
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

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	}
}

// removeMemberHandler serves DELETE /_gateway/pool/{name}/member/{nick} —
// removes a member (static or runtime-added) from pool selection.
func removeMemberHandler(pools *auto.Pools) http.HandlerFunc {
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

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	}
}
