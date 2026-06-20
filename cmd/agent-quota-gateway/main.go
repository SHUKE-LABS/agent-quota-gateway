// Command agent-quota-gateway is a loopback-only reverse proxy for the
// Anthropic Messages API. See the README for usage.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
	"github.com/shukebeta/agent-quota-gateway/internal/configfile"
	"github.com/shukebeta/agent-quota-gateway/internal/logging"
	"github.com/shukebeta/agent-quota-gateway/internal/persist"
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
	"github.com/shukebeta/agent-quota-gateway/internal/reqlog"
)

// defaultBackendKey is the quota cache key used as a defensive fallback
// if a forwarded response somehow lacks a resolved backend on its
// context. In normal operation the resolver middleware guarantees one.
const defaultBackendKey = "default"

// version is stamped at build time via -ldflags "-X main.version=...".
// It defaults to "dev" for a plain `go build`. The deploy script sets it
// from `git describe` so an upgraded service is verifiable with -version.
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	configPath := flag.String("config", "", "path to a JSON config file (overrides AQG_CONFIG and ./aqg.json)")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if err := run(*configPath); err != nil {
		fmt.Fprintf(os.Stderr, "agent-quota-gateway: %v\n", err)
		os.Exit(1)
	}
}

func run(configFlag string) error {
	var cfg config.Config
	var registry *backend.Registry
	var err error

	// Check for config file first: flag > AQG_CONFIG > ./aqg.json > env.
	if path, useFile := configfile.Resolve(configFlag); useFile {
		cfg, registry, err = configfile.LoadFile(path)
		if err != nil {
			return err
		}
	} else {
		cfg, err = config.Load()
		if err != nil {
			return fmt.Errorf("config: %w", err)
		}

		registry, err = backend.Load(cfg.AnthropicBaseURL)
		if err != nil {
			return fmt.Errorf("backend: %w", err)
		}
	}

	store := quota.NewStore()

	// Load persisted state from the state file (if configured). A missing
	// or corrupt file starts fresh; any other I/O error aborts startup.
	persisted, err := persist.Load(cfg.StateFile)
	if err != nil {
		return fmt.Errorf("persist: load %q: %w", cfg.StateFile, err)
	}

	// Restore quota snapshots first so controllers can read them when
	// deciding the initial exhaustion state from the store.
	for key, snap := range persisted.Snapshots {
		store.Put(key, snap)
	}

	// pools fronts every configured pool with its own sticky controller.
	// Each controller starts at a random member (start < 0) so no probe
	// traffic is needed to anchor it; its quota snapshot fills in from the
	// first real response. The controllers consult the shared store so a
	// member the poller or headers report fully consumed is failed off even
	// without a live 429 — the only exhaustion signal poller-tracked
	// backends (z.ai / MiniMaxi) ever produce.
	pools := auto.NewPools(registry, store, nil, nil)

	// Restore sticky pointers and exhausted maps from the persisted state.
	// Expired exhausted entries are silently dropped by LoadPersistState.
	pools.LoadPersistState(persisted.Pools)

	// Restore runtime configuration (priority overrides, disabled members).
	pools.LoadRuntimeConfig(persisted.Config)

	// Wire up the persister so state mutations are coalesced and flushed
	// atomically to disk. The persister goroutine is started below.
	statePersister := persist.NewPersister(cfg.StateFile, func() persist.GatewayState {
		return persist.GatewayState{
			Pools:     pools.PersistState(),
			Snapshots: store.Snapshot(),
			Config:    pools.PersistRuntimeConfig(),
		}
	})
	pools.SetOnMutate(statePersister.MarkDirty)
	store.SetOnChange(statePersister.MarkDirty)

	// observer is invoked once per upstream response, before the proxy
	// streams the body back to the client. It extracts the rate-limit
	// headers and files the snapshot under the backend the resolver
	// middleware selected for the request. Header-only inspection — no
	// body access.
	//
	// We only store snapshots that actually carry quota data. An
	// upstream response with no rate-limit headers (e.g. a 5xx page,
	// or a future endpoint that doesn't return them) would otherwise
	// overwrite the last known-good snapshot with an empty one, which
	// would look to consumers like the quota state was reset.
	observer := func(resp *http.Response) {
		snap := quota.Extract(resp)
		if !snap.HasData() {
			return
		}
		key := defaultBackendKey
		if resp.Request != nil {
			if b, ok := backend.FromContext(resp.Request.Context()); ok {
				key = b.QuotaKey()
			}
		}
		store.Put(key, snap)
	}

	// The pools' ModifyResponse hook runs after the observer: it dispatches
	// to the controller of the pool the request resolved through and fails
	// over (429 -> 503) or forwards an honest 429 within that pool.
	proxyHandler, err := proxy.New(observer, pools.ModifyResponse)
	if err != nil {
		return fmt.Errorf("proxy: %w", err)
	}

	// The proxy owns the catch-all so every non-gateway path forwards
	// to the upstream (the upstream is the authority on what it serves).
	// The resolver middleware sits in front of it so every forwarded
	// request carries a resolved backend; the gateway's own /_gateway
	// endpoints are mounted directly and take no selector.
	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/health", healthHandler())
	mux.HandleFunc("/_gateway/quota", quotaHandler(store, pools))
	mux.HandleFunc("/_gateway/pool", poolHandler(store, pools))
	mux.HandleFunc("/_gateway/clear", clearHandler(pools))
	mux.HandleFunc("/_gateway/config", configHandler(pools))
	mux.HandleFunc("/_gateway/ui", uiHandler())
	mux.HandleFunc("POST /_gateway/pool/{name}/priority", priorityHandler(pools))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/disable", disableMemberHandler(pools))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/enable", enableMemberHandler(pools))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/move", moveMemberHandler(pools))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}", addMemberHandler(pools))
	mux.HandleFunc("DELETE /_gateway/pool/{name}/member/{nick}", removeMemberHandler(pools))
	mux.Handle("/", backend.Middleware(pools, proxyHandler))

	handler := reqlog.Middleware(logging.Middleware(mux))

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// signal.NotifyContext gives us a context that cancels on SIGINT
	// or SIGTERM, which is the simplest way to wire graceful shutdown
	// without a hand-rolled signal channel.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The persister coalesces state mutations and writes them atomically to
	// the state file. It shares the shutdown context so the final flush
	// lands before the process exits.
	go statePersister.Run(ctx)

	// The poller fills the quota store for backends that never emit
	// Anthropic rate-limit headers (Z.ai / ZhipuAI, MiniMaxi) by polling
	// each provider's proprietary quota API for the active member of each
	// pool. It is a no-op for Anthropic and any other untracked backend.
	// It shares the shutdown context, so it stops when the process does.
	qp := poller.New(registry.PoolNames(), pools.Current, store, nil, 0, nil, nil)
	go qp.Run(ctx)

	// The preemptor returns a priority pool to a higher-priority member once
	// that member's quota window resets, so a freshly-reset preferred backend
	// is drained promptly instead of riding the active fallback until it 429s.
	// It only touches pools that declared AQG_POOL_<POOL>_PRIORITY and returns
	// immediately when none did. It shares the shutdown context.
	pre := auto.NewPreemptor(pools, store, 0, nil, nil)
	go pre.Run(ctx)

	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	if cfg.Shared {
		// Shared mode is off-loopback: every tailnet member that can reach
		// this port can drive the pools and read /_gateway/quota. The
		// gateway adds no auth — the Tailscale ACL is the only gate — so
		// make the exposure loud rather than let it pass as a normal start.
		fmt.Fprintf(os.Stderr, "agent-quota-gateway: SHARED MODE — bound to Tailscale address %s, reachable by tailnet members. A Tailscale ACL restricting this port is REQUIRED; the gateway adds no authentication of its own.\n", cfg.ListenAddr)
	}
	fmt.Fprintf(os.Stderr, "agent-quota-gateway %s listening on %s; default upstream %s; pools %s\n", version, cfg.ListenAddr, cfg.AnthropicBaseURL, strings.Join(registry.PoolNames(), ", "))

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "agent-quota-gateway shutting down")
	}

	// Give in-flight requests a short grace period before the process
	// exits. Streaming Messages responses can run for many seconds, so
	// 30s is the smallest reasonable window that does not interrupt
	// them. Future versions may make this configurable.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

// healthHandler returns a fixed {"status":"ok"} body. It is a loopback-
// only liveness probe — the loopback trust model means it carries no
// sensitive state, so the response shape is deliberately minimal and
// does not expose the version, uptime, or upstream reachability. Any
// additional readiness signal would belong on a separate endpoint so
// callers can tell "process is alive" from "upstream is reachable".
// Method is GET only; non-GET requests receive 405 — matching
// quotaHandler's policy so the two /_gateway/* endpoints agree.
func healthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

// poolQuotaView is the /_gateway/quota?backend=<pool> response: the
// pool's active sticky member's snapshot with an added active_backend
// field naming the member nick. The embedded Snapshot promotes its
// fields into the same JSON object, so a consumer that asks for a pool
// gets the active member's snapshot plus the member's name — it needs
// zero knowledge of pool membership, and the 99%->5% jump on a switch is
// self-explained because active_backend changes alongside it.
type poolQuotaView struct {
	quota.Snapshot
	ActiveBackend string `json:"active_backend"`
}

// quotaHandler returns the JSON snapshot for the requested pool.
//
// Method is GET only — POSTing here would suggest the endpoint mutates
// state, which it does not. The pool name comes from the `?backend=`
// query param. A known pool resolves to its active sticky member and adds
// active_backend. An unknown pool (or a missing param) returns 200 with
// an empty snapshot (just backend + as_of) — the distinction the caller
// cares about ("did I get quota data?") is answered by which fields are
// present in the JSON body, not by the status code.
func quotaHandler(store *quota.Store, pools *auto.Pools) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Normalize the pool name the same way the routing path does, so a
		// diagnostic query like ?backend=AUTO matches pool "auto".
		key := backend.NormalizeName(r.URL.Query().Get("backend"))
		w.Header().Set("Content-Type", "application/json")
		if pools != nil {
			if b, ok := pools.Current(key); ok {
				_ = json.NewEncoder(w).Encode(poolQuotaView{
					Snapshot:      store.Get(b.QuotaKey()),
					ActiveBackend: b.Nick,
				})
				return
			}
		}
		_ = json.NewEncoder(w).Encode(store.Get(key))
	}
}

// clearHandler serves POST /_gateway/clear — drops live-429 parks so a
// member wrongly marked exhausted (e.g. a transient/misconfigured 429 on an
// account that still has quota) becomes selectable again without waiting out
// the park or restarting the gateway. With ?pool=<name> it clears one pool;
// without the param it clears every pool. Only the reactive 429 parks are
// cleared — store-sourced exhaustion reflects polled reality and is left
// alone. Non-POST returns 405.
func clearHandler(pools *auto.Pools) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		poolName := backend.NormalizeName(r.URL.Query().Get("pool"))
		if poolName == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{"cleared": pools.ClearAllExhausted()})
			return
		}
		cleared, ok := pools.ClearExhausted(poolName)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"pool": poolName, "cleared": cleared})
	}
}

// poolHandler serves GET /_gateway/pool — the per-member health view for
// every configured pool. With ?pool=<name> it returns a single pool; without
// the param it returns all pools in sorted order. Non-GET returns 405.
func poolHandler(store *quota.Store, pools *auto.Pools) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		poolName := backend.NormalizeName(r.URL.Query().Get("pool"))
		if poolName == "" {
			_ = json.NewEncoder(w).Encode(pools.AllPoolStatuses(store))
			return
		}
		status, ok := pools.PoolStatus(poolName, store)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(status)
	}
}
