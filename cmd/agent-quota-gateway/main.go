// Command agent-quota-gateway is a loopback-only reverse proxy for the
// Anthropic Messages API. See the README for usage.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
	"github.com/shukebeta/agent-quota-gateway/internal/logging"
	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// defaultBackendKey is the quota cache key used as a defensive fallback
// if a forwarded response somehow lacks a resolved backend on its
// context. In normal operation the resolver middleware guarantees one.
const defaultBackendKey = "default"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "agent-quota-gateway: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	registry, err := backend.Load()
	if err != nil {
		return fmt.Errorf("backend: %w", err)
	}

	// autoCtl is the `auto` selector's global-sticky controller. The
	// random start index (start < 0) anchors each restart at a different
	// backend with no probe traffic; its quota snapshot fills in from the
	// first real response.
	autoCtl := auto.NewController(registry, -1, nil, nil)

	store := quota.NewStore()

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
				key = b.Nick
			}
		}
		store.Put(key, snap)
	}

	// The auto controller's ModifyResponse hook runs after the observer:
	// it fails over (429 -> 503) or forwards an honest 429 only for
	// auto-routed requests, leaving every explicit-selector response
	// untouched.
	proxyHandler, err := proxy.New(cfg.AnthropicBaseURL, observer, autoCtl.ModifyResponse)
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
	mux.HandleFunc("/_gateway/quota", quotaHandler(store, autoCtl))
	mux.Handle("/", backend.Middleware(registry, autoCtl, proxyHandler))

	handler := logging.Middleware(mux)

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

	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	fmt.Fprintf(os.Stderr, "agent-quota-gateway listening on %s -> %s\n", cfg.ListenAddr, cfg.AnthropicBaseURL)

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

// autoQuotaView is the /_gateway/quota?backend=auto response: the active
// sticky backend's snapshot with an added active_backend field naming the
// nick. The embedded Snapshot promotes its fields into the same JSON
// object, so a consumer that asks `auto` gets the snapshot it would have
// gotten by naming the backend directly, plus the name itself — it needs
// zero knowledge of pool membership, and the 99%->5% jump on a switch is
// self-explained because active_backend changes alongside it.
type autoQuotaView struct {
	quota.Snapshot
	ActiveBackend string `json:"active_backend"`
}

// quotaHandler returns the JSON snapshot for the requested backend.
//
// Method is GET only — POSTing here would suggest the endpoint mutates
// state, which it does not. The backend nick comes from the `?backend=`
// query param and defaults to defaultBackendKey, so a plain `curl
// /_gateway/quota` reads the default snapshot back. The reserved value
// `auto` resolves to the current sticky backend and adds active_backend.
// Unknown keys return 200 with an empty snapshot (just backend + as_of) —
// the distinction the caller cares about ("did I get quota data?") is
// answered by which fields are present in the JSON body, not by the
// status code.
func quotaHandler(store *quota.Store, autoCtl *auto.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("backend")
		if key == "" {
			key = defaultBackendKey
		}
		w.Header().Set("Content-Type", "application/json")
		if autoCtl != nil && backend.IsAutoSelector(key) {
			nick := autoCtl.Current()
			_ = json.NewEncoder(w).Encode(autoQuotaView{
				Snapshot:      store.Get(nick),
				ActiveBackend: nick,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(store.Get(key))
	}
}
