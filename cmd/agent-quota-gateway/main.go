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

	"github.com/shukebeta/agent-quota-gateway/internal/config"
	"github.com/shukebeta/agent-quota-gateway/internal/logging"
	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// defaultBackendKey is the cache key used when an inbound request does
// not set X-Mux-Backend-Nick. Documented in the README so single-tenant
// callers that never set the header can still read snapshots back.
const defaultBackendKey = "default"

// backendHeader is the inbound header that names the backend a request
// is bound for. It is a plain custom header (not on the hop-by-hop
// list), so httputil.ReverseProxy preserves it on resp.Request inside
// ModifyResponse.
const backendHeader = "X-Mux-Backend-Nick"

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

	store := quota.NewStore()

	// observer is invoked once per upstream response, before the proxy
	// streams the body back to the client. It extracts the rate-limit
	// headers and files the snapshot under the backend the inbound
	// request named. Header-only inspection — no body access.
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
			if v := resp.Request.Header.Get(backendHeader); v != "" {
				key = v
			}
		}
		store.Put(key, snap)
	}

	proxyHandler, err := proxy.New(cfg.AnthropicBaseURL, cfg.AnthropicAPIKey, observer)
	if err != nil {
		return fmt.Errorf("proxy: %w", err)
	}

	// The proxy still owns the catch-all so unknown paths get its
	// closed-route 404 (rather than a generic mux 404). The quota
	// endpoint is mounted on a gateway-specific prefix so it cannot
	// collide with anything the upstream Messages surface defines.
	mux := http.NewServeMux()
	mux.HandleFunc("/_gateway/quota", quotaHandler(store))
	mux.Handle("/", proxyHandler)

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

// quotaHandler returns the JSON snapshot for the requested backend.
//
// Method is GET only — POSTing here would suggest the endpoint mutates
// state, which it does not. The backend key defaults to defaultBackendKey
// so single-tenant clients that never set X-Mux-Backend-Nick can still
// read the snapshot back with a plain `curl /_gateway/quota`. Unknown
// keys return 200 with an empty snapshot (just backend + as_of) — the
// distinction the caller cares about ("did I get quota data?") is
// answered by which fields are present in the JSON body, not by the
// status code.
func quotaHandler(store *quota.Store) http.HandlerFunc {
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
		snap := store.Get(key)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	}
}
