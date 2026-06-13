// Command agent-quota-gateway is a loopback-only reverse proxy for the
// Anthropic Messages API. See the README for usage.
package main

import (
	"context"
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
)

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

	proxyHandler, err := proxy.New(cfg.AnthropicBaseURL, cfg.AnthropicAPIKey)
	if err != nil {
		return fmt.Errorf("proxy: %w", err)
	}

	handler := logging.Middleware(proxyHandler)

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