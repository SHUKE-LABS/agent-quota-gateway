// Package config loads gateway configuration from environment variables.
//
// The gateway is intentionally minimal: it reads three values from the
// environment, validates them, and exposes them through a single struct.
// Anything more elaborate (flag parsing, config files, hot reload) is out
// of scope for V1.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
)

const (
	// EnvAnthropicBaseURL points the proxy at the Anthropic upstream.
	// Default is the production endpoint; tests may override it.
	EnvAnthropicBaseURL = "ANTHROPIC_BASE_URL"

	// EnvAnthropicAPIKey is the credential used to authenticate against
	// the upstream. Required when serving real traffic.
	EnvAnthropicAPIKey = "ANTHROPIC_API_KEY"

	// EnvListenAddr is the loopback address the proxy binds to.
	// Default is 127.0.0.1:8080; intentionally loopback-only for V1.
	EnvListenAddr = "LISTEN_ADDR"

	// DefaultBaseURL is the Anthropic production endpoint.
	DefaultBaseURL = "https://api.anthropic.com"

	// DefaultListenAddr is the loopback address used when LISTEN_ADDR
	// is unset. Loopback-only is intentional; see the README.
	DefaultListenAddr = "127.0.0.1:8080"
)

// Config is the resolved gateway configuration.
type Config struct {
	// AnthropicBaseURL is the upstream scheme + host the proxy forwards
	// to. The path is appended at request time.
	AnthropicBaseURL string

	// AnthropicAPIKey is forwarded as x-api-key on every request. Empty
	// is allowed for tests that point at a fake upstream; production
	// callers must set it.
	AnthropicAPIKey string

	// ListenAddr is the loopback address the gateway binds to.
	ListenAddr string
}

// Load reads the gateway configuration from the process environment.
//
// Returns an error if any required value is missing or malformed. The
// Anthropic API key is required so a misconfigured deployment fails fast
// at startup rather than silently dropping auth on the first request.
func Load() (Config, error) {
	cfg := Config{
		AnthropicBaseURL: getEnv(EnvAnthropicBaseURL, DefaultBaseURL),
		AnthropicAPIKey:  os.Getenv(EnvAnthropicAPIKey),
		ListenAddr:       getEnv(EnvListenAddr, DefaultListenAddr),
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate enforces the V1 contract: non-empty API key, an upstream URL
// with a scheme and host, and a loopback listen address. Future versions
// may relax these.
func (c Config) validate() error {
	if strings.TrimSpace(c.AnthropicAPIKey) == "" {
		return errors.New("ANTHROPIC_API_KEY is required")
	}
	upstream, err := url.Parse(c.AnthropicBaseURL)
	if err != nil {
		return fmt.Errorf("ANTHROPIC_BASE_URL is invalid: %w", err)
	}
	if upstream.Scheme == "" || upstream.Host == "" {
		return fmt.Errorf("ANTHROPIC_BASE_URL=%q: scheme and host are required", c.AnthropicBaseURL)
	}
	if err := validateListenAddr(c.ListenAddr); err != nil {
		return err
	}
	return nil
}

// validateListenAddr enforces the loopback-only constraint for V1:
// the host part of LISTEN_ADDR must be a literal loopback address
// (127.0.0.1, ::1) or the loopback name "localhost". We accept the
// name because it is the conventional shortcut and resolves to a
// loopback IP, but we reject 0.0.0.0 and any routable address —
// exposing the proxy off-loopback would put the API key on the wire.
func validateListenAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("LISTEN_ADDR is invalid: %w", err)
	}
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return nil
	default:
		return fmt.Errorf("LISTEN_ADDR must be loopback (127.0.0.1, ::1, or localhost); got %q", host)
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}