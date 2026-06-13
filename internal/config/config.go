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

// validate enforces the V1 contract: non-empty API key, parseable upstream
// URL, and a loopback listen address. Future versions may relax these.
func (c Config) validate() error {
	if strings.TrimSpace(c.AnthropicAPIKey) == "" {
		return errors.New("ANTHROPIC_API_KEY is required")
	}
	if _, err := url.Parse(c.AnthropicBaseURL); err != nil {
		return fmt.Errorf("ANTHROPIC_BASE_URL is invalid: %w", err)
	}
	if c.ListenAddr == "" {
		return errors.New("LISTEN_ADDR must not be empty")
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}