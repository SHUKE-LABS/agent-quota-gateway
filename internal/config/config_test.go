package config

import (
	"testing"
)

func TestLoad_defaults(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "")
	t.Setenv(EnvAnthropicAPIKey, "test-key")
	t.Setenv(EnvListenAddr, "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AnthropicBaseURL != DefaultBaseURL {
		t.Errorf("BaseURL = %q, want %q", cfg.AnthropicBaseURL, DefaultBaseURL)
	}
	if cfg.ListenAddr != DefaultListenAddr {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, DefaultListenAddr)
	}
	if cfg.AnthropicAPIKey != "test-key" {
		t.Errorf("APIKey = %q, want %q", cfg.AnthropicAPIKey, "test-key")
	}
}

func TestLoad_overrides(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "https://example.test")
	t.Setenv(EnvAnthropicAPIKey, "k")
	t.Setenv(EnvListenAddr, "127.0.0.1:9000")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.AnthropicBaseURL != "https://example.test" {
		t.Errorf("BaseURL = %q", cfg.AnthropicBaseURL)
	}
	if cfg.ListenAddr != "127.0.0.1:9000" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
}

func TestLoad_missingKey(t *testing.T) {
	t.Setenv(EnvAnthropicAPIKey, "")
	t.Setenv(EnvAnthropicBaseURL, "")
	t.Setenv(EnvListenAddr, "")

	if _, err := Load(); err == nil {
		t.Fatal("Load() expected error when ANTHROPIC_API_KEY is empty")
	}
}