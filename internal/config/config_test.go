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

func TestLoad_loopbackVariants(t *testing.T) {
	cases := []string{
		"127.0.0.1:8080",
		"[::1]:8080",
		"localhost:8080",
	}
	for _, addr := range cases {
		t.Run(addr, func(t *testing.T) {
			t.Setenv(EnvAnthropicBaseURL, "")
			t.Setenv(EnvAnthropicAPIKey, "k")
			t.Setenv(EnvListenAddr, addr)
			if _, err := Load(); err != nil {
				t.Errorf("Load() addr=%q: unexpected error %v", addr, err)
			}
		})
	}
}

func TestLoad_rejectsNonLoopback(t *testing.T) {
	cases := []string{
		"0.0.0.0:8080",
		"192.168.1.1:8080",
		"example.com:8080",
	}
	for _, addr := range cases {
		t.Run(addr, func(t *testing.T) {
			t.Setenv(EnvAnthropicBaseURL, "")
			t.Setenv(EnvAnthropicAPIKey, "k")
			t.Setenv(EnvListenAddr, addr)
			if _, err := Load(); err == nil {
				t.Errorf("Load() addr=%q: expected loopback rejection", addr)
			}
		})
	}
}

func TestLoad_rejectsMalformedAddr(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "")
	t.Setenv(EnvAnthropicAPIKey, "k")
	t.Setenv(EnvListenAddr, "no-port")
	if _, err := Load(); err == nil {
		t.Error("Load() expected error for malformed LISTEN_ADDR")
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

func TestLoad_rejectsMalformedBaseURL(t *testing.T) {
	cases := []string{
		"foo",         // no scheme, no host
		"http://",     // empty host
		"not-a-url",   // no scheme, no host
		"://broken",   // missing scheme and host
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(EnvAnthropicBaseURL, raw)
			t.Setenv(EnvAnthropicAPIKey, "k")
			t.Setenv(EnvListenAddr, "127.0.0.1:8080")
			cfg, err := Load()
			if err == nil {
				t.Fatalf("Load() baseURL=%q: expected error, got cfg=%+v", raw, cfg)
			}
		})
	}
}

func TestLoad_acceptsValidBaseURL(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "https://api.anthropic.com")
	t.Setenv(EnvAnthropicAPIKey, "k")
	t.Setenv(EnvListenAddr, "127.0.0.1:8080")

	if _, err := Load(); err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
}