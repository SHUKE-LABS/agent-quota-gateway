package config

import (
	"testing"
)

func TestLoad_defaults(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "")
	t.Setenv(EnvListenAddr, "")
	t.Setenv(EnvSharedListenAddr, "")

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
}

func TestLoad_overrides(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "https://example.test")
	t.Setenv(EnvListenAddr, "127.0.0.1:9000")
	t.Setenv(EnvSharedListenAddr, "")

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
			t.Setenv(EnvSharedListenAddr, "")
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
			t.Setenv(EnvSharedListenAddr, "")
			t.Setenv(EnvListenAddr, addr)
			if _, err := Load(); err == nil {
				t.Errorf("Load() addr=%q: expected loopback rejection", addr)
			}
		})
	}
}

func TestLoad_rejectsMalformedAddr(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "")
	t.Setenv(EnvSharedListenAddr, "")
	t.Setenv(EnvListenAddr, "no-port")
	if _, err := Load(); err == nil {
		t.Error("Load() expected error for malformed LISTEN_ADDR")
	}
}

func TestLoad_sharedAcceptsTailscale(t *testing.T) {
	cases := []string{
		"100.64.0.0:8080",          // first address of the CGNAT /10
		"100.101.102.103:8080",     // a typical Tailscale IPv4
		"100.127.255.255:8080",     // last address of the CGNAT /10
		"[fd7a:115c:a1e0::1]:8080", // Tailscale IPv6 ULA
		"[fd7a:115c:a1e0:ab12::5]:8080",
	}
	for _, addr := range cases {
		t.Run(addr, func(t *testing.T) {
			t.Setenv(EnvAnthropicBaseURL, "")
			t.Setenv(EnvListenAddr, "")
			t.Setenv(EnvSharedListenAddr, addr)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() shared addr=%q: unexpected error %v", addr, err)
			}
			if !cfg.Shared {
				t.Errorf("Load() shared addr=%q: cfg.Shared = false, want true", addr)
			}
			if cfg.ListenAddr != addr {
				t.Errorf("Load() shared addr=%q: ListenAddr = %q", addr, cfg.ListenAddr)
			}
		})
	}
}

func TestLoad_sharedRejectsNonTailscale(t *testing.T) {
	cases := []string{
		"100.63.255.255:8080",         // just below the CGNAT /10
		"100.128.0.0:8080",            // just above the CGNAT /10 (the /10-vs-/16 trap)
		"10.0.0.1:8080",               // RFC1918
		"172.16.0.1:8080",             // RFC1918
		"192.168.1.1:8080",            // RFC1918
		"8.8.8.8:8080",                // public
		"0.0.0.0:8080",                // wildcard
		"[::]:8080",                   // wildcard
		"127.0.0.1:8080",              // loopback belongs on LISTEN_ADDR
		"[::1]:8080",                  // loopback
		"localhost:8080",              // names are rejected in shared mode
		"my-host.tailnet.ts.net:8080", // MagicDNS name
		"[fd00::1]:8080",              // a non-Tailscale ULA
	}
	for _, addr := range cases {
		t.Run(addr, func(t *testing.T) {
			t.Setenv(EnvAnthropicBaseURL, "")
			t.Setenv(EnvListenAddr, "")
			t.Setenv(EnvSharedListenAddr, addr)
			if _, err := Load(); err == nil {
				t.Errorf("Load() shared addr=%q: expected rejection", addr)
			}
		})
	}
}

func TestLoad_rejectsBothListenVars(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "")
	t.Setenv(EnvListenAddr, "127.0.0.1:8080")
	t.Setenv(EnvSharedListenAddr, "100.101.102.103:8080")
	if _, err := Load(); err == nil {
		t.Error("Load() expected error when both LISTEN_ADDR and SHARED_LISTEN_ADDR are set")
	}
}

func TestLoad_emptySharedFallsBackToLoopback(t *testing.T) {
	// An exported-but-empty SHARED_LISTEN_ADDR counts as unset, so the
	// default loopback contract holds and there is no mutual-exclusion error.
	t.Setenv(EnvAnthropicBaseURL, "")
	t.Setenv(EnvListenAddr, "")
	t.Setenv(EnvSharedListenAddr, "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Shared {
		t.Error("cfg.Shared = true, want false")
	}
	if cfg.ListenAddr != DefaultListenAddr {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, DefaultListenAddr)
	}
}

func TestLoad_rejectsMalformedBaseURL(t *testing.T) {
	cases := []string{
		"foo",       // no scheme, no host
		"http://",   // empty host
		"not-a-url", // no scheme, no host
		"://broken", // missing scheme and host
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(EnvAnthropicBaseURL, raw)
			t.Setenv(EnvSharedListenAddr, "")
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
	t.Setenv(EnvSharedListenAddr, "")
	t.Setenv(EnvListenAddr, "127.0.0.1:8080")

	if _, err := Load(); err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
}
