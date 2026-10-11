package config

import (
	"net/netip"
	"os"
	"strings"
	"testing"
)

// TestLoad_sharedWriteAllowParses covers the SHARED_WRITE_ALLOW env grammar
// (issue #349): comma list, whitespace trimmed, empty entries dropped, IP
// literals become single-address prefixes, CIDRs are masked, and
// IPv4-mapped IPv6 is unmapped.
func TestLoad_sharedWriteAllowParses(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "")
	t.Setenv(EnvListenAddr, "")
	t.Setenv(EnvSharedListenAddr, "100.91.4.56:9949")
	t.Setenv(EnvSharedWriteAllow, " 100.64.1.2 ,,100.91.7.9/16, fd7a:115c:a1e0::1,::ffff:10.0.0.5 ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []netip.Prefix{
		netip.MustParsePrefix("100.64.1.2/32"),
		netip.MustParsePrefix("100.91.0.0/16"),
		netip.MustParsePrefix("fd7a:115c:a1e0::1/128"),
		netip.MustParsePrefix("10.0.0.5/32"),
	}
	if len(cfg.SharedWriteAllow) != len(want) {
		t.Fatalf("SharedWriteAllow = %v, want %v", cfg.SharedWriteAllow, want)
	}
	for i := range want {
		if cfg.SharedWriteAllow[i] != want[i] {
			t.Errorf("SharedWriteAllow[%d] = %v, want %v", i, cfg.SharedWriteAllow[i], want[i])
		}
	}
}

func TestLoad_sharedWriteAllowUnsetMeansNoRestriction(t *testing.T) {
	t.Setenv(EnvAnthropicBaseURL, "")
	t.Setenv(EnvListenAddr, "")
	t.Setenv(EnvSharedListenAddr, "100.91.4.56:9949")
	// Absent, not empty: t.Setenv registers the restore, then Unsetenv
	// removes the variable for this test.
	t.Setenv(EnvSharedWriteAllow, "")
	os.Unsetenv(EnvSharedWriteAllow)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.SharedWriteAllow != nil {
		t.Fatalf("SharedWriteAllow = %v, want nil", cfg.SharedWriteAllow)
	}
}

func TestLoad_sharedWriteAllowRejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"empty":      "",
		"blank list": " , ",
		"name":       "admin.tailnet.ts.net",
		"bad cidr":   "100.64.0.0/33",
		"port":       "100.64.1.2:9949",
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv(EnvAnthropicBaseURL, "")
			t.Setenv(EnvListenAddr, "")
			t.Setenv(EnvSharedListenAddr, "100.91.4.56:9949")
			t.Setenv(EnvSharedWriteAllow, value)
			_, err := Load()
			if err == nil {
				t.Fatalf("Load() with %s=%q: want error", EnvSharedWriteAllow, value)
			}
			if !strings.Contains(err.Error(), EnvSharedWriteAllow) {
				t.Errorf("error %q does not name %s", err, EnvSharedWriteAllow)
			}
		})
	}
}

func TestFormatWriteAllowEntry_roundTrips(t *testing.T) {
	for _, entry := range []string{"100.64.1.2", "100.91.0.0/16", "fd7a:115c:a1e0::1", "fd7a:115c:a1e0::/48"} {
		prefix, err := ParseWriteAllowEntry(entry)
		if err != nil {
			t.Fatalf("ParseWriteAllowEntry(%q): %v", entry, err)
		}
		if got := FormatWriteAllowEntry(prefix); got != entry {
			t.Errorf("FormatWriteAllowEntry(Parse(%q)) = %q", entry, got)
		}
	}
}

func TestMatchesWriteAllow(t *testing.T) {
	allow := []netip.Prefix{
		netip.MustParsePrefix("100.64.1.2/32"),
		netip.MustParsePrefix("fd7a:115c:a1e0::/48"),
	}
	cases := map[string]bool{
		"100.64.1.2":          true,
		"::ffff:100.64.1.2":   true, // v4-mapped peer
		"100.64.1.3":          false,
		"fd7a:115c:a1e0::abc": true,
		"fd7a:115c:a1e1::1":   false,
		"127.0.0.1":           true, // loopback always allowed
		"::1":                 true,
	}
	for peer, want := range cases {
		if got := MatchesWriteAllow(allow, netip.MustParseAddr(peer)); got != want {
			t.Errorf("MatchesWriteAllow(%s) = %v, want %v", peer, got, want)
		}
	}
}
