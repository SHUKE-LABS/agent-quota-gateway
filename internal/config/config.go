// Package config loads gateway configuration from environment variables.
//
// The gateway is intentionally minimal: it reads the upstream URL and
// listen address from the environment, validates them, and exposes them
// through a single struct. Upstream credentials are not config — they
// live in the backend registry (see package backend). Anything more
// elaborate (flag parsing, config files, hot reload) is out of scope.
package config

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const (
	// EnvAnthropicBaseURL points the proxy at the Anthropic upstream.
	// Default is the production endpoint; tests may override it.
	EnvAnthropicBaseURL = "ANTHROPIC_BASE_URL"

	// EnvListenAddr is the loopback address the proxy binds to.
	// Default is 127.0.0.1:8080; intentionally loopback-only.
	EnvListenAddr = "LISTEN_ADDR"

	// EnvSharedListenAddr opts into shared mode: the gateway binds to a
	// single non-loopback overlay/IP address instead of loopback, so other
	// machines that can reach it (a Tailscale tailnet, an OpenVPN overlay,
	// a bare LAN, ...) can drive the same pools (sharing one authoritative
	// sticky pointer, failover state, and quota view). It is mutually
	// exclusive with EnvListenAddr — set exactly one. The trust boundary
	// in shared mode is whatever network ACL/firewall gates reachability
	// of that address, not the loopback interface; the gateway adds no
	// auth of its own. See the README "Shared mode" section.
	EnvSharedListenAddr = "SHARED_LISTEN_ADDR"

	// EnvStateFile sets the path for the persistent state file. When unset,
	// the gateway checks $STATE_DIRECTORY (set automatically by systemd when
	// StateDirectory= is configured) and uses $STATE_DIRECTORY/state.json if
	// present. An empty result disables persistence.
	EnvStateFile = "AQG_STATE_FILE"

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

	// ListenAddr is the address the gateway binds to. Loopback by
	// default; a non-loopback overlay/IP address when Shared is true.
	ListenAddr string

	// Shared reports whether shared mode is active (ListenAddr is a
	// non-loopback overlay/IP address). It selects the listen-address
	// validator and drives the loud startup warning in main.
	Shared bool

	// StateFile is the path for the persistent state file. Empty string
	// disables persistence (the gateway runs as before, all state in memory).
	StateFile string
}

// Inputs bundles all gateway configuration inputs for the Build function.
// Empty strings mean "unset" (unless otherwise noted).
type Inputs struct {
	// AnthropicBaseURL is the upstream URL. Empty string uses DefaultBaseURL.
	AnthropicBaseURL string

	// ListenAddr is the loopback bind address. Empty string uses DefaultListenAddr.
	ListenAddr string

	// SharedListenAddr is the overlay/IP bind address for shared mode.
	// Empty string means shared mode is off.
	SharedListenAddr string

	// StateFile is the path for the persistent state file. Empty string
	// means disabled; no $STATE_DIRECTORY consultation (that fallback
	// is env-only).
	StateFile string
}

// Load reads the gateway configuration from the process environment.
//
// Returns an error if any value is malformed so a misconfigured
// deployment fails fast at startup. Credentials are loaded separately by
// the backend registry.
func Load() (Config, error) {
	_, listenSet := lookupEnv(EnvListenAddr)
	shared, sharedSet := lookupEnv(EnvSharedListenAddr)

	// Exactly one listen knob may be set. Allowing both would force a
	// precedence rule that silently ignores the other; fail closed
	// instead so the operator's intent is never guessed.
	if listenSet && sharedSet {
		return Config{}, fmt.Errorf("%s and %s are mutually exclusive: set exactly one (loopback by default, or an overlay/IP address for shared mode)", EnvListenAddr, EnvSharedListenAddr)
	}

	inputs := Inputs{
		AnthropicBaseURL: getEnv(EnvAnthropicBaseURL, DefaultBaseURL),
		ListenAddr:       getEnv(EnvListenAddr, DefaultListenAddr),
		SharedListenAddr: shared,
		StateFile:        resolveStateFile(),
	}
	// Build's mutual-exclusion check uses non-emptiness, so we must
	// pass empty for the unset listen knob to avoid a false positive.
	if !listenSet {
		inputs.ListenAddr = ""
	}
	return Build(inputs)
}

// Build constructs a Config from Inputs. It validates the inputs and
// returns a Config or an error. The mutual-exclusion rule for listen
// knobs is enforced here (both non-empty is an error).
func Build(in Inputs) (Config, error) {
	// Exactly one listen knob may be set.
	listenSet := in.ListenAddr != ""
	sharedSet := in.SharedListenAddr != ""
	if listenSet && sharedSet {
		return Config{}, fmt.Errorf("listen_addr and shared_listen_addr are mutually exclusive: set exactly one (loopback by default, or an overlay/IP address for shared mode)")
	}

	cfg := Config{
		AnthropicBaseURL: in.AnthropicBaseURL,
		StateFile:        in.StateFile,
	}
	if in.AnthropicBaseURL == "" {
		cfg.AnthropicBaseURL = DefaultBaseURL
	}
	if sharedSet {
		cfg.ListenAddr = in.SharedListenAddr
		cfg.Shared = true
	} else {
		cfg.ListenAddr = in.ListenAddr
		if cfg.ListenAddr == "" {
			cfg.ListenAddr = DefaultListenAddr
		}
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate enforces the contract: an upstream URL with a scheme and
// host, and a listen address that matches the active mode — loopback by
// default, any non-loopback IP literal in shared mode.
func (c Config) validate() error {
	upstream, err := url.Parse(c.AnthropicBaseURL)
	if err != nil {
		return fmt.Errorf("ANTHROPIC_BASE_URL is invalid: %w", err)
	}
	if upstream.Scheme == "" || upstream.Host == "" {
		return fmt.Errorf("ANTHROPIC_BASE_URL=%q: scheme and host are required", c.AnthropicBaseURL)
	}
	if c.Shared {
		return validateSharedListenAddr(c.ListenAddr)
	}
	return validateListenAddr(c.ListenAddr)
}

// validateListenAddr enforces the loopback-only constraint: the host
// part of LISTEN_ADDR must be a literal loopback address (127.0.0.1,
// ::1) or the loopback name "localhost". We accept the name because it
// is the conventional shortcut and resolves to a loopback IP, but we
// reject 0.0.0.0 and any routable address — exposing the proxy
// off-loopback would put backend credentials on the wire.
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

// validateSharedListenAddr enforces the shared-mode constraint: the host
// part of SHARED_LISTEN_ADDR must be a non-loopback, non-wildcard IP
// literal — any overlay or LAN address that a fleet's own network trusts
// as its shared-mode boundary (Tailscale's CGNAT/ULA ranges, an OpenVPN
// overlay like 10.8.0.0/24, bare RFC1918, or a public address).
//
// Rejected at startup, fail-closed: loopback (use LISTEN_ADDR for that),
// 0.0.0.0 / :: (the wildcard binds every interface, defeating the
// single-address contract), and DNS / MagicDNS names. Names are rejected
// on purpose — proving a name resolves to the intended address is
// fragile, so shared mode requires the literal IP of this device's
// interface.
func validateSharedListenAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("SHARED_LISTEN_ADDR is invalid: %w", err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("SHARED_LISTEN_ADDR must be an IP literal, not a name; got %q", host)
	}
	if ip.IsLoopback() {
		return fmt.Errorf("SHARED_LISTEN_ADDR must not be loopback (use LISTEN_ADDR for that); got %q", host)
	}
	if ip.IsUnspecified() {
		return fmt.Errorf("SHARED_LISTEN_ADDR must not be the wildcard address; got %q", host)
	}
	return nil
}

// lookupEnv reports a variable's value and whether it is set to a
// non-empty string. An empty value counts as unset so that exporting
// LISTEN_ADDR="" does not trip the shared-mode mutual-exclusion check.
func lookupEnv(key string) (string, bool) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v, true
	}
	return "", false
}

func getEnv(key, fallback string) string {
	if v, ok := lookupEnv(key); ok {
		return v
	}
	return fallback
}

// resolveStateFile returns the path for the persistent state file.
// Priority: AQG_STATE_FILE > $STATE_DIRECTORY/state.json > "" (disabled).
// $STATE_DIRECTORY is set automatically by systemd when StateDirectory= is
// configured in the unit file.
func resolveStateFile() string {
	if v, ok := lookupEnv(EnvStateFile); ok {
		return v
	}
	if d, ok := lookupEnv("STATE_DIRECTORY"); ok {
		return filepath.Join(d, "state.json")
	}
	return ""
}
