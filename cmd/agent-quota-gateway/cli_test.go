package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// TestCLI drives the real entry flow with a recording start func. start is
// the only path to a bound listener, so "start not called" proves an exit
// binds no port (issue #358).
func TestCLI(t *testing.T) {
	cases := []struct {
		name       string
		args       []string
		startErr   error
		wantCode   int
		wantStart  bool
		wantConfig string
		wantStdout string // exact, when non-empty
		wantStderr string // substring, when non-empty
	}{
		{name: "no args starts with empty config", args: nil, wantCode: 0, wantStart: true},
		{name: "config flag passes through", args: []string{"--config", "x.json"}, wantCode: 0, wantStart: true, wantConfig: "x.json"},
		{name: "double-dash version", args: []string{"--version"}, wantCode: 0, wantStdout: version + "\n"},
		{name: "single-dash version", args: []string{"-version"}, wantCode: 0, wantStdout: version + "\n"},
		{name: "help", args: []string{"--help"}, wantCode: 0, wantStderr: "Usage of agent-quota-gateway"},
		{name: "unknown flag", args: []string{"--bogus"}, wantCode: 2, wantStderr: "Usage of agent-quota-gateway"},
		{name: "positional arg", args: []string{"version"}, wantCode: 2, wantStderr: `unexpected argument "version"`},
		{name: "positional after flag", args: []string{"--config", "x.json", "extra"}, wantCode: 2, wantStderr: "Usage of agent-quota-gateway"},
		{name: "start error exits 1", args: nil, startErr: errors.New("boom"), wantCode: 1, wantStart: true, wantStderr: "agent-quota-gateway: boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			started := false
			var gotConfig string
			code := cli(tc.args, &stdout, &stderr, func(configPath string) error {
				started = true
				gotConfig = configPath
				return tc.startErr
			})
			if code != tc.wantCode {
				t.Errorf("exit code = %d, want %d (stderr %q)", code, tc.wantCode, stderr.String())
			}
			if started != tc.wantStart {
				t.Errorf("start called = %v, want %v", started, tc.wantStart)
			}
			if gotConfig != tc.wantConfig {
				t.Errorf("config path = %q, want %q", gotConfig, tc.wantConfig)
			}
			if tc.wantStdout != "" && stdout.String() != tc.wantStdout {
				t.Errorf("stdout = %q, want %q", stdout.String(), tc.wantStdout)
			}
			if tc.wantStderr != "" && !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Errorf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}
