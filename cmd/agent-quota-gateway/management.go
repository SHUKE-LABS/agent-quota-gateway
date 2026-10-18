package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/config"
)

const managementHTTPTimeout = 10 * time.Second

type managementFlags struct {
	json            bool
	baseURL         string
	credentialStdin bool
}

type managementError struct {
	code int
	msg  string
}

func (e *managementError) Error() string { return e.msg }

func managementUsageError(format string, args ...any) *managementError {
	return &managementError{code: 2, msg: fmt.Sprintf(format, args...)}
}

func managementGatewayError(format string, args ...any) *managementError {
	return &managementError{code: 3, msg: fmt.Sprintf(format, args...)}
}

// looksLikeManagement reports whether the first command token is one of the
// management namespaces. Only the global --gateway flag is skipped here; all
// server flags continue through the legacy flag parser unchanged.
func looksLikeManagement(args []string) bool {
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--gateway":
			i++
			continue
		case strings.HasPrefix(args[i], "--gateway="):
			continue
		case args[i] == "pool", args[i] == "member":
			return true
		default:
			return false
		}
	}
	return false
}

// extractManagementGateway removes the global --gateway flag regardless of
// where it appears in a management command, leaving action-specific tokens
// for the small parser below. This supports both `--gateway URL pool ls` and
// `pool ls --gateway URL` without changing the server CLI grammar.
func extractManagementGateway(args []string) (string, []string, *managementError) {
	var gateway string
	var out []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--gateway":
			if gateway != "" || i+1 >= len(args) {
				return "", nil, managementUsageError("--gateway requires one origin")
			}
			i++
			gateway = args[i]
		case strings.HasPrefix(arg, "--gateway="):
			if gateway != "" {
				return "", nil, managementUsageError("--gateway may be supplied only once")
			}
			gateway = strings.TrimPrefix(arg, "--gateway=")
			if gateway == "" {
				return "", nil, managementUsageError("--gateway requires one origin")
			}
		default:
			out = append(out, arg)
		}
	}
	return gateway, out, nil
}

// parseManagementFlags accepts flags before or after positional arguments so
// the documented `member add pool nick --base-url ...` form stays natural.
func parseManagementFlags(args []string) (managementFlags, []string, *managementError) {
	var flags managementFlags
	var positionals []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--json":
			flags.json = true
		case arg == "--credential-stdin":
			flags.credentialStdin = true
		case arg == "--base-url":
			if i+1 >= len(args) {
				return managementFlags{}, nil, managementUsageError("--base-url requires a URL")
			}
			i++
			flags.baseURL = args[i]
		case strings.HasPrefix(arg, "--base-url="):
			flags.baseURL = strings.TrimPrefix(arg, "--base-url=")
			if flags.baseURL == "" {
				return managementFlags{}, nil, managementUsageError("--base-url requires a URL")
			}
		case arg == "--help", arg == "-h":
			return managementFlags{}, nil, managementUsageError("help")
		case strings.HasPrefix(arg, "-"):
			// Do not echo the raw token: an operator may have tried the
			// unsupported --credential=SECRET spelling, and diagnostics must
			// never become another credential sink.
			return managementFlags{}, nil, managementUsageError("unknown management flag")
		default:
			positionals = append(positionals, arg)
		}
	}
	return flags, positionals, nil
}

type managementClient struct {
	origin string
	http   *http.Client
}

func newManagementClient(explicit string) (*managementClient, *managementError) {
	origin := explicit
	if origin == "" {
		origin = os.Getenv(config.EnvListenAddr)
		if origin == "" {
			origin = config.DefaultListenAddr
		}
	}
	if !strings.Contains(origin, "://") {
		origin = "http://" + origin
	}
	u, err := url.Parse(origin)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, managementUsageError("invalid gateway origin %q", explicitOrDefaultOrigin(explicit, origin))
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, managementUsageError("gateway origin must use http or https: %q", origin)
	}
	if u.Path != "" && u.Path != "/" {
		return nil, managementUsageError("gateway origin must not include a path: %q", origin)
	}
	u.Path = ""
	u.RawPath = ""
	return &managementClient{
		origin: strings.TrimRight(u.String(), "/"),
		http:   &http.Client{Timeout: managementHTTPTimeout},
	}, nil
}

func explicitOrDefaultOrigin(explicit, resolved string) string {
	if explicit != "" {
		return explicit
	}
	return resolved
}

func (c *managementClient) endpoint(parts ...string) string {
	path := c.origin + "/_gateway"
	for _, part := range parts {
		path += "/" + url.PathEscape(part)
	}
	return path
}

type managementResponseError struct {
	Error string `json:"error"`
}

func (c *managementClient) request(method, endpoint string, body any, result any) *managementError {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return managementUsageError("encode management request: %v", err)
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		return managementUsageError("invalid gateway request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return managementGatewayError("gateway unreachable at %s: %v", c.origin, err)
	}
	defer resp.Body.Close()

	data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return managementGatewayError("gateway unreachable at %s: %v", c.origin, readErr)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		var apiErr managementResponseError
		if json.Unmarshal(data, &apiErr) == nil && apiErr.Error != "" {
			return &managementError{code: 1, msg: apiErr.Error}
		}
		return &managementError{code: 1, msg: fmt.Sprintf("gateway refused request with HTTP %d", resp.StatusCode)}
	}
	if result != nil && len(data) != 0 {
		if err := json.Unmarshal(data, result); err != nil {
			return managementGatewayError("gateway returned invalid JSON at %s: %v", c.origin, err)
		}
	}
	return nil
}

func (c *managementClient) config() ([]auto.PoolConfigView, *managementError) {
	var views []auto.PoolConfigView
	if err := c.request(http.MethodGet, c.endpoint("config"), nil, &views); err != nil {
		return nil, err
	}
	return views, nil
}

func findManagementPool(views []auto.PoolConfigView, pool string) (auto.PoolConfigView, bool) {
	normalized := backend.NormalizeName(pool)
	for _, view := range views {
		if view.Pool == normalized {
			return view, true
		}
	}
	return auto.PoolConfigView{}, false
}

func managementHasMember(view auto.PoolConfigView, nick string) bool {
	normalized := backend.NormalizeName(nick)
	for _, member := range view.Members {
		if member.Nick == normalized {
			return true
		}
	}
	return false
}

func readManagementCredential(stdin io.Reader) (string, *managementError) {
	data, err := io.ReadAll(stdin)
	if err != nil {
		return "", managementUsageError("read credential from stdin: %v", err)
	}
	credential := strings.TrimSpace(string(data))
	if credential == "" {
		return "", managementUsageError("credential stdin is empty")
	}
	return credential, nil
}

func runManagement(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	gateway, commandArgs, err := extractManagementGateway(args)
	if err != nil {
		return writeManagementError(stderr, err)
	}
	if len(commandArgs) < 2 {
		return writeManagementError(stderr, managementUsageError("management command requires a namespace and action"))
	}
	namespace, action := commandArgs[0], commandArgs[1]
	flags, positionals, parseErr := parseManagementFlags(commandArgs[2:])
	if parseErr != nil {
		if parseErr.msg == "help" {
			writeManagementUsage(stderr)
			return 0
		}
		return writeManagementError(stderr, parseErr)
	}

	client, clientErr := newManagementClient(gateway)
	if clientErr != nil {
		return writeManagementError(stderr, clientErr)
	}

	switch namespace {
	case "pool":
		return runPoolManagement(client, action, flags, positionals, stdout, stderr)
	case "member":
		return runMemberManagement(client, action, flags, positionals, stdin, stdout, stderr)
	default:
		return writeManagementError(stderr, managementUsageError("unknown management namespace %q", namespace))
	}
}

func runPoolManagement(client *managementClient, action string, flags managementFlags, positionals []string, stdout, stderr io.Writer) int {
	switch action {
	case "ls":
		if len(positionals) != 0 || flags.baseURL != "" || flags.credentialStdin {
			return writeManagementError(stderr, managementUsageError("pool ls accepts only --json and --gateway"))
		}
		views, err := client.config()
		if err != nil {
			return writeManagementError(stderr, err)
		}
		if flags.json {
			if err := json.NewEncoder(stdout).Encode(views); err != nil {
				return writeManagementError(stderr, managementGatewayError("write pool listing: %v", err))
			}
			return 0
		}
		for _, view := range views {
			fmt.Fprintf(stdout, "pool %s\n", view.Pool)
			if len(view.Priority) > 0 {
				fmt.Fprintf(stdout, "  priority: %s\n", strings.Join(view.Priority, ","))
			}
			for _, member := range view.Members {
				fmt.Fprintf(stdout, "  member %s base_url=%s disabled=%t status=%s\n", member.Nick, member.BaseURL, member.Disabled, member.Status)
			}
		}
		return 0
	case "add":
		if len(positionals) != 1 || flags.json || flags.baseURL != "" || flags.credentialStdin {
			return writeManagementError(stderr, managementUsageError("pool add requires exactly one pool name"))
		}
		var response map[string]any
		if err := client.request(http.MethodPost, client.endpoint("pool"), map[string]string{"name": positionals[0]}, &response); err != nil {
			return writeManagementError(stderr, err)
		}
		fmt.Fprintf(stdout, "pool %s added\n", backend.NormalizeName(positionals[0]))
		return 0
	case "rm":
		if len(positionals) != 1 || flags.json || flags.baseURL != "" || flags.credentialStdin {
			return writeManagementError(stderr, managementUsageError("pool rm requires exactly one pool name"))
		}
		if err := client.request(http.MethodDelete, client.endpoint("pool", positionals[0]), nil, nil); err != nil {
			return writeManagementError(stderr, err)
		}
		fmt.Fprintf(stdout, "pool %s removed\n", backend.NormalizeName(positionals[0]))
		return 0
	default:
		return writeManagementError(stderr, managementUsageError("unknown pool action %q", action))
	}
}

func runMemberManagement(client *managementClient, action string, flags managementFlags, positionals []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(positionals) != 2 {
		return writeManagementError(stderr, managementUsageError("member %s requires a pool and nick", action))
	}
	poolName, nick := positionals[0], positionals[1]
	switch action {
	case "add":
		if flags.json || !flags.credentialStdin {
			return writeManagementError(stderr, managementUsageError("member add requires --credential-stdin"))
		}
		credential, err := readManagementCredential(stdin)
		if err != nil {
			return writeManagementError(stderr, err)
		}
		views, configErr := client.config()
		if configErr != nil {
			return writeManagementError(stderr, configErr)
		}
		view, exists := findManagementPool(views, poolName)
		var placement []string
		if exists && !managementHasMember(view, nick) && len(view.Priority) > 0 {
			placement = append(placement, view.Priority...)
			placement = append(placement, backend.NormalizeName(nick))
		}
		body := map[string]any{
			"credential": credential,
			"upsert":     true,
		}
		if flags.baseURL != "" {
			body["base_url"] = flags.baseURL
		}
		if placement != nil {
			body["placement"] = placement
		}
		if err := client.request(http.MethodPost, client.endpoint("pool", poolName, "member", nick), body, nil); err != nil {
			return writeManagementError(stderr, err)
		}
		fmt.Fprintf(stdout, "member %s/%s updated\n", backend.NormalizeName(poolName), backend.NormalizeName(nick))
		return 0
	case "rm":
		if flags.json || flags.baseURL != "" || flags.credentialStdin {
			return writeManagementError(stderr, managementUsageError("member rm accepts only --gateway"))
		}
		views, configErr := client.config()
		if configErr != nil {
			return writeManagementError(stderr, configErr)
		}
		view, exists := findManagementPool(views, poolName)
		if exists && !managementHasMember(view, nick) {
			return 0
		}
		if err := client.request(http.MethodDelete, client.endpoint("pool", poolName, "member", nick), nil, nil); err != nil {
			if err.code == 1 && strings.Contains(err.msg, "not found in pool") {
				return 0
			}
			return writeManagementError(stderr, err)
		}
		fmt.Fprintf(stdout, "member %s/%s removed\n", backend.NormalizeName(poolName), backend.NormalizeName(nick))
		return 0
	default:
		return writeManagementError(stderr, managementUsageError("unknown member action %q", action))
	}
}

func writeManagementUsage(stderr io.Writer) {
	fmt.Fprintln(stderr, "Usage: agent-quota-gateway [--gateway ORIGIN] pool add|rm NAME")
	fmt.Fprintln(stderr, "       agent-quota-gateway [--gateway ORIGIN] pool ls [--json]")
	fmt.Fprintln(stderr, "       agent-quota-gateway [--gateway ORIGIN] member add POOL NICK --credential-stdin [--base-url URL]")
	fmt.Fprintln(stderr, "       agent-quota-gateway [--gateway ORIGIN] member rm POOL NICK")
}

func writeManagementError(stderr io.Writer, err *managementError) int {
	if err == nil {
		return 0
	}
	if err.code == 2 {
		if err.msg != "help" {
			fmt.Fprintf(stderr, "agent-quota-gateway: %s\n", err.msg)
		}
		writeManagementUsage(stderr)
		return 2
	}
	fmt.Fprintf(stderr, "agent-quota-gateway: %s\n", err.msg)
	return err.code
}
