// Package poller fills the quota store for backends that do not return
// Anthropic-style rate-limit headers.
//
// The gateway's primary quota signal is the anthropic-ratelimit-unified-*
// headers it captures off real upstream responses (see package quota).
// Some providers — Z.ai / ZhipuAI and MiniMaxi — never emit those headers,
// so their store entries would stay permanently empty no matter how much
// organic traffic flows. Each of those providers instead exposes a
// proprietary quota-polling endpoint. This package polls that endpoint for
// the *active* member of each pool on a fixed cadence and writes the
// result into the same store, under the same Backend.QuotaKey() the
// response-observer uses.
//
// The poller is deliberately narrow. It only polls the backend a pool is
// currently sticky on, so a pool that has failed over to an untracked
// member stops being polled until it fails back. It issues no synthetic
// probes against Anthropic, and it never changes behaviour for Anthropic
// or any other untracked backend — those are simply skipped. A poll
// failure is logged and dropped; the last good snapshot survives.
package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// defaultInterval is how often the poller refreshes each tracked pool's
// active backend. Two minutes is frequent enough to keep failover
// decisions current without hammering a provider's dashboard API.
const defaultInterval = 2 * time.Minute

// defaultTimeout caps a single quota poll. The endpoints are lightweight
// JSON; a slow one should not pin the loop past the next tick.
const defaultTimeout = 10 * time.Second

// maxBodyBytes bounds how much of a quota response we read. The payloads
// are a few hundred bytes; this guards against a misbehaving endpoint
// streaming an unbounded body into memory.
const maxBodyBytes = 1 << 20 // 1 MiB

// CurrentFunc reports the active sticky backend of a pool. It matches
// auto.Pools.Current, but the poller takes a function so it does not
// import package auto (which would create a cycle through backend/quota).
type CurrentFunc func(poolName string) (backend.Backend, bool)

// Poller refreshes the quota store for proprietary-API backends. The zero
// value is not usable; call New.
type Poller struct {
	poolNames []string
	current   CurrentFunc
	store     *quota.Store
	client    *http.Client
	interval  time.Duration
	now       func() time.Time
	logOut    io.Writer
}

// New builds a Poller over the given pool names. current resolves a pool's
// active backend; store is where snapshots are filed. client defaults to a
// 10s-timeout client, interval to 2 minutes, now to time.Now, and logOut
// to os.Stderr when their zero value is passed.
func New(poolNames []string, current CurrentFunc, store *quota.Store, client *http.Client, interval time.Duration, now func() time.Time, logOut io.Writer) *Poller {
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}
	if interval <= 0 {
		interval = defaultInterval
	}
	if now == nil {
		now = time.Now
	}
	if logOut == nil {
		logOut = os.Stderr
	}
	return &Poller{
		poolNames: poolNames,
		current:   current,
		store:     store,
		client:    client,
		interval:  interval,
		now:       now,
		logOut:    logOut,
	}
}

// Run polls every tracked pool once immediately, then on each interval
// tick, until ctx is cancelled. The immediate first pass means the store
// is populated well within the interval rather than only after the first
// tick elapses. Run blocks; callers start it in a goroutine.
func (p *Poller) Run(ctx context.Context) {
	p.pollAll(ctx)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollAll(ctx)
		}
	}
}

// pollAll polls the active backend of every pool that is currently sticky
// on a backend a provider recognises. Unknown pools and untracked
// backends are skipped; each poll is independent, so one failure never
// blocks the rest.
func (p *Poller) pollAll(ctx context.Context) {
	for _, name := range p.poolNames {
		b, ok := p.current(name)
		if !ok {
			continue
		}
		prov, ok := providerFor(b.BaseURL)
		if !ok {
			continue // untracked backend (e.g. Anthropic); leave it to the header observer
		}
		snap, err := p.pollOne(ctx, prov, b)
		if err != nil {
			fmt.Fprintf(p.logOut, "poller[%s]: %s poll failed: %v\n", name, prov.name, err)
			continue
		}
		p.store.Put(b.QuotaKey(), snap)
	}
}

// pollOne performs one provider poll for backend b and returns the parsed
// snapshot. Any network error, non-200 status, or unparseable body is
// returned as an error so the caller can log and keep the prior snapshot.
func (p *Poller) pollOne(ctx context.Context, prov provider, b backend.Backend) (quota.Snapshot, error) {
	target, err := prov.quotaURL(b.BaseURL)
	if err != nil {
		return quota.Snapshot{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return quota.Snapshot{}, err
	}
	name, value := prov.auth(b.Credential)
	req.Header.Set(name, value)

	resp, err := p.client.Do(req)
	if err != nil {
		return quota.Snapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return quota.Snapshot{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return quota.Snapshot{}, err
	}
	return prov.parse(body, p.now())
}

// provider describes how to poll one proprietary quota API. The set is a
// registry: adding support for a new API means appending one entry to
// providers, with no change to the poll loop.
type provider struct {
	// name labels the provider in log lines.
	name string
	// matches reports whether a backend's BaseURL belongs to this provider.
	matches func(baseURL string) bool
	// quotaURL builds the absolute quota-polling URL from the backend's
	// BaseURL. A fixed-endpoint provider ignores its argument.
	quotaURL func(baseURL string) (string, error)
	// auth returns the request header name and value carrying the
	// credential, capturing the per-provider scheme (raw vs Bearer).
	auth func(credential string) (name, value string)
	// parse turns a 200 response body into a Snapshot stamped with now.
	parse func(body []byte, now time.Time) (quota.Snapshot, error)
}

// providers is the ordered registry of supported proprietary quota APIs.
// ByteDance Ark is intentionally absent — it exposes no known quota API.
var providers = []provider{
	{
		name:     "z.ai/zhipu",
		matches:  containsAny("api.z.ai", "open.bigmodel.cn"),
		quotaURL: hostURL("/api/monitor/usage/quota/limit"),
		auth:     rawAuth,
		parse:    parseZhipu,
	},
	{
		name:     "minimaxi",
		matches:  containsAny("minimaxi.com"),
		quotaURL: fixedURL("https://www.minimaxi.com/v1/token_plan/remains"),
		auth:     bearerAuth,
		parse:    parseMinimaxi,
	},
}

// providerFor returns the provider that recognises baseURL, if any.
func providerFor(baseURL string) (provider, bool) {
	for _, p := range providers {
		if p.matches(baseURL) {
			return p, true
		}
	}
	return provider{}, false
}

// containsAny builds a matcher that reports whether the BaseURL contains
// any of the given host fragments, case-insensitively (hosts are ASCII).
func containsAny(fragments ...string) func(string) bool {
	return func(baseURL string) bool {
		lower := strings.ToLower(baseURL)
		for _, f := range fragments {
			if strings.Contains(lower, f) {
				return true
			}
		}
		return false
	}
}

// hostURL builds a quotaURL function that keeps the backend's scheme and
// host but replaces the path with a fixed quota path. Used by providers
// whose quota endpoint lives on the same host as the API base URL.
func hostURL(path string) func(string) (string, error) {
	return func(baseURL string) (string, error) {
		u, err := url.Parse(baseURL)
		if err != nil {
			return "", err
		}
		if u.Scheme == "" || u.Host == "" {
			return "", fmt.Errorf("base URL %q lacks scheme or host", baseURL)
		}
		return u.Scheme + "://" + u.Host + path, nil
	}
}

// fixedURL builds a quotaURL function that always returns target,
// ignoring the backend's BaseURL. Used by providers whose quota endpoint
// lives on a separate, fixed host.
func fixedURL(target string) func(string) (string, error) {
	return func(string) (string, error) {
		return target, nil
	}
}

// rawAuth sends the credential verbatim on Authorization (no scheme
// prefix) — Z.ai / ZhipuAI's dashboard API expects the raw key.
func rawAuth(credential string) (string, string) {
	return "Authorization", credential
}

// bearerAuth sends the credential as a Bearer token — MiniMaxi's quota
// API expects the standard Authorization: Bearer scheme.
func bearerAuth(credential string) (string, string) {
	return "Authorization", "Bearer " + credential
}

// parseZhipu parses the Z.ai / ZhipuAI quota response. Both platforms
// share the schema: data.limits[] entries keyed by type, where
// TOKENS_LIMIT is the short (5h-equivalent) window and TIME_LIMIT is the
// long window. percentage is the *used* fraction in 0..100, so it maps to
// utilization by dividing by 100. nextResetTime is epoch milliseconds.
func parseZhipu(body []byte, now time.Time) (quota.Snapshot, error) {
	var resp struct {
		Data struct {
			Limits []struct {
				Type          string  `json:"type"`
				Percentage    float64 `json:"percentage"`
				NextResetTime int64   `json:"nextResetTime"`
			} `json:"limits"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return quota.Snapshot{}, err
	}
	snap := quota.Snapshot{AsOf: now.UTC()}
	for _, l := range resp.Data.Limits {
		switch l.Type {
		case "TOKENS_LIMIT":
			snap.Unified5hUtilization = floatPtr(l.Percentage / 100)
			snap.Unified5hReset = msToTime(l.NextResetTime)
		case "TIME_LIMIT":
			snap.Unified7dUtilization = floatPtr(l.Percentage / 100)
			snap.Unified7dReset = msToTime(l.NextResetTime)
		}
	}
	if !snap.HasData() {
		return quota.Snapshot{}, fmt.Errorf("no usable limits in response")
	}
	return snap, nil
}

// parseMinimaxi parses the MiniMaxi quota response. Unlike Z.ai, MiniMaxi
// reports the *remaining* percentage (100 = full quota), so utilization is
// 100 minus that, divided by 100. The first model_remains entry drives the
// snapshot; end_time / weekly_end_time are epoch milliseconds.
func parseMinimaxi(body []byte, now time.Time) (quota.Snapshot, error) {
	var resp struct {
		ModelRemains []struct {
			CurrentIntervalRemainingPercent float64 `json:"current_interval_remaining_percent"`
			CurrentWeeklyRemainingPercent   float64 `json:"current_weekly_remaining_percent"`
			EndTime                         int64   `json:"end_time"`
			WeeklyEndTime                   int64   `json:"weekly_end_time"`
		} `json:"model_remains"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return quota.Snapshot{}, err
	}
	if len(resp.ModelRemains) == 0 {
		return quota.Snapshot{}, fmt.Errorf("no model_remains in response")
	}
	m := resp.ModelRemains[0]
	snap := quota.Snapshot{AsOf: now.UTC()}
	snap.Unified5hUtilization = floatPtr((100 - m.CurrentIntervalRemainingPercent) / 100)
	snap.Unified5hReset = msToTime(m.EndTime)
	snap.Unified7dUtilization = floatPtr((100 - m.CurrentWeeklyRemainingPercent) / 100)
	snap.Unified7dReset = msToTime(m.WeeklyEndTime)
	return snap, nil
}

// msToTime converts epoch milliseconds to an absolute UTC time. A
// non-positive value yields nil rather than the Unix epoch, so a missing
// reset never looks like "reset at 1970" to downstream consumers (the
// same posture quota.parseUnixTime takes for header timestamps).
func msToTime(ms int64) *time.Time {
	if ms <= 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

// floatPtr returns a pointer to f, so a real 0.0 utilization (window
// untouched, full quota) is distinguishable from an absent field.
func floatPtr(f float64) *float64 {
	return &f
}
