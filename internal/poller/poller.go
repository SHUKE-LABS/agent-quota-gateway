// Package poller fills the quota store for backends that do not return
// Anthropic-style rate-limit headers.
//
// The gateway's primary quota signal is the anthropic-ratelimit-unified-*
// headers it captures off real upstream responses (see package quota).
// Some providers — Z.ai / ZhipuAI, MiniMaxi, and Volcengine Ark — never
// emit those headers, so their store entries would stay permanently empty no
// matter how much organic traffic flows. Each of those providers instead
// exposes a proprietary quota-polling endpoint. This package polls that
// endpoint for the *active* member of each pool on a fixed cadence and
// writes the result into the same store, under the same Backend.QuotaKey()
// the response-observer uses.
//
// The poller is deliberately narrow. It only polls the backend a pool is
// currently sticky on, so a pool that has failed over to an untracked
// member stops being polled until it fails back. It issues no synthetic
// probes against Anthropic, and it never changes behaviour for Anthropic
// or any other untracked backend — those are simply skipped. A poll
// failure is logged and dropped; the last good snapshot survives.
package poller

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
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
	method := prov.method
	if method == "" {
		method = http.MethodGet
	}
	var bodyReader io.Reader
	if prov.body != nil {
		bodyReader = bytes.NewReader(prov.body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bodyReader)
	if err != nil {
		return quota.Snapshot{}, err
	}
	if err := prov.sign(req, b.Credential); err != nil {
		return quota.Snapshot{}, err
	}

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
	// sign stamps authentication onto req. It may set multiple headers
	// (e.g. X-Date + Authorization for HMAC schemes). Existing simple
	// providers set one header and return nil.
	sign func(req *http.Request, credential string) error
	// method is the HTTP method for the quota request; defaults to GET when empty.
	method string
	// body is the request body; nil means no body.
	body []byte
	// parse turns a 200 response body into a Snapshot stamped with now.
	parse func(body []byte, now time.Time) (quota.Snapshot, error)
}

// providers is the ordered registry of supported proprietary quota APIs.
var providers = []provider{
	{
		name:     "z.ai/zhipu",
		matches:  containsAny("api.z.ai", "open.bigmodel.cn"),
		quotaURL: hostURL("/api/monitor/usage/quota/limit"),
		sign:     rawAuth,
		parse:    parseZhipu,
	},
	{
		name:     "minimaxi",
		matches:  containsAny("minimaxi.com"),
		quotaURL: fixedURL("https://www.minimaxi.com/v1/token_plan/remains"),
		sign:     bearerAuth,
		parse:    parseMinimaxi,
	},
	{
		name:     "volcengine-ark",
		matches:  containsAny("volces.com"),
		quotaURL: fixedURL("https://open.volcengineapi.com/?Action=GetCodingPlanUsage&Version=2024-01-01"),
		sign:     volcengineSign,
		method:   http.MethodPost,
		body:     []byte("{}"),
		parse:    parseVolcengine,
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
func rawAuth(req *http.Request, credential string) error {
	req.Header.Set("Authorization", credential)
	return nil
}

// bearerAuth sends the credential as a Bearer token — MiniMaxi's quota
// API expects the standard Authorization: Bearer scheme.
func bearerAuth(req *http.Request, credential string) error {
	req.Header.Set("Authorization", "Bearer "+credential)
	return nil
}

// Volcengine IAM signing constants. The GetCodingPlanUsage action lives
// under the Ark service in the cn-beijing region.
const (
	volcRegion  = "cn-beijing"
	volcService = "ark"
)

// volcBodyHash is the SHA-256 of "{}", the fixed Volcengine request body,
// computed once at package init.
var volcBodyHash = func() string {
	h := sha256.Sum256([]byte("{}"))
	return hex.EncodeToString(h[:])
}()

// volcengineSign stamps Volcengine IAM HMAC-SHA256 authentication onto req.
// It reads VOLC_ACCESSKEY and VOLC_SECRETKEY from the environment,
// ignoring the credential argument (which holds the inference key and is
// unrelated to the account-level IAM pair needed here). Returns an error
// if either env var is absent.
func volcengineSign(req *http.Request, _ string) error {
	ak := os.Getenv("VOLC_ACCESSKEY")
	sk := os.Getenv("VOLC_SECRETKEY")
	if ak == "" {
		return fmt.Errorf("VOLC_ACCESSKEY is not set")
	}
	if sk == "" {
		return fmt.Errorf("VOLC_SECRETKEY is not set")
	}

	now := time.Now().UTC()
	dateTime := now.Format("20060102T150405Z")
	date := now.Format("20060102")

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Date", dateTime)
	req.Header.Set("X-Content-Sha256", volcBodyHash)

	host := req.URL.Host

	// Canonical query string: sort parameter names, then values.
	var qs string
	if req.URL.RawQuery != "" {
		params := req.URL.Query()
		keys := make([]string, 0, len(params))
		for k := range params {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0)
		for _, k := range keys {
			vals := params[k]
			sort.Strings(vals)
			for _, v := range vals {
				parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v))
			}
		}
		qs = strings.Join(parts, "&")
	}

	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	signedHeaders := "content-type;host;x-content-sha256;x-date"
	canonicalHeaders := "content-type:" + req.Header.Get("Content-Type") + "\n" +
		"host:" + host + "\n" +
		"x-content-sha256:" + volcBodyHash + "\n" +
		"x-date:" + dateTime + "\n"

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		qs,
		canonicalHeaders,
		signedHeaders,
		volcBodyHash,
	}, "\n")

	credentialScope := strings.Join([]string{date, volcRegion, volcService, "request"}, "/")
	reqHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"HMAC-SHA256",
		dateTime,
		credentialScope,
		hex.EncodeToString(reqHash[:]),
	}, "\n")

	kDate := hmacSHA256([]byte(sk), date)
	kRegion := hmacSHA256(kDate, volcRegion)
	kService := hmacSHA256(kRegion, volcService)
	kSigning := hmacSHA256(kService, "request")
	sig := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	req.Header.Set("Authorization", fmt.Sprintf(
		"HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		ak, credentialScope, signedHeaders, sig))
	return nil
}

// hmacSHA256 computes HMAC-SHA256 of data keyed by key.
func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
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

// parseVolcengine parses the Volcengine Ark GetCodingPlanUsage response.
// session maps to the 5h window; weekly maps to the 7d window; monthly is
// ignored. Percent is a used percentage in 0..100 (divide by 100 for
// utilization). ResetTimestamp is epoch seconds (not milliseconds).
func parseVolcengine(body []byte, now time.Time) (quota.Snapshot, error) {
	var resp struct {
		Result struct {
			QuotaUsage []struct {
				Level          string  `json:"Level"`
				Percent        float64 `json:"Percent"`
				ResetTimestamp int64   `json:"ResetTimestamp"`
			} `json:"QuotaUsage"`
		} `json:"Result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return quota.Snapshot{}, err
	}
	snap := quota.Snapshot{AsOf: now.UTC()}
	for _, u := range resp.Result.QuotaUsage {
		switch u.Level {
		case "session":
			snap.Unified5hUtilization = floatPtr(u.Percent / 100)
			snap.Unified5hReset = secToTime(u.ResetTimestamp)
		case "weekly":
			snap.Unified7dUtilization = floatPtr(u.Percent / 100)
			snap.Unified7dReset = secToTime(u.ResetTimestamp)
		// monthly: no Snapshot field; intentionally ignored
		}
	}
	if !snap.HasData() {
		return quota.Snapshot{}, fmt.Errorf("no usable quota levels in response")
	}
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

// secToTime converts epoch seconds to an absolute UTC time. Volcengine
// ResetTimestamp values are epoch seconds, unlike Z.ai's epoch-ms field.
// A non-positive value yields nil for the same reason as msToTime.
func secToTime(secs int64) *time.Time {
	if secs <= 0 {
		return nil
	}
	t := time.Unix(secs, 0).UTC()
	return &t
}

// floatPtr returns a pointer to f, so a real 0.0 utilization (window
// untouched, full quota) is distinguishable from an absent field.
func floatPtr(f float64) *float64 {
	return &f
}
