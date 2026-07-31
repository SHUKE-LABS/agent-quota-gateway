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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// defaultInterval is how often the poller refreshes each tracked pool's
// active backend. Two minutes is frequent enough to keep failover
// decisions current without hammering a provider's dashboard API.
const defaultInterval = 2 * time.Minute

// StaleAfterIntervals is how many poll intervals a tracked pool may
// go without a successful poll before it is considered stale for the
// /_gateway/health aggregate and the /_gateway/pool per-pool card.
//
// One named constant in this package drives every surface that reads
// "is this signal still live?" — handlers and the auto package derive
// staleness by multiplying StaleAfterIntervals by the configured
// interval, so a future bump is a one-line change.
//
// Three is the smallest multiplier that tolerates one missed poll
// (network blip) plus one in-flight tick (cancelled context at
// shutdown) without flagging healthy pools as stale, and is the
// threshold issue #247 names.
const StaleAfterIntervals = 3

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

// MarkLocalSnapshotFunc is the poller's "I just filed a snapshot for
// this pool/nick" callback. The poller takes a function so it does not
// import package auto; the caller (cmd/agent-quota-gateway) wires it to
// auto.Pools.MarkLocalSnapshot. issue #111.
type MarkLocalSnapshotFunc func(poolName, nick string)

// Poller refreshes the quota store for proprietary-API backends. The zero
// value is not usable; call New.
type Poller struct {
	// poolNames resolves the set of pools to poll fresh on every tick, so a
	// pool created at runtime (auto.Pools.AddPool) is polled without a restart
	// (issue #202). New wraps a fixed slice; NewDynamic takes the accessor.
	poolNames   func() []string
	current     CurrentFunc
	markLocal   MarkLocalSnapshotFunc
	store       *quota.Store
	client      *http.Client
	interval    time.Duration
	now         func() time.Time
	logOut      io.Writer

	// stateMu guards state. state is per-pool liveness observation for
	// the admin surface (issue #247): the last successful poll, the
	// last error and its time, and the consecutive-failure count.
	// A pool that has never been touched by the poller is absent from
	// the map; the absence is itself meaningful — a tracked pool whose
	// first tick has not yet fired reads "never polled", and a tracked
	// pool currently sticky on an untracked member reads "stale" the
	// same way as one whose polls are failing (pollAll `continue`s
	// past those pools without updating state).
	stateMu sync.Mutex
	state   map[string]*poolState
}

// poolState is the per-pool liveness observation surfaced through
// Status() and HealthSummary(). lastSuccess is the zero value when no
// poll has ever succeeded for the pool; the map absence and
// lastSuccess == zero are equivalent for callers.
type poolState struct {
	LastSuccess         time.Time
	LastErr             string // human-readable, stable for JSON consumers
	LastErrAt           time.Time
	ConsecutiveFailures int
}

// New builds a Poller over a fixed set of pool names. Production wants the
// pool set resolved dynamically (see NewDynamic); New is retained for tests and
// any caller whose pool set never changes. current resolves a pool's active
// backend; store is where snapshots are filed; markLocal is called after every
// successful poll so the originating pool's controller can stop suppressing the
// cross-pool snapshot for that nick (issue #111). markLocal may be nil for
// tests that do not care about the per-pool-snapshot signal. client defaults to
// a 10s-timeout client, interval to 2 minutes, now to time.Now, and logOut to
// os.Stderr when their zero value is passed.
func New(poolNames []string, current CurrentFunc, markLocal MarkLocalSnapshotFunc, store *quota.Store, client *http.Client, interval time.Duration, now func() time.Time, logOut io.Writer) *Poller {
	return NewDynamic(func() []string { return poolNames }, current, markLocal, store, client, interval, now, logOut)
}

// NewDynamic is New with the pool set resolved fresh on every tick from
// poolNames instead of frozen at construction, so a pool created at runtime
// (auto.Pools.AddPool) is polled on the next tick without a restart (issue
// #202). All other arguments behave as in New.
func NewDynamic(poolNames func() []string, current CurrentFunc, markLocal MarkLocalSnapshotFunc, store *quota.Store, client *http.Client, interval time.Duration, now func() time.Time, logOut io.Writer) *Poller {
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
		markLocal: markLocal,
		store:     store,
		client:    client,
		interval:  interval,
		now:       now,
		logOut:    logOut,
		state:     map[string]*poolState{},
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
//
// Per-pool liveness observation (issue #247) is recorded alongside every
// attempt — success zeroes the consecutive-failure count and clears the
// last error; failure increments and stamps the error/time. A pool that
// has never been polled is absent from the state map; the absence is
// itself a signal (a never-polled tracked pool, or a tracked pool
// currently sticky on an untracked member, both read "no lastSuccess").
func (p *Poller) pollAll(ctx context.Context) {
	for _, name := range p.poolNames() {
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
			p.recordFailure(name, err)
			continue
		}
		// Merge, not Put: a poll response may omit a window (e.g. z.ai with
		// no TIME_LIMIT, or NextResetTime 0), so a partial snapshot must not
		// blank the previously-cached reset for the absent window (issue #163).
		p.store.Merge(b.QuotaKey(), snap)
		// Mirror the observer path: a successful poll is the first local
		// evidence this pool has traffic-shaped state for the nick, so
		// un-suppress the cross-pool snapshot for it (issue #111).
		if p.markLocal != nil {
			p.markLocal(name, b.Nick)
		}
		p.recordSuccess(name)
	}
}

// PollAllForTest exposes the package-private pollAll loop so handler-level
// tests (issue #247) can drive a single poll cycle deterministically
// without owning a goroutine. Production code must not call this.
func (p *Poller) PollAllForTest(ctx context.Context) {
	p.pollAll(ctx)
}

// recordSuccess marks a successful poll for pool name: zeroes the
// consecutive-failure count and clears the last error. The pool's
// lastSuccess is stamped with the poller's injectable now so AC9's
// threshold-advance test can drive staleness without a clock into the
// admin handlers.
func (p *Poller) recordSuccess(name string) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	st, ok := p.state[name]
	if !ok {
		st = &poolState{}
		p.state[name] = st
	}
	st.LastSuccess = p.now().UTC()
	st.LastErr = ""
	st.LastErrAt = time.Time{}
	st.ConsecutiveFailures = 0
}

// recordFailure marks a failed poll for pool name: increments the
// consecutive-failure count and stamps the error and its time.
// lastSuccess is preserved so a recovering pool does not lose its
// "last good" timestamp; that timestamp drives the staleness math
// until a new success arrives.
func (p *Poller) recordFailure(name string, err error) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	st, ok := p.state[name]
	if !ok {
		st = &poolState{}
		p.state[name] = st
	}
	st.LastErr = err.Error()
	st.LastErrAt = p.now().UTC()
	st.ConsecutiveFailures++
}

// PoolStatus is the per-pool liveness observation surfaced through
// Status(). It mirrors the on-wire shape the admin handler emits:
// last_success is *time.Time so "never polled" is null in JSON, not
// the zero time masquerading as data; last_error / last_error_at /
// consecutive_failures follow the same omitempty convention.
type PoolStatus struct {
	LastSuccess         *time.Time `json:"last_success"`
	LastError           string     `json:"last_error,omitempty"`
	LastErrorAt         *time.Time `json:"last_error_at,omitempty"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	// Stale is the derived staleness verdict (>= StaleAfterIntervals since
	// lastSuccess, or no success ever recorded). Computed at read time
	// against the poller's injectable now so tests can drive it without
	// touching the admin handlers.
	Stale bool `json:"stale"`
}

// IsTracked reports whether the pool is currently being polled by this
// poller instance. The admin surface uses this to omit the poller block
// entirely for Anthropic / untracked pools (no delta in the JSON), so a
// caller reading the shape can tell "this pool has no poller signal at
// all" from "this pool's poller signal is stale or failing".
func (p *Poller) IsTracked(name string) bool {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	_, ok := p.state[name]
	return ok
}

// PoolTracked reports whether the named pool is configured for
// poller-tracking — a stable property of the pool's current active
// backend's base URL against the registered proprietary providers,
// independent of whether the poller has ticked yet. This is the
// single "is this pool tracked?" signal the admin surfaces should
// use, so a never-polled tracked pool surfaces the same shape
// (last_success:null, stale:true) on every endpoint instead of
// diverging between view-layer and state-map gates (review of #267).
//
// The gate splits cleanly:
//   - untracked (Anthropic): no poller block, no polled field, as_of
//     passes through unchanged
//   - tracked-but-never-polled (boot, no tick yet): poller block
//     present with last_success:null and stale:true; quota view
//     suppresses as_of
//   - tracked-and-polled: full state surface
//
// Returns false when pl is nil (tests bypass the poller).
func (p *Poller) PoolTracked(name, activeBaseURL string) bool {
	if p == nil {
		return false
	}
	_, ok := providerFor(activeBaseURL)
	if !ok {
		return false
	}
	return true
}

// StatusForPool returns the per-pool status entry for the named pool
// if it is a tracked pool (per PoolTracked). The boolean is the
// "tracked" verdict — false means "no poller signal at all", and the
// caller should omit the poller block entirely. Tracked pools whose
// state map has no entry yet (boot, before first tick) return a
// zero-valued status with Stale=true so the on-wire shape matches
// the never-polled contract from issue #247 / plan AC9.
func (p *Poller) StatusForPool(name, activeBaseURL string) (PoolStatus, bool) {
	if p == nil || !p.PoolTracked(name, activeBaseURL) {
		return PoolStatus{}, false
	}
	return p.lookupStatus(name)
}

// LookupStatus is the map-aware variant of StatusForPool: callers that
// already have a pre-built poller status map (AllPoolStatuses builds
// one per request, review of #267) pass it in to skip the per-pool
// mutex acquisition that Status() would otherwise repeat.
func (p *Poller) LookupStatus(prebuilt map[string]PoolStatus, name, activeBaseURL string) (PoolStatus, bool) {
	if p == nil || !p.PoolTracked(name, activeBaseURL) {
		return PoolStatus{}, false
	}
	if st, ok := prebuilt[name]; ok {
		return st, true
	}
	return p.lookupStatus(name)
}

// lookupStatus is the per-pool state-map read used by both the
// lazy-builder path (StatusForPool) and the map-aware path
// (LookupStatus). For a tracked pool with no state yet, returns a
// synthesised never-polled entry whose Stale verdict derives from
// the poller's current time and interval threshold — so callers do
// not need a clock of their own.
func (p *Poller) lookupStatus(name string) (PoolStatus, bool) {
	statuses := p.Status()
	if st, ok := statuses[name]; ok {
		return st, true
	}
	now := p.now()
	threshold := p.interval * StaleAfterIntervals
	return poolStatusFrom(&poolState{}, now, threshold), true
}

// Status returns a copy of the per-pool liveness observation map keyed
// by pool name. Pools the poller has never touched are absent from the
// returned map; callers that need to iterate every pool consult the
// pool registry directly. The map and its PoolStatus values are copies,
// so the caller can mutate them freely.
func (p *Poller) Status() map[string]PoolStatus {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	now := p.now()
	threshold := p.interval * StaleAfterIntervals
	out := make(map[string]PoolStatus, len(p.state))
	for name, st := range p.state {
		out[name] = poolStatusFrom(st, now, threshold)
	}
	return out
}

// HealthSummary returns the aggregate liveness verdict for
// /_gateway/health. Stale is true when at least one tracked pool is
// stale; absent-of-entry (a tracked pool never polled) is treated the
// same way — staleness has no "fresh enough" signal for it. The
// returned bool drives the additive `poller_health:"stale"` body field;
// absence of the field on the wire means "ok".
//
// `polled` is the count of tracked pools; `stale` is the count of
// those pools currently flagged stale (for an operator's at-a-glance
// read on the per-pool level). Both numbers are also useful to tests.
func (p *Poller) HealthSummary() (polled, stale int) {
	statuses := p.Status()
	return len(statuses), countStale(statuses)
}

func countStale(statuses map[string]PoolStatus) int {
	n := 0
	for _, s := range statuses {
		if s.Stale {
			n++
		}
	}
	return n
}

// poolStatusFrom builds the on-wire PoolStatus from the poller's
// internal state. now and threshold are passed in so the caller
// (Status) reads p.now() under the lock once and reuses the snapshot.
func poolStatusFrom(st *poolState, now time.Time, threshold time.Duration) PoolStatus {
	out := PoolStatus{
		ConsecutiveFailures: st.ConsecutiveFailures,
		LastError:           st.LastErr,
	}
	if !st.LastSuccess.IsZero() {
		ls := st.LastSuccess
		out.LastSuccess = &ls
	}
	if !st.LastErrAt.IsZero() {
		la := st.LastErrAt
		out.LastErrorAt = &la
	}
	// Stale: zero lastSuccess (never polled) OR lastSuccess older than
	// threshold. Either way the pool is missing fresh out-of-band data.
	if st.LastSuccess.IsZero() || now.Sub(st.LastSuccess) > threshold {
		out.Stale = true
	}
	return out
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

// Name returns the provider's registry label (e.g. "z.ai/zhipu"). It is
// the stable identifier the rest of the codebase uses to switch on
// provider-specific behaviour (see issue #138: the long-window column
// is monthly for z.ai/zhipu, weekly for everything else).
func (p provider) Name() string { return p.name }

const (
	// longWindow7d is the default long-window length: the Anthropic-style
	// 7-day rolling window.
	longWindow7d = 7 * 24 * time.Hour
	// longWindowMonthly is the fixed ~30-day approximation of Z.AI's
	// monthly TIME_LIMIT window (issue #140). A fixed constant keeps the
	// mapping a one-line switch and matches how the codebase already
	// approximates windows; it is faithful enough that the lead math no
	// longer collapses a monthly reset into a 7-day elapsed fraction.
	longWindowMonthly = 30 * 24 * time.Hour
)

// longWindowSpec bundles the per-provider long-window length and whether
// the window is a genuine chat-blocking signal, so the two cannot drift:
// both are produced by the single switch in longWindowSpecFor. The length
// feeds the lead-routing elapsed-fraction math (issue #140);
// blocksExhaustion feeds the auto package's exhaustion/failover/balance
// decisions (issue #192).
//
// blocksExhaustion is true for the default 7d window (Anthropic's real
// weekly window, and MiniMaxi's / Ark's weekly caps, are genuine chat
// quotas). It is false for Z.AI/Zhipu: its long slot carries the monthly
// TIME_LIMIT value, which is Z.AI's Total Monthly Web Search / Reader /
// Zread tool quota — Z.AI has no weekly/monthly *chat* quota at all — so
// letting it park a member would pull a chat-healthy backend out of
// rotation for a reason unrelated to chat throughput (issue #192).
type longWindowSpec struct {
	length           time.Duration
	blocksExhaustion bool
}

// longWindowSpecFor is the single provider switch behind LongWindowFor
// and LongWindowBlocksExhaustion. The default is the Anthropic-style
// 7-day / chat-blocking window. Z.AI's long window is monthly (issues
// #138/#140) and is NOT a chat-blocking signal (issue #192), so a Z.AI
// backend gets ~30-day / non-blocking. Adding a new provider with a
// non-default long window is a one-line change here — the only switch on
// provider name for window shape.
//
// The pre-#248 label half of the spec was retired together with the
// window_labels hint: every member in a pool was assumed to share one
// upstream provider, so the first member's BaseURL fed the UI column
// header. In a mixed pool the label flipped across failover (issue
// #154), and the UI fixed the header to "7/30D". The hint now has no
// consumer, and `label` was dropped from this struct alongside it.
func longWindowSpecFor(baseURL string) longWindowSpec {
	if p, ok := ProviderFor(baseURL); ok {
		switch p.Name() {
		case "z.ai/zhipu":
			return longWindowSpec{length: longWindowMonthly, blocksExhaustion: false}
		}
	}
	return longWindowSpec{length: longWindow7d, blocksExhaustion: true}
}

// LongWindowFor returns the per-pool long-window length used for the
// lead-routing elapsed-fraction (issue #140). It shares the single
// provider switch with LongWindowBlocksExhaustion so the routing math
// and the exhaustion gate always agree on which window a pool's long
// slot represents: ~30-day for Z.AI/Zhipu (its monthly TIME_LIMIT),
// 7-day otherwise.
func LongWindowFor(baseURL string) time.Duration {
	return longWindowSpecFor(baseURL).length
}

// LongWindowBlocksExhaustion reports whether a pool's long (7d/monthly)
// window is a genuine chat-blocking signal that the auto package should let
// drive exhaustion/failover/balance decisions. It shares the single
// provider switch with LongWindowFor. True for the default 7-day window
// (Anthropic/MiniMaxi/Ark weekly caps are real chat quotas); false for
// Z.AI/Zhipu, whose monthly TIME_LIMIT slot is a web-search/reader/zread
// tool quota, not chat throughput (issue #192). Unknown providers and an
// empty base URL fall back to the blocking default — fail closed rather
// than silently drop a real cap.
func LongWindowBlocksExhaustion(baseURL string) bool {
	return longWindowSpecFor(baseURL).blocksExhaustion
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

// ProviderFor exposes the provider registry to other packages. The recovery
// probe in internal/auto uses this to decide whether a parked member's base
// URL has a probeable quota endpoint (issue #124).
func ProviderFor(baseURL string) (provider, bool) {
	return providerFor(baseURL)
}

// ErrNoProvider is returned by Probe when the backend's base URL does not
// match any registered proprietary quota endpoint. Anthropic backends (and
// any other untracked provider) yield this error; the caller is expected
// to treat it as "no probe available, skip recovery" rather than a fault.
var ErrNoProvider = errors.New("poller: no provider registered for base URL")

// WithTestProviderForTest is the exported wrapper around the package-private
// test-only provider hook. Tests outside the poller package (notably
// internal/auto's recovery tests, see issue #124) use it to register a
// provider whose matcher accepts an httptest server's URL. The provider
// is removed via t.Cleanup. Production code must not call this.
func WithTestProviderForTest(t *testing.T, matchFragment string, quotaURL func(string) (string, error), sign func(*http.Request, string) error, parse func([]byte, time.Time) (quota.Snapshot, error)) {
	t.Helper()
	orig := providers
	providers = append([]provider{{
		name:     "test",
		matches:  func(u string) bool { return strings.Contains(u, matchFragment) },
		quotaURL: quotaURL,
		sign:     sign,
		parse:    parse,
	}}, providers...)
	t.Cleanup(func() { providers = orig })
}

// HostURLForTest, RawAuthForTest, ParseZhipuForTest expose the
// package-private builder helpers used by z.ai/zhipu's production provider
// entry, so external tests (notably internal/auto's recovery tests, see
// issue #124) can build a provider with the same behaviour against an
// httptest server. Production code must not call these.
func HostURLForTest(path string) func(string) (string, error) {
	return hostURL(path)
}

func RawAuthForTest(req *http.Request, credential string) error {
	return rawAuth(req, credential)
}

func ParseZhipuForTest(body []byte, now time.Time) (quota.Snapshot, error) {
	return parseZhipu(body, now)
}

// Probe fetches one quota snapshot for backend b via the registered
// proprietary endpoint (if any) and returns the parsed Snapshot. It mirrors
// (*Poller).pollOne so callers outside the poller's goroutine lifecycle
// (notably the recovery probe in internal/auto) can hit the same endpoint
// without owning a Poller instance. The supplied client must have a tight
// timeout — the recovery path expects probe latency to be bounded.
//
// Errors:
//   - ErrNoProvider when no provider matches b.BaseURL (Anthropic, etc.).
//   - The wrapped transport / non-200 / parse error from pollOne otherwise.
//
// As in pollOne, the parsed Snapshot is returned on a non-200 response only
// when parsing succeeds; on error the caller receives an empty Snapshot.
func Probe(ctx context.Context, b backend.Backend, client *http.Client, now func() time.Time) (quota.Snapshot, error) {
	prov, ok := providerFor(b.BaseURL)
	if !ok {
		return quota.Snapshot{}, ErrNoProvider
	}
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
	resp, err := client.Do(req)
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
	return prov.parse(body, now())
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
//
// Z.AI's TIME_LIMIT is the **monthly** quota, not a 7-day rolling window
// (issue #138). We keep storing it in the Unified7d* snapshot slot — that
// is the right data shape for a long-window utilization + reset — and let
// the UI label the column "monthly" for Z.AI pools.
//
// Any limit type that is not one of the two explicitly recognised ones
// (e.g. an upstream "MONTHLY_LIMIT" or "MONTH_LIMIT" string Z.AI may add
// later) falls into the long-window slot rather than being dropped, so a
// new upstream type does not silently lose data.
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
			// Monthly for Z.AI; long window in the snapshot.
			snap.Unified7dUtilization = floatPtr(l.Percentage / 100)
			snap.Unified7dReset = msToTime(l.NextResetTime)
		default:
			// Defensive: any other Z.AI limit type (e.g. "MONTHLY_LIMIT")
			// goes into the long-window slot rather than being dropped, so
			// the snapshot still surfaces whatever the upstream returned.
			// The first such entry wins; Z.AI only ever ships one long
			// window, but we tolerate multiple without panicking.
			if snap.Unified7dUtilization == nil {
				snap.Unified7dUtilization = floatPtr(l.Percentage / 100)
				snap.Unified7dReset = msToTime(l.NextResetTime)
			}
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
//
// The remaining-percent fields are decoded as *float64 so an absent field is
// distinguishable from a real 0: a plain float64 would read a missing
// current_interval_remaining_percent as 0, yielding utilization
// (100-0)/100 = 1.0 and fabricating full exhaustion from a partial/renamed
// body (issue #207). Each window is populated only when its remaining-percent
// is actually present, and the trailing !HasData() guard — matching parseZhipu
// / parseVolcengine — turns an unreadable entry (e.g. {}) into an error so
// store.Merge preserves the prior good snapshot instead of parking a healthy
// member.
func parseMinimaxi(body []byte, now time.Time) (quota.Snapshot, error) {
	var resp struct {
		ModelRemains []struct {
			CurrentIntervalRemainingPercent *float64 `json:"current_interval_remaining_percent"`
			CurrentWeeklyRemainingPercent   *float64 `json:"current_weekly_remaining_percent"`
			EndTime                         int64    `json:"end_time"`
			WeeklyEndTime                   int64    `json:"weekly_end_time"`
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
	if m.CurrentIntervalRemainingPercent != nil {
		snap.Unified5hUtilization = floatPtr((100 - *m.CurrentIntervalRemainingPercent) / 100)
		snap.Unified5hReset = msToTime(m.EndTime)
	}
	if m.CurrentWeeklyRemainingPercent != nil {
		snap.Unified7dUtilization = floatPtr((100 - *m.CurrentWeeklyRemainingPercent) / 100)
		snap.Unified7dReset = msToTime(m.WeeklyEndTime)
	}
	if !snap.HasData() {
		return quota.Snapshot{}, fmt.Errorf("no usable remaining-percent fields in response")
	}
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
