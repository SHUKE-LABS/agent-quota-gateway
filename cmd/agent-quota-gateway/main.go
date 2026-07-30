// Command agent-quota-gateway is a loopback-only reverse proxy for the
// Anthropic Messages API. See the README for usage.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/activity"
	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/configfile"
	"github.com/shukebeta/agent-quota-gateway/internal/logging"
	"github.com/shukebeta/agent-quota-gateway/internal/persist"
	"github.com/shukebeta/agent-quota-gateway/internal/poller"
	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
	"github.com/shukebeta/agent-quota-gateway/internal/reqlog"
)

// defaultBackendKey is the quota cache key used as a defensive fallback
// if a forwarded response somehow lacks a resolved backend on its
// context. In normal operation the resolver middleware guarantees one.
const defaultBackendKey = "default"

// hasQuotaWindow reports whether snap carries at least one quota-window
// field: 5h/7d status / utilization / reset, the legacy unified-status
// fields, or overage metadata. It deliberately excludes OrgID — that
// field is metadata only, and a Put on an org-id-only response would
// wipe the previously-cached 5h/7d resets to nil, causing the UI to
// flash the reset cells to "-" until the next quota-bearing response
// (issue #121). Distinct from Snapshot.HasData, whose contract is
// "any upstream-derived signal" and whose other callers (poolStatus,
// the test helper) want the broader form.
func hasQuotaWindow(s quota.Snapshot) bool {
	return s.UnifiedStatus != "" ||
		s.UnifiedReset != nil ||
		s.UnifiedRepresentativeClaim != "" ||
		s.Unified5hStatus != "" || s.Unified5hUtilization != nil || s.Unified5hReset != nil ||
		s.Unified7dStatus != "" || s.Unified7dUtilization != nil || s.Unified7dReset != nil ||
		s.UnifiedFallbackPercentage != nil ||
		s.UnifiedOverageStatus != "" || s.UnifiedOverageDisabledReason != ""
}

// version is stamped at build time via -ldflags "-X main.version=...".
// It defaults to "dev" for a plain `go build`. The deploy script sets it
// from `git describe` so an upgraded service is verifiable with -version.
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	configPath := flag.String("config", "", "path to a JSON config file (overrides AQG_CONFIG and ./aqg.json)")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if err := run(*configPath); err != nil {
		fmt.Fprintf(os.Stderr, "agent-quota-gateway: %v\n", err)
		os.Exit(1)
	}
}

func run(configFlag string) error {
	// Resolve the config source (issue #198): env-only local dev (no config
	// file), an existing aqg.json (env ignored), or first-deploy bootstrap
	// (generate aqg.json from env + legacy state, then read it). configPath is
	// "" in env-only mode, which disables config write-through.
	cfg, registry, configPath, err := resolveConfig(configFlag, os.Stderr)
	if err != nil {
		return err
	}

	store := quota.NewStore()

	// Load persisted state from the state file (if configured). A missing
	// or corrupt file starts fresh; any other I/O error aborts startup.
	persisted, err := persist.Load(cfg.StateFile)
	if err != nil {
		return fmt.Errorf("persist: load %q: %w", cfg.StateFile, err)
	}

	// Restore quota snapshots first so controllers can read them when
	// deciding the initial exhaustion state from the store.
	//
	// PR #115 changed Backend.QuotaKey() from "<pool>/<nick>" to "<nick>",
	// but the on-disk state file carries no version field and no migration
	// was added at the time. On restart, snapshots persisted under the old
	// shape are unreachable by any current lookup (Store.Get("<nick>")
	// returns the zero Snapshot until the next quota-bearing response
	// overwrites it). migrateSnapshotKeys rewrites them in place; nicks
	// not referenced by any current pool (env-declared or runtime-added)
	// are dropped with a logged warning so an operator can audit the loss.
	migrated, dropped := migrateSnapshotKeys(persisted, registry)
	for _, nick := range dropped {
		fmt.Fprintf(os.Stderr, "agent-quota-gateway: dropping orphaned quota snapshot for nick %q (no current pool references it)\n", nick)
	}
	for key, snap := range migrated {
		store.Put(key, snap)
	}

	// pools fronts every configured pool with its own sticky controller.
	// Each controller starts at a random member (start < 0) so no probe
	// traffic is needed to anchor it; its quota snapshot fills in from the
	// first real response. The controllers consult the shared store so a
	// member the poller or headers report fully consumed is failed off even
	// without a live 429 — the only exhaustion signal poller-tracked
	// backends (z.ai / MiniMaxi) ever produce.
	pools := auto.NewPools(registry, store, nil, nil)

	// Restore sticky pointers and exhausted maps from the persisted state.
	// Every member — including former runtime-added ones — is already present
	// from the config registry (issue #198), so a persisted sticky/snapshot
	// nick resolves immediately. Expired exhausted entries are dropped.
	pools.LoadPersistState(persisted.Pools)

	// Wire up the state persister (runtime observation only: sticky/exhausted/
	// snapshots). Operator intent is no longer persisted here — it lives in the
	// config file (issue #198). The persister goroutine is started below.
	statePersister := persist.NewPersister(cfg.StateFile, func() persist.GatewayState {
		return persist.GatewayState{
			Pools:     pools.PersistState(),
			Snapshots: store.Snapshot(),
		}
	})
	pools.SetOnMutate(statePersister.MarkDirty)
	store.SetOnChange(statePersister.MarkDirty)

	// Wire the config writer: every operator mutation re-serializes the whole
	// config registry to aqg.json (debounced atomic 0600). In env-only mode
	// configPath is "" and the writer is a no-op (no credentials on disk).
	configWriter := configfile.NewWriter(configPath, func() ([]byte, error) {
		return configfile.Marshal(cfg, pools.CurrentRegistry())
	})
	pools.SetOnConfigChange(configWriter.MarkDirty)

	// observer is invoked once per upstream response, before the proxy
	// streams the body back to the client. It extracts the rate-limit
	// headers and files the snapshot under the backend the resolver
	// middleware selected for the request. Header-only inspection — no
	// body access.
	//
	// We only Put snapshots that carry at least one quota-window field.
	// An upstream response with no rate-limit headers (e.g. a 5xx page,
	// or a future endpoint that doesn't return them) would otherwise
	// overwrite the last known-good snapshot with an empty one, which
	// would look to consumers like the quota state was reset. The same
	// shape fires for org-id-only responses (e.g. GET /v1/models carries
	// anthropic-organization-id but no rate-limit headers): a Put on such
	// a snapshot wipes the previously-cached 5h/7d resets to nil, and
	// the UI flashes the reset cells to "-" until the next quota-bearing
	// response lands (issue #121). hasQuotaWindow excludes OrgID — that
	// field is metadata and does not invalidate the last known quota
	// state.
	observer := func(resp *http.Response) {
		snap := quota.Extract(resp)
		if !hasQuotaWindow(snap) {
			return
		}
		key := defaultBackendKey
		if resp.Request != nil {
			if b, ok := backend.FromContext(resp.Request.Context()); ok {
				key = b.QuotaKey()
				// Mark this controller as having observed a snapshot for
				// the nick, so the pool status view does not flash another
				// pool's data for a runtime-added member (issue #111).
				pools.MarkLocalSnapshot(b.Pool, b.Nick)
			}
		}
		// Merge, not Put: a response reports only the windows it touched, so
		// a window absent from this response must not blank the reset it
		// taught us last time (issue #163).
		store.Merge(key, snap)
	}

	// The pools' ModifyResponse hook runs after the observer: it dispatches
	// to the controller of the pool the request resolved through and fails
	// over (429 -> 503) or forwards an honest 429 within that pool.
	proxyHandler, err := proxy.New(observer, pools.ModifyResponse)
	if err != nil {
		return fmt.Errorf("proxy: %w", err)
	}

	// The proxy owns the catch-all so every non-gateway path forwards
	// to the upstream (the upstream is the authority on what it serves).
	// The resolver middleware sits in front of it so every forwarded
	// request carries a resolved backend; the gateway's own /_gateway
	// endpoints are mounted directly and take no selector.
	// activityStore aggregates per-endpoint volume/error-rate/latency into a
	// rolling 60-minute window for /_gateway/activity and its UI panel. In
	// memory only, lost on restart (issue #217).
	activityStore := activity.New()

	// The poller fills the quota store for backends that never emit
	// Anthropic rate-limit headers (Z.ai / ZhipuAI, MiniMaxi) by polling
	// each provider's proprietary quota API for the active member of each
	// pool. It is a no-op for Anthropic and any other untracked backend.
	// NewDynamic (not New): the pool set is re-read from pools each tick, so a
	// pool created at runtime via POST /_gateway/pool is polled without a
	// restart (issue #202). Constructed here (before the mux wiring) so the
	// pool handler can attach per-pool poller liveness to its response
	// (issue #247); Run is started below.
	qp := poller.NewDynamic(pools.PoolNames, pools.Current, pools.MarkLocalSnapshot, store, nil, 0, nil, nil)

	mux := http.NewServeMux()
	persistence := configWriter.PersistenceStateOf()
	mux.HandleFunc("/_gateway/health", healthHandler(persistence, qp))
	mux.HandleFunc("/_gateway/quota", quotaHandler(store, pools, qp))
	mux.HandleFunc("/_gateway/activity", activityHandler(activityStore))
	mux.HandleFunc("/_gateway/pool", poolHandler(store, pools, qp))
	mux.HandleFunc("POST /_gateway/pool", createPoolHandler(pools, persistence))
	mux.HandleFunc("DELETE /_gateway/pool/{name}", deletePoolHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/rename", renamePoolHandler(pools, persistence))
	mux.HandleFunc("/_gateway/clear", clearHandler(pools))
	mux.HandleFunc("/_gateway/config", configHandler(pools, persistence))
	mux.HandleFunc("/_gateway/ui", uiHandler())
	mux.HandleFunc("POST /_gateway/pool/{name}/priority", priorityHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/disable", disableMemberHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/enable", enableMemberHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}/move", moveMemberHandler(pools, persistence))
	mux.HandleFunc("POST /_gateway/pool/{name}/member/{nick}", addMemberHandler(pools, persistence))
	mux.HandleFunc("DELETE /_gateway/pool/{name}/member/{nick}", removeMemberHandler(pools, persistence))
	mux.Handle("/", backend.Middleware(pools, proxyHandler))

	// activity.Middleware sits innermost (closest to the mux) so it observes
	// the same status the handler emits; it excludes /_gateway/* itself. Its
	// statusRecorder implements Unwrap, so logging's outer recorder and the
	// proxy's flusher still reach the real writer and SSE keeps streaming.
	handler := reqlog.Middleware(logging.Middleware(activity.Middleware(activityStore, mux)))

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// signal.NotifyContext gives us a context that cancels on SIGINT
	// or SIGTERM, which is the simplest way to wire graceful shutdown
	// without a hand-rolled signal channel.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The persister and config writer run on their own context, cancelled
	// only AFTER the HTTP server has drained (see the shutdown block below),
	// not on the signal. A mutation can complete during the 30s grace window
	// srv.Shutdown grants in-flight requests; if the writers had already
	// exited on the signal, that mutation's 200 would be acknowledged but its
	// MarkDirty() dropped (no reader on the buffered dirty channel), silently
	// losing operator intent — and any runtime-added credential — on the next
	// boot (issue #201). Draining the server first guarantees no mutation
	// outlives its writer.
	writersCtx, cancelWriters := context.WithCancel(context.Background())
	// defer covers the early errCh return path below; the shutdown path calls
	// cancelWriters() explicitly and then waits for the final flush.
	defer cancelWriters()
	var writersWG sync.WaitGroup
	writersWG.Add(2)

	// The persister coalesces state mutations and writes them atomically to
	// the state file.
	go func() { defer writersWG.Done(); statePersister.Run(writersCtx) }()

	// The config writer flushes operator mutations to aqg.json. No-op in
	// env-only mode (empty config path).
	go func() { defer writersWG.Done(); configWriter.Run(writersCtx) }()

	// The poller fills the quota store for backends that never emit
	// Anthropic rate-limit headers (Z.ai / ZhipuAI, MiniMaxi) by polling
	// each provider's proprietary quota API for the active member of each
	// pool. It is a no-op for Anthropic and any other untracked backend.
	// It shares the shutdown context, so it stops when the process does.
	// qp is constructed earlier in this function so the pool/quota/health
	// handlers can attach per-pool poller liveness to their responses
	// (issue #247).
	go qp.Run(ctx)

	// The preemptor returns a priority pool to a higher-priority member once
	// that member's quota window resets, so a freshly-reset preferred backend
	// is drained promptly instead of riding the active fallback until it 429s.
	// It only touches pools that declared AQG_POOL_<POOL>_PRIORITY and returns
	// immediately when none did. It shares the shutdown context.
	pre := auto.NewPreemptor(pools, store, 0, nil, nil)
	go pre.Run(ctx)

	// The recovery loop re-checks parked non-active members on a bounded
	// cadence (issue #242). It fills the gap left by tryRecoverParked
	// (allExhausted only) and the preemptor (priority pools only): a plain
	// pool whose parked nick is no longer the active sticky backend — e.g.
	// because a healthy sibling has taken over, including one added after
	// the park — had no remaining self-heal path. It only clears the live
	// park; it never moves the sticky pointer. It is a no-op when no
	// controller has a parked non-active member, so a healthy pool pays no
	// extra probe traffic. It shares the shutdown context.
	rec := auto.NewRecovery(pools, 0, nil)
	go rec.Run(ctx)

	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	if cfg.Shared {
		// Shared mode is off-loopback: every tailnet member that can reach
		// this port can drive the pools and read /_gateway/quota. The
		// gateway adds no auth — the Tailscale ACL is the only gate — so
		// make the exposure loud rather than let it pass as a normal start.
		fmt.Fprintf(os.Stderr, "agent-quota-gateway: SHARED MODE — bound to Tailscale address %s, reachable by tailnet members. A Tailscale ACL restricting this port is REQUIRED; the gateway adds no authentication of its own.\n", cfg.ListenAddr)
	}
	fmt.Fprintf(os.Stderr, "agent-quota-gateway %s listening on %s; default upstream %s; pools %s\n", version, cfg.ListenAddr, cfg.AnthropicBaseURL, strings.Join(registry.PoolNames(), ", "))

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "agent-quota-gateway shutting down")
	}

	// Give in-flight requests a short grace period before the process
	// exits. Streaming Messages responses can run for many seconds, so
	// 30s is the smallest reasonable window that does not interrupt
	// them. Future versions may make this configurable.
	if err := gracefulShutdown(srv, 30*time.Second, cancelWriters, &writersWG); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

// gracefulShutdown drains the HTTP server, then stops the background writers
// and waits for their final flush. The ordering is load-bearing (issue #201):
// draining the server FIRST guarantees no mutation can still arrive, and a
// mutation acked 200 during the grace window is observed by a still-running
// writer (its debounce flush, or the writer's final flush after cancelWriters).
// Cancelling the writers before the drain would drop any grace-window
// mutation's MarkDirty — a silently-lost operator change, including a
// runtime-added credential, on the next boot.
func gracefulShutdown(srv *http.Server, timeout time.Duration, cancelWriters context.CancelFunc, writers *sync.WaitGroup) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := srv.Shutdown(shutdownCtx)
	cancelWriters()
	writers.Wait()
	return err
}

// migrateSnapshotKeys rewrites persisted quota snapshots from the
// pre-#115 "<pool>/<nick>" key shape to the current "<nick>" shape, and
// drops any nick not referenced by any current pool (env-declared or
// runtime-added). When both old and new keys exist for the same nick,
// the new key wins (a two-pass rewrite where Pass 2 overwrites Pass 1).
//
// The known-nicks set is the live registry (PoolNicks for each PoolNames
// entry). With config as the single source of truth (issue #198), the
// registry already contains every member — including former runtime-added
// ones — so no separate overlay source is needed here.
//
// The function returns the rewritten map and the de-duplicated list of
// dropped nicks (first-seen order) so the caller can log the loss.
func migrateSnapshotKeys(state persist.GatewayState, registry *backend.Registry) (map[string]quota.Snapshot, []string) {
	knownNicks := make(map[string]bool)
	for _, name := range registry.PoolNames() {
		for _, nick := range registry.PoolNicks(name) {
			knownNicks[nick] = true
		}
	}

	// nickFromKey returns the nick portion of a snapshot key — the suffix
	// after the last "/" if the key carries a pool prefix, otherwise the
	// key itself. Used to translate old-shape "<pool>/<nick>" keys into
	// the current nick-only shape.
	nickFromKey := func(key string) string {
		if i := strings.LastIndex(key, "/"); i >= 0 {
			return key[i+1:]
		}
		return key
	}

	// Pass 1: rewrite every key into nick form, dropping unknown nicks.
	migrated := make(map[string]quota.Snapshot, len(state.Snapshots))
	droppedSet := make(map[string]bool)
	var dropped []string
	for key, snap := range state.Snapshots {
		nick := nickFromKey(key)
		if !knownNicks[nick] {
			if !droppedSet[nick] {
				droppedSet[nick] = true
				dropped = append(dropped, nick)
			}
			continue
		}
		migrated[nick] = snap
	}
	// Pass 2: re-walk new-shape (no-slash) keys so a collisional pair
	// (e.g. {"auto/ccw": old, "ccw": new}) resolves to the new-key value.
	// Pass 1 already wrote the old-key value under "ccw"; Pass 2 overwrites
	// it only when a new-shape key is present, which is exactly the
	// "prefer the new key" guarantee from issue #116's AC.
	for key, snap := range state.Snapshots {
		if strings.Contains(key, "/") {
			continue
		}
		if !knownNicks[key] {
			continue
		}
		migrated[key] = snap
	}
	return migrated, dropped
}

// healthHandler returns a fixed {"status":"ok"} body. It is a loopback-
// only liveness probe — the loopback trust model means it carries no
// sensitive state, so the response shape is deliberately minimal and
// does not expose the version, uptime, or upstream reachability. Any
// additional readiness signal would belong on a separate endpoint so
// callers can tell "process is alive" from "upstream is reachable".
// Method is GET only; non-GET requests receive 405 — matching
// quotaHandler's policy so the two /_gateway/* endpoints agree.
//
// persistence (issue #246) carries the three-state config-durability
// signal on a single additive body field:
//   - clean persisted:      {"status":"ok"}
//   - persisted, unsaved:   {"status":"ok","unsaved_config_changes":true}
//   - env-only:             {"status":"ok","persistence":"env_only"}
//
// poller_health (issue #247) is emitted conditionally as the additive
// "poller_health":"stale" field when the poller reports at least one
// tracked pool as stale. It mirrors unsaved_config_changes (also
// conditional) — absence of the field means "ok". The poller is the
// source of truth for the staleness math (owning the StaleAfterIntervals
// constant); the handler just reads the aggregate. pl may be nil in
// tests, in which case the field is never emitted.
//
// Status code stays 200 in every case; a readiness probe asserts on
// "status", not on a byte-for-byte body match (README §Health).
func healthHandler(persistence configfile.PersistenceState, pl *poller.Poller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		configfile.ApplyPersistenceHeader(w, persistence)
		w.WriteHeader(http.StatusOK)
		var body string
		switch {
		case persistence.IsEnvOnly():
			body = `{"status":"ok","persistence":"env_only"`
		case persistence.Unsaved != nil && persistence.Unsaved():
			body = `{"status":"ok","unsaved_config_changes":true`
		default:
			body = `{"status":"ok"`
		}
		// poller_health is additive and conditional. Emit only when stale,
		// matching unsaved_config_changes / persistence — absence means
		// "ok". The polled-pool count and stale count are kept internal
		// to the poller package; the wire shape is the same either way.
		if pl != nil {
			if _, stale := pl.HealthSummary(); stale > 0 {
				body += `,"poller_health":"stale"`
			}
		}
		body += `}`
		_, _ = w.Write([]byte(body))
	}
}

// WindowLabels is a local alias for poller.WindowLabels so the JSON
// shape stays package-local. The mapping itself (z.ai/zhipu → monthly,
// everything else → 7d) lives in poller.WindowLabelsFor — adding a new
// provider with a non-7d long window is a one-line change there, and
// both this server and the auto package pick it up automatically.
type WindowLabels = poller.WindowLabels

// WindowLabelsFor returns the per-pool rolling-window label hint the UI
// consumes to render the long-window column. Wraps poller.WindowLabelsFor
// so the JSON-tagged alias above can stay a one-liner.
func WindowLabelsFor(baseURL string) WindowLabels {
	return poller.WindowLabelsFor(baseURL)
}

// quotaViewNoAsOf is the same shape as poolQuotaView but with AsOf
// re-declared as *time.Time so json.Marshal skips it on the zero value
// (the standard omitempty convention). Used only when quotaHandler
// suppresses AsOf for a tracked never-polled pool (AC5). The field name
// matches quota.Snapshot's `json:"as_of"` tag so the on-wire shape is
// identical when present and cleanly absent when suppressed.
type quotaViewNoAsOf struct {
	quota.Snapshot
	ActiveBackend string       `json:"active_backend"`
	WindowLabels  WindowLabels `json:"window_labels"`
	Polled        bool         `json:"polled,omitempty"`
	// AsOf shadows the embedded Snapshot.AsOf so the json tag wins.
	AsOf *time.Time `json:"as_of,omitempty"`
}

// encodeViewWithoutAsOf marshals the view to w with AsOf suppressed.
// Used only for the AC5 "tracked never-polled pool" branch.
func encodeViewWithoutAsOf(w http.ResponseWriter, view poolQuotaView) error {
	return json.NewEncoder(w).Encode(quotaViewNoAsOf{
		Snapshot:      view.Snapshot,
		ActiveBackend: view.ActiveBackend,
		WindowLabels:  view.WindowLabels,
		Polled:        view.Polled,
		// AsOf intentionally nil — omitempty drops the key.
	})
}

// poolQuotaView is the /_gateway/quota?backend=<pool> response: the
// pool's active sticky member's snapshot with an added active_backend
// field naming the member nick. The embedded Snapshot promotes its
// fields into the same JSON object, so a consumer that asks for a pool
// gets the active member's snapshot plus the member's name — it needs
// zero knowledge of pool membership, and the 99%->5% jump on a switch is
// self-explained because active_backend changes alongside it.
//
// WindowLabels is per-pool because the long-window column means different
// things for different providers: a 7-day rolling window for Anthropic
// and MiniMaxi, a monthly window for Z.AI (issue #138). The snapshot
// struct's 5h/7d field names stay — they are the right data shape; only
// the human-facing label changes.
//
// Polled (issue #247) reports whether this pool has ever been polled
// successfully. It is omitted entirely (key absent) for pools the
// poller does not track — Anthropic and any other untracked backend.
// The known-pool branch in quotaHandler uses Polled to decide whether
// to forward Snapshot.AsOf: for a tracked never-polled pool the field
// is dropped (Store.Get's read-time stamp is not data, issue #121
// framing); for a tracked polled pool or any untracked pool AsOf is
// forwarded as today.
type poolQuotaView struct {
	quota.Snapshot
	ActiveBackend string       `json:"active_backend"`
	WindowLabels  WindowLabels `json:"window_labels"`
	Polled        bool         `json:"polled,omitempty"`
}

// quotaHandler returns the JSON snapshot for the requested pool.
//
// Method is GET only — POSTing here would suggest the endpoint mutates
// state, which it does not. The pool name comes from the `?backend=`
// query param. A known pool resolves to its active sticky member and adds
// active_backend; the embedded Snapshot's AsOf is suppressed when this
// is a tracked never-polled pool (Store.Get's read-time stamp is not
// data for that case — issue #247). An unknown pool (or a missing
// param) returns 200 with a bare quota.Snapshot; that branch is left
// untouched on purpose (AC5 targets known pools with no snapshot, not
// unknown pool names — see plan review).
func quotaHandler(store *quota.Store, pools *auto.Pools, pl *poller.Poller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Normalize the pool name the same way the routing path does, so a
		// diagnostic query like ?backend=AUTO matches pool "auto".
		key := backend.NormalizeName(r.URL.Query().Get("backend"))
		w.Header().Set("Content-Type", "application/json")
		if pools != nil {
			if b, ok := pools.Current(key); ok {
				snap := store.Get(b.QuotaKey())
				view := poolQuotaView{
					Snapshot:      snap,
					ActiveBackend: b.Nick,
					WindowLabels:  WindowLabelsFor(b.BaseURL),
				}
				// AC5 (issue #247) and review of #267: use the same
				// config-based "is this pool tracked?" signal as
				// /_gateway/pool — the active backend's BaseURL matches
				// a registered poller provider. The two surfaces must
				// agree on the never-polled case: tracked pool with no
				// successful poll yet → suppress AsOf and set Polled
				// only when a LastSuccess exists. Untracked (Anthropic)
				// pools see no Polled key and forward AsOf unchanged.
				if ps, ok := pl.StatusForPool(key, b.BaseURL); ok {
					if ps.LastSuccess != nil {
						view.Polled = true
					} else {
						// Tracked but never polled — suppress the
						// read-time stamp (Snapshot.AsOf has no
						// `omitempty` tag, so re-marshal without it).
						_ = encodeViewWithoutAsOf(w, view)
						return
					}
				}
				_ = json.NewEncoder(w).Encode(view)
				return
			}
		}
		_ = json.NewEncoder(w).Encode(store.Get(key))
	}
}

// clearHandler serves POST /_gateway/clear — drops live-429 parks so a
// member wrongly marked exhausted (e.g. a transient/misconfigured 429 on an
// account that still has quota) becomes selectable again without waiting out
// the park or restarting the gateway. With ?pool=<name> it clears one pool;
// without the param it clears every pool. With ?pool=<name>&nick=<nick> it
// clears only that one member's park (issue #147) — the per-nick escape hatch
// for an over-parked member when the rest of the pool should stay parked. Only
// the reactive 429 parks are cleared — store-sourced exhaustion reflects polled
// reality and is left alone. Non-POST returns 405.
func clearHandler(pools *auto.Pools) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		poolName := backend.NormalizeName(r.URL.Query().Get("pool"))
		nick := backend.NormalizeName(r.URL.Query().Get("nick"))
		// A nick is meaningless without the pool it lives in. Reject it
		// explicitly BEFORE the pool-less all-pools branch below — otherwise
		// ?nick=x with no pool would silently nuke every pool's parks.
		if nick != "" {
			if poolName == "" {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "nick requires pool"})
				return
			}
			cleared, ok := pools.ClearExhaustedNick(poolName, nick)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool not found"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"pool": poolName, "nick": nick, "cleared": cleared})
			return
		}
		if poolName == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{"cleared": pools.ClearAllExhausted()})
			return
		}
		cleared, ok := pools.ClearExhausted(poolName)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"pool": poolName, "cleared": cleared})
	}
}

// poolHandler serves GET /_gateway/pool — the per-member health view for
// every configured pool. With ?pool=<name> it returns a single pool; without
// the param it returns all pools in sorted order. Non-GET returns 405.
//
// pl carries the per-pool poller liveness observation (issue #247). It may
// be nil (e.g. tests that bypass the poller goroutine), in which case the
// poller field is omitted from every response — same behaviour as the
// handler omitting it for untracked pools. Handler-owned: it does not own
// the poller goroutine; the caller wires that.
func poolHandler(store *quota.Store, pools *auto.Pools, pl *poller.Poller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		poolName := backend.NormalizeName(r.URL.Query().Get("pool"))
		if poolName == "" {
			_ = json.NewEncoder(w).Encode(pools.AllPoolStatuses(store, pl))
			return
		}
		status, ok := pools.PoolStatus(poolName, store, pl)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "pool not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(status)
	}
}
