// Package codexhello starts a new Codex weekly session after its stored
// secondary window reset expires. It sends only a fixed gateway-created
// prompt from a background loop; client traffic remains opaque.
package codexhello

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/auto"
	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/proxy"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

const (
	requestTimeout = 30 * time.Second
	idleFallback   = 10 * time.Minute
	requestBody    = `{"model":"gpt-5.2-codex","input":"hi"}`
	responsesPath  = "/responses"
)

// Config supplies the live member view and side effects for the background
// hello loop. Members must resolve the current registry on each call so
// runtime disable/remove changes take effect without restarting the process.
type Config struct {
	Members   func() []backend.Backend
	MarkLocal func(poolName, nick string)
	Store     *quota.Store
	Client    *http.Client
	Timeout   time.Duration
	IdleEvery time.Duration
	Now       func() time.Time
	Log       io.Writer
}

// Service attempts one hello per account and weekly-reset pair. Its attempt
// set is process-local by design; a restart may send one additional hello for
// the same reset window.
type Service struct {
	members   func() []backend.Backend
	markLocal func(poolName, nick string)
	store     *quota.Store
	client    *http.Client
	timeout   time.Duration
	idleEvery time.Duration
	now       func() time.Time
	log       io.Writer

	attemptMu sync.Mutex
	attempted map[attemptKey]struct{}
}

type attemptKey struct {
	quotaKey string
	resetNS  int64
}

// New constructs a hello service. Zero-valued timing and output options use
// the production timeout, idle cadence, wall clock, and stderr.
func New(cfg Config) *Service {
	if cfg.Client == nil {
		cfg.Client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = requestTimeout
	}
	if cfg.IdleEvery <= 0 {
		cfg.IdleEvery = idleFallback
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Log == nil {
		cfg.Log = os.Stderr
	}
	return &Service{
		members:   cfg.Members,
		markLocal: cfg.MarkLocal,
		store:     cfg.Store,
		client:    cfg.Client,
		timeout:   cfg.Timeout,
		idleEvery: cfg.IdleEvery,
		now:       cfg.Now,
		log:       cfg.Log,
		attempted: make(map[attemptKey]struct{}),
	}
}

// Run scans immediately, then sleeps until the next known weekly reset or
// the idle fallback cadence, whichever comes first. It exits on cancellation.
func (s *Service) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		nextReset := s.tick(ctx)
		wait := s.idleEvery
		if !nextReset.IsZero() {
			until := nextReset.Sub(s.now())
			if until <= 0 {
				until = time.Millisecond
			}
			if until < wait {
				wait = until
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}
}

// tick scans a stable current-registry view. It returns the earliest known
// future reset that still has an older snapshot, allowing Run to wake at that
// reset while the idle cadence catches registry changes and missing data.
func (s *Service) tick(ctx context.Context) time.Time {
	if s.members == nil || s.store == nil {
		return time.Time{}
	}
	now := s.now().UTC()
	members := s.members()
	var nextReset time.Time
	for _, b := range members {
		if ctx.Err() != nil {
			return nextReset
		}
		if b.Disabled || !auto.IsCodexBackend(b) {
			continue
		}
		snap := s.store.Get(b.QuotaKey())
		if snap.Unified7dReset == nil || !snap.AsOf.Before(*snap.Unified7dReset) {
			continue
		}
		reset := snap.Unified7dReset.UTC()
		if reset.After(now) {
			if nextReset.IsZero() || reset.Before(nextReset) {
				nextReset = reset
			}
			continue
		}
		// Re-read both sources before claiming so a concurrent organic response
		// or runtime registry mutation can retire this candidate before send.
		latest := s.store.Get(b.QuotaKey())
		if latest.Unified7dReset == nil || !latest.Unified7dReset.Equal(reset) || !latest.AsOf.Before(reset) {
			continue
		}
		current, ok := s.enabledCodexMember(b.QuotaKey())
		if !ok {
			continue
		}
		if !s.claim(b.QuotaKey(), reset) {
			continue
		}
		s.send(ctx, current)
	}
	return nextReset
}

func (s *Service) enabledCodexMember(key string) (backend.Backend, bool) {
	if s.members == nil {
		return backend.Backend{}, false
	}
	for _, b := range s.members() {
		if !b.Disabled && b.QuotaKey() == key && auto.IsCodexBackend(b) {
			return b, true
		}
	}
	return backend.Backend{}, false
}

func (s *Service) claim(key string, reset time.Time) bool {
	k := attemptKey{quotaKey: key, resetNS: reset.UnixNano()}
	s.attemptMu.Lock()
	defer s.attemptMu.Unlock()
	if _, exists := s.attempted[k]; exists {
		return false
	}
	// Claim before the network attempt: failures must not prompt on every tick.
	s.attempted[k] = struct{}{}
	return true
}

func (s *Service) send(parent context.Context, b backend.Backend) {
	endpoint, err := responsesURL(b.BaseURL)
	if err != nil {
		s.logFailure(b, "invalid upstream URL")
		return
	}
	ctx, cancel := context.WithTimeout(parent, s.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(requestBody))
	if err != nil {
		s.logFailure(b, "request construction failed")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	proxy.StampAuth(req.Header, b.Credential)

	resp, err := s.client.Do(req)
	if err != nil {
		s.logFailure(b, "request failed")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		s.logFailure(b, fmt.Sprintf("upstream returned %s", resp.Status))
		return
	}

	// Admit only quota-bearing Codex headers. In particular, an empty 2xx
	// must not overlay the old snapshot and advance AsOf past the reset.
	observedAt := s.now().UTC()
	codex := quota.ExtractCodex(resp.Header, observedAt)
	if !codex.HasData() {
		return
	}
	snap := quota.Snapshot{AsOf: observedAt}
	snap.OverlayCodex(s.store.Get(b.QuotaKey()), codex)
	s.store.Merge(b.QuotaKey(), snap)

	// The hello is observed through this pool. Mark only that local view; the
	// quota snapshot itself remains shared by account key across all pools.
	if s.markLocal != nil {
		s.markLocal(b.Pool, b.Nick)
	}
}

func (s *Service) logFailure(b backend.Backend, message string) {
	if s.log != nil {
		fmt.Fprintf(s.log, "codex-hello[%s/%s]: %s\n", b.Pool, b.Nick, strings.ReplaceAll(message, "\n", " "))
	}
}

func responsesURL(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid base URL")
	}
	path := strings.TrimRight(u.EscapedPath(), "/") + responsesPath
	u.Path, err = url.PathUnescape(path)
	if err != nil {
		return "", fmt.Errorf("invalid base URL path")
	}
	u.RawPath = path
	u.RawQuery = ""
	u.Fragment = ""
	u.RawFragment = ""
	u.User = nil
	return u.String(), nil
}
