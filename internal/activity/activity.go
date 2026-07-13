// Package activity is a bounded, in-memory, time-bucketed aggregator of
// per-endpoint request health. It turns the same per-request signal the
// logging middleware emits to stderr (path/status/duration/ts) into a
// rolling time series the gateway can serve from /_gateway/activity, giving
// operators a live read on which endpoint is healthier/faster over the recent
// past — signal the point-in-time /_gateway/{health,quota,pool,config}
// endpoints don't carry (issue #217).
//
// Ephemeral by design: in-memory only, lost on restart. No persistence, no
// state-file overlay. The same V1 credential-boundary constraint as the log
// line holds — only path/status/duration/ts are ever recorded; never bodies,
// headers, or credentials.
//
// Memory is bounded regardless of traffic: a fixed ring of buckets, a per-
// bucket cap on distinct paths (overflow folded into a single "(other)"
// counter), and a fixed-size latency sample ring per path. Nothing here grows
// with request volume or path cardinality.
package activity

import (
	"sort"
	"sync"
	"time"
)

const (
	// DefaultBuckets × DefaultGranularity = the rolling window. 60 one-minute
	// buckets == the "last 60 minutes" the UI panel advertises.
	DefaultBuckets     = 60
	DefaultGranularity = time.Minute

	// sampleCap bounds latency memory per path per bucket. Percentiles are
	// nearest-rank over the most recent sampleCap durations; older samples in
	// the same bucket are overwritten ring-style. 256 keeps p50/p95 stable for
	// a busy minute without unbounded growth.
	sampleCap = 256

	// defaultMaxPaths caps distinct paths tracked per bucket. The real
	// endpoint set is tiny (/v1/messages, /v1/messages/count_tokens, /, …); the
	// cap only defends against a pathological client varying the path to blow
	// memory. Overflow paths are folded into the "(other)" sentinel so their
	// volume/errors are still counted, just not broken out or latency-summarized.
	defaultMaxPaths = 64

	// otherKey collects volume/errors for paths beyond the per-bucket cap.
	// Parenthesized so it can never collide with a real request path.
	otherKey = "(other)"
)

// pathStat accumulates one path's activity within one bucket.
type pathStat struct {
	count      int
	errorCount int
	// samples is a ring of the most recent latencies (nanoseconds). nSamples
	// is the total observed; index nSamples%sampleCap is the next write slot,
	// so once full the oldest sample is overwritten.
	samples  [sampleCap]int64
	nSamples int
}

func (p *pathStat) record(status int, d time.Duration) {
	p.count++
	if !ok2xx(status) {
		p.errorCount++
	}
	p.samples[p.nSamples%sampleCap] = int64(d)
	p.nSamples++
}

// percentiles returns nearest-rank p50/p95 and the max over the retained
// samples, in nanoseconds. Zero for an empty stat.
func (p *pathStat) percentiles() (p50, p95, max int64) {
	n := p.nSamples
	if n > sampleCap {
		n = sampleCap
	}
	if n == 0 {
		return 0, 0, 0
	}
	s := make([]int64, n)
	copy(s, p.samples[:n])
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[rankIndex(n, 50)], s[rankIndex(n, 95)], s[n-1]
}

// bucket is one granularity slice of activity. epoch is now/granularity at the
// time it was (re)opened; a slot whose stored epoch != the epoch being written
// is stale and reset before use, which is how the ring recycles slots without
// a background sweeper.
type bucket struct {
	epoch int64
	paths map[string]*pathStat
	other struct {
		count      int
		errorCount int
	}
}

// Store is the goroutine-safe aggregator. All access is under mu; the read and
// write paths are both cheap (a handful of map ops), so a single mutex keeps it
// race-clean without the complexity of finer-grained locking.
type Store struct {
	mu          sync.Mutex
	buckets     []bucket
	granularity time.Duration
	maxPaths    int
}

// New returns a Store with the default 60×1min window.
func New() *Store { return NewWith(DefaultBuckets, DefaultGranularity, defaultMaxPaths) }

// NewWith builds a Store with an explicit ring size, granularity, and per-bucket
// path cap. Exposed for tests to drive a small, fast window.
func NewWith(buckets int, granularity time.Duration, maxPaths int) *Store {
	if buckets < 1 {
		buckets = 1
	}
	if granularity <= 0 {
		granularity = DefaultGranularity
	}
	if maxPaths < 1 {
		maxPaths = 1
	}
	return &Store{
		buckets:     make([]bucket, buckets),
		granularity: granularity,
		maxPaths:    maxPaths,
	}
}

// Record files one request. now is passed in (not read from the clock) so tests
// are deterministic and the caller controls the timestamp. Paths beyond the
// per-bucket cap fold into "(other)".
func (s *Store) Record(path string, status int, d time.Duration, now time.Time) {
	epoch := now.UnixNano() / int64(s.granularity)
	slot := int(mod(epoch, int64(len(s.buckets))))

	s.mu.Lock()
	defer s.mu.Unlock()

	b := &s.buckets[slot]
	if b.epoch != epoch || b.paths == nil {
		// Slot belongs to an older (or never-used) epoch — recycle it.
		b.epoch = epoch
		b.paths = make(map[string]*pathStat)
		b.other.count = 0
		b.other.errorCount = 0
	}

	if ps, ok := b.paths[path]; ok {
		ps.record(status, d)
		return
	}
	if len(b.paths) >= s.maxPaths {
		b.other.count++
		if !ok2xx(status) {
			b.other.errorCount++
		}
		return
	}
	ps := &pathStat{}
	ps.record(status, d)
	b.paths[path] = ps
}

// Latency carries a bucket's latency summary in milliseconds. For streaming
// /v1/messages these are full-SSE end-to-end wall times, not TTFT (the
// middleware measures the whole request), which the UI labels accordingly.
type Latency struct {
	P50Ms float64 `json:"p50Ms"`
	P95Ms float64 `json:"p95Ms"`
	MaxMs float64 `json:"maxMs"`
}

// Point is one bucket's summary for one endpoint.
type Point struct {
	BucketStart time.Time `json:"bucketStart"`
	Volume      int       `json:"volume"`
	Errors      int       `json:"errors"`
	ErrorRate   float64   `json:"errorRate"`
	Latency     Latency   `json:"latency"`
}

// Snapshot returns the live series keyed by endpoint path, each a chronological
// (oldest→newest) slice of up to len(buckets) points covering the window ending
// at now. Stale slots (belonging to epochs outside the window) are skipped, so
// a quiet endpoint simply has fewer points.
func (s *Store) Snapshot(now time.Time) map[string][]Point {
	epoch := now.UnixNano() / int64(s.granularity)
	n := int64(len(s.buckets))

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string][]Point)
	for e := epoch - n + 1; e <= epoch; e++ {
		b := &s.buckets[mod(e, n)]
		if b.paths == nil || b.epoch != e {
			continue
		}
		start := time.Unix(0, e*int64(s.granularity)).UTC()
		for path, ps := range b.paths {
			p50, p95, mx := ps.percentiles()
			out[path] = append(out[path], Point{
				BucketStart: start,
				Volume:      ps.count,
				Errors:      ps.errorCount,
				ErrorRate:   errorRate(ps.errorCount, ps.count),
				Latency: Latency{
					P50Ms: msFloat(p50),
					P95Ms: msFloat(p95),
					MaxMs: msFloat(mx),
				},
			})
		}
		if b.other.count > 0 {
			out[otherKey] = append(out[otherKey], Point{
				BucketStart: start,
				Volume:      b.other.count,
				Errors:      b.other.errorCount,
				ErrorRate:   errorRate(b.other.errorCount, b.other.count),
			})
		}
	}
	return out
}

func ok2xx(status int) bool { return status >= 200 && status < 300 }

// rankIndex maps a percentile to a 0-based nearest-rank index into a sorted
// slice of length n (n >= 1). p50 of 1 sample is that sample; p95 rounds up.
func rankIndex(n, pct int) int {
	// ceil(pct/100 * n) - 1, clamped to [0, n-1].
	idx := (pct*n+99)/100 - 1
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}

func errorRate(errors, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(errors) / float64(total)
}

func msFloat(ns int64) float64 { return float64(ns) / float64(time.Millisecond) }

// mod is a non-negative modulo. Epochs are always positive in practice, but
// the explicit wrap keeps the ring index safe if that ever changes.
func mod(a, m int64) int64 {
	r := a % m
	if r < 0 {
		r += m
	}
	return r
}
