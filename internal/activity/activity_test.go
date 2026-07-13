package activity

import (
	"testing"
	"time"
)

// base is a fixed, granularity-aligned instant so bucket epochs are
// predictable across the tests.
var base = time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)

func TestStore_bucketsByPath(t *testing.T) {
	s := New()
	s.Record("/v1/messages", 200, 100*time.Millisecond, base)
	s.Record("/v1/messages", 500, 200*time.Millisecond, base)
	s.Record("/v1/messages/count_tokens", 200, 10*time.Millisecond, base)

	snap := s.Snapshot(base)
	if len(snap) != 2 {
		t.Fatalf("want 2 endpoints, got %d: %v", len(snap), keys(snap))
	}

	msgs := snap["/v1/messages"]
	if len(msgs) != 1 {
		t.Fatalf("/v1/messages: want 1 bucket, got %d", len(msgs))
	}
	if msgs[0].Volume != 2 || msgs[0].Errors != 1 {
		t.Errorf("/v1/messages: volume=%d errors=%d, want 2/1", msgs[0].Volume, msgs[0].Errors)
	}
	if msgs[0].ErrorRate != 0.5 {
		t.Errorf("/v1/messages: errorRate=%v, want 0.5", msgs[0].ErrorRate)
	}
	if msgs[0].Latency.MaxMs != 200 {
		t.Errorf("/v1/messages: maxMs=%v, want 200", msgs[0].Latency.MaxMs)
	}

	ct := snap["/v1/messages/count_tokens"]
	if len(ct) != 1 || ct[0].Volume != 1 || ct[0].Errors != 0 {
		t.Errorf("count_tokens: %+v, want volume 1 / errors 0", ct)
	}
}

func TestStore_nonGatewayStatusClasses(t *testing.T) {
	s := New()
	// 2xx and 3xx boundaries: only 200-299 are non-error.
	s.Record("/x", 204, time.Millisecond, base)
	s.Record("/x", 301, time.Millisecond, base) // error (redirect is non-2xx)
	s.Record("/x", 199, time.Millisecond, base) // error
	s.Record("/x", 503, time.Millisecond, base) // error

	pt := s.Snapshot(base)["/x"][0]
	if pt.Volume != 4 || pt.Errors != 3 {
		t.Fatalf("volume=%d errors=%d, want 4/3", pt.Volume, pt.Errors)
	}
}

func TestStore_percentiles(t *testing.T) {
	s := New()
	// 100 samples of 1..100 ms.
	for i := 1; i <= 100; i++ {
		s.Record("/p", 200, time.Duration(i)*time.Millisecond, base)
	}
	pt := s.Snapshot(base)["/p"][0]
	// nearest-rank: p50 -> index ceil(0.5*100)-1 = 49 -> 50ms; p95 -> index 94 -> 95ms.
	if pt.Latency.P50Ms != 50 {
		t.Errorf("p50=%v, want 50", pt.Latency.P50Ms)
	}
	if pt.Latency.P95Ms != 95 {
		t.Errorf("p95=%v, want 95", pt.Latency.P95Ms)
	}
	if pt.Latency.MaxMs != 100 {
		t.Errorf("max=%v, want 100", pt.Latency.MaxMs)
	}
}

func TestStore_separateBuckets(t *testing.T) {
	s := NewWith(60, time.Minute, defaultMaxPaths)
	s.Record("/v1/messages", 200, time.Millisecond, base)
	s.Record("/v1/messages", 200, time.Millisecond, base.Add(time.Minute))
	s.Record("/v1/messages", 200, time.Millisecond, base.Add(time.Minute))

	pts := s.Snapshot(base.Add(time.Minute))["/v1/messages"]
	if len(pts) != 2 {
		t.Fatalf("want 2 chronological buckets, got %d", len(pts))
	}
	if pts[0].Volume != 1 || pts[1].Volume != 2 {
		t.Errorf("volumes=%d,%d, want 1,2 (oldest first)", pts[0].Volume, pts[1].Volume)
	}
	if !pts[0].BucketStart.Before(pts[1].BucketStart) {
		t.Errorf("buckets not chronological: %v then %v", pts[0].BucketStart, pts[1].BucketStart)
	}
}

func TestStore_windowExpiry(t *testing.T) {
	s := NewWith(60, time.Minute, defaultMaxPaths)
	s.Record("/old", 200, time.Millisecond, base)

	// Advance 60 minutes: the slot that held /old is now reused by the current
	// epoch, so the old sample must not surface.
	later := base.Add(60 * time.Minute)
	s.Record("/new", 200, time.Millisecond, later)

	snap := s.Snapshot(later)
	if _, ok := snap["/old"]; ok {
		t.Errorf("expired /old still present: %v", snap["/old"])
	}
	if len(snap["/new"]) != 1 {
		t.Errorf("/new: want 1 bucket, got %d", len(snap["/new"]))
	}
}

func TestStore_cardinalityCap(t *testing.T) {
	const cap = 4
	s := NewWith(60, time.Minute, cap)
	// cap distinct paths + 6 overflow paths, all in one bucket.
	for i := 0; i < cap; i++ {
		s.Record(pathN(i), 200, time.Millisecond, base)
	}
	for i := cap; i < cap+6; i++ {
		status := 200
		if i%2 == 0 {
			status = 500
		}
		s.Record(pathN(i), status, time.Millisecond, base)
	}

	snap := s.Snapshot(base)
	// Exactly cap named paths + the (other) sentinel.
	if len(snap) != cap+1 {
		t.Fatalf("want %d keys (cap + other), got %d: %v", cap+1, len(snap), keys(snap))
	}
	other := snap[otherKey]
	if len(other) != 1 || other[0].Volume != 6 {
		t.Fatalf("(other): %+v, want volume 6", other)
	}
	// 3 of the 6 overflow requests were 500 (i = cap, cap+2, cap+4).
	if other[0].Errors != 3 {
		t.Errorf("(other) errors=%d, want 3", other[0].Errors)
	}
}

// TestStore_concurrentRecordRead is the -race guard: concurrent writers and a
// reader must not data-race. Run with `go test -race`.
func TestStore_concurrentRecordRead(t *testing.T) {
	s := New()
	done := make(chan struct{})
	for w := 0; w < 8; w++ {
		go func(w int) {
			for i := 0; i < 1000; i++ {
				s.Record(pathN(w%4), 200, time.Duration(i)*time.Microsecond, base.Add(time.Duration(i)*time.Second))
			}
			done <- struct{}{}
		}(w)
	}
	go func() {
		for i := 0; i < 1000; i++ {
			_ = s.Snapshot(base.Add(time.Duration(i) * time.Second))
		}
		done <- struct{}{}
	}()
	for i := 0; i < 9; i++ {
		<-done
	}
}

func keys(m map[string][]Point) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func pathN(i int) string { return "/p" + string(rune('a'+i)) }
