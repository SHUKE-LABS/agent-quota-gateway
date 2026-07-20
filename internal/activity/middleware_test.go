package activity

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// stubRouter is a minimal backend.PoolRouter for driving backend.Middleware
// in these tests: any pool in known resolves (ok == true), everything else
// fails closed (unknown-selector 403). When exhausted is set, a known pool
// resolves but reports the whole pool rate-limited (503).
type stubRouter struct {
	known     map[string]bool
	exhausted bool
}

func (s stubRouter) Route(pool string) (backend.Backend, time.Duration, bool, bool) {
	if !s.known[pool] {
		return backend.Backend{}, 0, false, false
	}
	return backend.Backend{Nick: pool, Credential: "sk-ant-oat-test"}, time.Second, true, s.exhausted
}

// chain wires the production shape: activity.Middleware (outer) over
// backend.Middleware (inner) over the given handler, so the reached-pool
// marker bridge is exercised end to end.
func chain(store *Store, router backend.PoolRouter, h http.Handler) http.Handler {
	return Middleware(store, backend.Middleware(router, h))
}

// TestMiddleware_recordsThroughMux drives requests through a ServeMux wrapped
// by the middleware and asserts the store buckets them by path with the right
// status, and that /_gateway/* is excluded.
func TestMiddleware_recordsThroughMux(t *testing.T) {
	store := New()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("/v1/boom", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/_gateway/activity", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	router := stubRouter{known: map[string]bool{"auto": true}}
	srv := httptest.NewServer(chain(store, router, mux))
	t.Cleanup(srv.Close)

	get(t, srv.URL+"/v1/messages", "auto")
	get(t, srv.URL+"/v1/messages", "auto")
	get(t, srv.URL+"/v1/boom", "auto")
	// Gateway self-traffic must not be recorded.
	get(t, srv.URL+"/_gateway/activity", "auto")

	snap := store.Snapshot(time.Now())

	if _, ok := snap["/_gateway/activity"]; ok {
		t.Errorf("/_gateway/* was recorded but must be excluded: %v", snap["/_gateway/activity"])
	}

	msgs := total(snap["/v1/messages"])
	if msgs.volume != 2 || msgs.errors != 0 {
		t.Errorf("/v1/messages: volume=%d errors=%d, want 2/0", msgs.volume, msgs.errors)
	}
	boom := total(snap["/v1/boom"])
	if boom.volume != 1 || boom.errors != 1 {
		t.Errorf("/v1/boom: volume=%d errors=%d, want 1/1", boom.volume, boom.errors)
	}
}

// TestMiddleware_dropsUnknownSelector confirms a request that names no valid
// pool (unknown-selector 403 at the gateway boundary) is not recorded, while a
// valid-pool request to the same path is — the fix for issue #230.
func TestMiddleware_dropsUnknownSelector(t *testing.T) {
	store := New()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	router := stubRouter{known: map[string]bool{"auto": true}}
	srv := httptest.NewServer(chain(store, router, handler))
	t.Cleanup(srv.Close)

	// No/unknown selector: fails closed with 403, must be dropped.
	if code := get(t, srv.URL+"/favicon.ico", ""); code != http.StatusForbidden {
		t.Fatalf("unknown selector: status=%d, want 403", code)
	}
	if code := get(t, srv.URL+"/v1/messages", "nope"); code != http.StatusForbidden {
		t.Fatalf("bad pool: status=%d, want 403", code)
	}
	// Valid pool to the same path: must be recorded.
	if code := get(t, srv.URL+"/v1/messages", "auto"); code != http.StatusOK {
		t.Fatalf("valid pool: status=%d, want 200", code)
	}

	snap := store.Snapshot(time.Now())
	if _, ok := snap["/favicon.ico"]; ok {
		t.Errorf("/favicon.ico (unknown selector) was recorded but must be dropped: %v", snap["/favicon.ico"])
	}
	msgs := total(snap["/v1/messages"])
	if msgs.volume != 1 || msgs.errors != 0 {
		t.Errorf("/v1/messages: volume=%d errors=%d, want 1/0 (only the valid-pool request)", msgs.volume, msgs.errors)
	}
}

// TestMiddleware_recordsExhaustedPool confirms a valid pool that is exhausted
// (503) is still recorded — it is genuine pool/endpoint-health signal, per the
// issue #230 decision to gate on "named a valid pool", not "was forwarded".
func TestMiddleware_recordsExhaustedPool(t *testing.T) {
	store := New()

	// next must never run for an exhausted pool; backend.Middleware writes 503
	// itself. Fail loudly if it does.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("handler ran for an exhausted pool; backend.Middleware should short-circuit")
	})
	router := stubRouter{known: map[string]bool{"auto": true}, exhausted: true}
	srv := httptest.NewServer(chain(store, router, handler))
	t.Cleanup(srv.Close)

	if code := get(t, srv.URL+"/v1/messages", "auto"); code != http.StatusServiceUnavailable {
		t.Fatalf("exhausted pool: status=%d, want 503", code)
	}

	pt := total(store.Snapshot(time.Now())["/v1/messages"])
	if pt.volume != 1 || pt.errors != 1 {
		t.Errorf("/v1/messages: volume=%d errors=%d, want 1/1 (exhausted-503 recorded as an error)", pt.volume, pt.errors)
	}
}

// TestMiddleware_defaultStatusIsOK confirms a handler that writes a body
// without calling WriteHeader is recorded as 200, not an error.
func TestMiddleware_defaultStatusIsOK(t *testing.T) {
	store := New()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "implicit 200")
	})
	router := stubRouter{known: map[string]bool{"auto": true}}
	srv := httptest.NewServer(chain(store, router, handler))
	t.Cleanup(srv.Close)

	get(t, srv.URL+"/anything", "auto")

	pt := total(store.Snapshot(time.Now())["/anything"])
	if pt.volume != 1 || pt.errors != 0 {
		t.Errorf("volume=%d errors=%d, want 1/0", pt.volume, pt.errors)
	}
}

// get issues a GET carrying selector as the Authorization bearer token (empty
// bearer sends no Authorization header) and returns the response status.
func get(t *testing.T, url, selector string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}
	if selector != "" {
		req.Header.Set("Authorization", "Bearer "+selector)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

type totals struct{ volume, errors int }

func total(pts []Point) totals {
	var out totals
	for _, p := range pts {
		out.volume += p.Volume
		out.errors += p.Errors
	}
	return out
}
