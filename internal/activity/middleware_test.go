package activity

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

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

	srv := httptest.NewServer(Middleware(store, mux))
	t.Cleanup(srv.Close)

	get(t, srv.URL+"/v1/messages")
	get(t, srv.URL+"/v1/messages")
	get(t, srv.URL+"/v1/boom")
	// Gateway self-traffic must not be recorded.
	get(t, srv.URL+"/_gateway/activity")

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

// TestMiddleware_defaultStatusIsOK confirms a handler that writes a body
// without calling WriteHeader is recorded as 200, not an error.
func TestMiddleware_defaultStatusIsOK(t *testing.T) {
	store := New()
	h := Middleware(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "implicit 200")
	}))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	get(t, srv.URL+"/anything")

	pt := total(store.Snapshot(time.Now())["/anything"])
	if pt.volume != 1 || pt.errors != 0 {
		t.Errorf("volume=%d errors=%d, want 1/0", pt.volume, pt.errors)
	}
}

func get(t *testing.T, url string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
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
