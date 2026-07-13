package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/activity"
)

// TestActivityHandler_servesJSON confirms GET /_gateway/activity returns the
// per-endpoint series as JSON.
func TestActivityHandler_servesJSON(t *testing.T) {
	store := activity.New()
	store.Record("/v1/messages", 200, 42*time.Millisecond, time.Now())

	srv := httptest.NewServer(activityHandler(store))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type=%q, want application/json", ct)
	}

	var series map[string][]activity.Point
	if err := json.NewDecoder(resp.Body).Decode(&series); err != nil {
		t.Fatalf("decode: %v", err)
	}
	pts := series["/v1/messages"]
	if len(pts) != 1 || pts[0].Volume != 1 {
		t.Fatalf("/v1/messages series=%+v, want one bucket of volume 1", pts)
	}
}

// TestActivityHandler_methodNotAllowed confirms non-GET returns 405 + Allow:
// GET, matching the sibling read endpoints.
func TestActivityHandler_methodNotAllowed(t *testing.T) {
	srv := httptest.NewServer(activityHandler(activity.New()))
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodPost, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status=%d, want 405", resp.StatusCode)
	}
	if got := resp.Header.Get("Allow"); got != http.MethodGet {
		t.Errorf("Allow=%q, want GET", got)
	}
}
