package backend

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// stubRouter is a fixed PoolRouter for exercising the middleware without
// the real per-pool controllers. It records the pool name it was asked
// to route so a normalization assertion can check it.
type stubRouter struct {
	b          Backend
	retryAfter time.Duration
	ok         bool
	exhausted  bool

	gotPool string
}

func (s *stubRouter) Route(pool string) (Backend, time.Duration, bool, bool) {
	s.gotPool = pool
	return s.b, s.retryAfter, s.ok, s.exhausted
}

func TestMiddleware_resolvesAndInjects(t *testing.T) {
	want := Backend{Pool: "auto", Nick: "claude-a", Credential: "cred-a", BaseURL: testDefaultBaseURL}
	var seen Backend
	var called bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		seen, _ = FromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	router := &stubRouter{b: want, ok: true}
	h := Middleware(router, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer AUTO") // upper-case selector
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatal("next handler not called for a valid selector")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if seen != want {
		t.Errorf("injected backend = %+v, want %+v", seen, want)
	}
	if router.gotPool != "auto" {
		t.Errorf("router saw pool %q, want normalized %q", router.gotPool, "auto")
	}
}

func TestMiddleware_unknownPoolFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		method string
		auth   string // Authorization header; "" means unset
	}{
		{"unknown pool", http.MethodPost, "Bearer claude-z"},
		{"missing header", http.MethodPost, ""},
		{"empty bearer", http.MethodPost, "Bearer "},
		{"non-bearer scheme", http.MethodPost, "Basic claude-a"},
		{"raw token no scheme", http.MethodPost, "claude-a"},
		// The gate is method-agnostic: a GET with an unknown selector
		// fails closed exactly like a POST (#141 lifted the proxy's
		// POST-only check; the selector boundary must still hold).
		{"unknown pool via GET", http.MethodGet, "Bearer claude-z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("next must not be called on a fail-closed request")
			})
			// ok=false → the router does not recognise the pool.
			h := Middleware(&stubRouter{ok: false}, next)

			req := httptest.NewRequest(tc.method, "/v1/models", nil)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Errorf("status = %d, want 403", rec.Code)
			}
			// The rejected selector value must never appear in the body.
			if strings.Contains(rec.Body.String(), "claude") {
				t.Errorf("response body leaked selector/config: %q", rec.Body.String())
			}
		})
	}
}

func TestMiddleware_exhaustedReturns429(t *testing.T) {
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next must not be called when the pool is exhausted")
	})
	router := &stubRouter{b: Backend{Pool: "auto", Nick: "claude-a"}, retryAfter: 90 * time.Second, ok: true, exhausted: true}
	h := Middleware(router, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer auto")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status=%d, want 429", rec.Code)
	}
	if ra := rec.Header().Get("Retry-After"); ra != "90" {
		t.Errorf("Retry-After=%q, want 90", ra)
	}
}

// TestMiddleware_halfOpenForwards is the issue #134 boundary test: when
// the router returns a backend with exhausted=false (the half-open
// signal from ResolveAuto on an all-parked pool), the middleware must
// forward the request to that backend — not emit a 429. The half-open
// forward is the only path that breaks the deadlock where an all-parked
// pool never sees a forwarded request and never refreshes its quota
// store. The downstream next handler is the contract: it must be called
// with the backend injected on the context, and no Retry-After header
// must be written.
func TestMiddleware_halfOpenForwards(t *testing.T) {
	want := Backend{Pool: "auto", Nick: "claude-a", Credential: "cred-a", BaseURL: testDefaultBaseURL}
	var seen Backend
	var called int
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		called++
		seen, _ = FromContext(r.Context())
	})
	// Half-open: router returns a real backend, exhausted=false,
	// retryAfter=0. The middleware must NOT treat this as an exhausted
	// pool and must NOT short-circuit to writeRateLimited.
	router := &stubRouter{b: want, retryAfter: 0, ok: true, exhausted: false}
	h := Middleware(router, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer auto")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if called != 1 {
		t.Fatalf("next called %d times, want 1 (half-open must forward)", called)
	}
	if seen.Nick != want.Nick {
		t.Errorf("next saw nick=%q, want %q (half-open backend must be injected)", seen.Nick, want.Nick)
	}
	if rec.Code == http.StatusTooManyRequests {
		t.Errorf("status=%d, half-open path must not return 429", rec.Code)
	}
	if ra := rec.Header().Get("Retry-After"); ra != "" {
		t.Errorf("Retry-After=%q, half-open path must not advertise a Retry-After", ra)
	}
}

// poolAwareRouter routes only the single named pool, rejecting everything else.
type poolAwareRouter struct {
	pool string
	b    Backend
}

func (r *poolAwareRouter) Route(pool string) (Backend, time.Duration, bool, bool) {
	if pool == r.pool {
		return r.b, 0, true, false
	}
	return Backend{}, 0, false, false
}

func TestMiddleware_xApiKeyFallback(t *testing.T) {
	want := Backend{Pool: "claude", Nick: "k1", Credential: "cred", BaseURL: testDefaultBaseURL}
	router := &poolAwareRouter{pool: "claude", b: want}

	cases := []struct {
		name    string
		auth    string
		xApiKey string
		wantOK  bool
	}{
		{"x-api-key only", "", "claude", true},
		{"x-api-key uppercase", "", "CLAUDE", true},        // normalized
		{"bearer wins, no x-api-key", "Bearer claude", "", true},
		{"bearer unknown, x-api-key fallback", "Bearer unknown", "claude", true},
		{"both unknown", "Bearer unknown", "unknown", false},
		{"neither header", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called bool
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			})
			h := Middleware(router, next)

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			if tc.xApiKey != "" {
				req.Header.Set("X-Api-Key", tc.xApiKey)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if tc.wantOK {
				if rec.Code != http.StatusOK {
					t.Errorf("status = %d, want 200", rec.Code)
				}
				if !called {
					t.Error("next handler not called")
				}
			} else {
				if rec.Code != http.StatusForbidden {
					t.Errorf("status = %d, want 403", rec.Code)
				}
				if called {
					t.Error("next handler should not be called on unknown selector")
				}
			}
		})
	}
}

func TestBearerToken(t *testing.T) {
	cases := map[string]string{
		"Bearer abc":   "abc",
		"bearer abc":   "abc", // scheme is case-insensitive
		"BEARER  abc ": "abc", // surrounding space trimmed
		"Basic abc":    "",
		"abc":          "",
		"":             "",
		"Bearer":       "",
	}
	for in, want := range cases {
		if got := bearerToken(in); got != want {
			t.Errorf("bearerToken(%q) = %q, want %q", in, got, want)
		}
	}
}
