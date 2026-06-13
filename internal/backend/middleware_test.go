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
		name string
		auth string // Authorization header; "" means unset
	}{
		{"unknown pool", "Bearer claude-z"},
		{"missing header", ""},
		{"empty bearer", "Bearer "},
		{"non-bearer scheme", "Basic claude-a"},
		{"raw token no scheme", "claude-a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("next must not be called on a fail-closed request")
			})
			// ok=false → the router does not recognise the pool.
			h := Middleware(&stubRouter{ok: false}, next)

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
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
