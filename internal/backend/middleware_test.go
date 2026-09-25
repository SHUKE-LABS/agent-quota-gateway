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

type workerStubRouter struct {
	stubRouter
	gotWorker string
}

func (s *workerStubRouter) RouteWorker(pool, worker string) (Backend, time.Duration, bool, bool) {
	if pool != s.b.Pool {
		return Backend{}, 0, false, false
	}
	s.gotPool = pool
	s.gotWorker = worker
	return s.b, s.retryAfter, s.ok, s.exhausted
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

func TestWorkerNamespaceMiddleware_stripsPathAndRoutesByWorker(t *testing.T) {
	want := Backend{Pool: "auto", Nick: "seat1", Credential: "cred-a", BaseURL: testDefaultBaseURL}
	router := &workerStubRouter{stubRouter: stubRouter{b: want, ok: true}}
	var gotPath, gotEscapedPath, gotQuery string
	var gotBackend Backend
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotEscapedPath = r.URL.EscapedPath()
		gotQuery = r.URL.RawQuery
		gotBackend, _ = FromContext(r.Context())
	})
	h := WorkerNamespaceMiddleware(Middleware(router, next))
	req := httptest.NewRequest(http.MethodPost, "/_aqg/w/agent-a/v1/responses", nil)
	req.URL.Path = "/_aqg/w/agent-a/v1/responses/a/b"
	req.URL.RawPath = "/_aqg/w/agent-a/v1/responses/a%2Fb"
	req.URL.RawQuery = "q=one%2ftwo&empty"
	req.Header.Set("Authorization", "Bearer AUTO")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200: %s", rec.Code, rec.Body.String())
	}
	if router.gotPool != "auto" || router.gotWorker != "agent-a" {
		t.Errorf("route identity=(%q,%q), want (auto,agent-a)", router.gotPool, router.gotWorker)
	}
	if gotBackend != want {
		t.Errorf("backend=%+v, want %+v", gotBackend, want)
	}
	if gotPath != "/v1/responses/a/b" {
		t.Errorf("path=%q, want stripped decoded API path", gotPath)
	}
	if gotEscapedPath != "/v1/responses/a%2Fb" {
		t.Errorf("escaped path=%q, want original escaped API suffix", gotEscapedPath)
	}
	if gotQuery != "q=one%2ftwo&empty" {
		t.Errorf("query=%q, want original raw query", gotQuery)
	}
}

func TestWorkerNamespaceMiddleware_rejectsMalformedIdentity(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{name: "empty", path: "/_aqg/w//v1/responses"},
		{name: "missing API suffix", path: "/_aqg/w/agent-a"},
		{name: "encoded slash", path: "/_aqg/w/agent%2Fa/v1/responses"},
		{name: "literal traversal", path: "/_aqg/w/../v1/responses"},
		{name: "encoded traversal", path: "/_aqg/w/%2e%2e/v1/responses"},
		{name: "nested escape", path: "/_aqg/w/%252e%252e/v1/responses"},
		{name: "nested encoded slash", path: "/_aqg/w/%252F/v1/responses"},
		{name: "invalid UTF-8", path: "/_aqg/w/%FF/v1/responses"},
		{name: "overlong", path: "/_aqg/w/" + strings.Repeat("a", maxWorkerNicknameSize+1) + "/v1/responses"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("malformed worker path reached downstream")
			})
			req := httptest.NewRequest(http.MethodPost, tc.path, nil)
			req.Header.Set("Authorization", "Bearer auto")
			rec := httptest.NewRecorder()
			WorkerNamespaceMiddleware(Middleware(&stubRouter{ok: true}, next)).ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status=%d, want 400", rec.Code)
			}
		})
	}
}

func TestWorkerNamespaceMiddleware_acceptsOpaquePercentNickname(t *testing.T) {
	var gotWorker string
	h := WorkerNamespaceMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotWorker, _ = WorkerNicknameFromContext(r.Context())
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_aqg/w/a%25b/v1/messages", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if gotWorker != "a%b" {
		t.Errorf("worker=%q, want decoded opaque nickname a%%b", gotWorker)
	}
}

func TestWorkerNamespaceMiddleware_keepsXApiKeyPoolFallback(t *testing.T) {
	want := Backend{Pool: "auto", Nick: "seat1", Credential: "cred-a", BaseURL: testDefaultBaseURL}
	router := &workerStubRouter{stubRouter: stubRouter{b: want, ok: true}}
	var called bool
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		called = true
		if _, ok := FromContext(r.Context()); !ok {
			t.Error("resolved backend missing from request context")
		}
	})
	h := WorkerNamespaceMiddleware(Middleware(router, next))
	req := httptest.NewRequest(http.MethodPost, "/_aqg/w/agent-a/v1/messages", nil)
	req.Header.Set("X-Api-Key", "AUTO")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !called {
		t.Fatalf("status=%d called=%v, want forwarded fallback request", rec.Code, called)
	}
	if router.gotPool != "auto" || router.gotWorker != "agent-a" {
		t.Errorf("route identity=(%q,%q), want (auto,agent-a)", router.gotPool, router.gotWorker)
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

func TestMiddleware_exhaustedReturns503(t *testing.T) {
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next must not be called when the pool is exhausted")
	})
	router := &stubRouter{b: Backend{Pool: "auto", Nick: "claude-a"}, retryAfter: 90 * time.Second, ok: true, exhausted: true}
	h := Middleware(router, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer auto")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// 503, not 429: a 429 ends the Claude Code turn; a 503 is retried so
	// the agent auto-resumes once the advertised window resets (issue #203).
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status=%d, want 503", rec.Code)
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
		{"x-api-key uppercase", "", "CLAUDE", true}, // normalized
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
