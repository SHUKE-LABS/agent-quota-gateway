package backend

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// stubAuto is a fixed AutoResolver for exercising the middleware's auto
// branch without the real controller.
type stubAuto struct {
	b          Backend
	retryAfter time.Duration
	exhausted  bool
}

func (s stubAuto) ResolveAuto() (Backend, time.Duration, bool) {
	return s.b, s.retryAfter, s.exhausted
}

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	reg, err := loadFrom([]string{"AQG_BACKEND_CLAUDE_A=cred-a"})
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	return reg
}

func TestMiddleware_resolvesAndInjects(t *testing.T) {
	var seen Backend
	var called bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		seen, _ = FromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	h := Middleware(testRegistry(t), nil, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer claude-a")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatal("next handler not called for a valid selector")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if seen.Nick != "claude-a" || seen.Credential != "cred-a" {
		t.Errorf("injected backend = %+v, want {claude-a cred-a}", seen)
	}
}

func TestMiddleware_failsClosed(t *testing.T) {
	cases := []struct {
		name string
		auth string // Authorization header; "" means unset
	}{
		{"unknown selector", "Bearer claude-z"},
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
			h := Middleware(testRegistry(t), nil, next)

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

func TestMiddleware_autoRoutesAndFlags(t *testing.T) {
	want := Backend{Nick: "claude-a", Credential: "cred-a"}
	var seen Backend
	var sawAuto, called bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		seen, _ = FromContext(r.Context())
		sawAuto = IsAutoRequest(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	h := Middleware(testRegistry(t), stubAuto{b: want}, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer auto")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatal("next not called for auto selector")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status=%d, want 200", rec.Code)
	}
	if seen != want {
		t.Errorf("injected backend=%+v, want %+v", seen, want)
	}
	if !sawAuto {
		t.Error("auto request not flagged on context")
	}
}

func TestMiddleware_autoExhaustedReturns429(t *testing.T) {
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next must not be called when the pool is exhausted")
	})
	resolver := stubAuto{b: Backend{Nick: "claude-a"}, retryAfter: 90 * time.Second, exhausted: true}
	h := Middleware(testRegistry(t), resolver, next)

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

func TestMiddleware_autoSelectorWithoutResolverFailsClosed(t *testing.T) {
	// With no AutoResolver wired, "auto" has no special meaning and is
	// rejected by the registry (it is a reserved, unconfigurable nick).
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next must not be called")
	})
	h := Middleware(testRegistry(t), nil, next)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer auto")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status=%d, want 403", rec.Code)
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
