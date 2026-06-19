// Command agent-quota-gateway is a loopback-only reverse proxy for the
// Anthropic Messages API. See the README for usage.
package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"net/http"
)

// uiHTML is the single-file management page served at GET /_gateway/ui.
// The file lives next to this source under cmd/agent-quota-gateway/ui/ so
// the relative embed path is obvious and a future Phase 2 page can be added
// alongside without restructuring. The page must contain no credentials;
// see TestUIHandler_noCredentialSubstring for the static guard.
//
//go:embed ui/index.html
var uiHTMLBytes []byte

// uiHTML is the embedded asset as a string, materialized once at init so
// the handler does not re-convert on every request.
var uiHTML = string(uiHTMLBytes)

// uiSHA256 is the hex-encoded SHA-256 of the bundled page, computed once
// at init. Operators can compare this against the build's `git describe`
// to confirm the deployed binary matches the source they reviewed.
var uiSHA256 = func() string {
	sum := sha256.Sum256(uiHTMLBytes)
	return hex.EncodeToString(sum[:])
}()

// uiHandler returns the http.HandlerFunc for GET /_gateway/ui. It serves
// the bundled HTML with no caching (so an upgraded binary takes effect on
// the next reload) and a 405 on any non-GET request, matching the policy
// of the other /_gateway/* endpoints.
func uiHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-UI-SHA256", uiSHA256)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(uiHTMLBytes)
	}
}