package auto

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// Issue #351 (TEMPORARY — remove with zai_capture.go): every z.ai upstream
// 429 logs one whitelisted capture line before classification.

const zaiCaptureBody = `{"error":{"code":"1302","message":"Rate limit reached for requests"}}`

// zaiCapture429 drives a z.ai 429 carrying whitelisted and non-whitelisted
// headers through ModifyResponse for member nick.
func zaiCapture429(t *testing.T, c *Controller, nick string, body []byte, encoding string) *http.Response {
	t.Helper()
	ctx := backend.WithBackend(context.Background(), c.resolve(t, nick))
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx)
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Retry-After", "17")
	h.Set("X-Ratelimit-Remaining-Requests", "0")
	h.Set("X-Request-Id", "req-abc")
	h.Set("Anthropic-Ratelimit-Unified-Status", "rejected")
	h.Set("Set-Cookie", "session=secret-cookie")
	h.Set("X-Upstream-Auth", "secret-token")
	if encoding != "" {
		h.Set("Content-Encoding", encoding)
	}
	resp := &http.Response{
		StatusCode:    http.StatusTooManyRequests,
		Header:        h,
		Request:       req,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	return resp
}

// TestZai429Capture_everyLegLogsOnceBeforeClassification pins the hook
// placement: the full-window park, stale-poller park, and live-poller absorb
// legs each emit exactly one capture line, ahead of the leg's own line, and
// the client-facing outcome is the leg's existing one.
func TestZai429Capture_everyLegLogsOnceBeforeClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stale     bool
		fullStore bool
		legLine   string
		absorb    bool // same-member throttle; otherwise park and fail over
	}{
		{"full-window park", false, true, "with a full quota window", false},
		{"stale-poller park", true, false, "z.ai 429 with stale poller signal", false},
		{"live-poller absorb", false, false, "z.ai 429 concurrency throttle", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
			var logBuf bytes.Buffer
			p, store := issue249Pools(t, clock, &logBuf, nil, "a", "b")
			c := p.byPool["auto"]
			issue249SetStale(c, tc.stale)
			if tc.fullStore {
				full := 1.0
				store.Put("a", quota.Snapshot{Unified5hUtilization: &full, AsOf: clock.now().Add(-time.Minute)})
			}

			resp := zaiCapture429(t, c, "a", []byte(zaiCaptureBody), "")

			log := logBuf.String()
			if n := strings.Count(log, "z.ai 429 capture (issue #351)"); n != 1 {
				t.Fatalf("capture lines = %d, want 1; log=%q", n, log)
			}
			capAt, legAt := strings.Index(log, "z.ai 429 capture"), strings.Index(log, tc.legLine)
			if legAt < 0 || capAt > legAt {
				t.Fatalf("capture at %d, leg line %q at %d; want capture first; log=%q", capAt, tc.legLine, legAt, log)
			}
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", resp.StatusCode)
			}
			if tc.absorb {
				if ra := issue249RetryAfter(t, resp); ra != zaiThrottleRetryAfterSeconds || c.Current() != "a" {
					t.Fatalf("Retry-After=%d active=%q; want absorb %d on a", ra, c.Current(), zaiThrottleRetryAfterSeconds)
				}
			} else if c.Current() != "b" {
				t.Fatalf("active = %q; want park and failover to b", c.Current())
			}
			body, _ := io.ReadAll(resp.Body)
			if strings.Contains(string(body), "1302") {
				t.Fatalf("client body leaked the upstream body: %q", body)
			}
		})
	}
}

// TestZai429Capture_whitelistOnly pins redaction: whitelisted headers and the
// error code are logged; other headers and the body message are not.
func TestZai429Capture_whitelistOnly(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := zaiController(t, clock, &logBuf, "a")
	zaiCapture429(t, c, "a", []byte(zaiCaptureBody), "")

	line := strings.SplitN(logBuf.String(), "\n", 2)[0]
	for _, want := range []string{
		`status=429`, `code="1302"`,
		`Anthropic-Ratelimit-Unified-Status="rejected"`,
		`Retry-After="17"`,
		`X-Ratelimit-Remaining-Requests="0"`,
		`X-Request-Id="req-abc"`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("capture line missing %s: %q", want, line)
		}
	}
	for _, leak := range []string{"secret-cookie", "secret-token", "Set-Cookie", "Rate limit reached", "cred-a"} {
		if strings.Contains(logBuf.String(), leak) {
			t.Errorf("log leaked %q: %q", leak, logBuf.String())
		}
	}
}

// TestZai429Capture_encodedBodyNotDecoded: a compressed body is not read.
func TestZai429Capture_encodedBodyNotDecoded(t *testing.T) {
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	w.Write([]byte(zaiCaptureBody))
	w.Close()
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := zaiController(t, clock, &logBuf, "a")
	zaiCapture429(t, c, "a", gz.Bytes(), "gzip")
	if !strings.Contains(logBuf.String(), "code=?(encoded)") {
		t.Fatalf("log = %q; want code=?(encoded)", logBuf.String())
	}
}

// TestZai429Capture_bodyReplayed: the read prefix is replayed verbatim, so
// the body the classifier sees is byte-identical to the upstream's.
func TestZai429Capture_bodyReplayed(t *testing.T) {
	big := `{"error":{"message":"` + strings.Repeat("x", zaiCaptureBodyMaxBytes) + `","code":"1302"}}`
	resp := &http.Response{Header: http.Header{}, Body: io.NopCloser(strings.NewReader(big))}
	if code := zaiCaptureCode(resp); code != "?" {
		t.Fatalf("code = %s for an oversized body; want ?", code)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != big {
		t.Fatalf("replayed body differs from upstream (len %d vs %d)", len(got), len(big))
	}
}

// TestZai429Capture_nonZaiNotLogged: only z.ai backends are captured.
func TestZai429Capture_nonZaiNotLogged(t *testing.T) {
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	var logBuf bytes.Buffer
	c := newController(t, 0, clock, &logBuf, "a", "b")
	zaiCapture429(t, c, "a", []byte(zaiCaptureBody), "")
	if strings.Contains(logBuf.String(), "z.ai 429 capture") {
		t.Fatalf("non-z.ai 429 captured: %q", logBuf.String())
	}
}
