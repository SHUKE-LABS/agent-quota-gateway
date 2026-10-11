package auto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
)

// TEMPORARY (issue #351): remove this file and its single call site in
// ModifyResponse once #351's verdict is recorded, or 30 days after it ships
// with no quota-wall capture.
//
// #249 deferred Option 4 (park on z.ai's own exhaustion 429) until the wire
// shows whether a quota-wall 429 differs from the 1302 concurrency throttle.
// Every z.ai 429 leg — full-window park, stale-poller park, throttle absorb —
// rewrites the status, headers, and body before the client sees them, so the
// upstream shape is otherwise never observable. logZai429Capture records it
// once per z.ai 429, before classification; the leg's own log line that
// follows labels which classification the sample got.
//
// Scope is a whitelist: the upstream status, the headers below, and the z.ai
// error code. Credentials, cookies, and body text other than the code never
// reach the log (the V1 logging constraint).

// zaiCaptureBodyMaxBytes bounds the body prefix read for the error code. A
// z.ai error object is well under this; a larger body is logged as code=?.
const zaiCaptureBodyMaxBytes = 4 << 10

// zaiCaptureHeaders is the exact-name half of the whitelist; any
// x-ratelimit-* header is also captured.
var zaiCaptureHeaders = map[string]bool{
	"retry-after":                        true,
	"x-request-id":                       true,
	"anthropic-ratelimit-unified-status": true,
}

// logZai429Capture writes the whitelisted wire shape of z.ai upstream 429
// resp. The body prefix it reads is replayed in front of the live upstream,
// so resp.Body stays byte-identical for whatever the classifier does next.
func (c *Controller) logZai429Capture(resp *http.Response, b backend.Backend) {
	var keys []string
	for k := range resp.Header {
		lk := strings.ToLower(k)
		if zaiCaptureHeaders[lk] || strings.HasPrefix(lk, "x-ratelimit-") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, " %s=%s", k, strconv.Quote(strings.Join(resp.Header.Values(k), ", ")))
	}
	fmt.Fprintf(c.logOut, "auto[%s]: %s z.ai 429 capture (issue #351): status=%d code=%s%s\n",
		c.name(), b.Nick, resp.StatusCode, zaiCaptureCode(resp), sb.String())
}

// zaiCaptureCode extracts error.code from the head of resp's body, which z.ai
// sends as {"error":{"code":"1302","message":"..."}}. A compressed body is not
// decoded (the gateway never decodes upstream bytes): it reads ?(encoded).
// Anything unparseable reads ?.
func zaiCaptureCode(resp *http.Response) string {
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && !strings.EqualFold(enc, "identity") {
		return "?(encoded)"
	}
	if resp.Body == nil || resp.Body == http.NoBody {
		return "?"
	}
	upstream := resp.Body
	held, err := io.ReadAll(io.LimitReader(upstream, zaiCaptureBodyMaxBytes))
	var rest io.Reader = upstream
	if err != nil {
		rest = errReader{err}
	}
	resp.Body = replayBody{Reader: io.MultiReader(bytes.NewReader(held), rest), Closer: upstream}
	var v struct {
		Error struct {
			Code json.RawMessage `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(held, &v) != nil || len(v.Error.Code) == 0 {
		return "?"
	}
	// Strings and numbers alike; quoting keeps a hostile code on one line.
	var s string
	if json.Unmarshal(v.Error.Code, &s) != nil {
		s = string(v.Error.Code)
	}
	return strconv.Quote(s)
}
