package auto

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/shukebeta/agent-quota-gateway/internal/backend"
	"github.com/shukebeta/agent-quota-gateway/internal/quota"
)

// Issue #345 fixtures: ChatGPT-Codex Responses SSE events as the backend
// emits them — an "event:" line plus a JSON data line whose "type" repeats
// the event name.
const (
	sseCreated    = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\",\"instructions\":\"be brief\"}}\n\n"
	sseInProgress = "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\n"
	sseQueued     = "event: response.queued\ndata: {\"type\":\"response.queued\",\"response\":{\"id\":\"resp_1\",\"status\":\"queued\"}}\n\n"
	sseMetadata   = "event: response.metadata\ndata: {\"type\":\"response.metadata\",\"metadata\":{}}\n\n"
	sseOverload   = "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_1\",\"status\":\"failed\",\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity. Please try a different model.\"}}}\n\n"
	sseRateLimit  = "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"Please try again in 11.054s.\"}}}\n\n"
	sseCtxWindow  = "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"context_length_exceeded\",\"message\":\"too long\"}}}\n\n"
	sseItemAdded  = "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\"}}\n\n"
	sseTextDelta  = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"server_is_overloaded\"}\n\n"
	sseCompleted  = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n"
)

// trackedBody records reads and closes so tests can prove a gated-out
// response is never read and that close reaches the upstream body.
type trackedBody struct {
	r      io.Reader
	reads  int
	closed bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	b.reads++
	return b.r.Read(p)
}

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}

func codexStreamResp(b backend.Backend, method, path, contentType string, body io.Reader) (*http.Response, *trackedBody) {
	ctx := backend.WithBackend(context.Background(), b)
	req := httptest.NewRequest(method, "http://chatgpt.com"+path, nil).WithContext(ctx)
	h := http.Header{}
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	h.Set("x-codex-primary-used-percent", "12")
	tb := &trackedBody{r: body}
	return &http.Response{
		StatusCode:    http.StatusOK,
		Status:        "200 OK",
		Header:        h,
		Request:       req,
		Body:          tb,
		ContentLength: -1,
	}, tb
}

func sseResp(b backend.Backend, stream string) (*http.Response, *trackedBody) {
	return codexStreamResp(b, http.MethodPost, "/backend-api/codex/responses", "text/event-stream; charset=utf-8", strings.NewReader(stream))
}

func newCodexOverloadController(t *testing.T, logOut io.Writer, nicks ...string) *Controller {
	t.Helper()
	clock := &fixedClock{t: time.Unix(1_700_000_000, 0).UTC()}
	return codexController(t, clock, logOut, nil, nicks...)
}

// assertCodexOverload503 pins the synthetic same-member 503 shape and that
// the member stays in rotation.
func assertCodexOverload503(t *testing.T, c *Controller, resp *http.Response, tb *trackedBody, before string) {
	t.Helper()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != strconv.Itoa(codexOverloadRetryAfterSeconds) {
		t.Errorf("Retry-After=%q, want %d", ra, codexOverloadRetryAfterSeconds)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type=%q, want application/json", ct)
	}
	if got := resp.Header.Get("x-codex-primary-used-percent"); got != "" {
		t.Errorf("x-codex-* header leaked on synthetic 503: %q", got)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"error":"backend throttled; same member"}` {
		t.Errorf("body=%q, want the same-member throttle body", body)
	}
	// Codex maps a 503 whose error.code is server_is_overloaded straight back
	// to the terminal ServerOverloaded; the synthetic body must not carry it.
	if strings.Contains(string(body), codexOverloadErrorCode) {
		t.Errorf("synthetic 503 body carries %q, which Codex treats as terminal", codexOverloadErrorCode)
	}
	if resp.ContentLength != int64(len(body)) {
		t.Errorf("ContentLength=%d, want %d", resp.ContentLength, len(body))
	}
	if !tb.closed {
		t.Error("upstream stream body not closed after absorbing the overload")
	}
	if got := c.Current(); got != before {
		t.Errorf("Current()=%q, want %q (capacity overload must not park/switch)", got, before)
	}
	if _, _, exhausted := c.ResolveAuto(); exhausted {
		t.Error("ResolveAuto exhausted=true after capacity overload, want false")
	}
}

// TestModifyResponse_codexStreamOverloadAbsorbed is the issue #345 bug: a
// 200 Responses stream that fails with server_is_overloaded before any
// output becomes a same-member 503, across every framing the stream can
// arrive in.
func TestModifyResponse_codexStreamOverloadAbsorbed(t *testing.T) {
	cases := []struct {
		name   string
		stream string
	}{
		{"first event", sseOverload},
		{"after created and in_progress", sseCreated + sseInProgress + sseOverload},
		{"after every preamble type", sseCreated + sseQueued + sseInProgress + sseMetadata + sseOverload},
		{"CRLF line endings", strings.ReplaceAll(sseCreated+sseOverload, "\n", "\r\n")},
		{"keep-alive comments and blank lines", ": ping\n\n\n" + sseCreated + ":keepalive\n\n" + sseOverload},
		{"data lines without event lines", "data: {\"type\":\"response.created\"}\n\ndata:{\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n"},
		{"JSON split across data lines", "event: response.failed\ndata: {\"type\":\"response.failed\",\ndata: \"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n"},
		{"trailing events after the failure", sseCreated + sseOverload + sseCompleted},
		{"unknown fields ignored", "id: 7\nretry: 1000\n" + sseCreated + sseOverload},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			c := newCodexOverloadController(t, &logBuf, "seat1")
			before := c.Current()
			resp, tb := sseResp(c.resolve(t, "seat1"), tc.stream)
			if err := c.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse: %v", err)
			}
			assertCodexOverload503(t, c, resp, tb, before)
			if !strings.Contains(logBuf.String(), "codex in-stream server_is_overloaded") {
				t.Errorf("overload not logged; got %q", logBuf.String())
			}
		})
	}
}

// TestModifyResponse_codexStreamOverloadAcrossReadBoundaries proves the
// scanner reassembles lines split across reads: one byte per read is the
// worst case of an upstream flushing mid-line.
func TestModifyResponse_codexStreamOverloadAcrossReadBoundaries(t *testing.T) {
	for _, nl := range []string{"\n", "\r\n"} {
		c := newCodexOverloadController(t, io.Discard, "seat1")
		before := c.Current()
		stream := strings.ReplaceAll(sseCreated+sseInProgress+sseOverload, "\n", nl)
		resp, tb := codexStreamResp(c.resolve(t, "seat1"), http.MethodPost, "/backend-api/codex/responses",
			"text/event-stream", iotest.OneByteReader(strings.NewReader(stream)))
		if err := c.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}
		assertCodexOverload503(t, c, resp, tb, before)
	}
}

// TestModifyResponse_codexOverloadDoesNotFailOver pins the same-member
// decision on a multi-seat pool: model capacity is not seat quota, so the
// sticky member keeps serving and no seat is parked.
func TestModifyResponse_codexOverloadDoesNotFailOver(t *testing.T) {
	c := newCodexOverloadController(t, io.Discard, "seat1", "seat2")
	before := c.Current()
	for i := 0; i < 3; i++ {
		resp, tb := sseResp(c.resolve(t, before), sseCreated+sseOverload)
		if err := c.ModifyResponse(resp); err != nil {
			t.Fatalf("ModifyResponse: %v", err)
		}
		assertCodexOverload503(t, c, resp, tb, before)
	}
	ps := c.poolStatus(quota.NewStore(), nil, nil)
	for _, nick := range []string{"seat1", "seat2"} {
		if memberParked(ps, nick) {
			t.Errorf("member %q parked after capacity overloads", nick)
		}
		if st := memberStatus(ps, nick); st == "exhausted" {
			t.Errorf("member %q status=%q after capacity overloads", nick, st)
		}
	}
}

// TestModifyResponse_codexStreamPassThroughVerbatim covers every stream the
// guard must not rewrite: the client must receive the upstream bytes exactly,
// with the upstream status and headers.
func TestModifyResponse_codexStreamPassThroughVerbatim(t *testing.T) {
	bigPreamble := strings.Repeat(sseCreated, codexOverloadPeekMaxBytes/len(sseCreated)+1)
	cases := []struct {
		name   string
		stream string
	}{
		{"normal completion", sseCreated + sseInProgress + sseItemAdded + sseTextDelta + sseCompleted},
		{"output before a later overload", sseCreated + sseItemAdded + sseOverload},
		{"overload text inside output", sseCreated + sseTextDelta + sseCompleted},
		{"rate-limit failure", sseCreated + sseRateLimit},
		{"context-window failure", sseCreated + sseCtxWindow},
		{"failed without error", "data: {\"type\":\"response.failed\",\"response\":{}}\n\n"},
		{"error event type", "data: {\"type\":\"error\",\"code\":\"server_is_overloaded\"}\n\n"},
		{"non-JSON data", "data: [DONE]\n\n" + sseOverload},
		{"JSON without type", "data: {\"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n" + sseOverload},
		{"preamble then EOF", sseCreated + sseInProgress},
		{"unterminated overload at EOF", sseCreated + strings.TrimSuffix(sseOverload, "\n\n")},
		{"empty stream", ""},
		{"comments only", ": ping\n\n: ping\n\n"},
		{"lone CR line endings", strings.ReplaceAll(sseCreated+sseOverload, "\n", "\r")},
		{"preamble beyond peek bound", bigPreamble + sseOverload},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			c := newCodexOverloadController(t, &logBuf, "seat1")
			resp, tb := sseResp(c.resolve(t, "seat1"), tc.stream)
			if err := c.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status=%d, want untouched 200", resp.StatusCode)
			}
			if got := resp.Header.Get("x-codex-primary-used-percent"); got != "12" {
				t.Errorf("upstream header altered: %q", got)
			}
			if resp.Header.Get("Retry-After") != "" {
				t.Errorf("Retry-After added to a pass-through stream")
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if string(body) != tc.stream {
				t.Errorf("body altered: got %d bytes, want %d", len(body), len(tc.stream))
			}
			if err := resp.Body.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			if !tb.closed {
				t.Error("closing the replayed body did not close the upstream body")
			}
			if strings.Contains(logBuf.String(), "in-stream") {
				t.Errorf("pass-through logged as an overload: %q", logBuf.String())
			}
		})
	}
}

// TestModifyResponse_codexStreamPeekStopsAtDecision proves the peek reads no
// further than the decisive event's chunk, so the rest of a live stream is
// not buffered behind the gateway.
func TestModifyResponse_codexStreamPeekStopsAtDecision(t *testing.T) {
	c := newCodexOverloadController(t, io.Discard, "seat1")
	head := sseCreated + sseItemAdded
	tail := sseTextDelta + sseCompleted
	tailReader := &trackedBody{r: strings.NewReader(tail)}
	body := io.MultiReader(strings.NewReader(head), tailReader)
	resp, _ := codexStreamResp(c.resolve(t, "seat1"), http.MethodPost, "/backend-api/codex/responses", "text/event-stream", body)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if tailReader.reads != 0 {
		t.Fatalf("peek read past the decisive event (%d tail reads)", tailReader.reads)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != head+tail {
		t.Errorf("body=%q, want %q", got, head+tail)
	}
}

// TestModifyResponse_codexStreamReadErrorReplayed proves a read error during
// the peek reaches the proxy after the held-back bytes, exactly as it would
// have without the peek.
func TestModifyResponse_codexStreamReadErrorReplayed(t *testing.T) {
	c := newCodexOverloadController(t, io.Discard, "seat1")
	boom := errors.New("upstream reset")
	body := io.MultiReader(strings.NewReader(sseCreated), iotest.ErrReader(boom))
	resp, _ := codexStreamResp(c.resolve(t, "seat1"), http.MethodPost, "/backend-api/codex/responses", "text/event-stream", body)
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want untouched 200", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if !errors.Is(err, boom) {
		t.Fatalf("read err=%v, want %v", err, boom)
	}
	if string(got) != sseCreated {
		t.Errorf("replayed bytes=%q, want %q", got, sseCreated)
	}
}

// TestModifyResponse_codexStreamGate pins the scope of the opaque-body
// exception: outside a 2xx identity-encoded SSE answer to a Codex POST
// /responses, the body is never read.
func TestModifyResponse_codexStreamGate(t *testing.T) {
	stream := sseCreated + sseOverload
	cases := []struct {
		name        string
		baseURL     string
		method      string
		path        string
		contentType string
		encoding    string
		status      int
	}{
		{"non-codex host", "https://api.openai.com/v1", http.MethodPost, "/v1/responses", "text/event-stream", "", 200},
		{"anthropic host", testDefaultBaseURL, http.MethodPost, "/v1/messages", "text/event-stream", "", 200},
		{"GET", "", http.MethodGet, "/backend-api/codex/responses", "text/event-stream", "", 200},
		{"compact endpoint", "", http.MethodPost, "/backend-api/codex/responses/compact", "text/event-stream", "", 200},
		{"other endpoint", "", http.MethodPost, "/backend-api/codex/models", "text/event-stream", "", 200},
		{"JSON response", "", http.MethodPost, "/backend-api/codex/responses", "application/json", "", 200},
		{"missing content type", "", http.MethodPost, "/backend-api/codex/responses", "", "", 200},
		{"gzip encoded", "", http.MethodPost, "/backend-api/codex/responses", "text/event-stream", "gzip", 200},
		{"non-2xx", "", http.MethodPost, "/backend-api/codex/responses", "text/event-stream", "", 500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCodexOverloadController(t, io.Discard, "seat1")
			b := c.resolve(t, "seat1")
			if tc.baseURL != "" {
				b.BaseURL = tc.baseURL
			}
			resp, tb := codexStreamResp(b, tc.method, tc.path, tc.contentType, strings.NewReader(stream))
			resp.StatusCode = tc.status
			if tc.encoding != "" {
				resp.Header.Set("Content-Encoding", tc.encoding)
			}
			if err := c.ModifyResponse(resp); err != nil {
				t.Fatalf("ModifyResponse: %v", err)
			}
			if resp.StatusCode != tc.status {
				t.Errorf("status=%d, want untouched %d", resp.StatusCode, tc.status)
			}
			if tb.reads != 0 {
				t.Errorf("gated-out body was read %d times", tb.reads)
			}
		})
	}
}

// codexOverloadProxy fronts upstream with an httputil.ReverseProxy wired the
// way internal/proxy wires it (FlushInterval -1, ModifyResponse hook), while
// the request context carries a chatgpt.com backend so the Codex gate is
// exercised end to end against a loopback upstream.
func codexOverloadProxy(t *testing.T, c *Controller, upstream *httptest.Server) *httptest.Server {
	t.Helper()
	target, _ := url.Parse(upstream.URL)
	b := c.resolve(t, "seat1")
	rp := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = target.Scheme
			r.URL.Host = target.Host
			r.Host = target.Host
		},
		ModifyResponse: c.ModifyResponse,
		FlushInterval:  -1,
	}
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rp.ServeHTTP(w, r.WithContext(backend.WithBackend(r.Context(), b)))
	}))
	t.Cleanup(front.Close)
	return front
}

// TestCodexStreamOverload_endToEnd drives the bug through a real reverse
// proxy: the upstream commits a 200 and flushes the preamble, pauses, then
// fails with server_is_overloaded; the client must see the synthetic 503.
func TestCodexStreamOverload_endToEnd(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, sseCreated+sseInProgress)
		w.(http.Flusher).Flush()
		time.Sleep(20 * time.Millisecond)
		io.WriteString(w, sseOverload)
	}))
	defer upstream.Close()
	c := newCodexOverloadController(t, io.Discard, "seat1")
	front := codexOverloadProxy(t, c, upstream)

	resp, err := http.Post(front.URL+"/backend-api/codex/responses", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%q, want 503", resp.StatusCode, body)
	}
	if ra := resp.Header.Get("Retry-After"); ra != strconv.Itoa(codexOverloadRetryAfterSeconds) {
		t.Errorf("Retry-After=%q, want %d", ra, codexOverloadRetryAfterSeconds)
	}
	if string(body) != `{"error":"backend throttled; same member"}` {
		t.Errorf("body=%q", body)
	}
}

// TestCodexStreamPassThrough_streamsLive proves the peek does not turn a
// healthy stream into a buffered one: once the first output event arrives
// the client receives it while the upstream is still holding the rest.
func TestCodexStreamPassThrough_streamsLive(t *testing.T) {
	release := make(chan struct{})
	head := sseCreated + sseInProgress + sseItemAdded
	tail := sseTextDelta + sseCompleted
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, head)
		w.(http.Flusher).Flush()
		<-release
		io.WriteString(w, tail)
	}))
	defer upstream.Close()
	defer close(release)
	c := newCodexOverloadController(t, io.Discard, "seat1")
	front := codexOverloadProxy(t, c, upstream)

	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := http.Post(front.URL+"/backend-api/codex/responses", "application/json", strings.NewReader(`{}`))
		done <- result{resp, err}
	}()
	var res result
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("response headers withheld after the first output event")
	}
	if res.err != nil {
		t.Fatalf("POST: %v", res.err)
	}
	defer res.resp.Body.Close()
	if res.resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", res.resp.StatusCode)
	}
	got := make([]byte, len(head))
	if _, err := io.ReadFull(res.resp.Body, got); err != nil {
		t.Fatalf("read head before upstream released: %v", err)
	}
	if string(got) != head {
		t.Errorf("head=%q, want %q", got, head)
	}
	release <- struct{}{}
	rest, err := io.ReadAll(res.resp.Body)
	if err != nil {
		t.Fatalf("read tail: %v", err)
	}
	if string(rest) != tail {
		t.Errorf("tail=%q, want %q", rest, tail)
	}
}

// dataErrReader returns its whole payload together with err on the first
// read, the shape of an upstream that delivers a final chunk and fails in
// the same read.
type dataErrReader struct {
	data string
	err  error
	done bool
}

func (r *dataErrReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	r.done = true
	return copy(p, r.data), r.err
}

// TestModifyResponse_codexStreamDecisionWithReadError pins both outcomes when
// the decisive event and a read error arrive in one read: the overload is
// still absorbed (the whole event is in hand), and a pass-through decision
// still replays the error after the bytes.
func TestModifyResponse_codexStreamDecisionWithReadError(t *testing.T) {
	boom := errors.New("upstream reset")

	c := newCodexOverloadController(t, io.Discard, "seat1")
	before := c.Current()
	resp, tb := codexStreamResp(c.resolve(t, "seat1"), http.MethodPost, "/backend-api/codex/responses",
		"text/event-stream", &dataErrReader{data: sseCreated + sseOverload, err: boom})
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	assertCodexOverload503(t, c, resp, tb, before)

	c = newCodexOverloadController(t, io.Discard, "seat1")
	resp, _ = codexStreamResp(c.resolve(t, "seat1"), http.MethodPost, "/backend-api/codex/responses",
		"text/event-stream", &dataErrReader{data: sseCreated + sseItemAdded, err: boom})
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	if !errors.Is(err, boom) {
		t.Fatalf("read err=%v, want %v", err, boom)
	}
	if string(got) != sseCreated+sseItemAdded {
		t.Errorf("replayed bytes=%q", got)
	}
}

// onceErrReader yields data, then err exactly once, then EOF — a body whose
// error is not sticky, so only an explicit replay can surface it.
type onceErrReader struct {
	data string
	err  error
	step int
}

func (r *onceErrReader) Read(p []byte) (int, error) {
	r.step++
	switch r.step {
	case 1:
		return copy(p, r.data), nil
	case 2:
		return 0, r.err
	default:
		return 0, io.EOF
	}
}

// TestModifyResponse_codexStreamNonStickyReadErrorReplayed proves the peek
// itself carries a pre-decision read error to the proxy, rather than relying
// on the upstream body to repeat it.
func TestModifyResponse_codexStreamNonStickyReadErrorReplayed(t *testing.T) {
	c := newCodexOverloadController(t, io.Discard, "seat1")
	boom := errors.New("upstream reset")
	resp, _ := codexStreamResp(c.resolve(t, "seat1"), http.MethodPost, "/backend-api/codex/responses",
		"text/event-stream", &onceErrReader{data: sseCreated, err: boom})
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	if !errors.Is(err, boom) {
		t.Fatalf("read err=%v, want %v", err, boom)
	}
	if string(got) != sseCreated {
		t.Errorf("replayed bytes=%q, want %q", got, sseCreated)
	}
}

func shortenPeekTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := codexOverloadPeekTimeout
	codexOverloadPeekTimeout = d
	t.Cleanup(func() { codexOverloadPeekTimeout = prev })
}

// TestModifyResponse_codexStreamPeekTimeoutHandsOffInFlightRead proves the
// wall-clock bound: a stream stalled after its preamble is released as a
// pass-through once the bound elapses, and the read that was in flight at
// that moment is delivered in order — nothing is lost or duplicated, and a
// late overload is no longer rewritten because the stream has committed.
func TestModifyResponse_codexStreamPeekTimeoutHandsOffInFlightRead(t *testing.T) {
	shortenPeekTimeout(t, 50*time.Millisecond)
	c := newCodexOverloadController(t, io.Discard, "seat1")
	pr, pw := io.Pipe()
	release := make(chan struct{})
	go func() {
		io.WriteString(pw, sseCreated)
		<-release
		io.WriteString(pw, sseOverload)
		pw.Close()
	}()
	ctx := backend.WithBackend(context.Background(), c.resolve(t, "seat1"))
	req := httptest.NewRequest(http.MethodPost, "http://chatgpt.com/backend-api/codex/responses", nil).WithContext(ctx)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Request:    req,
		Body:       pr,
	}

	start := time.Now()
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("peek held the stream %v past its bound", elapsed)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want pass-through 200 after the peek bound", resp.StatusCode)
	}
	close(release)
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(got) != sseCreated+sseOverload {
		t.Errorf("body=%q, want the upstream stream verbatim", got)
	}
}

// TestModifyResponse_codexStreamPeekTimeoutCloseUnblocks proves closing the
// body after a timed-out peek releases the in-flight upstream read, so a
// client that gives up does not strand the reader goroutine.
func TestModifyResponse_codexStreamPeekTimeoutCloseUnblocks(t *testing.T) {
	shortenPeekTimeout(t, 20*time.Millisecond)
	c := newCodexOverloadController(t, io.Discard, "seat1")
	pr, pw := io.Pipe()
	defer pw.Close()
	ctx := backend.WithBackend(context.Background(), c.resolve(t, "seat1"))
	req := httptest.NewRequest(http.MethodPost, "http://chatgpt.com/backend-api/codex/responses", nil).WithContext(ctx)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Request:    req,
		Body:       pr,
	}
	if err := c.ModifyResponse(resp); err != nil {
		t.Fatalf("ModifyResponse: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(resp.Body)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("read after close err=%v, want %v", err, io.ErrClosedPipe)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight read not released by Close")
	}
}

// TestCodexStreamStalledPreamble_endToEnd drives the liveness bound through a
// real reverse proxy: an upstream that stalls after the preamble must still
// get its headers and preamble to the client, so Codex's own idle timeout can
// run, and the rest must follow once the upstream resumes.
func TestCodexStreamStalledPreamble_endToEnd(t *testing.T) {
	shortenPeekTimeout(t, 100*time.Millisecond)
	release := make(chan struct{})
	head := sseCreated + sseInProgress
	tail := sseItemAdded + sseCompleted
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, head)
		w.(http.Flusher).Flush()
		<-release
		io.WriteString(w, tail)
	}))
	defer upstream.Close()
	var once bool
	releaseOnce := func() {
		if !once {
			once = true
			close(release)
		}
	}
	defer releaseOnce()
	c := newCodexOverloadController(t, io.Discard, "seat1")
	front := codexOverloadProxy(t, c, upstream)

	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := http.Post(front.URL+"/backend-api/codex/responses", "application/json", strings.NewReader(`{}`))
		done <- result{resp, err}
	}()
	var res result
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("headers withheld from the client past the peek bound")
	}
	if res.err != nil {
		t.Fatalf("POST: %v", res.err)
	}
	defer res.resp.Body.Close()
	if res.resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", res.resp.StatusCode)
	}
	got := make([]byte, len(head))
	if _, err := io.ReadFull(res.resp.Body, got); err != nil {
		t.Fatalf("read preamble while upstream stalled: %v", err)
	}
	if string(got) != head {
		t.Errorf("preamble=%q, want %q", got, head)
	}
	releaseOnce()
	rest, err := io.ReadAll(res.resp.Body)
	if err != nil {
		t.Fatalf("read tail: %v", err)
	}
	if string(rest) != tail {
		t.Errorf("tail=%q, want %q", rest, tail)
	}
}
