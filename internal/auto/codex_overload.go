package auto

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
)

// codexOverloadRetryAfterSeconds is the Retry-After the synthetic same-member
// 503 carries when a Codex stream opens with an in-stream
// server_is_overloaded failure (issue #345). Codex sleeps exactly the
// advertised delay before retrying an HTTP 503 — first at its request layer
// (request_max_retries, default 4), then again per turn retry
// (stream_max_retries, default 5) — so 30s rides out minutes of a
// model-capacity wobble while a lone blip costs one brief pause rather than
// a stalled turn.
const codexOverloadRetryAfterSeconds = 30

// codexOverloadPeekMaxBytes bounds how much of a Codex stream prefix the
// gateway holds back while waiting for the first decisive event. The
// preamble events echo the response object (instructions, tool schemas), so
// the bound is generous; a prefix that outgrows it passes through verbatim.
const codexOverloadPeekMaxBytes = 1 << 20

// codexOverloadPeekTimeout bounds how long the peek holds response headers
// back. Codex's stream idle timeout only starts once headers arrive, and its
// wait for headers has no timeout of its own, so an upstream that stalls
// after the preamble would otherwise hang the client indefinitely. The
// observed capacity failure lands ~2s in; past this bound the held bytes go
// out and the stream continues exactly as it would have without the peek.
// A var so tests can shorten it.
var codexOverloadPeekTimeout = 15 * time.Second

// codexOverloadErrorCode is the Responses error code ChatGPT-Codex puts on
// the "Selected model is at capacity" response.failed event. Codex classifies
// it as ServerOverloaded, which it retries only with server retry advice —
// advice an in-stream event never carries, so the turn ends (issue #345).
const codexOverloadErrorCode = "server_is_overloaded"

// codexStreamPreamble lists the Responses event types that may precede a
// stream's first decisive event. They carry no output, so holding them back
// until the stream commits costs the client nothing it could act on.
var codexStreamPreamble = map[string]bool{
	"response.created":     true,
	"response.in_progress": true,
	"response.queued":      true,
	"response.metadata":    true,
}

// isCodexResponsesStream reports whether resp is a successful, uncompressed
// SSE answer to a Codex Responses request — the only response shape whose
// prefix peekCodexStreamOverload may read. Everything else stays opaque:
// a compressed body cannot be read without decoding it, and any other path
// or content type is outside the failure mode this guard exists for.
func isCodexResponsesStream(resp *http.Response) bool {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || resp.Body == nil || resp.Body == http.NoBody {
		return false
	}
	if resp.Request.Method != http.MethodPost || !strings.HasSuffix(resp.Request.URL.Path, "/responses") {
		return false
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && !strings.EqualFold(enc, "identity") {
		return false
	}
	mt, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	return err == nil && mt == "text/event-stream"
}

// peekCodexStreamOverload reads the prefix of a Codex Responses SSE stream
// until its first decisive event and reports whether that event is a
// response.failed carrying server_is_overloaded (issue #345).
//
// This is the one deliberate exception to the opaque-body invariant: ChatGPT
// answers an at-capacity model with HTTP 200 and then fails inside the
// stream, so no status- or header-only classifier can see it. The exception
// is scoped to the window before any response header reaches the client:
// only each event's type and, on response.failed, its error code are read,
// and nothing is rewritten unless the stream fails before producing output.
//
// When it returns false resp.Body is replaced by a reader that replays the
// held-back bytes verbatim and then continues the live upstream body —
// including a read still in flight when the peek timed out — so the client
// sees exactly the upstream stream. A read error before a decision is
// replayed after the held-back bytes, preserving the proxy's existing failure
// behavior. When it returns true the upstream body is closed and the caller
// owns the rewrite.
func peekCodexStreamOverload(resp *http.Response) bool {
	upstream := resp.Body
	var held []byte
	var readErr error
	var inFlight <-chan peekRead
	scanner := sseEventScanner{}
	timer := time.NewTimer(codexOverloadPeekTimeout)
	defer timer.Stop()
	for inFlight == nil {
		// Each read runs on its own goroutine so the timer can give up on
		// it; reads stay strictly sequential because the next one starts
		// only after this one is received, here or by pendingReader.
		ch := make(chan peekRead, 1)
		go func() {
			buf := make([]byte, 4096)
			n, err := upstream.Read(buf)
			ch <- peekRead{buf[:n], err}
		}()
		var r peekRead
		select {
		case r = <-ch:
		case <-timer.C:
			inFlight = ch
			continue
		}
		held = append(held, r.data...)
		if r.err != nil && r.err != io.EOF {
			readErr = r.err
		}
		decided, overloaded := scanner.feed(held)
		if overloaded {
			upstream.Close()
			return true
		}
		if decided || r.err != nil || len(held) >= codexOverloadPeekMaxBytes {
			break
		}
	}
	var rest io.Reader = upstream
	switch {
	case readErr != nil:
		rest = errReader{readErr}
	case inFlight != nil:
		rest = &pendingReader{pending: inFlight, upstream: upstream}
	}
	resp.Body = replayBody{Reader: io.MultiReader(bytes.NewReader(held), rest), Closer: upstream}
	return false
}

// peekRead is one upstream read performed during the peek.
type peekRead struct {
	data []byte
	err  error
}

// pendingReader continues a stream whose peek timed out mid-read: it first
// delivers the in-flight read's result, then reads the upstream directly.
// Closing the upstream (the replay body's Closer) unblocks the in-flight
// read, and its buffered channel lets that goroutine exit unreceived.
type pendingReader struct {
	pending  <-chan peekRead
	data     []byte
	err      error
	upstream io.Reader
}

func (r *pendingReader) Read(p []byte) (int, error) {
	if r.pending != nil {
		res := <-r.pending
		r.pending = nil
		r.data, r.err = res.data, res.err
	}
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	return r.upstream.Read(p)
}

// sseEventScanner incrementally splits held-back SSE bytes into events and
// classifies them. Only complete lines are consumed, so a line split across
// reads is reassembled on the next feed. Lines end in LF or CRLF; the event
// type comes from the data payload's "type" field, the same field Codex
// dispatches on (it ignores the SSE "event:" line).
type sseEventScanner struct {
	pos  int
	data []string
}

// feed scans buf from where the previous call stopped. decided is true once
// the first non-preamble event has been seen; overloaded reports whether that
// event is the server_is_overloaded failure.
func (s *sseEventScanner) feed(buf []byte) (decided, overloaded bool) {
	for {
		i := bytes.IndexByte(buf[s.pos:], '\n')
		if i < 0 {
			return false, false
		}
		line := strings.TrimSuffix(string(buf[s.pos:s.pos+i]), "\r")
		s.pos += i + 1
		switch {
		case line == "":
			if len(s.data) == 0 {
				continue
			}
			payload := strings.Join(s.data, "\n")
			s.data = s.data[:0]
			if decided, overloaded := classifyCodexStreamEvent(payload); decided {
				return true, overloaded
			}
		case strings.HasPrefix(line, ":"):
			// Comment / keep-alive: carries nothing.
		case line == "data" || strings.HasPrefix(line, "data:"):
			v := strings.TrimPrefix(strings.TrimPrefix(line, "data"), ":")
			s.data = append(s.data, strings.TrimPrefix(v, " "))
		}
	}
}

// classifyCodexStreamEvent classifies one SSE data payload. A preamble event
// is undecided; a payload that is not a JSON object with a type decides
// pass-through, because a stream the gateway cannot read is never rewritten.
func classifyCodexStreamEvent(payload string) (decided, overloaded bool) {
	var ev struct {
		Type     string `json:"type"`
		Response struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		return true, false
	}
	if codexStreamPreamble[ev.Type] {
		return false, false
	}
	return true, ev.Type == "response.failed" && ev.Response.Error.Code == codexOverloadErrorCode
}

// replayBody is the response body after a pass-through peek: held-back bytes
// then the live upstream, closed through the original upstream body.
type replayBody struct {
	io.Reader
	io.Closer
}

// errReader replays a read error captured while peeking.
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }
