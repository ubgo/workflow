package httprun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/ubgo/workflow"
)

// The Server-Sent Events the events route sends. Each event's data is one
// line of JSON.
const (
	// EventStatus: the run's status, at the start and on every change
	// (and on every lifecycle event: a step started, retried, reported
	// progress). Data: status, done, kind (the workflow.LifecycleKind,
	// absent for a change seen by polling), step, attempt, progress, and
	// error with Config.Errors.
	EventStatus = "status"
	// EventChunk: one chunk a step sent with workflow.Emit. The SSE id is
	// its Pos, so a browser's EventSource reconnects with Last-Event-ID
	// and gets only what it missed. Data: pos, step, attempt, seq, and
	// text (valid UTF-8) or base64 (anything else).
	EventChunk = "chunk"
	// EventEnd: the run is terminal; the stream closes after it. Data:
	// status, done, error with Config.Errors, and the result (result or
	// result_base64) when the run succeeded and the client has
	// CapResult.
	EventEnd = "end"
	// EventError: the route could not go on (the run was deleted, the
	// backend failed); the stream closes after it. Data: code, message.
	EventError = "error"
)

type eventJSON struct {
	Status   workflow.Status `json:"status"`
	Done     bool            `json:"done"`
	Kind     string          `json:"kind,omitempty"`
	Step     string          `json:"step,omitempty"`
	Attempt  int             `json:"attempt,omitempty"`
	Progress *progressJSON   `json:"progress,omitempty"`
	Error    string          `json:"error,omitempty"`
	At       time.Time       `json:"at,omitzero"`
	// Result and ResultBase64 are set on EventEnd only.
	Result       json.RawMessage `json:"result,omitempty"`
	ResultBase64 []byte          `json:"result_base64,omitempty"`
}

type chunkJSON struct {
	Pos     int64  `json:"pos"`
	Step    string `json:"step,omitempty"`
	Attempt int    `json:"attempt,omitempty"`
	Seq     int64  `json:"seq,omitempty"`
	Text    string `json:"text,omitempty"`
	Base64  []byte `json:"base64,omitempty"`
}

// events serves GET /runs/{id}/events.
//
//ds:def id=httprun-handler-p-events-x4bwpc9w owner=@khanakia stability=stable
func (h *handler[P]) events(w http.ResponseWriter, r *http.Request) {
	ref, info, ok := h.open(w, r, ActionEvents)
	if !ok {
		return
	}
	ctx := r.Context()
	st, err := h.c.Status(ctx, ref)
	if err != nil {
		h.fail(w, ActionEvents, err)
		return
	}
	var after int64
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			after = n
		}
	}
	hd := w.Header()
	hd.Set("Content-Type", "text/event-stream")
	hd.Set("Cache-Control", "no-store")
	hd.Set("X-Content-Type-Options", "nosniff")
	hd.Set("X-Accel-Buffering", "no") // ask a fronting proxy not to buffer the stream
	w.WriteHeader(http.StatusOK)
	f := &follower[P]{h: h, w: w, rc: http.NewResponseController(w), ref: ref, last: after}
	if info != nil {
		f.run = info.Ref.RunID
	}
	if f.run.IsZero() {
		f.run = ref.RunID
	}
	f.follow(ctx, st)
}

// follower is one events route following one run.
type follower[P any] struct {
	h    *handler[P]
	w    http.ResponseWriter
	rc   *http.ResponseController
	ref  workflow.RunRef
	run  workflow.RunID // zero until a delayed run's RunID is known
	last int64          // the Pos of the last chunk sent
	st   workflow.Status
	gone bool // a write failed: the client left

	lc         <-chan workflow.LifecycleEvent
	stopLC     func()
	chunks     <-chan workflow.StreamChunk
	stopChunks func()
}

// attach starts following lifecycle events and stream chunks once the
// run's ID is known.
func (f *follower[P]) attach(ctx context.Context) {
	if f.run.IsZero() {
		return
	}
	if f.h.hub != nil && f.lc == nil {
		f.lc, f.stopLC = f.h.hub.follow(f.run)
	}
	if f.h.cfg.Stream != nil && f.chunks == nil {
		f.chunks, f.stopChunks = f.h.cfg.Stream.Subscribe(ctx, f.run)
	}
}

func (f *follower[P]) detach() {
	if f.stopLC != nil {
		f.stopLC()
	}
	if f.stopChunks != nil {
		f.stopChunks()
	}
}

//ds:def id=httprun-follower-p-follow-492nbx3j owner=@khanakia stability=stable
func (f *follower[P]) follow(ctx context.Context, st workflow.Status) {
	defer f.detach()
	f.attach(ctx)
	f.catchUp(ctx)
	f.status(eventJSON{Status: st})
	if st.IsTerminal() {
		f.end(ctx, st)
		return
	}
	clk := f.h.cfg.Clock
	poll, beat := clk.NewTimer(f.h.cfg.Poll), clk.NewTimer(f.h.cfg.Heartbeat)
	defer func() { poll.Stop(); beat.Stop() }()
	for !f.gone {
		select {
		case <-ctx.Done():
			return
		case ev := <-f.lc:
			f.catchUp(ctx) // chunks written before this event go first
			f.status(f.lifecycleJSON(ev))
			if ev.Status.IsTerminal() {
				f.end(ctx, ev.Status)
				return
			}
		case c, ok := <-f.chunks:
			if !ok { // closed or fell behind: catch up, re-subscribe on the next poll
				f.chunks, f.stopChunks = nil, nil
				f.catchUp(ctx)
				continue
			}
			f.chunk(c)
		case <-poll.C():
			poll = clk.NewTimer(f.h.cfg.Poll)
			if !f.poll(ctx) {
				return
			}
		case <-beat.C():
			beat = clk.NewTimer(f.h.cfg.Heartbeat)
			f.write(": keepalive\n\n")
		}
	}
}

// poll asks Status, sends a change, and ends the stream on a terminal
// status or an error. It reports whether to go on.
func (f *follower[P]) poll(ctx context.Context) bool {
	st, err := f.h.c.Status(ctx, f.ref)
	if err != nil {
		if ctx.Err() == nil {
			f.fail(err)
		}
		return false
	}
	if f.run.IsZero() {
		if info, err := f.h.lookup(ctx, f.ref); err == nil && info != nil {
			f.run = info.Ref.RunID
		}
	}
	f.attach(ctx)
	f.catchUp(ctx)
	if st != f.st {
		f.status(eventJSON{Status: st})
	}
	if st.IsTerminal() {
		f.end(ctx, st)
		return false
	}
	return true
}

func (f *follower[P]) lifecycleJSON(ev workflow.LifecycleEvent) eventJSON {
	out := eventJSON{Status: ev.Status, Kind: string(ev.Kind), Step: ev.Step, Attempt: ev.Attempt, At: ev.At}
	if ev.Progress != nil {
		out.Progress = &progressJSON{Percent: ev.Progress.Percent, Message: ev.Progress.Message, Step: ev.Progress.Step}
	}
	if f.h.cfg.Errors && ev.Err != nil {
		out.Error = ev.Err.Error()
	}
	return out
}

// catchUp sends every stored chunk after the last one sent.
func (f *follower[P]) catchUp(ctx context.Context) {
	if f.h.cfg.Stream == nil || f.run.IsZero() {
		return
	}
	cs, err := f.h.cfg.Stream.Read(ctx, f.run, f.last)
	if err != nil {
		f.h.cfg.Logger.Warn("httprun: stream read failed", "run", f.run.String(), "err", err)
		return
	}
	for _, c := range cs {
		f.chunk(c)
	}
}

//ds:def id=httprun-follower-p-chunk-whmhyt5v owner=@khanakia stability=stable
func (f *follower[P]) chunk(c workflow.StreamChunk) {
	if c.Pos <= f.last {
		return // already sent (seen by both Read and Subscribe)
	}
	f.last = c.Pos
	out := chunkJSON{Pos: c.Pos, Step: c.Step, Attempt: c.Attempt, Seq: c.Seq}
	if utf8.Valid(c.Data) {
		out.Text = string(c.Data)
	} else {
		out.Base64 = c.Data
	}
	f.send(strconv.FormatInt(c.Pos, 10), EventChunk, out)
}

func (f *follower[P]) status(ev eventJSON) {
	f.st = ev.Status
	ev.Done = ev.Status.IsTerminal()
	f.send("", EventStatus, ev)
}

// end sends the last chunks and the end event.
//
//ds:def id=httprun-follower-p-end-ajms2jdv owner=@khanakia stability=stable
func (f *follower[P]) end(ctx context.Context, st workflow.Status) {
	f.catchUp(ctx)
	ev := eventJSON{Status: st, Done: true}
	if f.h.cfg.Errors {
		if info, err := f.h.lookup(ctx, f.ref); err == nil && info != nil {
			ev.Error = info.Error
		}
	}
	if st == workflow.StatusSucceeded && f.h.hasResults() {
		if res, err := f.h.c.(workflow.ResultReader).Result(ctx, f.ref); err == nil && len(res) > 0 {
			if json.Valid(res) {
				ev.Result = json.RawMessage(res)
			} else {
				ev.ResultBase64 = res
			}
		}
	}
	f.send("", EventEnd, ev)
}

//ds:def id=httprun-follower-p-fail-dyxmv9y8 owner=@khanakia stability=stable
func (f *follower[P]) fail(err error) {
	code, msg := CodeInternal, "internal error"
	switch {
	case errors.Is(err, workflow.ErrNotFound):
		code, msg = CodeNotFound, "the run no longer exists"
	case workflow.IsUnavailable(err):
		code, msg = CodeUnavailable, "the job backend is unavailable"
	default:
		f.h.cfg.Logger.Error("httprun: events: status failed", "run", refString(f.ref), "err", err)
	}
	f.send("", EventError, struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, msg})
}

// send writes one SSE event and flushes it.
func (f *follower[P]) send(id, event string, v any) {
	b, err := json.Marshal(v)
	if err != nil { // unreachable: every value sent is plain data
		f.h.cfg.Logger.Error("httprun: events: encode failed", "err", err)
		return
	}
	msg := "event: " + event + "\ndata: " + string(b) + "\n\n"
	if id != "" {
		msg = "id: " + id + "\n" + msg
	}
	f.write(msg)
}

func (f *follower[P]) write(s string) {
	if f.gone {
		return
	}
	if _, err := fmt.Fprint(f.w, s); err != nil {
		f.gone = true
		return
	}
	if err := f.rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		f.gone = true
	}
}

func refString(r workflow.RunRef) string {
	if !r.RunID.IsZero() {
		return r.RunID.String()
	}
	return r.ScheduledID.String()
}
