package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// redoFixture is a memory client with a two-step workflow "pipe" (load
// runs after extract and fails while failLoad is set), a workflow "gate"
// (prep, then a wait on event "go" key "k") and counters of each step's
// runs.
type redoFixture struct {
	c                *memory.Client
	failLoad         atomic.Bool
	extracts, loads  atomic.Int32
	gateRef, pipeRef workflow.RunRef
}

func newRedoFixture(t *testing.T) *redoFixture {
	t.Helper()
	f := &redoFixture{c: memory.New()}
	f.failLoad.Store(true)
	pipe := workflow.Workflow{Name: "pipe", Steps: []workflow.Step{
		{Name: "extract", Run: func(context.Context, []byte) ([]byte, error) { f.extracts.Add(1); return []byte("rows"), nil }},
		{Name: "load", After: []string{"extract"}, Run: func(context.Context, []byte) ([]byte, error) {
			f.loads.Add(1)
			if f.failLoad.Load() {
				return nil, workflow.Permanent(errors.New("bad rows"))
			}
			return nil, nil
		}},
	}}
	gate := workflow.Workflow{Name: "gate", Steps: []workflow.Step{
		{Name: "prep", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
		{Name: "w", After: []string{"prep"}, Wait: &workflow.WaitSpec{Event: "go", Key: "k"}},
	}}
	for _, wf := range []workflow.Workflow{pipe, gate} {
		if err := f.c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	var err error
	if f.pipeRef, err = f.c.Enqueue(t.Context(), workflow.Job{Workflow: "pipe", Tenant: "a"}); err != nil {
		t.Fatal(err)
	}
	if f.gateRef, err = f.c.Enqueue(t.Context(), workflow.Job{Workflow: "gate", Tenant: "a"}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, f.c, f.pipeRef, workflow.StatusFailed)
	waitStatus(t, f.c, f.gateRef, workflow.StatusWaiting)
	return f
}

func waitStatus(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, _ := c.Status(t.Context(), ref)
		if st == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s is %s, never reached %s", ref.RunID, st, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestAdmin_ResetForkSignalWaiting drives the three operations on the
// memory driver: a reset of a failed run keeps the steps before the
// chosen one, a reset of a succeeded run needs Succeeded, a fork starts a
// new run with the original's tenant and lineage labels, and
// SignalWaiting reaches only the runs waiting now.
func TestAdmin_ResetForkSignalWaiting(t *testing.T) {
	f := newRedoFixture(t)
	ctx := t.Context()
	a := New(f.c)

	for name, call := range map[string]func() error{
		"reset without a step": func() error { _, err := a.ResetRun(ctx, f.pipeRef, "", workflow.ResetOptions{}); return err },
		"reset of an empty ref": func() error {
			_, err := a.ResetRun(ctx, workflow.RunRef{}, "load", workflow.ResetOptions{})
			return err
		},
		"reset of an odd step": func() error { _, err := a.ResetRun(ctx, f.pipeRef, "nope", workflow.ResetOptions{}); return err },
		"fork without a step":  func() error { _, err := a.ForkRun(ctx, f.pipeRef, "", workflow.ForkOptions{}); return err },
		"fork of an empty ref": func() error { _, err := a.ForkRun(ctx, workflow.RunRef{}, "load", workflow.ForkOptions{}); return err },
		"fork with a reserved label": func() error {
			_, err := a.ForkRun(ctx, f.pipeRef, "load", workflow.ForkOptions{Labels: map[string]string{workflow.LabelForkOf: "x"}})
			return err
		},
		"signal without a name": func() error { _, err := a.SignalWaiting(ctx, workflow.Event{Key: "k"}); return err },
	} {
		if err := call(); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s = %v, want ErrInvalid", name, err)
		}
	}
	if _, err := a.ForkRun(ctx, f.gateRef, "w", workflow.ForkOptions{}); !errors.Is(err, workflow.ErrWrongState) {
		t.Errorf("fork of a waiting run = %v, want ErrWrongState", err)
	}

	// Reset the failed run from load: extract keeps its output.
	f.failLoad.Store(false)
	got, err := a.ResetRun(ctx, f.pipeRef, "load", workflow.ResetOptions{})
	if err != nil || got != f.pipeRef {
		t.Fatalf("ResetRun = %v, %v", got, err)
	}
	waitStatus(t, f.c, f.pipeRef, workflow.StatusSucceeded)
	if f.extracts.Load() != 1 || f.loads.Load() != 2 {
		t.Fatalf("extract ran %d, load %d; want 1 and 2", f.extracts.Load(), f.loads.Load())
	}

	// A succeeded run is reset only with Succeeded, and is marked a redo.
	if _, err := a.ResetRun(ctx, f.pipeRef, "extract", workflow.ResetOptions{}); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("reset of a succeeded run without Succeeded = %v, want ErrWrongState", err)
	}
	if _, err := a.ResetRun(ctx, f.pipeRef, "extract", workflow.ResetOptions{Succeeded: true}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, f.c, f.pipeRef, workflow.StatusSucceeded)
	if f.extracts.Load() != 2 {
		t.Fatalf("extract ran %d after the redo, want 2", f.extracts.Load())
	}
	info, err := a.Run(ctx, f.pipeRef)
	if err != nil || info.Labels[workflow.LabelRedo] != "true" {
		t.Fatalf("redo labels = %v, %v", info.Labels, err)
	}

	// Fork from load: a new run of the same tenant, extract kept.
	fork, err := a.ForkRun(ctx, f.pipeRef, "load", workflow.ForkOptions{Labels: map[string]string{"why": "replay"}})
	if err != nil || fork.RunID == f.pipeRef.RunID || fork.RunID == "" {
		t.Fatalf("ForkRun = %v, %v", fork, err)
	}
	waitStatus(t, f.c, fork, workflow.StatusSucceeded)
	if f.extracts.Load() != 2 || f.loads.Load() != 4 {
		t.Fatalf("after the fork extract ran %d, load %d; want 2 and 4", f.extracts.Load(), f.loads.Load())
	}
	info, err = a.Run(ctx, fork)
	if err != nil || info.Tenant != "a" || info.Labels[workflow.LabelForkOf] != string(f.pipeRef.RunID) || info.Labels["why"] != "replay" {
		t.Fatalf("fork = %+v, %v", info, err)
	}

	// SignalWaiting resumes the waiting gate run, then nobody.
	if n, err := a.SignalWaiting(ctx, workflow.Event{Name: "go", Key: "other"}); err != nil || n != 0 {
		t.Fatalf("SignalWaiting(other key) = %d, %v", n, err)
	}
	if n, err := a.SignalWaiting(ctx, workflow.Event{Name: "go", Key: "k"}); err != nil || n != 1 {
		t.Fatalf("SignalWaiting = %d, %v, want 1", n, err)
	}
	waitStatus(t, f.c, f.gateRef, workflow.StatusSucceeded)
	if n, err := a.SignalWaiting(ctx, workflow.Event{Name: "go", Key: "k"}); err != nil || n != 0 {
		t.Fatalf("SignalWaiting with nobody waiting = %d, %v, want 0", n, err)
	}
}

// fwdClient is the documented wrapper shape, Client plus Forward: it has
// every optional method whether or not its inner client does.
type fwdClient struct {
	workflow.Client
	workflow.Forward
}

func wrapFwd(c workflow.Client) fwdClient {
	return fwdClient{Client: c, Forward: workflow.Forward{Inner: c}}
}

// resetOnly is a client that resets failed runs and nothing more.
type resetOnly struct {
	workflow.Client
	workflow.RunResetter
}

// TestAdmin_ResetForkSignalWaitingUnsupported: a client without an
// interface, and a wrapper whose inner client lacks it (workflow.Forward
// implements every method), is refused with an UnsupportedError naming
// the operation, and the capabilities report says so.
func TestAdmin_ResetForkSignalWaitingUnsupported(t *testing.T) {
	ctx := t.Context()
	ref := workflow.RunRef{RunID: "r1"}
	for name, c := range map[string]workflow.Client{
		"bare":    bareClient{caps: allCaps},
		"wrapper": wrapFwd(bareClient{caps: allCaps}),
	} {
		a := New(c)
		for _, tc := range []struct {
			op   Operation
			call func() error
		}{
			{OpResetRun, func() error { _, err := a.ResetRun(ctx, ref, "s", workflow.ResetOptions{}); return err }},
			{OpResetSucceeded, func() error {
				_, err := a.ResetRun(ctx, ref, "s", workflow.ResetOptions{Succeeded: true})
				return err
			}},
			{OpForkRun, func() error { _, err := a.ForkRun(ctx, ref, "s", workflow.ForkOptions{}); return err }},
			{OpSignalWaiting, func() error { _, err := a.SignalWaiting(ctx, workflow.Event{Name: "go"}); return err }},
		} {
			var ue *workflow.UnsupportedError
			if err := tc.call(); !errors.As(err, &ue) || ue.Field != "admin."+string(tc.op) {
				t.Errorf("%s: %s = %v, want an UnsupportedError for admin.%s", name, tc.op, err, tc.op)
			}
			if a.Supports(tc.op) {
				t.Errorf("%s: %s reported supported", name, tc.op)
			}
		}
	}

	// A client that resets failed runs only: reset_run yes,
	// reset_succeeded no.
	f := newRedoFixture(t)
	a := New(resetOnly{Client: f.c, RunResetter: f.c})
	if !a.Supports(OpResetRun) || a.Supports(OpResetSucceeded) || a.Supports(OpForkRun) {
		t.Fatalf("reset-only client: reset %v, succeeded %v, fork %v", a.Supports(OpResetRun), a.Supports(OpResetSucceeded), a.Supports(OpForkRun))
	}
	var ue *workflow.UnsupportedError
	if _, err := a.ResetRun(ctx, f.pipeRef, "load", workflow.ResetOptions{Succeeded: true}); !errors.As(err, &ue) || ue.Field != "admin.reset_succeeded" {
		t.Fatalf("Succeeded on a reset-only client = %v", err)
	}
	if _, err := a.ResetRun(ctx, f.pipeRef, "load", workflow.ResetOptions{}); err != nil {
		t.Fatalf("reset on a reset-only client = %v", err)
	}

	// The memory client supports all four.
	full := New(f.c)
	for _, op := range []Operation{OpResetRun, OpResetSucceeded, OpForkRun, OpSignalWaiting} {
		if !full.Supports(op) {
			t.Errorf("memory client: %s unsupported", op)
		}
	}
}

// recordingRedo implements the four operations, recording each call.
type recordingRedo struct {
	bareClient
	calls []string
}

func (r *recordingRedo) ResetRun(context.Context, workflow.RunRef, string) (workflow.RunRef, error) {
	r.calls = append(r.calls, "ResetRun")
	return workflow.RunRef{}, nil
}

func (r *recordingRedo) ResetRunWith(context.Context, workflow.RunRef, string, workflow.ResetOptions) (workflow.RunRef, error) {
	r.calls = append(r.calls, "ResetRunWith")
	return workflow.RunRef{}, nil
}

func (r *recordingRedo) ForkRun(context.Context, workflow.RunRef, string, workflow.ForkOptions) (workflow.RunRef, error) {
	r.calls = append(r.calls, "ForkRun")
	return workflow.RunRef{}, nil
}

func (r *recordingRedo) SignalWaiting(context.Context, workflow.Event) (int, error) {
	r.calls = append(r.calls, "SignalWaiting")
	return 0, nil
}

// TestAdmin_EmptyRedoTargetsReachNoClient: an empty ref, step or event
// name is refused by Admin itself with ErrInvalid, whatever the driver
// would say; a well-formed call reaches the client.
func TestAdmin_EmptyRedoTargetsReachNoClient(t *testing.T) {
	ctx := t.Context()
	r := &recordingRedo{}
	a := New(r)
	ref := workflow.RunRef{RunID: "r1"}
	for name, call := range map[string]func() error{
		"reset, empty ref":   func() error { _, err := a.ResetRun(ctx, workflow.RunRef{}, "s", workflow.ResetOptions{}); return err },
		"reset, empty step":  func() error { _, err := a.ResetRun(ctx, ref, "", workflow.ResetOptions{}); return err },
		"redo, empty step":   func() error { _, err := a.ResetRun(ctx, ref, "", workflow.ResetOptions{Succeeded: true}); return err },
		"fork, empty ref":    func() error { _, err := a.ForkRun(ctx, workflow.RunRef{}, "s", workflow.ForkOptions{}); return err },
		"fork, empty step":   func() error { _, err := a.ForkRun(ctx, ref, "", workflow.ForkOptions{}); return err },
		"signal, empty name": func() error { _, err := a.SignalWaiting(ctx, workflow.Event{Key: "k"}); return err },
		"fork, reserved label": func() error {
			_, err := a.ForkRun(ctx, ref, "s", workflow.ForkOptions{Labels: map[string]string{workflow.LabelRedo: "true"}})
			return err
		},
	} {
		if err := call(); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s = %v, want ErrInvalid", name, err)
		}
	}
	if len(r.calls) != 0 {
		t.Fatalf("refused calls reached the client: %v", r.calls)
	}
	_, _ = a.ResetRun(ctx, ref, "s", workflow.ResetOptions{})
	_, _ = a.ResetRun(ctx, ref, "s", workflow.ResetOptions{Succeeded: true})
	_, _ = a.ForkRun(ctx, ref, "s", workflow.ForkOptions{})
	_, _ = a.SignalWaiting(ctx, workflow.Event{Name: "go"})
	if got := strings.Join(r.calls, ","); got != "ResetRun,ResetRunWith,ForkRun,SignalWaiting" {
		t.Fatalf("calls = %s", got)
	}
}

// TestHandler_ResetForkSignalWaiting: the three routes answer with the
// run's ref, the new run's ref and the resumed count; bodies are checked;
// errors map to 400, 409 and 501; the Authorizer and the audit log see
// the step, the options and the event.
func TestHandler_ResetForkSignalWaiting(t *testing.T) {
	f := newRedoFixture(t)
	var seen []Request[user]
	auth := bearerAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error {
		seen = append(seen, req)
		return nil
	}
	audit := &MemoryAudit{}
	h := NewHandler(New(f.c), auth, WithAudit(audit))
	pipe := "/runs/" + string(f.pipeRef.RunID)
	gate := "/runs/" + string(f.gateRef.RunID)

	for _, c := range []struct{ path, body string }{
		{pipe + "/reset", `{}`},
		{pipe + "/reset", `{"step":"load","bogus":1}`},
		{pipe + "/reset", `{"step":"nope"}`},
		{pipe + "/fork", `{"step":""}`},
		{pipe + "/fork", `{"step":"load","labels":{"ubgo.workflow/redo":"true"}}`},
		{pipe + "/fork", `[1]`},
		{"/signals/waiting", `{}`},
		{"/signals/waiting", `{"name":"go","payload":nope}`},
	} {
		if rec := do(t, h, "POST", c.path, c.body); rec.Code != http.StatusBadRequest || errCode(t, rec) != CodeInvalid {
			t.Errorf("POST %s %s = %d %s, want 400", c.path, c.body, rec.Code, rec.Body)
		}
	}
	for _, c := range []struct{ path, body string }{
		{gate + "/reset", `{"step":"w"}`},
		{gate + "/fork", `{"step":"w"}`},
	} {
		if rec := do(t, h, "POST", c.path, c.body); rec.Code != http.StatusConflict || errCode(t, rec) != CodeConflict {
			t.Errorf("POST %s on a waiting run = %d %s, want 409", c.path, rec.Code, rec.Body)
		}
	}
	if rec := do(t, h, "POST", "/runs/nope/fork", `{"step":"load"}`); rec.Code != http.StatusNotFound {
		t.Errorf("fork of an unknown run = %d %s, want 404", rec.Code, rec.Body)
	}

	// Reset the failed run.
	f.failLoad.Store(false)
	rec := do(t, h, "POST", pipe+"/reset", `{"step":"load"}`)
	var out struct {
		Ref       refJSON `json:"ref"`
		Duplicate bool    `json:"duplicate"`
		Resumed   *int    `json:"resumed"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil || out.Ref.RunID != string(f.pipeRef.RunID) {
		t.Fatalf("reset = %d %s", rec.Code, rec.Body)
	}
	waitStatus(t, f.c, f.pipeRef, workflow.StatusSucceeded)
	if rec := do(t, h, "POST", pipe+"/reset", `{"step":"extract"}`); rec.Code != http.StatusConflict {
		t.Fatalf("reset of a succeeded run without succeeded = %d %s, want 409", rec.Code, rec.Body)
	}
	if rec := do(t, h, "POST", pipe+"/reset", `{"step":"extract","succeeded":true}`); rec.Code != http.StatusOK {
		t.Fatalf("reset with succeeded = %d %s", rec.Code, rec.Body)
	}
	waitStatus(t, f.c, f.pipeRef, workflow.StatusSucceeded)

	// Fork the gate run (now succeeded after a signal) from its wait
	// step: the fork waits for a new event; the same key again returns it.
	if n, err := f.c.SignalWaiting(t.Context(), workflow.Event{Name: "go", Key: "k"}); err != nil || n != 1 {
		t.Fatalf("SignalWaiting = %d, %v", n, err)
	}
	waitStatus(t, f.c, f.gateRef, workflow.StatusSucceeded)
	rec = do(t, h, "POST", gate+"/fork", `{"step":"w","idempotency_key":"once","labels":{"why":"replay"}}`)
	out = struct {
		Ref       refJSON `json:"ref"`
		Duplicate bool    `json:"duplicate"`
		Resumed   *int    `json:"resumed"`
	}{}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil || out.Ref.RunID == "" || out.Ref.RunID == string(f.gateRef.RunID) || out.Duplicate {
		t.Fatalf("fork = %d %s", rec.Code, rec.Body)
	}
	fork := workflow.RunRef{RunID: workflow.RunID(out.Ref.RunID)}
	waitStatus(t, f.c, fork, workflow.StatusWaiting)
	rec = do(t, h, "POST", gate+"/fork", `{"step":"w","idempotency_key":"once"}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"duplicate":true`) || !strings.Contains(rec.Body.String(), out.Ref.RunID) {
		t.Fatalf("fork with a held key = %d %s, want the first fork as a duplicate", rec.Code, rec.Body)
	}

	// Signal the waiting fork.
	rec = do(t, h, "POST", "/signals/waiting", `{"name":"go","key":"k","payload":{"n":1}}`)
	if rec.Code != http.StatusOK || rec.Body.String() != "{\"resumed\":1}\n" {
		t.Fatalf("signal-waiting = %d %q", rec.Code, rec.Body)
	}
	waitStatus(t, f.c, fork, workflow.StatusSucceeded)
	if rec := do(t, h, "POST", "/signals/waiting", `{"name":"go","key":"k"}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"resumed":0`) {
		t.Fatalf("signal-waiting with nobody waiting = %d %s", rec.Code, rec.Body)
	}

	// What the Authorizer saw.
	var resets, forks, signals int
	for _, req := range seen {
		switch req.Action {
		case ActionResetRun:
			if req.Resource.Run.IsZero() || req.Resource.Reset == nil {
				t.Errorf("reset resource %+v", req.Resource)
			}
			if req.Resource.Step == "extract" && req.Resource.Reset.Succeeded {
				resets++
			}
		case ActionForkRun:
			if req.Resource.Step == "w" && req.Resource.Fork != nil && req.Resource.Fork.IdempotencyKey == "once" {
				forks++
			}
		case ActionSignalWaiting:
			if req.Resource.Event != nil && req.Resource.Event.Name == "go" && string(req.Resource.Event.Payload) == `{"n":1}` {
				signals++
			}
		}
	}
	if resets != 1 || forks != 2 || signals != 1 {
		t.Errorf("Authorizer saw %d redo resets, %d keyed forks, %d signals with a payload; want 1, 2, 1", resets, forks, signals)
	}

	// Every one of them was audited, with its outcome.
	byAction := map[Action][]AuditEntry{}
	for _, e := range audit.Entries() {
		byAction[e.Action] = append(byAction[e.Action], e)
	}
	for action, want := range map[Action]int{ActionResetRun: 6, ActionForkRun: 6, ActionSignalWaiting: 3} {
		if got := len(byAction[action]); got != want {
			t.Errorf("%s audited %d times, want %d", action, got, want)
		}
	}
	last := byAction[ActionSignalWaiting][2]
	if last.Status != http.StatusOK || last.Err != nil || last.Resource.Event == nil || last.Resource.Event.Key != "k" {
		t.Errorf("signal-waiting audit entry %+v", last)
	}
	conflict := byAction[ActionResetRun][2]
	if conflict.Status != http.StatusConflict || !errors.Is(conflict.Err, workflow.ErrWrongState) {
		t.Errorf("refused reset audit entry %+v", conflict)
	}
}

// TestHandler_ResetForkSignalWaitingUnsupported: on a client without the
// interfaces each route answers 501 and capabilities report them
// unsupported.
func TestHandler_ResetForkSignalWaitingUnsupported(t *testing.T) {
	h := NewHandler(New(wrapFwd(bareClient{caps: allCaps})), bearerAuth())
	for _, c := range []struct{ path, body string }{
		{"/runs/r1/reset", `{"step":"s"}`},
		{"/runs/r1/reset", `{"step":"s","succeeded":true}`},
		{"/runs/r1/fork", `{"step":"s"}`},
		{"/signals/waiting", `{"name":"go"}`},
	} {
		if rec := do(t, h, "POST", c.path, c.body); rec.Code != http.StatusNotImplemented || errCode(t, rec) != CodeUnsupported {
			t.Errorf("POST %s %s = %d %s, want 501", c.path, c.body, rec.Code, rec.Body)
		}
	}
	rec := do(t, h, "GET", "/capabilities", "")
	for _, op := range []Operation{OpResetRun, OpResetSucceeded, OpForkRun, OpSignalWaiting} {
		if !strings.Contains(rec.Body.String(), `{"operation":"`+string(op)+`","supported":false`) {
			t.Errorf("capabilities do not report %s unsupported: %s", op, rec.Body)
		}
	}
}

// TestTenantScope_ResetForkSignalWaiting: a scoped caller resets and
// forks its own runs (the fork keeps the tenant), gets 404 for another
// tenant's run, 403 for POST /signals/waiting (an event reaches every
// tenant's runs), and never learns another tenant's run through a fork's
// idempotency key.
func TestTenantScope_ResetForkSignalWaiting(t *testing.T) {
	ts := newTenants(t)
	h := NewHandler(New(ts.c), scopedAuth(), WithTenantScope(byName))
	own, other := "/runs/"+string(ts.aFail.RunID), "/runs/"+string(ts.bFail.RunID)

	if rec := as(t, h, "a", "POST", own+"/reset", `{"step":"b"}`); rec.Code != http.StatusOK {
		t.Fatalf("a resets its run: %d %s", rec.Code, rec.Body)
	}
	waitStatus(t, ts.c, ts.aFail, workflow.StatusFailed)
	rec := as(t, h, "a", "POST", own+"/fork", `{"step":"b"}`)
	var out struct {
		Ref refJSON `json:"ref"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil || out.Ref.RunID == "" {
		t.Fatalf("a forks its run: %d %s", rec.Code, rec.Body)
	}
	if info, err := New(ts.c).Run(t.Context(), workflow.RunRef{RunID: workflow.RunID(out.Ref.RunID)}); err != nil || info.Tenant != "a" {
		t.Fatalf("a's fork = %+v, %v; want tenant a", info, err)
	}
	if rec := as(t, h, "a", "GET", "/runs/"+out.Ref.RunID, ""); rec.Code != http.StatusOK {
		t.Fatalf("a reads its fork: %d %s", rec.Code, rec.Body)
	}
	for _, path := range []string{other + "/reset", other + "/fork"} {
		if rec := as(t, h, "a", "POST", path, `{"step":"b"}`); rec.Code != http.StatusNotFound {
			t.Errorf("a POST %s = %d %s, want 404", path, rec.Code, rec.Body)
		}
	}
	if rec := as(t, h, "a", "POST", "/signals/waiting", `{"name":"go"}`); rec.Code != http.StatusForbidden {
		t.Errorf("a POST /signals/waiting = %d %s, want 403", rec.Code, rec.Body)
	}
	if st, _ := ts.c.Status(t.Context(), ts.aWait); st != workflow.StatusWaiting {
		t.Fatalf("a's waiting run is %s after the refused signal", st)
	}
	// a's capabilities hide signal-waiting and keep reset and fork.
	caps := as(t, h, "a", "GET", "/capabilities", "").Body.String()
	for op, want := range map[Operation]bool{OpSignalWaiting: false, OpResetRun: true, OpResetSucceeded: true, OpForkRun: true} {
		if !strings.Contains(caps, `{"operation":"`+string(op)+`","supported":`+strconv.FormatBool(want)) {
			t.Errorf("a's capabilities: %s supported should be %v: %s", op, want, caps)
		}
	}
	if rec := as(t, h, "ops", "POST", "/signals/waiting", `{"name":"go"}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"resumed":2`) {
		t.Errorf("ops POST /signals/waiting = %d %s, want 2 resumed", rec.Code, rec.Body)
	}

	// b holds the key "k" with an unfinished run of the same workflow; a's
	// fork with that key is refused without naming b's run.
	held, err := ts.c.Enqueue(t.Context(), workflow.Job{Workflow: "boom", Tenant: "b", IdempotencyKey: "k", RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	rec = as(t, h, "a", "POST", own+"/fork", `{"step":"b","idempotency_key":"k"}`)
	if rec.Code != http.StatusConflict || strings.Contains(rec.Body.String(), string(held.ScheduledID)) || (held.RunID != "" && strings.Contains(rec.Body.String(), string(held.RunID))) {
		t.Fatalf("a's fork with b's key = %d %s, want a 409 without b's run", rec.Code, rec.Body)
	}
	// An operator who sees every tenant gets the holder, marked duplicate.
	rec = as(t, h, "ops", "POST", own+"/fork", `{"step":"b","idempotency_key":"k"}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"duplicate":true`) {
		t.Fatalf("ops fork with b's key = %d %s", rec.Code, rec.Body)
	}
}

// TestUI_RedoForms: the run page offers reset and fork behind the
// operations the capabilities report, posts to the routes, and asks for
// the explicit opt-in before redoing a run that succeeded. Every
// operation name the script checks is one Admin reports.
func TestUI_RedoForms(t *testing.T) {
	js := string(mustRead(t, "ui/app.js"))
	for _, want := range []string{
		"supported('reset_run') && r.status === 'failed'",
		"supported('reset_succeeded') && r.status === 'succeeded'",
		"supported('fork_run') && TERMINAL.has(r.status)",
		"withSuffix(path, '/reset')", "withSuffix(path, '/fork')",
		"body.succeeded = true", "body.idempotency_key",
		"if (!redoBox.checked) throw",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("app.js lacks %q", want)
		}
	}
	for _, m := range regexp.MustCompile(`supported\('([a-z_]+)'\)`).FindAllStringSubmatch(js, -1) {
		if !slices.Contains(Operations, Operation(m[1])) {
			t.Errorf("app.js checks operation %q, which Admin does not report", m[1])
		}
	}
}
