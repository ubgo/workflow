package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// EscalationHarness builds what the run-signal cases run on. Start must
// apply the Journal plugin (over the driver's durable JournalStore where
// it has one) and then plugins to the driver client with workflow.Use,
// register wfs on the result, start it, return it, and stop it when t
// ends. The driver client itself must be the one Use wraps, so
// OnSignals binds its RunSignalReader.
type EscalationHarness struct {
	Start func(t *testing.T, plugins []workflow.Plugin, wfs ...workflow.Workflow) workflow.Client
}

// sigLog is what handlers and steps saw, shared with the test.
type sigLog struct {
	mu   sync.Mutex
	seen []string
}

func (l *sigLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, s)
}

func (l *sigLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.seen)
}

// waitLog waits until the log holds n entries.
func waitLog(t *testing.T, l *sigLog, n int) []string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if got := l.get(); len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d log entries, have %q", n, l.get())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// sigGate is a channel a step blocks on until the test opens it; in
// closes when a step first blocks on it, so the test knows the step body
// (and the middleware before it) is running.
type sigGate struct {
	once, inOnce sync.Once
	ch, in       chan struct{}
}

func newSigGate() *sigGate { return &sigGate{ch: make(chan struct{}), in: make(chan struct{})} }

func (g *sigGate) open() { g.once.Do(func() { close(g.ch) }) }

func (g *sigGate) wait(ctx context.Context) error {
	g.inOnce.Do(func() { close(g.in) })
	select {
	case <-g.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// entered waits until a step blocks on the gate.
func (g *sigGate) entered(t *testing.T) {
	t.Helper()
	select {
	case <-g.in:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the gated step to start")
	}
}

// RunEscalationFlows proves run-addressed signals on a driver (WP-22,
// WP-23): a wait step with WaitSpec.OwnRun taking only the signals sent
// to its own run, kept when sent before the wait begins; SignalParent
// from a child resuming its parent's wait (escalation); OnSignals
// handlers running between steps in send order and leaving other names
// alone; escalations handled while the parent joins its children; a
// handler error retried with the step and the signal handled again; the
// RunSignalReader contract (take once, unknown ids); ApproveRun,
// RejectRun, SignalChoiceRun and UpdateRun reaching only the run they
// name; a handler that ends the run. A driver without a case's capability or RunSignalReader skips
// that case.
func RunEscalationFlows(t *testing.T, h EscalationHarness) {
	echo := func(_ context.Context, in []byte) ([]byte, error) { return in, nil }

	t.Run("own-run-wait", func(t *testing.T) {
		wf := workflow.Workflow{Name: "esc-own", Steps: []workflow.Step{
			{Name: "wait", Wait: &workflow.WaitSpec{Event: "go", OwnRun: true}},
			{Name: "echo", After: []string{"wait"}, Run: echo},
		}}
		c := h.Start(t, nil, wf)
		needCaps(t, c, workflow.CapWaitOwnRun, workflow.CapResult)
		ctx := context.Background()
		a, err := c.Enqueue(ctx, workflow.Job{Workflow: "esc-own"})
		if err != nil {
			t.Fatal(err)
		}
		b, err := c.Enqueue(ctx, workflow.Job{Workflow: "esc-own"})
		if err != nil {
			t.Fatal(err)
		}
		flowStatus(t, c, a, workflow.StatusWaiting)
		flowStatus(t, c, b, workflow.StatusWaiting)
		// A plain signal on the event name, with or without a key, is not
		// addressed to either run.
		if err := c.Signal(ctx, workflow.Event{Name: "go", Payload: []byte(`"stray"`)}); err != nil {
			t.Fatal(err)
		}
		if err := workflow.SignalRun(ctx, c, a, "go", []byte(`"for a"`)); err != nil {
			t.Fatal(err)
		}
		if out, err := flowAwait(t, c, a); err != nil || string(out) != `"for a"` {
			t.Fatalf("run a: %q %v", out, err)
		}
		time.Sleep(50 * time.Millisecond)
		if st, err := c.Status(ctx, b); err != nil || st != workflow.StatusWaiting {
			t.Fatalf("run b took a signal addressed to run a: %s %v", st, err)
		}
		if err := workflow.SignalRun(ctx, c, b, "go", []byte(`"for b"`)); err != nil {
			t.Fatal(err)
		}
		if out, err := flowAwait(t, c, b); err != nil || string(out) != `"for b"` {
			t.Fatalf("run b: %q %v", out, err)
		}
	})

	t.Run("own-run-kept", func(t *testing.T) {
		g := newSigGate()
		defer g.open()
		wf := workflow.Workflow{Name: "esc-kept", Steps: []workflow.Step{
			{Name: "gate", Run: func(ctx context.Context, _ []byte) ([]byte, error) { return nil, g.wait(ctx) }},
			{Name: "wait", After: []string{"gate"}, Wait: &workflow.WaitSpec{Event: "go", OwnRun: true}},
			{Name: "echo", After: []string{"wait"}, Run: echo},
		}}
		c := h.Start(t, nil, wf)
		needCaps(t, c, workflow.CapWaitOwnRun, workflow.CapResult)
		ctx := context.Background()
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "esc-kept"})
		if err != nil {
			t.Fatal(err)
		}
		g.entered(t)
		if err := workflow.SignalRun(ctx, c, ref, "go", []byte(`"early"`)); err != nil {
			t.Fatal(err)
		}
		g.open()
		if out, err := flowAwait(t, c, ref); err != nil || string(out) != `"early"` {
			t.Fatalf("a signal sent before the wait began: %q %v", out, err)
		}
	})

	t.Run("escalate-to-waiting-parent", func(t *testing.T) {
		var c workflow.Client
		child := workflow.Workflow{Name: "esc-child", Steps: []workflow.Step{{Name: "work", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if err := workflow.SignalParent(ctx, c, "escalation", in); err != nil {
				return nil, err
			}
			return []byte(`"child done"`), nil
		}}}}
		parent := workflow.Workflow{Name: "esc-parent", Steps: []workflow.Step{
			{Name: "spawn", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				_, err := workflow.Spawn(ctx, c, "kid", []workflow.Job{{Workflow: "esc-child", Payload: []byte(`"need approval"`)}})
				return nil, err
			}},
			{Name: "escalated", After: []string{"spawn"}, Wait: &workflow.WaitSpec{Event: "escalation", OwnRun: true, Timeout: time.Minute}},
			{Name: "echo", After: []string{"escalated"}, Run: echo},
		}}
		c = h.Start(t, nil, child, parent)
		needCaps(t, c, workflow.CapWaitOwnRun, workflow.CapResult, workflow.CapIdempotencyKey)
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "esc-parent"})
		if err != nil {
			t.Fatal(err)
		}
		if out, err := flowAwait(t, c, ref); err != nil || string(out) != `"need approval"` {
			t.Fatalf("parent: %q %v", out, err)
		}
	})

	t.Run("signal-parent-refused-outside-child", func(t *testing.T) {
		var c workflow.Client
		var got error
		wf := workflow.Workflow{Name: "esc-orphan", Steps: []workflow.Step{{Name: "work", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			got = workflow.SignalParent(ctx, c, "escalation", nil)
			return nil, nil
		}}}}
		c = h.Start(t, nil, wf)
		needCaps(t, c, workflow.CapResult)
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "esc-orphan"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := flowAwait(t, c, ref); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(got, workflow.ErrInvalid) || !workflow.IsPermanent(got) {
			t.Fatalf("SignalParent from a run without a parent: %v", got)
		}
	})

	t.Run("handlers-between-steps", func(t *testing.T) {
		g := newSigGate()
		defer g.open()
		var log sigLog
		var c workflow.Client
		plugin := workflow.OnSignals(workflow.SignalHandlers{Workflow: "esc-handled", On: map[string]workflow.SignalHandler{
			"note": func(ctx context.Context, s workflow.RunSignal) error {
				info, _ := workflow.StepInfoFrom(ctx)
				log.add(info.Step + ":" + string(s.Payload))
				return nil
			},
		}})
		wf := workflow.Workflow{Name: "esc-handled", Steps: []workflow.Step{
			{Name: "first", Run: func(ctx context.Context, _ []byte) ([]byte, error) { return nil, g.wait(ctx) }},
			{Name: "second", After: []string{"first"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				other, err := c.(workflow.RunSignalReader).PendingSignals(ctx, info.RunID, []string{"other"}, 0)
				if err != nil {
					return nil, err
				}
				return json.Marshal(map[string]any{"seen": log.get(), "other": len(other)})
			}},
		}}
		c = h.Start(t, []workflow.Plugin{plugin}, wf)
		if !workflow.Supports(c, workflow.OpRunSignals) {
			t.Skip("driver has no RunSignalReader")
		}
		needCaps(t, c, workflow.CapResult)
		ctx := context.Background()
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "esc-handled"})
		if err != nil {
			t.Fatal(err)
		}
		g.entered(t)
		for _, s := range []struct{ name, payload string }{{"note", "n1"}, {"other", "o1"}, {"note", "n2"}} {
			if err := workflow.SignalRun(ctx, c, ref, s.name, []byte(s.payload)); err != nil {
				t.Fatal(err)
			}
		}
		g.open()
		out, err := flowAwait(t, c, ref)
		if err != nil {
			t.Fatal(err)
		}
		if want := `{"other":1,"seen":["second:n1","second:n2"]}`; string(out) != want {
			t.Fatalf("second step saw %s, want %s (handlers before the step, in send order; other names left pending)", out, want)
		}
		if got := log.get(); len(got) != 2 {
			t.Fatalf("handled %q, want each signal once", got)
		}
	})

	t.Run("escalate-while-joining", func(t *testing.T) {
		release := newSigGate()
		defer release.open()
		var log sigLog
		var c workflow.Client
		plugin := workflow.OnSignals(workflow.SignalHandlers{Workflow: "esc-joiner", On: map[string]workflow.SignalHandler{
			"escalation": func(_ context.Context, s workflow.RunSignal) error { log.add(string(s.Payload)); return nil },
		}})
		child := workflow.Workflow{Name: "esc-kid", Steps: []workflow.Step{{Name: "work", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if err := workflow.SignalParent(ctx, c, "escalation", in); err != nil {
				return nil, err
			}
			return in, release.wait(ctx) // the child goes on after escalating
		}}}}
		parent := workflow.Workflow{Name: "esc-joiner", Steps: []workflow.Step{{Name: "join", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			res, err := workflow.JoinChildren(ctx, c, "kids", []workflow.Job{
				{Workflow: "esc-kid", Payload: []byte(`"k1"`)}, {Workflow: "esc-kid", Payload: []byte(`"k2"`)},
			}, workflow.JoinSpec{Poll: 20 * time.Millisecond})
			if err != nil {
				return nil, err
			}
			return json.Marshal(len(res.Outputs))
		}}}}
		c = h.Start(t, []workflow.Plugin{plugin}, child, parent)
		if !workflow.Supports(c, workflow.OpRunSignals) {
			t.Skip("driver has no RunSignalReader")
		}
		needCaps(t, c, workflow.CapSnooze, workflow.CapResult, workflow.CapIdempotencyKey)
		ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "esc-joiner"})
		if err != nil {
			t.Fatal(err)
		}
		got := waitLog(t, &log, 2) // handled while both children still run
		slices.Sort(got)
		if fmt.Sprint(got) != `["k1" "k2"]` {
			t.Fatalf("escalations handled: %q", got)
		}
		if st, err := c.Status(context.Background(), ref); err != nil || st.IsTerminal() {
			t.Fatalf("parent ended before its children: %s %v", st, err)
		}
		release.open()
		if out, err := flowAwait(t, c, ref); err != nil || string(out) != "2" {
			t.Fatalf("parent: %q %v", out, err)
		}
		if n := len(log.get()); n != 2 {
			t.Fatalf("escalations handled %d times, want 2 (each once)", n)
		}
	})

	t.Run("handler-error-retries", func(t *testing.T) {
		g := newSigGate()
		defer g.open()
		var calls, bodies sigLog
		plugin := workflow.OnSignals(workflow.SignalHandlers{Workflow: "esc-retry", On: map[string]workflow.SignalHandler{
			"flaky": func(ctx context.Context, s workflow.RunSignal) error {
				calls.add(s.ID)
				if len(calls.get()) == 1 {
					return errors.New("handler not ready")
				}
				return nil
			},
		}})
		wf := workflow.Workflow{Name: "esc-retry", Steps: []workflow.Step{
			{Name: "first", Run: func(ctx context.Context, _ []byte) ([]byte, error) { return nil, g.wait(ctx) }},
			{Name: "second", After: []string{"first"}, Retry: &workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: 10 * time.Millisecond}},
				Run: func(context.Context, []byte) ([]byte, error) { bodies.add("ran"); return []byte(`"ok"`), nil }},
		}}
		c := h.Start(t, []workflow.Plugin{plugin}, wf)
		if !workflow.Supports(c, workflow.OpRunSignals) {
			t.Skip("driver has no RunSignalReader")
		}
		needCaps(t, c, workflow.CapResult)
		ctx := context.Background()
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "esc-retry"})
		if err != nil {
			t.Fatal(err)
		}
		g.entered(t)
		if err := workflow.SignalRun(ctx, c, ref, "flaky", nil); err != nil {
			t.Fatal(err)
		}
		g.open()
		if out, err := flowAwait(t, c, ref); err != nil || string(out) != `"ok"` {
			t.Fatalf("run: %q %v", out, err)
		}
		got := calls.get()
		if len(got) != 2 || got[0] != got[1] {
			t.Fatalf("handler calls %q: want the same signal twice (failed, then retried with the step)", got)
		}
		if n := len(bodies.get()); n != 1 {
			t.Fatalf("step body ran %d times, want 1 (the failed handler stops the attempt before it)", n)
		}
	})

	t.Run("reader-contract", func(t *testing.T) {
		g := newSigGate()
		defer g.open()
		wf := workflow.Workflow{Name: "esc-reader", Steps: []workflow.Step{
			{Name: "hold", Run: func(ctx context.Context, _ []byte) ([]byte, error) { return nil, g.wait(ctx) }},
		}}
		c := h.Start(t, nil, wf)
		r, ok := c.(workflow.RunSignalReader)
		if !ok || !workflow.Supports(c, workflow.OpRunSignals) {
			t.Skip("driver has no RunSignalReader")
		}
		ctx := context.Background()
		a, err := c.Enqueue(ctx, workflow.Job{Workflow: "esc-reader"})
		if err != nil {
			t.Fatal(err)
		}
		b, err := c.Enqueue(ctx, workflow.Job{Workflow: "esc-reader"})
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{"1", "2", "3"} {
			if err := workflow.SignalRun(ctx, c, a, "m", []byte(p)); err != nil {
				t.Fatal(err)
			}
		}
		if err := workflow.SignalRun(ctx, c, b, "m", []byte("b")); err != nil {
			t.Fatal(err)
		}
		if err := c.Signal(ctx, workflow.Event{Name: "m", Key: "not-a-run", Payload: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		all, err := r.PendingSignals(ctx, a.RunID, []string{"m", "absent"}, 0)
		if err != nil || len(all) != 3 || string(all[0].Payload) != "1" || string(all[2].Payload) != "3" || all[0].Name != "m" {
			t.Fatalf("pending of run a: %+v %v; want its three, oldest first", all, err)
		}
		two, err := r.PendingSignals(ctx, a.RunID, []string{"m"}, 2)
		if err != nil || len(two) != 2 || two[1].ID != all[1].ID {
			t.Fatalf("limit 2: %+v %v", two, err)
		}
		if took, err := r.TakeSignal(ctx, a.RunID, all[0].ID); err != nil || !took {
			t.Fatalf("first take: %v %v", took, err)
		}
		if took, err := r.TakeSignal(ctx, a.RunID, all[0].ID); err != nil || took {
			t.Fatalf("second take of one signal: %v %v; want false, nil", took, err)
		}
		if _, err := r.TakeSignal(ctx, b.RunID, all[1].ID); !errors.Is(err, workflow.ErrNotFound) {
			t.Fatalf("run b taking run a's signal: %v; want ErrNotFound", err)
		}
		if _, err := r.TakeSignal(ctx, a.RunID, "999999999"); !errors.Is(err, workflow.ErrNotFound) {
			t.Fatalf("unknown id: %v; want ErrNotFound", err)
		}
		left, err := r.PendingSignals(ctx, a.RunID, []string{"m"}, 0)
		if err != nil || len(left) != 2 || left[0].ID != all[1].ID {
			t.Fatalf("pending after a take: %+v %v", left, err)
		}
		if bs, err := r.PendingSignals(ctx, b.RunID, []string{"m"}, 0); err != nil || len(bs) != 1 || string(bs[0].Payload) != "b" {
			t.Fatalf("pending of run b: %+v %v", bs, err)
		}
		g.open()
		flowStatus(t, c, a, workflow.StatusSucceeded)
		if done, err := r.PendingSignals(ctx, a.RunID, []string{"m"}, 0); err != nil || len(done) != 0 {
			t.Fatalf("pending of a finished run: %+v %v; want none", done, err)
		}
	})

	t.Run("by-run-helpers", func(t *testing.T) {
		promises := workflow.NewMemoryPromises()
		approval, err := workflow.ApprovalStep(workflow.ApprovalSpec{Name: "approve", OwnRun: true})
		if err != nil {
			t.Fatal(err)
		}
		approve := workflow.Workflow{Name: "esc-approve", Steps: []workflow.Step{approval,
			{Name: "echo", After: []string{"approve"}, Run: echo}}}
		race, err := workflow.Race(workflow.RaceSpec{Name: "pick", Event: "pick", OwnRun: true, Branches: []workflow.Branch{
			{Name: "left", Run: func(_ context.Context, in []byte) ([]byte, error) { return append([]byte("left:"), in...), nil }},
			{Name: "right", Run: func(_ context.Context, in []byte) ([]byte, error) { return append([]byte("right:"), in...), nil }},
		}})
		if err != nil {
			t.Fatal(err)
		}
		racer := workflow.Workflow{Name: "esc-race", Steps: race}
		updater := workflow.Workflow{Name: "esc-update", Steps: []workflow.Step{
			{Name: "ask", Wait: &workflow.WaitSpec{Event: "ask", OwnRun: true}},
			{Name: "answer", After: []string{"ask"}, Run: workflow.UpdateHandler(promises, func(_ context.Context, in []byte) ([]byte, error) {
				return append([]byte("answered "), in...), nil
			})},
		}}
		c := h.Start(t, nil, approve, racer, updater)
		needCaps(t, c, workflow.CapWaitOwnRun, workflow.CapResult)
		ctx := context.Background()
		enqueue := func(wf string) [2]workflow.RunRef {
			var refs [2]workflow.RunRef
			for i := range refs {
				ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf})
				if err != nil {
					t.Fatal(err)
				}
				flowStatus(t, c, ref, workflow.StatusWaiting)
				refs[i] = ref
			}
			return refs
		}

		ap := enqueue("esc-approve")
		if err := workflow.ApproveRun(ctx, c, ap[0], "ana", "ok"); err != nil {
			t.Fatal(err)
		}
		if err := workflow.RejectRun(ctx, c, ap[1], "bo", "no"); err != nil {
			t.Fatal(err)
		}
		for i, want := range []bool{true, false} {
			out, err := flowAwait(t, c, ap[i])
			var d workflow.Decision
			if err != nil || json.Unmarshal(out, &d) != nil || d.Approved != want {
				t.Fatalf("approval %d: %s %v; want approved=%v", i, out, err, want)
			}
		}

		rc := enqueue("esc-race")
		if err := workflow.SignalChoiceRun(ctx, c, rc[1], "pick", "right", []byte("b")); err != nil {
			t.Fatal(err)
		}
		if err := workflow.SignalChoiceRun(ctx, c, rc[0], "pick", "left", []byte("a")); err != nil {
			t.Fatal(err)
		}
		for i, want := range []string{"left:a", "right:b"} {
			if out, err := flowAwait(t, c, rc[i]); err != nil || string(out) != want {
				t.Fatalf("race %d: %q %v; want %q", i, out, err, want)
			}
		}

		up := enqueue("esc-update")
		uctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		out, err := workflow.UpdateRun(uctx, c, promises, up[1], "ask", []byte("q1"), workflow.AwaitOptions{Poll: 5 * time.Millisecond})
		if err != nil || string(out) != "answered q1" {
			t.Fatalf("UpdateRun: %q %v", out, err)
		}
		time.Sleep(50 * time.Millisecond)
		if st, err := c.Status(ctx, up[0]); err != nil || st != workflow.StatusWaiting {
			t.Fatalf("the other run took the update: %s %v", st, err)
		}
	})

	t.Run("handler-interrupts", func(t *testing.T) {
		g := newSigGate()
		defer g.open()
		var bodies sigLog
		plugin := workflow.OnSignals(workflow.SignalHandlers{Workflow: "esc-stop", On: map[string]workflow.SignalHandler{
			"abort": func(context.Context, workflow.RunSignal) error {
				return workflow.Permanent(errors.New("aborted by signal"))
			},
		}})
		wf := workflow.Workflow{Name: "esc-stop", Steps: []workflow.Step{
			{Name: "first", Run: func(ctx context.Context, _ []byte) ([]byte, error) { return nil, g.wait(ctx) }},
			{Name: "second", After: []string{"first"}, Run: func(context.Context, []byte) ([]byte, error) { bodies.add("ran"); return nil, nil }},
		}}
		c := h.Start(t, []workflow.Plugin{plugin}, wf)
		if !workflow.Supports(c, workflow.OpRunSignals) {
			t.Skip("driver has no RunSignalReader")
		}
		ctx := context.Background()
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "esc-stop"})
		if err != nil {
			t.Fatal(err)
		}
		g.entered(t)
		if err := workflow.SignalRun(ctx, c, ref, "abort", nil); err != nil {
			t.Fatal(err)
		}
		g.open()
		flowStatus(t, c, ref, workflow.StatusFailed)
		if n := len(bodies.get()); n != 0 {
			t.Fatalf("the step after an interrupting handler ran %d times", n)
		}
	})
}
