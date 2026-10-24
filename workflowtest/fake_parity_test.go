package workflowtest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/memory"
)

// spyErrs runs assert against a tbSpy and returns what it reported.
func spyErrs(assert func(testing.TB)) []string {
	spy := &tbSpy{}
	assert(spy)
	return spy.errs
}

func TestFakeBatches(t *testing.T) {
	ctx := context.Background()
	f := startedFake(t, nil, oneStep("resize", noopHandler), oneStep("email", noopHandler))
	m := batch.New(f, batch.NewMemoryStore(), batch.WithManualCallbackRetry())

	f.AssertNothingBatched(t)
	if _, err := f.Enqueue(ctx, workflow.Job{Workflow: "email"}); err != nil {
		t.Fatal(err)
	}
	f.AssertNothingBatched(t) // a plain job is not a batch

	id, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{
		{Workflow: "resize", Payload: []byte("a.png")},
		{Workflow: "resize", Payload: []byte("b.png")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "email"}}}); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(ctx, id, workflow.Job{Workflow: "resize", Payload: []byte("c.png")}); err != nil {
		t.Fatal(err)
	}

	f.AssertBatchCount(t, 2)
	f.AssertBatched(t, BatchSize(3), BatchHasJob("resize", HasPayload([]byte("c.png"))))
	f.AssertBatched(t, BatchHasJob("email"))
	bs := f.Batches()
	if bs[0].ID != id || len(bs[0].Jobs) != 3 || string(bs[0].Jobs[2].Payload) != "c.png" {
		t.Errorf("Batches()[0] = %+v", bs[0])
	}

	errs := spyErrs(func(tb testing.TB) { f.AssertBatched(tb, BatchSize(5)) })
	if len(errs) != 1 || !strings.Contains(errs[0], "no batch with 5 jobs") || !strings.Contains(errs[0], "resize x3") {
		t.Errorf("AssertBatched failure = %q", errs)
	}
	if errs := spyErrs(func(tb testing.TB) { f.AssertBatchCount(tb, 1) }); len(errs) != 1 {
		t.Errorf("AssertBatchCount(1) with 2 batches reported %q", errs)
	}
	if errs := spyErrs(f.AssertNothingBatched); len(errs) != 1 {
		t.Errorf("AssertNothingBatched with batches reported %q", errs)
	}
	if errs := spyErrs(func(tb testing.TB) { f.AssertBatched(tb, BatchWhere("never", func(FakeBatch) bool { return false })) }); len(errs) != 1 {
		t.Errorf("a matcher that matches nothing passed")
	}
}

func chain(name string, steps ...string) workflow.Workflow {
	wf := workflow.Workflow{Name: name}
	for i, s := range steps {
		st := workflow.Step{Name: s, Run: noopHandler}
		if i > 0 {
			st.After = []string{steps[i-1]}
		}
		wf.Steps = append(wf.Steps, st)
	}
	return wf
}

func TestFakeAssertChained(t *testing.T) {
	fan := workflow.Workflow{Name: "fan", Steps: []workflow.Step{
		{Name: "a", Run: noopHandler},
		{Name: "b", Run: noopHandler, After: []string{"a"}},
		{Name: "c", Run: noopHandler, After: []string{"a"}},
	}}
	f := startedFake(t, nil, chain("order", "charge", "ship", "email"), fan)
	if errs := spyErrs(func(tb testing.TB) { f.AssertChained(tb, "order", "charge", "ship", "email") }); len(errs) != 1 || !strings.Contains(errs[0], "not enqueued") {
		t.Errorf("a chain never enqueued: %q", errs)
	}
	for _, wf := range []string{"order", "fan"} {
		if _, err := f.Enqueue(context.Background(), workflow.Job{Workflow: wf}); err != nil {
			t.Fatal(err)
		}
	}
	f.AssertChained(t, "order", "charge", "ship", "email")
	for name, assert := range map[string]func(testing.TB){
		"order":        func(tb testing.TB) { f.AssertChained(tb, "order", "charge", "email", "ship") },
		"missing step": func(tb testing.TB) { f.AssertChained(tb, "order", "charge", "ship") },
		"a fan-out":    func(tb testing.TB) { f.AssertChained(tb, "fan", "a", "b", "c") },
		"unregistered": func(tb testing.TB) { f.AssertChained(tb, "nope", "a") },
	} {
		if errs := spyErrs(assert); len(errs) != 1 {
			t.Errorf("%s: AssertChained reported %q, want one error", name, errs)
		}
	}
	if wf, ok := f.Workflow("order"); !ok || len(wf.Steps) != 3 {
		t.Errorf("Workflow(order) = %+v, %t", wf, ok)
	}
}

// startedMemory is a memory client with workflow "real", whose step
// sends its payload on got, and "mail", a no-op, registered through f.
func passEnv(t *testing.T, opt func(workflow.Client) FakeOption) (*Fake, chan string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	inner := memory.New()
	got := make(chan string, 4)
	f := NewFake(opt(inner))
	if err := f.Register(oneStep("real", func(_ context.Context, in []byte) ([]byte, error) { got <- string(in); return nil, nil })); err != nil {
		t.Fatal(err)
	}
	if err := f.Register(oneStep("mail", noopHandler)); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return f, got
}

func TestFakePassThrough(t *testing.T) {
	for name, opt := range map[string]func(workflow.Client) FakeOption{
		"FakeExcept": func(c workflow.Client) FakeOption { return FakeExcept(c, "real") },
		"FakeOnly":   func(c workflow.Client) FakeOption { return FakeOnly(c, "mail") },
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f, got := passEnv(t, opt)
			ref, err := f.Enqueue(ctx, workflow.Job{Workflow: "real", Payload: []byte("hi")})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case p := <-got:
				if p != "hi" {
					t.Errorf("real ran with %q", p)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the passed-through workflow never ran")
			}
			if _, err := f.Status(ctx, ref); err != nil {
				t.Errorf("Status of a passed-through run = %v", err)
			}
			if _, err := f.Enqueue(ctx, workflow.Job{Workflow: "mail"}); err != nil {
				t.Fatal(err)
			}
			f.AssertEnqueuedTimes(t, "mail", 1)
			f.AssertNotEnqueued(t, "real") // passed through, not recorded
			if _, ok := f.Workflow("real"); !ok {
				t.Error("Workflow(real) not found")
			}
			if err := f.Cancel(ctx, workflow.RunRef{RunID: "unknown"}); !errors.Is(err, workflow.ErrNotFound) {
				t.Errorf("Cancel of an unknown ref = %v, want the inner client's ErrNotFound", err)
			}
		})
	}
}

func TestRunStep(t *testing.T) {
	ok := RunStep(func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		return append(in, []byte(info.Step)...), nil
	}, []byte("in:"))
	ok.AssertSucceeded(t)
	if string(ok.Output) != "in:step" {
		t.Errorf("output = %q", ok.Output)
	}

	released := RunStep(func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.RetryAfter(errors.New("429"), 30*time.Second)
	}, nil)
	released.AssertReleased(t, 30*time.Second)
	if !released.Failed() || released.Permanent() {
		t.Errorf("released outcome: failed=%t permanent=%t", released.Failed(), released.Permanent())
	}

	perm := RunStep(func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errors.New("bad input")) }, nil)
	perm.AssertPermanent(t)
	if _, ok := perm.Released(); ok {
		t.Error("a permanent failure reported as released")
	}
	both := RunStep(func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.RetryAfter(workflow.Permanent(errors.New("gone")), time.Minute)
	}, nil)
	if _, ok := both.Released(); ok || !both.Permanent() {
		t.Error("RetryAfter(Permanent(err)) must be permanent, not released: Permanent wins")
	}
	plain := RunStep(func(context.Context, []byte) ([]byte, error) { return nil, errors.New("flaky") }, nil)
	plain.AssertFailed(t)

	for name, assert := range map[string]func(testing.TB){
		"succeeded on failure":   func(tb testing.TB) { plain.AssertSucceeded(tb) },
		"failed on release":      func(tb testing.TB) { released.AssertFailed(tb) },
		"failed on permanent":    func(tb testing.TB) { perm.AssertFailed(tb) },
		"permanent on ordinary":  func(tb testing.TB) { plain.AssertPermanent(tb) },
		"released wrong delay":   func(tb testing.TB) { released.AssertReleased(tb, time.Second) },
		"released on success":    func(tb testing.TB) { ok.AssertReleased(tb, 0) },
		"released on a no-delay": func(tb testing.TB) { plain.AssertReleased(tb, 0) },
	} {
		if errs := spyErrs(assert); len(errs) != 1 {
			t.Errorf("%s: reported %q, want one error", name, errs)
		}
	}

	var order []string
	mw := func(tag string) workflow.Middleware {
		return func(next workflow.Handler) workflow.Handler {
			return func(ctx context.Context, in []byte) ([]byte, error) {
				order = append(order, tag)
				return next(ctx, in)
			}
		}
	}
	out := RunStep(func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		return []byte(info.Workflow), nil
	}, nil, StepMiddleware(mw("outer"), mw("inner")), StepInfoOf(workflow.StepInfo{Workflow: "wf", Attempt: 3}))
	if string(out.Output) != "wf" || strings.Join(order, ",") != "outer,inner" {
		t.Errorf("output %q, middleware order %v", out.Output, order)
	}
}

func TestRunStepDiscard(t *testing.T) {
	o := RunStep(func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.Discard(workflow.RetryAfter(errors.New("order gone"), time.Second))
	}, nil)
	o.AssertDiscarded(t)
	if _, released := o.Released(); released || o.Permanent() {
		t.Error("a discard reported as released or permanent")
	}
	if errs := spyErrs(o.AssertFailed); len(errs) != 1 {
		t.Errorf("AssertFailed on a discard reported %q", errs)
	}
	if errs := spyErrs(RunStep(noopHandler, nil).AssertDiscarded); len(errs) != 1 {
		t.Errorf("AssertDiscarded on success reported %q", errs)
	}
}

// The doubles implement the optional interfaces workflow.Forward covers
// and refuse them when no real client is behind them.
var (
	_ workflow.RunQuerier     = (*Fake)(nil)
	_ workflow.Pauser         = (*Fake)(nil)
	_ workflow.RunQuerier     = (*Recorder)(nil)
	_ workflow.ScheduleLister = (*Recorder)(nil)
)

func TestDoublesForwardOptionalInterfaces(t *testing.T) {
	ctx := context.Background()
	if _, err := NewFake().Runs(ctx, workflow.RunQuery{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Fake.Runs without a real client = %v, want ErrUnsupported", err)
	}
	if _, err := NewRecorder(nil).Runs(ctx, workflow.RunQuery{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("NewRecorder(nil).Runs = %v, want ErrUnsupported", err)
	}
	inner := memory.New()
	if err := inner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRecorder(inner).Runs(ctx, workflow.RunQuery{}); err != nil {
		t.Errorf("Recorder over memory: Runs = %v, want forwarded", err)
	}
}
