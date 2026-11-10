package recorder_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/recordertest"
	"github.com/ubgo/workflow/workflowtest"
)

var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// TestMemoryStoreConformance holds MemoryStore to the Store contract.
func TestMemoryStoreConformance(t *testing.T) {
	recordertest.RunStore(t, func(t *testing.T, cfg recordertest.StoreConfig) recorder.Store {
		s, err := recorder.NewMemoryStore(recorder.WithMemoryClock(cfg.Clock), recorder.WithMemoryRetention(cfg.Retention))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

// TestRecorderOnMemoryDriver runs the recorder driver suite on the memory
// driver with a fake clock.
func TestRecorderOnMemoryDriver(t *testing.T) {
	recordertest.RunDriver(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		return workflowtest.Instance{Client: memory.New(memory.WithClock(clk)), Clock: clk}
	}})
}

// TestRecorderOnMemoryDriverRealTime runs it once more on real time, the
// way an engine runs it.
func TestRecorderOnMemoryDriverRealTime(t *testing.T) {
	recordertest.RunDriver(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		return workflowtest.Instance{Client: memory.New()}
	}, Wait: 20 * time.Second, Poll: 5 * time.Millisecond})
}

func TestNewRefusals(t *testing.T) {
	store := mustStore(t)
	cases := map[string]recorder.Option{
		"nil clock":        recorder.WithClock(nil),
		"negative payload": recorder.WithMaxPayload(-1),
		"negative history": recorder.WithErrorHistory(-1),
		"bad source":       recorder.WithDefaultSource("Bad Source"),
		"nil on error":     recorder.WithOnError(nil),
		"zero timeout":     recorder.WithWriteTimeout(0),
		"empty name":       recorder.WithName(""),
	}
	for name, opt := range cases {
		if _, err := recorder.New(store, opt); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
	if _, err := recorder.New(nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("nil store: %v", err)
	}
	rec, err := recorder.New(store, nil, recorder.WithName("audit"))
	if err != nil {
		t.Fatalf("a nil option must be skipped: %v", err)
	}
	if rec.Plugin().Name() != "audit" || rec.Store() != recorder.Store(store) {
		t.Fatalf("name %q", rec.Plugin().Name())
	}
}

func mustStore(t *testing.T) *recorder.MemoryStore {
	t.Helper()
	s, err := recorder.NewMemoryStore()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// failingStore fails or panics on every write; reads go to a MemoryStore.
type failingStore struct {
	*recorder.MemoryStore
	panics bool
}

var errStoreDown = errors.New("store down")

func (f failingStore) fail() error {
	if f.panics {
		panic("store exploded")
	}
	return errStoreDown
}
func (f failingStore) UpsertRun(context.Context, recorder.RunWrite) error  { return f.fail() }
func (f failingStore) StartStep(context.Context, recorder.StepStart) error { return f.fail() }
func (f failingStore) EndStep(context.Context, recorder.StepEnd) error     { return f.fail() }
func (f failingStore) AddEvent(context.Context, recorder.EventWrite) error { return f.fail() }
func (f failingStore) FinishRun(context.Context, recorder.Finish) error    { return f.fail() }
func (f failingStore) SetOutcome(context.Context, recorder.OutcomeWrite) error {
	return errStoreDown
}

// TestStoreFailureNeverFailsARun: every write fails (or panics), the run
// still succeeds, each failure reaches OnError, and SetOutcome's own error
// goes back to the step.
func TestStoreFailureNeverFailsARun(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "errors", true: "panics"}[panics], func(t *testing.T) {
			var mu sync.Mutex
			ops := map[recorder.WriteOp]int{}
			rec, err := recorder.New(failingStore{MemoryStore: mustStore(t), panics: panics}, recorder.WithOnError(func(_ context.Context, op recorder.WriteOp, _ workflow.RunRef, err error) {
				mu.Lock()
				defer mu.Unlock()
				ops[op]++
				if !panics && !errors.Is(err, errStoreDown) {
					t.Errorf("op %s: %v", op, err)
				}
			}))
			if err != nil {
				t.Fatal(err)
			}
			var outcomeErr error
			c := startClient(t, rec, workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				workflow.RecordEvent(ctx, "e")
				outcomeErr = workflow.SetOutcome(ctx, workflow.OutcomeSuccess, "")
				return in, nil
			}}}})
			ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w"})
			if err != nil {
				t.Fatalf("enqueue failed because the store did: %v", err)
			}
			st, err := workflow.Await(context.Background(), c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
			if err != nil || st != workflow.StatusSucceeded {
				t.Fatalf("run = %q %v", st, err)
			}
			if !errors.Is(outcomeErr, errStoreDown) {
				t.Fatalf("SetOutcome = %v, want the store's error", outcomeErr)
			}
			waitFor(t, "every write to fail", func() bool {
				mu.Lock()
				defer mu.Unlock()
				return ops[recorder.OpUpsertRun] == 1 && ops[recorder.OpStartStep] == 1 && ops[recorder.OpAddEvent] == 1 &&
					ops[recorder.OpEndStep] == 1 && ops[recorder.OpFinishRun] == 1
			})
		})
	}
}

// startClient applies rec to a fresh memory client, registers wfs and
// starts it.
func startClient(t *testing.T, rec *recorder.Recorder, wfs ...workflow.Workflow) workflow.Client {
	t.Helper()
	c, err := workflow.Use(memory.New(), rec.Plugin())
	if err != nil {
		t.Fatal(err)
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestStepCancelledStillRecordsEnd: a step whose ctx was cancelled (a
// timeout) still records its end, because writes are detached.
func TestStepCancelledStillRecordsEnd(t *testing.T) {
	store := mustStore(t)
	rec, err := recorder.New(store)
	if err != nil {
		t.Fatal(err)
	}
	wf := workflow.Workflow{Name: "slow", Timeout: 20 * time.Millisecond, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}}}
	c := startClient(t, rec, wf)
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "slow"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a failed record", func() bool {
		r, err := store.Run(context.Background(), ref)
		return err == nil && r.Status == workflow.StatusFailed && len(r.Steps) == 1 && r.Steps[0].Status == recorder.StepFailed
	})
}

// TestSnoozeRecordedAsSnoozed checks a snoozed attempt is recorded as such
// and the next try reuses its record.
func TestSnoozeRecordedAsSnoozed(t *testing.T) {
	store := mustStore(t)
	rec, err := recorder.New(store)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	tries := 0
	wf := workflow.Workflow{Name: "snooze", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		tries++
		if tries == 1 {
			return nil, workflow.Snooze(time.Millisecond)
		}
		return []byte("ok"), nil
	}}}}
	c := startClient(t, rec, wf)
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "snooze"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a succeeded record", func() bool {
		r, err := store.Run(context.Background(), ref)
		return err == nil && r.Status == workflow.StatusSucceeded
	})
	r, _ := store.Run(context.Background(), ref)
	if len(r.Steps) != 1 || r.Steps[0].Status != recorder.StepSucceeded || r.Attempts != 1 || len(r.Errors) != 0 {
		t.Fatalf("record = attempts %d errors %v steps %+v", r.Attempts, r.Errors, r.Steps)
	}
}

// TestHandlerOutsideDriverPassesThrough: a ctx without StepInfo is not
// recorded and the handler still runs.
func TestHandlerOutsideDriverPassesThrough(t *testing.T) {
	store := mustStore(t)
	rec, _ := recorder.New(store)
	h := rec.StepMiddleware()(func(_ context.Context, in []byte) ([]byte, error) { return in, nil })
	out, err := h(context.Background(), []byte("x"))
	if err != nil || string(out) != "x" {
		t.Fatalf("out %q err %v", out, err)
	}
	if p, _ := store.Records(context.Background(), recorder.Query{}); len(p.Runs) != 0 {
		t.Fatalf("recorded %d runs", len(p.Runs))
	}
}

// TestEnqueueErrorRecordsNothing: a refused enqueue leaves no record and
// the error passes through unchanged.
func TestEnqueueErrorRecordsNothing(t *testing.T) {
	store := mustStore(t)
	rec, _ := recorder.New(store)
	boom := errors.New("refused")
	enq := rec.EnqueueMiddleware()(func(context.Context, workflow.Job) (workflow.RunRef, error) { return workflow.RunRef{}, boom })
	if _, err := enq(context.Background(), workflow.Job{Workflow: "w"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if p, _ := store.Records(context.Background(), recorder.Query{}); len(p.Runs) != 0 {
		t.Fatalf("recorded %d runs", len(p.Runs))
	}
}

// TestMaxPayloadZeroKeepsSizesOnly is the privacy mode.
func TestMaxPayloadZeroKeepsSizesOnly(t *testing.T) {
	store := mustStore(t)
	rec, _ := recorder.New(store, recorder.WithMaxPayload(0), recorder.WithStepBodies(false))
	c := startClient(t, rec, workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return []byte("out"), nil }}}})
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w", Payload: []byte("secret")})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a finished record", func() bool {
		r, err := store.Run(context.Background(), ref)
		return err == nil && r.Status.IsTerminal()
	})
	r, _ := store.Run(context.Background(), ref)
	if r.Payload != nil || r.PayloadSize != 6 || r.Steps[0].Input != nil || r.Steps[0].Output != nil || r.Steps[0].InputSize != 6 || r.Steps[0].OutputSize != 3 {
		t.Fatalf("bodies kept: %+v / %+v", r.RunInfo, r.Steps[0])
	}
}

// resultClient is a client whose ResultReader fails, to show a result read
// error reaches OnError while the end is still recorded.
type resultFailer struct{ err error }

func (r resultFailer) Result(context.Context, workflow.RunRef) ([]byte, error) { return nil, r.err }

func TestResultReadErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		wantOp bool
	}{
		"failure reported":     {errors.New("engine down"), true},
		"unsupported is quiet": {workflow.Unsupported("Result", "none"), false},
	} {
		t.Run(name, func(t *testing.T) {
			store := mustStore(t)
			var got []recorder.WriteOp
			rec, _ := recorder.New(store, recorder.WithResultReader(resultFailer{tc.err}), recorder.WithOnError(func(_ context.Context, op recorder.WriteOp, _ workflow.RunRef, _ error) {
				got = append(got, op)
			}))
			ref := workflow.RunRef{RunID: "r1"}
			rec.OnRunFinished(context.Background(), workflow.RunEvent{Ref: ref, Workflow: "w", Status: workflow.StatusSucceeded})
			r, err := store.Run(context.Background(), ref)
			if err != nil || r.Status != workflow.StatusSucceeded || r.FinishedAt.IsZero() || r.Output != nil {
				t.Fatalf("record %+v err %v", r, err)
			}
			if (len(got) == 1 && got[0] == recorder.OpReadResult) != tc.wantOp {
				t.Fatalf("ops = %v", got)
			}
		})
	}
}

// TestRunFinishedFailureRecordsError: a failed run end carries its error.
func TestRunFinishedFailureRecordsError(t *testing.T) {
	store := mustStore(t)
	clk := clock.NewFake(start)
	rec, _ := recorder.New(store, recorder.WithClock(clk))
	ref := workflow.RunRef{RunID: "r1", ScheduledID: "s1"}
	rec.OnRunFinished(context.Background(), workflow.RunEvent{Ref: ref, Workflow: "w", Status: workflow.StatusFailed, Err: errors.New("bad"), Step: "s", FinishedAt: start.Add(time.Minute)})
	r, err := store.Run(context.Background(), workflow.RunRef{ScheduledID: "s1"})
	if err != nil || r.Error != "bad" || r.FailedStep != "s" || !r.FinishedAt.Equal(start.Add(time.Minute)) || r.Outcome != workflow.OutcomeError {
		t.Fatalf("record %+v err %v", r, err)
	}
}

func TestRecordEventLevelAndRedactedAttrsReachStore(t *testing.T) {
	store := mustStore(t)
	red, _ := recorder.JSONPaths("secret")
	rec, _ := recorder.New(store, recorder.WithRedactor(red))
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "w", Step: "s", RunID: "r1", Attempt: 1})
	h := rec.StepMiddleware()(func(ctx context.Context, _ []byte) ([]byte, error) {
		workflow.RecordEventLevel(ctx, slog.LevelError, "bad", slog.String("secret", "x"), slog.String("ok", "y"))
		return nil, nil
	})
	if _, err := h(ctx, nil); err != nil {
		t.Fatal(err)
	}
	r, err := store.Run(context.Background(), workflow.RunRef{RunID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	e := r.Steps[0].Events[0]
	if e.Level != slog.LevelError || e.Attrs[0].Value != recorder.Redacted || e.Attrs[1].Value != "y" || e.At.IsZero() {
		t.Fatalf("event = %+v", e)
	}
	if r.Steps[0].Status != recorder.StepSucceeded {
		t.Fatalf("step = %+v", r.Steps[0])
	}
}

// noNotifier hides every optional interface of the client it wraps, the
// way an enqueue-only client lacks a run-finished hook.
type noNotifier struct{ workflow.Client }

// TestEnqueuePluginOnAClientWithoutRunNotifier: Plugin is refused on a
// client without a run-finished hook; EnqueuePlugin is accepted there and
// records the enqueue, and a worker client with Plugin over the same store
// completes the record.
func TestEnqueuePluginOnAClientWithoutRunNotifier(t *testing.T) {
	ctx := context.Background()
	store := mustStore(t)
	rec, _ := recorder.New(store)
	web := noNotifier{memory.New()}
	if _, err := workflow.Use(web, rec.Plugin()); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Plugin on a client without RunNotifier: %v", err)
	}
	wc, err := workflow.Use(web, rec.EnqueuePlugin())
	if err != nil {
		t.Fatalf("EnqueuePlugin: %v", err)
	}
	if _, ok := rec.EnqueuePlugin().(workflow.StepMiddlewareProvider); ok {
		t.Fatal("EnqueuePlugin contributes step middleware")
	}
	if err := wc.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := wc.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	ref, err := wc.Enqueue(ctx, workflow.WithSource(workflow.Job{Workflow: "w", Payload: []byte("p")}, "api", "req-1"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Run(ctx, ref)
	if err != nil || r.Source != "api" || string(r.Payload) != "p" {
		t.Fatalf("enqueue record = %+v, %v", r, err)
	}
	// The steps still ran (the memory client is both nodes here) but no
	// step middleware recorded them, and no end was observed.
	waitFor(t, "the run to end", func() bool {
		st, err := wc.Status(ctx, ref)
		return err == nil && st.IsTerminal()
	})
	if r, _ = store.Run(ctx, ref); len(r.Steps) != 0 || r.Status.IsTerminal() {
		t.Fatalf("enqueue-only plugin recorded more than the enqueue: %+v", r)
	}
}
