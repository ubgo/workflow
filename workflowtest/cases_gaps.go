package workflowtest

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// gapGroups are the groups of schedule idempotency keys, step checkpoints,
// completing a step inside the caller's unit of work, and coalescing
// workflows.
var gapGroups = []suiteGroup{
	{"schedulekey", append(scheduleKeyCases, scheduleKeyConcurrencyCases...)},
	{"checkpoint", append(checkpointCases, checkpointConcurrencyCases...)},
	{"completeintx", append(completeInTxCases, completeInTxConcurrencyCases...)},
	{"coalesce", append(coalesceCases, coalesceConcurrencyCases...)},
}

// triggerWithOutcome triggers id and reports whether the driver answered
// with an existing run.
func (e *env) triggerWithOutcome(id workflow.ScheduleID) (workflow.RunRef, workflow.EnqueueOutcome, error) {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	ctx, out := workflow.WithEnqueueOutcome(ctx)
	ref, err := as[workflow.ScheduleTriggerer](e, "workflow.ScheduleTriggerer").TriggerSchedule(ctx, id)
	return ref, *out, err
}

// sameRef reports whether two RunRefs name one run: by RunID when both
// have one, else by ScheduledID.
func sameRef(a, b workflow.RunRef) bool {
	if !a.RunID.IsZero() && !b.RunID.IsZero() {
		return a.RunID == b.RunID
	}
	return !a.ScheduledID.IsZero() && a.ScheduledID == b.ScheduledID
}

var scheduleKeyCases = []testCase{
	{"a fixed key dedups a trigger and a hand enqueue against the running fire", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapCron, workflow.CapScheduleTrigger, workflow.CapScheduleKey))
		var c counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		e.up(wf)
		key := e.name("sync")
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: farCron, Location: time.UTC, IdempotencyKey: key})
		first := e.mustTrigger(id)
		e.eventually("the first run to start", 0, func() bool { return c.count() == 1 })
		again, out, err := e.triggerWithOutcome(id)
		if err != nil {
			e.t.Fatalf("a trigger while the key is held: %v, want the holder", err)
		}
		if !sameRef(again, first) {
			e.t.Errorf("a trigger while the key is held returned %+v, want the holder %+v", again, first)
		}
		if out.Reported && !out.Duplicate {
			e.t.Errorf("the trigger's outcome = %+v, want a duplicate", out)
		}
		byHand := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		if !sameRef(byHand, first) {
			e.t.Errorf("a hand enqueue under the schedule's key returned %+v, want the holder %+v", byHand, first)
		}
		g.open()
		e.waitStatus(first, workflow.StatusSucceeded, 0)
		next := e.mustTrigger(id)
		if sameRef(next, first) {
			e.t.Fatalf("a trigger after the holder finished returned the old run")
		}
		e.waitStatus(next, workflow.StatusSucceeded, 0)
		if got := c.count(); got != 2 {
			e.t.Errorf("the step ran %d times, want 2", got)
		}
	}},
	{"OnHeld HeldRefuse refuses a trigger while the key is held", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapCron, workflow.CapScheduleTrigger, workflow.CapScheduleKey))
		var c counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		e.up(wf)
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: farCron, Location: time.UTC,
			IdempotencyKey: e.name("sync"), OnHeld: workflow.HeldRefuse})
		first := e.mustTrigger(id)
		e.eventually("the first run to start", 0, func() bool { return c.count() == 1 })
		if _, err := e.trigger(id); !errors.Is(err, workflow.ErrDuplicate) {
			e.t.Fatalf("a refusing trigger while the key is held: %v, want ErrDuplicate", err)
		}
		g.open()
		e.waitStatus(first, workflow.StatusSucceeded, 0)
		e.waitStatus(e.mustTrigger(id), workflow.StatusSucceeded, 0)
	}},
	{"a fire-time key is computed by ExpandScheduleKey and shared with a hand enqueue", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapCron, workflow.CapScheduleTrigger, workflow.CapScheduleKey))
		e.needFake("triggering twice at one instant")
		var c counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		e.up(wf)
		tmpl := e.name("sync") + ":" + workflow.KeyFire
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: farCron, Location: time.UTC, IdempotencyKey: tmpl})
		first := e.mustTrigger(id)
		e.eventually("the first run to start", 0, func() bool { return c.count() == 1 })
		key, err := workflow.ExpandScheduleKey(tmpl, e.now(), time.UTC)
		if err != nil {
			e.t.Fatalf("ExpandScheduleKey: %v", err)
		}
		if byHand := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key}); !sameRef(byHand, first) {
			e.t.Errorf("a hand enqueue under the fire's key returned %+v, want the triggered run %+v", byHand, first)
		}
		e.clk.Advance(time.Second)
		later := e.mustTrigger(id) // a new fire time: a new key, a new run
		if sameRef(later, first) {
			e.t.Errorf("a trigger at another fire time joined the first run")
		}
		g.open()
		e.waitStatus(first, workflow.StatusSucceeded, 0)
		e.waitStatus(later, workflow.StatusSucceeded, 0)
	}},
	{"a period key makes a trigger join the tick of the same day", func(e *env) {
		needCronFiring(e)
		e.need(workflow.CapsOf(workflow.CapScheduleTrigger, workflow.CapScheduleKey))
		e.needFake("comparing a tick with a trigger on one day")
		var c counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		e.up(wf)
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: everyMinute, Location: time.UTC,
			IdempotencyKey: e.name("digest") + ":" + workflow.KeyDay})
		tick(e, &c, 1, 1)
		ref, out, err := e.triggerWithOutcome(id)
		if err != nil {
			e.t.Fatalf("a trigger on the tick's day: %v", err)
		}
		if out.Reported && !out.Duplicate {
			e.t.Errorf("a trigger on the tick's day started a new run (%+v), want the tick's run", ref)
		}
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Errorf("the step ran %d times, want 1: the trigger must join the tick's run", got)
		}
		g.open()
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
	}},
	{"a driver without CapScheduleKey refuses Schedule.IdempotencyKey", func(e *env) {
		e.need(workflow.CapCron)
		if e.c.Capabilities().Has(workflow.CapScheduleKey) {
			e.t.Skip("the driver has CapScheduleKey")
		}
		e.up(oneStep(e.name("w"), (&counter{}).echo()))
		err := workflow.CheckScheduleCaps(e.c.Capabilities(), workflow.Schedule{Workflow: e.name("w"), Cron: farCron, Location: time.UTC, IdempotencyKey: "k"})
		assertUnsupported(e.t, err, "Schedule.IdempotencyKey", workflow.CapScheduleKey)
	}},
}

// gapRetry is a retry policy with one retry after d.
func gapRetry(d time.Duration) workflow.RetryPolicy {
	return workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: d, Factor: 1}}
}

// cpLog records what each attempt of a checkpointing step loaded.
type cpLog struct {
	mu    sync.Mutex
	loads []string // "" for none
	errs  []error
}

func (l *cpLog) load(ctx context.Context) {
	b, ok, err := workflow.LoadCheckpoint(ctx)
	l.mu.Lock()
	defer l.mu.Unlock()
	if !ok {
		b = nil
	}
	l.loads = append(l.loads, string(b))
	if err != nil {
		l.errs = append(l.errs, err)
	}
}

func (l *cpLog) got() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.loads...)
}

var checkpointCases = []testCase{
	{"a checkpoint saved by one attempt is read back by the next", func(e *env) {
		e.need(workflow.CapCheckpoint)
		var l cpLog
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			l.load(ctx)
			if c.count() == 1 {
				if err := workflow.SaveCheckpoint(ctx, []byte("page-2")); err != nil {
					return nil, workflow.Permanent(err)
				}
				return nil, errBoom
			}
			return nil, nil
		})
		wf.Retry = gapRetry(e.tm.unit)
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := l.got(); len(got) != 2 || got[0] != "" || got[1] != "page-2" {
			e.t.Fatalf("the attempts loaded %q, want [\"\" \"page-2\"]", got)
		}
		if len(l.errs) > 0 {
			e.t.Fatalf("LoadCheckpoint failed: %v", l.errs)
		}
	}},
	{"the typed checkpoint round-trips through a snooze", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapCheckpoint, workflow.CapSnooze))
		type cursor struct {
			Page  int    `json:"page"`
			Token string `json:"token"`
		}
		cp := workflow.DefineCheckpoint[cursor]()
		var c counter
		var mu sync.Mutex
		var seen cursor
		var seenOK bool
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if c.count() == 1 {
				if err := cp.Save(ctx, cursor{Page: 3, Token: "t3"}); err != nil {
					return nil, workflow.Permanent(err)
				}
				return nil, workflow.Snooze(e.tm.unit)
			}
			v, ok, err := cp.Load(ctx)
			if err != nil {
				return nil, workflow.Permanent(err)
			}
			mu.Lock()
			seen, seenOK = v, ok
			mu.Unlock()
			return nil, nil
		})
		wf.Retry = *workflow.RetriesNone()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		mu.Lock()
		defer mu.Unlock()
		if !seenOK || seen != (cursor{Page: 3, Token: "t3"}) {
			e.t.Fatalf("after the snooze the step loaded %+v (ok %v), want {3 t3}", seen, seenOK)
		}
	}},
	{"checkpoints are per step and per run, and gone once the run ends", func(e *env) {
		e.need(workflow.CapCheckpoint)
		var l cpLog
		save := func(ctx context.Context, in []byte) ([]byte, error) {
			l.load(ctx)
			if err := workflow.SaveCheckpoint(ctx, []byte("mine")); err != nil {
				return nil, workflow.Permanent(err)
			}
			return in, nil
		}
		wf := workflow.Workflow{Name: e.name("w"), Steps: []workflow.Step{
			{Name: "a", Run: save},
			{Name: "b", After: []string{"a"}, Run: save},
		}}
		e.up(wf)
		first := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(first, workflow.StatusSucceeded, 0)
		second := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(second, workflow.StatusSucceeded, 0)
		for i, got := range l.got() {
			if got != "" {
				e.t.Errorf("load %d saw %q: a step must not see another step's or another run's checkpoint", i, got)
			}
		}
	}},
	{"a checkpoint past its CheckpointTTL is not returned", func(e *env) {
		e.need(workflow.CapCheckpoint)
		e.needFake("expiring a checkpoint on the clock")
		var l cpLog
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			l.load(ctx)
			if c.count() == 1 {
				if err := workflow.SaveCheckpoint(ctx, []byte("stale"), workflow.CheckpointTTL(e.tm.unit)); err != nil {
					return nil, workflow.Permanent(err)
				}
				return nil, errBoom
			}
			return nil, nil
		})
		wf.Retry = gapRetry(3 * e.tm.unit) // the retry runs after the TTL
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := l.got(); len(got) != 2 || got[1] != "" {
			e.t.Fatalf("the attempts loaded %q, want the expired checkpoint gone on the retry", got)
		}
	}},
	{"a checkpoint over the limit is refused with ErrCheckpointTooLarge", func(e *env) {
		e.need(workflow.CapCheckpoint)
		errs := make(chan error, 1)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			errs <- workflow.SaveCheckpoint(ctx, bytes.Repeat([]byte("x"), workflow.DefaultMaxCheckpoint+1))
			return nil, nil
		})
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusSucceeded, 0)
		err := <-errs
		var tl *workflow.CheckpointTooLargeError
		if !errors.Is(err, workflow.ErrCheckpointTooLarge) || !errors.As(err, &tl) || tl.Limit != workflow.DefaultMaxCheckpoint {
			e.t.Fatalf("an oversized checkpoint: %v, want a CheckpointTooLargeError at the default limit", err)
		}
	}},
	{"an attempt whose run was cancelled cannot save a checkpoint", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapCheckpoint, workflow.CapCancel))
		started := make(chan struct{})
		errs := make(chan error, 1)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			close(started)
			<-ctx.Done()
			errs <- workflow.SaveCheckpoint(ctx, []byte("late"))
			return nil, ctx.Err()
		})
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		<-started
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Cancel(ctx, ref); err != nil {
			e.t.Fatalf("Cancel: %v", err)
		}
		e.waitStatus(ref, workflow.StatusCancelled, 0)
		select {
		case err := <-errs:
			if !errors.Is(err, workflow.ErrAttemptLost) {
				e.t.Fatalf("a save after the run was cancelled: %v, want ErrAttemptLost", err)
			}
		case <-time.After(e.tm.wait):
			e.t.Fatal("the cancelled step's handler never tried its save")
		}
	}},
	{"outside a step SaveCheckpoint is unsupported", func(e *env) {
		err := workflow.SaveCheckpoint(context.Background(), []byte("x"))
		assertUnsupported(e.t, err, "SaveCheckpoint", workflow.CapCheckpoint)
	}},
}

// txCompleter returns the client's TxCompleter, skipping the subtest when
// the client does not complete in a unit of work (Supports with
// OpCompleteInTx).
func (e *env) txCompleter() workflow.TxCompleter {
	e.t.Helper()
	if !workflow.Supports(e.c, workflow.OpCompleteInTx) {
		e.t.Skip("the client does not implement workflow.TxCompleter")
	}
	return e.c.(workflow.TxCompleter)
}

// resultOf returns ref's output when the driver keeps results, and ok
// false otherwise.
func (e *env) resultOf(ref workflow.RunRef) ([]byte, bool) {
	e.t.Helper()
	r, ok := e.c.(workflow.ResultReader)
	if !ok || !e.c.Capabilities().Has(workflow.CapResult) {
		return nil, false
	}
	ctx, cancel := e.callCtx()
	defer cancel()
	out, err := r.Result(ctx, ref)
	if err != nil {
		e.t.Fatalf("Result: %v", err)
	}
	return out, true
}

var completeInTxCases = []testCase{
	{"CompleteInTx records the output and the handler's return is ignored", func(e *env) {
		tc := e.txCompleter()
		var c counter
		var ran bool
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if err := tc.CompleteInTx(ctx, []byte(`"done"`), func(context.Context) error { ran = true; return nil }); err != nil {
				return nil, workflow.Permanent(err)
			}
			return nil, errBoom // ignored: the step is already done
		})
		wf.Retry = gapRetry(e.tm.unit)
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		e.quiet()
		if !ran || c.count() != 1 {
			e.t.Fatalf("fn ran %v and the step ran %d times, want true and 1", ran, c.count())
		}
		if out, ok := e.resultOf(ref); ok && string(out) != `"done"` {
			e.t.Errorf("the run's output = %s, want the completed output", out)
		}
	}},
	{"a failing fn records nothing and the handler's own result stands", func(e *env) {
		tc := e.txCompleter()
		errs := make(chan error, 1)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			errs <- tc.CompleteInTx(ctx, []byte(`"done"`), func(context.Context) error { return errBoom })
			return []byte(`"own"`), nil
		})
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		if err := <-errs; !errors.Is(err, errBoom) {
			e.t.Fatalf("CompleteInTx with a failing fn: %v, want fn's error", err)
		}
		if out, ok := e.resultOf(ref); ok && string(out) != `"own"` {
			e.t.Errorf("the run's output = %s, want the handler's own", out)
		}
	}},
	{"a second completion and a ctx that is not a step's are ErrInvalid", func(e *env) {
		tc := e.txCompleter()
		if err := tc.CompleteInTx(context.Background(), nil, func(context.Context) error { return nil }); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Errorf("CompleteInTx outside a step: %v, want ErrInvalid", err)
		}
		errs := make(chan error, 1)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			if err := tc.CompleteInTx(ctx, nil, func(context.Context) error { return nil }); err != nil {
				return nil, workflow.Permanent(err)
			}
			errs <- tc.CompleteInTx(ctx, nil, func(context.Context) error { return nil })
			return nil, nil
		})
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusSucceeded, 0)
		if err := <-errs; !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("a second CompleteInTx: %v, want ErrInvalid", err)
		}
	}},
	{"an attempt whose run was cancelled cannot complete", func(e *env) {
		tc := e.txCompleter()
		e.need(workflow.CapCancel)
		started := make(chan struct{})
		errs := make(chan error, 1)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			close(started)
			<-ctx.Done()
			errs <- tc.CompleteInTx(context.WithoutCancel(ctx), nil, func(context.Context) error { return nil })
			return nil, ctx.Err()
		})
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		<-started
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Cancel(ctx, ref); err != nil {
			e.t.Fatalf("Cancel: %v", err)
		}
		e.waitStatus(ref, workflow.StatusCancelled, 0)
		select {
		case err := <-errs:
			if !errors.Is(err, workflow.ErrAttemptLost) {
				e.t.Fatalf("CompleteInTx after the run was cancelled: %v, want ErrAttemptLost", err)
			}
		case <-time.After(e.tm.wait):
			e.t.Fatal("the cancelled step's handler never tried to complete")
		}
	}},
}

// coalesceWorkflow is a one-step workflow with Coalesce whose handler
// records its inputs and holds the first call until g opens; failFirst
// makes that first call fail for good once released.
func coalesceWorkflow(e *env, payload workflow.CoalescePayload, g *gate, c *counter, inputs *[]string, mu *sync.Mutex, failFirst bool) workflow.Workflow {
	wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
		c.hit(in)
		mu.Lock()
		*inputs = append(*inputs, string(in))
		mu.Unlock()
		if c.count() == 1 {
			if err := g.wait(ctx); err != nil {
				return nil, err
			}
			if failFirst {
				return nil, workflow.Permanent(errBoom)
			}
		}
		return in, nil
	})
	wf.Coalesce = &workflow.Coalesce{Payload: payload}
	wf.Retry = *workflow.RetriesNone()
	return wf
}

// coalesceRun is the scenario every coalesce case shares: a first run
// holds the key and runs, then three more enqueues arrive.
func coalesceRun(e *env, payload workflow.CoalescePayload, failFirst bool, wantSecondInput string) {
	e.t.Helper()
	e.need(workflow.CapCoalesce)
	var c counter
	var mu sync.Mutex
	var inputs []string
	g := newGate(e.t)
	wf := coalesceWorkflow(e, payload, g, &c, &inputs, &mu, failFirst)
	e.up(wf)
	key := e.name("rebuild")
	first := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key, Payload: []byte(`"a"`)})
	e.eventually("the first run to start", 0, func() bool { return c.count() == 1 })
	second := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key, Payload: []byte(`"b"`)})
	if sameRef(second, first) {
		e.t.Fatalf("an enqueue while the holder runs returned the holder; want a pending run")
	}
	if st := e.status(second); st != workflow.StatusScheduled {
		e.t.Errorf("the pending run's status = %s, want %s until the holder ends", st, workflow.StatusScheduled)
	}
	third := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key, Payload: []byte(`"c"`)})
	fourth := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key, Payload: []byte(`"d"`)})
	if !sameRef(third, second) || !sameRef(fourth, second) {
		e.t.Fatalf("further enqueues returned %+v and %+v, want the pending run %+v", third, fourth, second)
	}
	e.quiet()
	if got := c.count(); got != 1 {
		e.t.Fatalf("the step ran %d times while the holder ran, want 1", got)
	}
	g.open()
	want := workflow.StatusSucceeded
	if failFirst {
		want = workflow.StatusFailed
	}
	e.waitStatus(first, want, 0)
	e.waitStatus(second, workflow.StatusSucceeded, 0)
	e.quiet()
	mu.Lock()
	defer mu.Unlock()
	if len(inputs) != 2 || inputs[1] != wantSecondInput {
		e.t.Fatalf("the runs took %q, want 2 runs, the second with %s", inputs, wantSecondInput)
	}
}

var coalesceCases = []testCase{
	{"with no run holding the key an enqueue starts a run", func(e *env) {
		e.need(workflow.CapCoalesce)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Coalesce = &workflow.Coalesce{}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: e.name("k")})
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		if c.count() != 1 {
			e.t.Fatalf("the step ran %d times, want 1", c.count())
		}
	}},
	{"while one runs one more is kept and later enqueues fold into it (keep first)", func(e *env) {
		coalesceRun(e, "", false, `"b"`)
	}},
	{"CoalesceKeepLast gives the pending run the newest payload", func(e *env) {
		coalesceRun(e, workflow.CoalesceKeepLast, false, `"d"`)
	}},
	{"the pending run starts after the running one fails", func(e *env) {
		coalesceRun(e, workflow.CoalesceKeepFirst, true, `"b"`)
	}},
	{"a holder still in the queue absorbs the enqueue", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapCoalesce, workflow.CapDelayedStart))
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Coalesce = &workflow.Coalesce{}
		e.up(wf)
		key := e.name("k")
		first := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key, RunAt: e.now().Add(time.Hour)})
		second := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		if !sameRef(second, first) {
			e.t.Fatalf("an enqueue while the holder waits to start returned %+v, want the holder %+v", second, first)
		}
	}},
	{"a driver without CapCoalesce refuses the workflow at Start", func(e *env) {
		if e.c.Capabilities().Has(workflow.CapCoalesce) {
			e.t.Skip("the driver has CapCoalesce")
		}
		wf := oneStep(e.name("w"), (&counter{}).echo())
		wf.Coalesce = &workflow.Coalesce{}
		err := workflow.CheckCaps(e.c.Capabilities(), wf)
		assertUnsupported(e.t, err, "Workflow", workflow.CapCoalesce)
	}},
}
