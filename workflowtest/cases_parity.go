package workflowtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
)

// The cases in this file cover the queue, retry, run management, pause,
// schedule control and drain capabilities: named
// queues, per-dispatch retries, retry deadlines and error caps, discard,
// run queries, retry from the failed step, delete, bulk cancel, pause,
// schedule conditions, windows, tick events, schedule listing and pause,
// named-schedule upserts and graceful drain. Each is skipped, naming the
// capability, on a driver that does not declare it; the refusal cases
// run only on a driver that lacks it.

// Queue names the cases use. They are short and fixed, so every engine
// accepts them, and the workflow names already keep subtests apart.
const (
	queueA = "wftest-a"
	queueB = "wftest-b"
)

// lacks skips the subtest unless the client lacks every capability in c,
// for the cases that check a refusal.
func (e *env) lacks(c workflow.Caps) {
	e.t.Helper()
	if e.c.Capabilities()&c != 0 {
		e.t.Skipf("driver has %s; this case checks its refusal", e.c.Capabilities()&c)
	}
}

// needFake skips a subtest that moves time to chosen instants.
func (e *env) needFake(why string) {
	e.t.Helper()
	if e.clk == nil {
		e.t.Skip(why + " needs a fake clock")
	}
}

// as asserts that the client implements T, which a driver declaring the
// matching capability must.
func as[T any](e *env, what string) T {
	e.t.Helper()
	v, ok := e.c.(T)
	if !ok {
		e.t.Fatalf("the client declares the capability but does not implement %s", what)
	}
	return v
}

// stepInfos records the StepInfo of every attempt.
type stepInfos struct {
	mu    sync.Mutex
	infos []workflow.StepInfo
}

func (s *stepInfos) handler() workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		s.mu.Lock()
		s.infos = append(s.infos, info)
		s.mu.Unlock()
		return in, nil
	}
}

func (s *stepInfos) last() workflow.StepInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.infos) == 0 {
		return workflow.StepInfo{}
	}
	return s.infos[len(s.infos)-1]
}

var queueCases = []testCase{
	{"a run carries its workflow's queue", func(e *env) {
		e.need(workflow.CapQueue)
		var s stepInfos
		wf := oneStep(e.name("w"), s.handler())
		wf.Queue = queueA
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := s.last().Queue; got != queueA {
			e.t.Fatalf("StepInfo.Queue = %q, want %q", got, queueA)
		}
	}},
	{"Job.Queue overrides the workflow's queue", func(e *env) {
		e.need(workflow.CapQueue | workflow.CapQueueOverride)
		var s stepInfos
		wf := oneStep(e.name("w"), s.handler())
		wf.Queue = queueA
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, Queue: queueB})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := s.last().Queue; got != queueB {
			e.t.Fatalf("StepInfo.Queue = %q, want %q", got, queueB)
		}
	}},
	{"without CapQueueOverride a different Job.Queue is refused", func(e *env) {
		e.need(workflow.CapQueue)
		e.lacks(workflow.CapQueueOverride)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Queue = queueA
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Queue: queueB})
		assertUnsupported(e.t, err, "Job.Queue", workflow.CapQueueOverride)
	}},
	{"without CapQueue a queue is refused", func(e *env) {
		e.lacks(workflow.CapQueue)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Queue = queueA
		assertUnsupported(e.t, e.registerAndStart(wf), "", workflow.CapQueue)
	}},
	{"an invalid queue name is ErrInvalid", func(e *env) {
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Queue = "no spaces"
		if err := e.c.Register(wf); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("Register with Queue %q: %v, want ErrInvalid", wf.Queue, err)
		}
	}},
}

var jobRetryCases = []testCase{
	{"Job.Retry overrides the workflow and step policies", func(e *env) {
		e.need(workflow.CapJobRetry)
		var more, fewer counter
		wfMore := oneStep(e.name("more"), more.fail(errBoom)) // no retries declared
		wfFewer := oneStep(e.name("fewer"), fewer.fail(errBoom))
		wfFewer.Retry = workflow.RetryPolicy{Max: 3}
		e.up(wfMore, wfFewer)
		r1 := e.enqueue(workflow.Job{Workflow: wfMore.Name, Retry: &workflow.RetryPolicy{Max: 2}})
		r2 := e.enqueue(workflow.Job{Workflow: wfFewer.Name, Retry: workflow.RetriesNone()})
		e.waitStatus(r1, workflow.StatusFailed, e.tm.unit)
		e.waitStatus(r2, workflow.StatusFailed, e.tm.unit)
		if got := more.count(); got != 3 {
			e.t.Errorf("Job.Retry{Max: 2} made %d attempts, want 3", got)
		}
		if got := fewer.count(); got != 1 {
			e.t.Errorf("Job.Retry RetriesNone made %d attempts, want 1", got)
		}
	}},
	{"without CapJobRetry Job.Retry is refused", func(e *env) {
		e.lacks(workflow.CapJobRetry)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Retry: &workflow.RetryPolicy{Max: 1}})
		assertUnsupported(e.t, err, "Job.Retry", workflow.CapJobRetry)
	}},
	{"a Job.Retry with functions is ErrInvalid", func(e *env) {
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		p := &workflow.RetryPolicy{Max: 1, NoRetryOn: func(error) bool { return false }}
		if _, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Retry: p}); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("Enqueue with Job.Retry.NoRetryOn: %v, want ErrInvalid", err)
		}
	}},
	{"RetryPolicy.Until stops retries at the deadline", func(e *env) {
		e.need(workflow.CapRetryUntil)
		var c counter
		wf := oneStep(e.name("w"), c.fail(errBoom))
		// Retries at 1u, 2u and 3u after the first; the deadline at 2.5u
		// lets the first two through and stops the third.
		wf.Retry = workflow.RetryPolicy{Max: 100, Backoff: workflow.Backoff{Initial: e.tm.unit}, Until: e.tm.unit * 5 / 2}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		if e.clk == nil {
			e.waitStatus(ref, workflow.StatusFailed, 0)
		} else {
			// Move the clock only once each failure is recorded and its
			// backoff timer armed, so every retry decision sees the
			// attempt's own time, never a clock a poll pushed on.
			for k := 1; ; k++ {
				var st workflow.Status
				// Recorded means ended or with its backoff timer armed: a
				// status read between the attempt's return and the retry
				// decision says neither, and must not be taken for either.
				e.eventually(fmt.Sprintf("attempt %d recorded", k), 0, func() bool {
					st = e.status(ref)
					return c.count() >= k && (st.IsTerminal() || (st != workflow.StatusRunning && e.clk.Pending() > 0))
				})
				if st.IsTerminal() {
					break
				}
				if k > 5 {
					e.t.Fatalf("still retrying after %d attempts", k)
				}
				e.blockUntilTimers(1)
				e.clk.Advance(e.tm.unit)
			}
			if st := e.status(ref); st != workflow.StatusFailed {
				e.t.Fatalf("run ended %s, want failed", st)
			}
		}
		if got := c.count(); got != 3 {
			e.t.Fatalf("Until 2.5 units with 1-unit backoff made %d attempts, want 3", got)
		}
	}},
	{"RetryPolicy.MaxErrors counts errors but not releases", func(e *env) {
		e.need(workflow.CapRetryMaxErrors)
		var n atomic.Int64
		wf := oneStep(e.name("w"), func(context.Context, []byte) ([]byte, error) {
			if n.Add(1) <= 2 {
				return nil, workflow.RetryAfter(errBoom, 0) // two releases
			}
			return nil, errBoom
		})
		wf.Retry = workflow.RetryPolicy{Max: 10, MaxErrors: 2}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		if got := n.Load(); got != 4 {
			e.t.Fatalf("two releases then errors under MaxErrors 2 made %d attempts, want 4", got)
		}
	}},
	{"without CapRetryUntil or CapRetryMaxErrors the policy is refused by Start at the latest", func(e *env) {
		e.lacks(workflow.CapRetryUntil)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Retry = workflow.RetryPolicy{Max: 1, Until: time.Hour}
		assertUnsupported(e.t, e.registerAndStart(wf), "", workflow.CapRetryUntil)
	}},
}

var discardCases = []testCase{
	{"Discard ends the run discarded (or cancelled without CapRunStates), without retries or OnFailure", func(e *env) {
		e.need(workflow.CapDiscard | workflow.CapRunFinished)
		var c, onFail counter
		wf := oneStep(e.name("w"), c.fail(workflow.Discard(errBoom)))
		wf.Retry = workflow.RetryPolicy{Max: 3}
		wf.OnFailure = onFail.echo()
		var mu sync.Mutex
		var got []workflow.RunEvent
		if err := as[workflow.RunNotifier](e, "workflow.RunNotifier").OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
			mu.Lock()
			got = append(got, ev)
			mu.Unlock()
		}); err != nil {
			e.t.Fatalf("OnRunFinished: %v", err)
		}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		want := workflow.EndStatus(e.c.Capabilities(), workflow.Discard(errBoom))
		e.waitStatus(ref, want, e.tm.unit)
		e.eventually("the run-finished event", 0, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) > 0 })
		mu.Lock()
		ev := got[0]
		mu.Unlock()
		if ev.Status != want {
			e.t.Errorf("RunEvent.Status = %q, want %q", ev.Status, want)
		}
		if !errors.Is(ev.Err, workflow.ErrDiscarded) || !errors.Is(ev.Err, workflow.ErrCancelled) {
			e.t.Errorf("RunEvent.Err = %v, want ErrDiscarded and ErrCancelled", ev.Err)
		}
		e.quiet()
		if c.count() != 1 || onFail.count() != 0 {
			e.t.Fatalf("attempts = %d, OnFailure runs = %d; want 1 and 0", c.count(), onFail.count())
		}
	}},
}

// failingOnce is a two-step workflow whose second step fails until open
// is set, counting the runs of each step.
func failingOnce(e *env, first, second *counter, open *atomic.Bool) workflow.Workflow {
	return workflow.Workflow{Name: e.name("w"), Steps: []workflow.Step{
		{Name: "first", Run: first.echo()},
		{Name: "second", After: []string{"first"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			second.hit(in)
			if !open.Load() {
				return nil, errBoom
			}
			return in, nil
		}},
	}}
}

var runQueryCases = []testCase{
	{"Runs filters by workflow, status and labels, newest first, in pages", func(e *env) {
		e.need(workflow.CapRunQuery)
		var ok counter
		wfOK := oneStep(e.name("ok"), ok.echo())
		wfBad := oneStep(e.name("bad"), ok.fail(errBoom))
		e.up(wfOK, wfBad)
		var refs []workflow.RunRef
		for i := range 3 {
			ref := e.enqueue(workflow.Job{Workflow: wfOK.Name, Labels: map[string]string{"tenant": []string{"t1", "t1", "t2"}[i]}})
			e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
			refs = append(refs, ref)
			if e.clk != nil {
				e.clk.Advance(time.Second) // distinct enqueue times
			}
		}
		bad := e.enqueue(workflow.Job{Workflow: wfBad.Name})
		e.waitStatus(bad, workflow.StatusFailed, e.tm.unit)
		q := as[workflow.RunQuerier](e, "workflow.RunQuerier")
		ctx, cancel := e.callCtx()
		defer cancel()

		all, err := q.Runs(ctx, workflow.RunQuery{Workflow: wfOK.Name})
		if err != nil {
			e.t.Fatalf("Runs: %v", err)
		}
		if len(all.Runs) != 3 || all.Next != "" {
			e.t.Fatalf("Runs(workflow) = %d runs, Next %q; want 3 and none", len(all.Runs), all.Next)
		}
		if all.Runs[0].Ref.RunID != refs[2].RunID || all.Runs[2].Ref.RunID != refs[0].RunID {
			e.t.Errorf("Runs is not newest first: %v", all.Runs)
		}
		for _, r := range all.Runs {
			if r.Workflow != wfOK.Name || r.Status != workflow.StatusSucceeded || r.EnqueuedAt.IsZero() || r.FinishedAt.IsZero() || r.Attempts < 1 {
				e.t.Errorf("RunInfo = %+v; want workflow, status, enqueue and finish times and attempts", r)
			}
		}

		tenant, err := q.Runs(ctx, workflow.RunQuery{Workflow: wfOK.Name, Labels: map[string]string{"tenant": "t1"}})
		if err != nil || len(tenant.Runs) != 2 {
			e.t.Errorf("Runs(labels tenant=t1) = %d runs, %v; want 2", len(tenant.Runs), err)
		}

		failed, err := q.Runs(ctx, workflow.RunQuery{Workflow: wfBad.Name, Statuses: []workflow.Status{workflow.StatusFailed}})
		if err != nil || len(failed.Runs) != 1 {
			e.t.Fatalf("Runs(failed) = %+v, %v; want one run", failed.Runs, err)
		}
		if f := failed.Runs[0]; f.Error == "" || f.FailedStep != "only" {
			e.t.Errorf("failed RunInfo Error %q FailedStep %q; want the error and step \"only\"", f.Error, f.FailedStep)
		}

		p1, err := q.Runs(ctx, workflow.RunQuery{Workflow: wfOK.Name, Limit: 2})
		if err != nil || len(p1.Runs) != 2 || p1.Next == "" {
			e.t.Fatalf("page 1 = %d runs, Next %q, %v; want 2 and a cursor", len(p1.Runs), p1.Next, err)
		}
		p2, err := q.Runs(ctx, workflow.RunQuery{Workflow: wfOK.Name, Limit: 2, Cursor: p1.Next})
		if err != nil || len(p2.Runs) != 1 || p2.Next != "" {
			e.t.Fatalf("page 2 = %d runs, Next %q, %v; want 1 and none", len(p2.Runs), p2.Next, err)
		}
		if p2.Runs[0].Ref.RunID != refs[0].RunID {
			e.t.Errorf("page 2 holds %v, want the oldest run %v", p2.Runs[0].Ref, refs[0])
		}

		one, err := q.Runs(ctx, workflow.RunQuery{Ref: refs[1]})
		if err != nil || len(one.Runs) != 1 || one.Runs[0].Ref.RunID != refs[1].RunID || one.Next != "" {
			e.t.Errorf("Runs(Ref) = %+v, %v; want exactly run %v", one, err, refs[1])
		}
		none, err := q.Runs(ctx, workflow.RunQuery{Ref: workflow.RunRef{RunID: "no-such-run"}})
		if err != nil || len(none.Runs) != 0 {
			e.t.Errorf("Runs(Ref of an unknown run) = %+v, %v; want no runs and no error", none, err)
		}

		if _, err := q.Runs(ctx, workflow.RunQuery{Limit: workflow.MaxRunsLimit + 1}); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Errorf("Runs with Limit over MaxRunsLimit: %v, want ErrInvalid", err)
		}
	}},
	{"Retry re-runs a failed run from the failed step", func(e *env) {
		e.need(workflow.CapRetryRun)
		var first, second counter
		var open atomic.Bool
		wf := failingOnce(e, &first, &second, &open)
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, Payload: []byte(`"p"`)})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		r := as[workflow.RunRetrier](e, "workflow.RunRetrier")
		ctx, cancel := e.callCtx()
		defer cancel()
		open.Store(true)
		again, err := r.Retry(ctx, ref)
		if err != nil {
			e.t.Fatalf("Retry: %v", err)
		}
		e.waitStatus(again, workflow.StatusSucceeded, e.tm.unit)
		if again.RunID != ref.RunID {
			e.t.Errorf("Retry returned run %v, want the same run %v", again.RunID, ref.RunID)
		}
		if first.count() != 1 || second.count() != 2 {
			e.t.Fatalf("first ran %d times and second %d; want 1 and 2", first.count(), second.count())
		}
		if _, err := r.Retry(ctx, ref); !errors.Is(err, workflow.ErrWrongState) {
			e.t.Fatalf("Retry of a succeeded run: %v, want ErrWrongState", err)
		}
	}},
	{"Delete forgets a finished run and refuses an unfinished one", func(e *env) {
		e.need(workflow.CapDeleteRun)
		g := newGate(e.t)
		var c counter
		done := oneStep(e.name("done"), c.echo())
		held := oneStep(e.name("held"), func(ctx context.Context, in []byte) ([]byte, error) { return in, g.wait(ctx) })
		e.up(done, held)
		d := as[workflow.RunDeleter](e, "workflow.RunDeleter")
		ref := e.enqueue(workflow.Job{Workflow: done.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		running := e.enqueue(workflow.Job{Workflow: held.Name})
		e.waitStatus(running, workflow.StatusRunning, 0)
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := d.Delete(ctx, running); !errors.Is(err, workflow.ErrWrongState) {
			e.t.Errorf("Delete of a running run: %v, want ErrWrongState", err)
		}
		if err := d.Delete(ctx, ref); err != nil {
			e.t.Fatalf("Delete: %v", err)
		}
		if _, err := e.c.Status(ctx, ref); !errors.Is(err, workflow.ErrNotFound) {
			e.t.Fatalf("Status after Delete: %v, want ErrNotFound", err)
		}
		g.open()
	}},
	{"CancelAll cancels the runs not yet started and leaves running ones", func(e *env) {
		e.need(workflow.CapCancelAll | workflow.CapDelayedStart)
		g := newGate(e.t)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) { return in, g.wait(ctx) })
		e.up(wf)
		running := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(running, workflow.StatusRunning, 0)
		later := e.now().Add(100 * e.tm.unit)
		a := e.enqueue(workflow.Job{Workflow: wf.Name, RunAt: later})
		b := e.enqueue(workflow.Job{Workflow: wf.Name, RunAt: later})
		bc := as[workflow.BulkCanceller](e, "workflow.BulkCanceller")
		ctx, cancel := e.callCtx()
		defer cancel()
		n, err := bc.CancelAll(ctx, workflow.RunQuery{Workflow: wf.Name})
		if err != nil {
			e.t.Fatalf("CancelAll: %v", err)
		}
		if n != 2 {
			e.t.Errorf("CancelAll cancelled %d runs, want 2", n)
		}
		e.waitStatus(a, workflow.StatusCancelled, 0)
		e.waitStatus(b, workflow.StatusCancelled, 0)
		if st := e.status(running); st != workflow.StatusRunning {
			e.t.Errorf("the running run is %s after CancelAll, want running", st)
		}
		if _, err := bc.CancelAll(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusSucceeded}}); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Errorf("CancelAll of a terminal status: %v, want ErrInvalid", err)
		}
		g.open()
	}},
}

var pauseCases = []testCase{
	{"a paused workflow holds its runs until resumed", func(e *env) {
		e.need(workflow.CapPauseWorkflow)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		p := as[workflow.Pauser](e, "workflow.Pauser")
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := p.Pause(ctx, workflow.PauseWorkflow(wf.Name)); err != nil {
			e.t.Fatalf("Pause: %v", err)
		}
		paused, err := p.Paused(ctx)
		if err != nil || len(paused) != 1 || paused[0] != workflow.PauseWorkflow(wf.Name) {
			e.t.Errorf("Paused = %v, %v; want the workflow", paused, err)
		}
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.quiet()
		if c.count() != 0 {
			e.t.Fatal("a paused workflow ran a step")
		}
		if st := e.status(ref); st != workflow.StatusQueued {
			e.t.Errorf("a held run is %s, want queued", st)
		}
		if err := p.Resume(ctx, workflow.PauseWorkflow(wf.Name)); err != nil {
			e.t.Fatalf("Resume: %v", err)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
	}},
	{"a paused queue holds its runs and not other queues'", func(e *env) {
		e.need(workflow.CapPauseQueue | workflow.CapQueue)
		var a, b counter
		wfA := oneStep(e.name("a"), a.echo())
		wfA.Queue = queueA
		wfB := oneStep(e.name("b"), b.echo())
		wfB.Queue = queueB
		e.up(wfA, wfB)
		p := as[workflow.Pauser](e, "workflow.Pauser")
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := p.Pause(ctx, workflow.PauseQueue(queueA)); err != nil {
			e.t.Fatalf("Pause: %v", err)
		}
		refA := e.enqueue(workflow.Job{Workflow: wfA.Name})
		refB := e.enqueue(workflow.Job{Workflow: wfB.Name})
		e.waitStatus(refB, workflow.StatusSucceeded, e.tm.unit)
		if a.count() != 0 {
			e.t.Fatal("a paused queue ran a step")
		}
		if err := p.Resume(ctx, workflow.PauseAll()); err != nil {
			e.t.Fatalf("Resume(All): %v", err)
		}
		e.waitStatus(refA, workflow.StatusSucceeded, e.tm.unit)
	}},
	{"an invalid pause target is ErrInvalid", func(e *env) {
		e.need(workflow.CapPauseWorkflow)
		e.up(oneStep(e.name("w"), (&counter{}).echo()))
		ctx, cancel := e.callCtx()
		defer cancel()
		err := as[workflow.Pauser](e, "workflow.Pauser").Pause(ctx, workflow.PauseTarget{})
		if !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("Pause of an empty target: %v, want ErrInvalid", err)
		}
	}},
}

// tickLog records ticks.
type tickLog struct {
	mu    sync.Mutex
	ticks []workflow.Tick
}

func (l *tickLog) observe(_ context.Context, t workflow.Tick) {
	l.mu.Lock()
	l.ticks = append(l.ticks, t)
	l.mu.Unlock()
}

func (l *tickLog) count(o workflow.TickOutcome, reason string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, t := range l.ticks {
		if t.Outcome == o && (reason == "" || t.Reason == reason) {
			n++
		}
	}
	return n
}

func (l *tickLog) first(o workflow.TickOutcome) (workflow.Tick, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, t := range l.ticks {
		if t.Outcome == o {
			return t, true
		}
	}
	return workflow.Tick{}, false
}

// schedule adds an every-minute UTC schedule of wf named after the
// subtest, or fails.
func (e *env) schedule(s workflow.Schedule) workflow.ScheduleID {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	id, err := e.c.Schedule(ctx, s)
	if err != nil {
		e.t.Fatalf("Schedule: %v", err)
	}
	return id
}

// advanceMinutes moves a fake clock n minute boundaries, waiting for the
// driver's timers before each.
func (e *env) advanceMinutes(n int) {
	e.t.Helper()
	for range n {
		e.blockUntilTimers(1)
		e.clk.Advance(time.Minute)
		e.quiet()
	}
}

var scheduleCases = []testCase{
	{"a named Schedule is upserted, not duplicated", func(e *env) {
		needCronFiring(e)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		s := workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: everyMinute, Location: time.UTC}
		id1 := e.schedule(s)
		s.Input = []byte(`"second"`)
		id2 := e.schedule(s)
		if id1 != id2 {
			e.t.Fatalf("the same name gave ids %q and %q, want one", id1, id2)
		}
		tick(e, &c, 1, 1)
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("one tick of an upserted schedule fired %d runs, want 1", got)
		}
		if got := string(c.input()); got != `"second"` {
			e.t.Errorf("the run's input = %s, want the second call's", got)
		}
	}},
	{"Schedule with time.Local is ErrInvalid", func(e *env) {
		e.need(workflow.CapCron)
		wf := oneStep(e.name("w"), (&counter{}).echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Cron: everyMinute, Location: time.Local})
		if !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("Schedule with time.Local: %v, want ErrInvalid", err)
		}
	}},
	{"ScheduleWhen skips ticks and tick observers see why", func(e *env) {
		e.need(workflow.CapCron | workflow.CapScheduleWhen | workflow.CapTickEvents)
		e.needFake("counting ticks")
		var c counter
		var asked atomic.Int64
		wf := oneStep(e.name("w"), c.echo())
		wf.ScheduleWhen = func(context.Context, workflow.Tick) bool { return asked.Add(1) > 1 } // skip the first
		var log tickLog
		if err := as[workflow.TickNotifier](e, "workflow.TickNotifier").OnTick(log.observe); err != nil {
			e.t.Fatalf("OnTick: %v", err)
		}
		e.up(wf)
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: everyMinute, Location: time.UTC})
		e.advanceMinutes(2)
		e.eventually("two ticks", 0, func() bool { return log.count(workflow.TickFired, "")+log.count(workflow.TickSkipped, "") >= 2 })
		// A tick is reported when the firing commits, before its run
		// executes: wait for the run, then check none more came.
		e.eventually("the fired run", 0, func() bool { return c.count() >= 1 })
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("two ticks with the first skipped ran %d times, want 1", got)
		}
		if log.count(workflow.TickSkipped, workflow.TickReasonWhen) != 1 {
			e.t.Errorf("ticks = %+v, want one skipped for %q", log.ticks, workflow.TickReasonWhen)
		}
		fired, ok := log.first(workflow.TickFired)
		if !ok || fired.ScheduleID != id || fired.Run.IsZero() || fired.Workflow != wf.Name {
			e.t.Errorf("fired tick = %+v, want the schedule, its workflow and the run", fired)
		}
	}},
	{"Windows keep ticks outside them from firing", func(e *env) {
		e.need(workflow.CapCron | workflow.CapScheduleWindow)
		e.needFake("placing ticks in and out of a window")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		// The window excludes the hour the clock is in, so no tick of it
		// fires.
		h := e.clk.Now().UTC().Hour()
		from := time.Date(2000, 1, 1, h, 0, 0, 0, time.UTC).Format("15:04")
		to := time.Date(2000, 1, 1, (h+1)%24, 0, 0, 0, time.UTC).Format("15:04")
		e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: everyMinute, Location: time.UTC,
			Windows: workflow.Windows{Unless: []workflow.Window{{From: from, To: to}}}})
		e.advanceMinutes(2)
		if got := c.count(); got != 0 {
			e.t.Fatalf("a schedule inside its Unless window fired %d runs", got)
		}
	}},
	{"Schedules lists schedules with their next fire time", func(e *env) {
		e.need(workflow.CapCron | workflow.CapScheduleList)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Cron = []string{"0 3 * * *"}
		e.up(wf)
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: "30 4 * * *", Location: time.UTC})
		ctx, cancel := e.callCtx()
		defer cancel()
		list, err := as[workflow.ScheduleLister](e, "workflow.ScheduleLister").Schedules(ctx)
		if err != nil {
			e.t.Fatalf("Schedules: %v", err)
		}
		var declared, made *workflow.ScheduleInfo
		for i := range list {
			switch {
			case list[i].Workflow == wf.Name && list[i].Declared:
				declared = &list[i]
			case list[i].ID == id:
				made = &list[i]
			}
		}
		if declared == nil || made == nil {
			e.t.Fatalf("Schedules = %+v, want the declared cron and the schedule", list)
		}
		if n := made.Next.UTC(); n.Hour() != 4 || n.Minute() != 30 || !n.After(e.now()) {
			e.t.Errorf("schedule Next = %v, want the next 04:30 UTC", made.Next)
		}
		if n := declared.Next.UTC(); n.Hour() != 3 || n.Minute() != 0 {
			e.t.Errorf("declared Next = %v, want the next 03:00 UTC", declared.Next)
		}
	}},
	{"a paused schedule does not fire, not even on resume", func(e *env) {
		e.need(workflow.CapCron | workflow.CapSchedulePause)
		e.needFake("moving time while paused")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		// Every five minutes: the clock starts between ticks and the pause
		// spans one tick. Under the default misfire policy too, that tick
		// must not fire on resume.
		start := e.clk.Now()
		next := start.Truncate(5 * time.Minute).Add(5 * time.Minute)
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: "*/5 * * * *", Location: time.UTC})
		sp := as[workflow.SchedulePauser](e, "workflow.SchedulePauser")
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := sp.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
			e.t.Fatalf("PauseSchedule: %v", err)
		}
		e.blockUntilTimers(1)
		e.clk.Set(next.Add(2*workflow.MisfireGrace + 30*time.Second))
		e.quiet()
		if got := c.count(); got != 0 {
			e.t.Fatalf("a paused schedule fired %d runs", got)
		}
		if err := sp.ResumeSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
			e.t.Fatalf("ResumeSchedule: %v", err)
		}
		e.quiet()
		if got := c.count(); got != 0 {
			e.t.Fatalf("resuming fired %d ticks of the pause, want none", got)
		}
		e.blockUntilTimers(1)
		e.clk.Set(next.Add(5 * time.Minute))
		e.eventually("the next tick after the resume to fire", 0, func() bool { return c.count() == 1 })
	}},
}

var drainCases = []testCase{
	{"Drain lets the running step finish, then stops", func(e *env) {
		e.need(workflow.CapDrain)
		g := newGate(e.t)
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			err := g.wait(ctx)
			c.hit(in)
			return in, err
		})
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusRunning, 0)
		d := as[workflow.Drainer](e, "workflow.Drainer")
		errc := make(chan error, 1)
		go func() {
			ctx, cancel := e.callCtx()
			defer cancel()
			errc <- d.Drain(ctx)
		}()
		e.quiet()
		g.open()
		select {
		case err := <-errc:
			if err != nil {
				e.t.Fatalf("Drain: %v, want nil after the step finished", err)
			}
		case <-time.After(e.tm.wait):
			e.t.Fatal("Drain did not return")
		}
		select {
		case <-d.Done():
		case <-time.After(e.tm.wait):
			e.t.Fatal("Done was not closed after Drain")
		}
		if c.count() != 1 {
			e.t.Fatalf("the running step finished %d times, want 1", c.count())
		}
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name}); !errors.Is(err, workflow.ErrNotStarted) {
			e.t.Fatalf("Enqueue after Drain: %v, want ErrNotStarted", err)
		}
	}},
	{"Drain returns the ctx error when a step outlives it", func(e *env) {
		e.need(workflow.CapDrain)
		g := newGate(e.t)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) { return in, g.wait(ctx) })
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusRunning, 0)
		ctx, cancel := context.WithTimeout(context.Background(), e.tm.quiet)
		defer cancel()
		if err := as[workflow.Drainer](e, "workflow.Drainer").Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
			e.t.Fatalf("Drain past its ctx: %v, want DeadlineExceeded", err)
		}
	}},
}

// registerAndStart registers wf and starts the client, returning the first
// error: the refusal of a workflow field may come from Register already
// (the Hatchet driver checks there) or from Start at the latest (the memory
// and SQL drivers), and the refusal cases accept either, as the
// capability refusal cases in cases_flow.go do.
func (e *env) registerAndStart(wf workflow.Workflow) error {
	if err := e.c.Register(wf); err != nil {
		return err
	}
	e.wfs = append(e.wfs, wf)
	return e.tryStart()
}
