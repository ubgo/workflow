package workflowtest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// The cases in this file cover run states (StatusRetrying with the next
// attempt, StatusDiscarded), job chains and their editing from a running
// step, per-queue worker scaling, and closures. Each is skipped, naming
// the capability, on a driver that does not declare it; the refusal cases
// run only on a driver that lacks it.

var runStateCases = []testCase{
	{"a step in its retry backoff reads Retrying with the next attempt", func(e *env) {
		e.need(workflow.CapRunStates)
		var c counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			if c.n.Add(1) == 1 {
				return nil, errBoom
			}
			return nil, g.wait(ctx)
		})
		wf.Retry = workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: e.tm.unit, Factor: 1}}
		e.up(wf)
		before := e.now()
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusRetrying, 0)
		if q, ok := e.c.(workflow.RunQuerier); ok && e.c.Capabilities().Has(workflow.CapRunQuery) {
			ctx, cancel := e.callCtx()
			page, err := q.Runs(ctx, workflow.RunQuery{Ref: ref})
			cancel()
			if err != nil || len(page.Runs) != 1 {
				e.t.Fatalf("Runs(ref) = %v, %v", page.Runs, err)
			}
			info := page.Runs[0]
			if info.Status == workflow.StatusRetrying {
				if info.NextAttempt != 2 {
					e.t.Errorf("RunInfo.NextAttempt = %d, want 2", info.NextAttempt)
				}
				if info.NextAttemptAt.Before(before.Add(e.tm.unit)) {
					e.t.Errorf("RunInfo.NextAttemptAt = %s, before enqueue + backoff %s", info.NextAttemptAt, before.Add(e.tm.unit))
				}
			}
		}
		e.eventually("the retry to start", e.tm.unit, func() bool { return c.count() >= 2 })
		if st := e.status(ref); st != workflow.StatusRunning {
			e.t.Fatalf("status during the retry = %q, want running", st)
		}
		g.open()
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
	}},
	{"a run out of retries, MaxErrors or Until reads Failed, never Discarded", func(e *env) {
		e.need(workflow.CapRunStates)
		var c counter
		wf := oneStep(e.name("w"), c.fail(errBoom))
		wf.Retry = workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: e.tm.unit / 4, Factor: 1}}
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusFailed, e.tm.unit/4)
	}},
	{"Runs filters on Retrying and Discarded", func(e *env) {
		e.need(workflow.CapRunStates | workflow.CapRunQuery)
		var c counter
		wf := oneStep(e.name("d"), c.fail(workflow.Discard(errBoom)))
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusDiscarded, 0)
		q := as[workflow.RunQuerier](e, "workflow.RunQuerier")
		ctx, cancel := e.callCtx()
		defer cancel()
		page, err := q.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, Statuses: []workflow.Status{workflow.StatusDiscarded}})
		if err != nil || len(page.Runs) != 1 || page.Runs[0].Ref.RunID != ref.RunID {
			e.t.Fatalf("Runs(discarded) = %v, %v; want the run", page.Runs, err)
		}
		if page.Runs[0].Error == "" {
			e.t.Error("a discarded run keeps no reason")
		}
		page, err = q.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, Statuses: []workflow.Status{workflow.StatusRetrying, workflow.StatusFailed}})
		if err != nil || len(page.Runs) != 0 {
			e.t.Fatalf("Runs(retrying, failed) = %v, %v; want none", page.Runs, err)
		}
	}},
}

// chainRecorder records the order chained workflows ran in.
type chainRecorder struct {
	mu   sync.Mutex
	seen []string
}

func (r *chainRecorder) step(name string) workflow.Handler {
	return func(_ context.Context, in []byte) ([]byte, error) {
		r.mu.Lock()
		r.seen = append(r.seen, name+":"+string(in))
		r.mu.Unlock()
		return in, nil
	}
}

func (r *chainRecorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.seen)
}

var chainCases = []testCase{
	{"Job.Chain runs each job after the one before succeeds, with its own payload", func(e *env) {
		e.need(workflow.CapChain)
		var rec chainRecorder
		a, b, c := oneStep(e.name("a"), rec.step("a")), oneStep(e.name("b"), rec.step("b")), oneStep(e.name("c"), rec.step("c"))
		e.up(a, b, c)
		e.enqueue(workflow.Job{Workflow: a.Name, Payload: []byte("1"), Chain: []workflow.Job{
			{Workflow: b.Name, Payload: []byte("2")},
			{Workflow: c.Name, Payload: []byte("3")},
		}})
		e.eventually("the chain to finish", e.tm.unit, func() bool { return len(rec.got()) == 3 })
		if got := rec.got(); !slices.Equal(got, []string{"a:1", "b:2", "c:3"}) {
			e.t.Fatalf("chain ran %v, want a:1 b:2 c:3", got)
		}
		e.quiet()
		if n := len(rec.got()); n != 3 {
			e.t.Fatalf("chain ran %d jobs, want 3", n)
		}
	}},
	{"a failed job ends its chain", func(e *env) {
		e.need(workflow.CapChain)
		var rec chainRecorder
		var bad counter
		a, b := oneStep(e.name("a"), bad.fail(errBoom)), oneStep(e.name("b"), rec.step("b"))
		e.up(a, b)
		ref := e.enqueue(workflow.Job{Workflow: a.Name, Chain: []workflow.Job{{Workflow: b.Name}}})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		e.quiet()
		if got := rec.got(); len(got) != 0 {
			e.t.Fatalf("a failed job's chain ran %v", got)
		}
	}},
	{"a step prepends and appends to its chain; a failed attempt's edits are dropped", func(e *env) {
		e.need(workflow.CapChain)
		var rec chainRecorder
		var attempts counter
		b, c, d, x := oneStep(e.name("b"), rec.step("b")), oneStep(e.name("c"), rec.step("c")),
			oneStep(e.name("d"), rec.step("d")), oneStep(e.name("x"), rec.step("x"))
		a := oneStep(e.name("a"), func(ctx context.Context, in []byte) ([]byte, error) {
			attempts.hit(in)
			if err := workflow.AppendToChain(ctx, workflow.Job{Workflow: d.Name}); err != nil {
				return nil, workflow.Permanent(err)
			}
			if attempts.count() == 1 {
				if err := workflow.PrependToChain(ctx, workflow.Job{Workflow: x.Name}); err != nil {
					return nil, workflow.Permanent(err)
				}
				return nil, errBoom // this attempt's edits must not survive
			}
			if err := workflow.PrependToChain(ctx, workflow.Job{Workflow: b.Name}); err != nil {
				return nil, workflow.Permanent(err)
			}
			return nil, nil
		})
		a.Retry = workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: e.tm.unit / 4, Factor: 1}}
		e.up(a, b, c, d, x)
		e.enqueue(workflow.Job{Workflow: a.Name, Chain: []workflow.Job{{Workflow: c.Name}}})
		e.eventually("the edited chain to finish", e.tm.unit/4, func() bool { return len(rec.got()) >= 3 })
		e.quiet()
		if got := rec.got(); !slices.Equal(got, []string{"b:", "c:", "d:"}) {
			e.t.Fatalf("edited chain ran %v, want b c d (x was prepended by a failed attempt)", got)
		}
	}},
	{"Job.Chain is refused without CapChain", func(e *env) {
		e.lacks(workflow.CapChain)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Chain: []workflow.Job{{Workflow: wf.Name}}})
		assertUnsupported(e.t, err, "Job.Chain", workflow.CapChain)
	}},
	{"PrependToChain in a step is refused without CapChain", func(e *env) {
		e.lacks(workflow.CapChain)
		got := make(chan error, 1)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			got <- workflow.PrependToChain(ctx, workflow.Job{Workflow: "anything"})
			return nil, nil
		})
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusSucceeded, 0)
		select {
		case err := <-got:
			assertUnsupported(e.t, err, "PrependToChain", workflow.CapChain)
		case <-time.After(e.tm.wait):
			e.t.Fatal("the step did not run")
		}
	}},
}

var scaleCases = []testCase{
	{"WorkerScaler lists sorted loads and checks SetWorkers", func(e *env) {
		e.need(workflow.CapWorkerScale)
		s := as[workflow.WorkerScaler](e, "workflow.WorkerScaler")
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := s.QueueLoads(ctx); !errors.Is(err, workflow.ErrNotStarted) {
			e.t.Errorf("QueueLoads before Start = %v, want ErrNotStarted", err)
		}
		e.start()
		loads, err := s.QueueLoads(ctx)
		if err != nil {
			e.t.Fatalf("QueueLoads: %v", err)
		}
		if !slices.IsSortedFunc(loads, func(a, b workflow.QueueLoad) int {
			if a.Queue < b.Queue {
				return -1
			}
			return 1
		}) {
			e.t.Errorf("QueueLoads not sorted by name: %v", loads)
		}
		if err := s.SetWorkers(ctx, e.name("no-such-queue"), 2); !errors.Is(err, workflow.ErrNotFound) {
			e.t.Errorf("SetWorkers(unknown queue) = %v, want ErrNotFound", err)
		}
		if len(loads) == 0 {
			return // the harness configured no queues; nothing to resize
		}
		q := loads[0].Queue
		if err := s.SetWorkers(ctx, q, 0); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Errorf("SetWorkers(0) = %v, want ErrInvalid", err)
		}
		if err := s.SetWorkers(ctx, q, 3); err != nil {
			e.t.Fatalf("SetWorkers(%s, 3): %v", q, err)
		}
		loads, err = s.QueueLoads(ctx)
		if err != nil || loads[0].Workers != 3 {
			e.t.Fatalf("after SetWorkers(3) loads = %v, %v", loads, err)
		}
	}},
}

var closureCases = []testCase{
	{"a closure runs as a run of ClosureWorkflow, retried under its policy", func(e *env) {
		e.need(workflow.CapClosure)
		e.start()
		var calls counter
		var mu sync.Mutex
		var info workflow.StepInfo
		ctx, cancel := e.callCtx()
		defer cancel()
		ref, err := workflow.EnqueueClosure(ctx, e.c, workflow.ClosureJob{
			Name:  "warm-cache",
			Retry: &workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: e.tm.unit / 4, Factor: 1}},
			Fn: func(ctx context.Context) error {
				calls.hit(nil)
				mu.Lock()
				info, _ = workflow.StepInfoFrom(ctx)
				mu.Unlock()
				if calls.count() == 1 {
					return errBoom
				}
				return nil
			},
		})
		if err != nil {
			e.t.Fatalf("EnqueueClosure: %v", err)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit/4)
		mu.Lock()
		defer mu.Unlock()
		if calls.count() != 2 || info.Workflow != workflow.ClosureWorkflow || info.Step != workflow.ClosureStep ||
			info.Labels[workflow.LabelClosure] != "warm-cache" || info.Attempt != 2 {
			e.t.Fatalf("calls %d, StepInfo %+v", calls.count(), info)
		}
	}},
	{"an invalid closure is refused", func(e *env) {
		e.need(workflow.CapClosure)
		e.start()
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := workflow.EnqueueClosure(ctx, e.c, workflow.ClosureJob{Name: "no-fn"}); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("closure without Fn = %v, want ErrInvalid", err)
		}
	}},
	{"closures are refused without CapClosure", func(e *env) {
		e.lacks(workflow.CapClosure)
		e.start()
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := workflow.EnqueueClosure(ctx, e.c, workflow.ClosureJob{Name: "x", Fn: func(context.Context) error { return nil }})
		assertUnsupported(e.t, err, "EnqueueClosure", workflow.CapClosure)
	}},
}
