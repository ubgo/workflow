package workflowtest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// The cases in this file cover durable closures (workflow.Func: a named
// function enqueued by name with a JSON argument) and the error a
// Retrying run reports. A Func is a one-step workflow, so its cases run
// on every driver.

// funcArg is the argument the Func cases encode.
type funcArg struct {
	To string `json:"to"`
}

var funcCases = []testCase{
	{"a Func runs by name with its decoded argument, retried under FuncRetry", func(e *env) {
		var calls counter
		var mu sync.Mutex
		var got funcArg
		var info workflow.StepInfo
		f := workflow.NewFunc(e.name("send"), func(ctx context.Context, in funcArg) error {
			calls.hit(nil)
			mu.Lock()
			got = in
			info, _ = workflow.StepInfoFrom(ctx)
			mu.Unlock()
			if calls.count() == 1 {
				return errBoom
			}
			return nil
		}, workflow.FuncRetry(&workflow.RetryPolicy{Max: 1}))
		e.up(f.Workflow())
		ctx, cancel := e.callCtx()
		defer cancel()
		ref, err := f.Enqueue(ctx, e.c, funcArg{To: "a@example.com"})
		if err != nil {
			e.t.Fatalf("Func.Enqueue: %v", err)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit/4)
		mu.Lock()
		defer mu.Unlock()
		if calls.count() != 2 || got.To != "a@example.com" || info.Workflow != f.Name() || info.Step != workflow.FuncStep || info.Attempt != 2 {
			e.t.Fatalf("calls %d, arg %+v, StepInfo %+v", calls.count(), got, info)
		}
	}},
	{"FuncJob enqueues a registered Func by name alone", func(e *env) {
		var calls counter
		f := workflow.NewFunc(e.name("byname"), func(_ context.Context, in funcArg) error {
			calls.hit([]byte(in.To))
			return nil
		})
		e.up(f.Workflow())
		job, err := workflow.FuncJob(f.Name(), funcArg{To: "x"})
		if err != nil {
			e.t.Fatalf("FuncJob: %v", err)
		}
		e.waitStatus(e.enqueue(job), workflow.StatusSucceeded, 0)
		if string(calls.input()) != "x" {
			e.t.Fatalf("argument = %q, want x", calls.input())
		}
	}},
	{"FuncCatch runs once with the argument when the Func fails for good", func(e *env) {
		var calls, caught counter
		f := workflow.NewFunc(e.name("fails"), func(_ context.Context, _ funcArg) error {
			calls.hit(nil)
			return errBoom
		}, workflow.FuncCatch(func(_ context.Context, in funcArg) error {
			caught.hit([]byte(in.To))
			return nil
		}))
		e.up(f.Workflow())
		ctx, cancel := e.callCtx()
		defer cancel()
		ref, err := f.Enqueue(ctx, e.c, funcArg{To: "ops"})
		if err != nil {
			e.t.Fatalf("Func.Enqueue: %v", err)
		}
		e.waitStatus(ref, workflow.StatusFailed, 0)
		e.eventually("FuncCatch to run", 0, func() bool { return caught.count() > 0 })
		e.quiet()
		if caught.count() != 1 || string(caught.input()) != "ops" {
			e.t.Fatalf("catch ran %d times with %q, want once with ops", caught.count(), caught.input())
		}
	}},
	{"a Func name no process registered is refused", func(e *env) {
		e.start()
		job, err := workflow.FuncJob(e.name("never-registered"), funcArg{})
		if err != nil {
			e.t.Fatalf("FuncJob: %v", err)
		}
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := e.c.Enqueue(ctx, job); err == nil {
			e.t.Fatal("Enqueue of an unregistered Func name succeeded")
		}
		if _, err := workflow.FuncJob("", nil); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("FuncJob without a name = %v, want ErrInvalid", err)
		}
	}},
}

var retryingErrorCases = []testCase{
	{"a Retrying run reports the failed attempt's error and no FailedStep", func(e *env) {
		e.need(workflow.CapRunStates | workflow.CapRunQuery)
		var c counter
		wf := oneStep(e.name("w"), c.fail(errBoom))
		// A backoff as long as the whole wait keeps the run Retrying while
		// the test reads it, on real time as on a fake clock.
		wf.Retry = workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: e.tm.wait, Factor: 1}}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusRetrying, 0)
		q := as[workflow.RunQuerier](e, "workflow.RunQuerier")
		ctx, cancel := e.callCtx()
		defer cancel()
		page, err := q.Runs(ctx, workflow.RunQuery{Ref: ref})
		if err != nil || len(page.Runs) != 1 {
			e.t.Fatalf("Runs(ref) = %v, %v", page.Runs, err)
		}
		info := page.Runs[0]
		if info.Status != workflow.StatusRetrying {
			e.t.Fatalf("RunInfo.Status = %q, want retrying", info.Status)
		}
		if !strings.Contains(info.Error, errBoom.Error()) {
			e.t.Errorf("RunInfo.Error = %q, want the failed attempt's error %q", info.Error, errBoom)
		}
		if info.FailedStep != "" {
			e.t.Errorf("RunInfo.FailedStep = %q while Retrying, want empty", info.FailedStep)
		}
		if e.c.Capabilities().Has(workflow.CapCancel) {
			if err := e.c.Cancel(ctx, ref); err != nil {
				e.t.Fatalf("Cancel: %v", err)
			}
		}
	}},
}

var labelPrefixCases = []testCase{
	{"Runs matches a label value by prefix", func(e *env) {
		e.need(workflow.CapRunQuery | workflow.CapLabelPrefix)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		want := map[workflow.RunID]bool{}
		for _, order := range []string{"eu-1", "eu-2", "us-1", "e"} {
			ref := e.enqueue(workflow.Job{Workflow: wf.Name, Labels: map[string]string{"order": order}})
			e.waitStatus(ref, workflow.StatusSucceeded, 0)
			if strings.HasPrefix(order, "eu-") {
				want[ref.RunID] = true
			}
		}
		e.enqueue(workflow.Job{Workflow: wf.Name}) // no label: never matched
		q := as[workflow.RunQuerier](e, "workflow.RunQuerier")
		ctx, cancel := e.callCtx()
		defer cancel()
		page, err := q.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, LabelPrefixes: map[string]string{"order": "eu-"}})
		if err != nil {
			e.t.Fatalf("Runs(prefix): %v", err)
		}
		got := map[workflow.RunID]bool{}
		for _, r := range page.Runs {
			got[r.Ref.RunID] = true
		}
		if len(got) != len(want) {
			e.t.Fatalf("Runs(order=eu-*) = %v, want %v", got, want)
		}
		for id := range want {
			if !got[id] {
				e.t.Fatalf("Runs(order=eu-*) misses %s: %v", id, got)
			}
		}
		all, err := q.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, LabelPrefixes: map[string]string{"order": ""}})
		if err != nil || len(all.Runs) != 4 {
			e.t.Fatalf("Runs(order=*) = %d runs, %v; want the 4 labelled runs", len(all.Runs), err)
		}
	}},
	{"LabelPrefixes is refused without CapLabelPrefix", func(e *env) {
		e.need(workflow.CapRunQuery)
		e.lacks(workflow.CapLabelPrefix)
		e.start()
		q := as[workflow.RunQuerier](e, "workflow.RunQuerier")
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := q.Runs(ctx, workflow.RunQuery{LabelPrefixes: map[string]string{"k": "v"}})
		assertUnsupported(e.t, err, "RunQuery.LabelPrefixes", workflow.CapLabelPrefix)
	}},
}

// doneCases pin the order every stop path keeps: a caller woken only by
// Drainer.Done (as Serve, stop-when-idle and recycle callers are) finds
// the client already refusing calls with ErrNotStarted.
var doneCases = []testCase{
	{"calls are refused once Done closes after Drain", func(e *env) {
		e.need(workflow.CapDrain)
		e.up(oneStep(e.name("w"), (&counter{}).echo()))
		d := as[workflow.Drainer](e, "workflow.Drainer")
		go func() {
			ctx, cancel := e.callCtx()
			defer cancel()
			_ = d.Drain(ctx)
		}()
		e.assertRefusedOnceDone(d)
	}},
	{"calls are refused once Done closes after the Start ctx ends", func(e *env) {
		e.need(workflow.CapDrain)
		e.up(oneStep(e.name("w"), (&counter{}).echo()))
		d := as[workflow.Drainer](e, "workflow.Drainer")
		e.stop()
		e.assertRefusedOnceDone(d)
	}},
}

// assertRefusedOnceDone waits on d.Done only, then expects Enqueue,
// Signal and (with CapCron) Schedule to return ErrNotStarted.
func (e *env) assertRefusedOnceDone(d workflow.Drainer) {
	e.t.Helper()
	select {
	case <-d.Done():
	case <-time.After(e.tm.wait):
		e.t.Fatal("Done was not closed")
	}
	ctx, cancel := e.callCtx()
	defer cancel()
	wf := e.wfs[0].Name
	if _, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf}); !errors.Is(err, workflow.ErrNotStarted) {
		e.t.Fatalf("Enqueue once Done closed = %v, want ErrNotStarted", err)
	}
	if err := e.c.Signal(ctx, workflow.Event{Name: "e"}); !errors.Is(err, workflow.ErrNotStarted) {
		e.t.Fatalf("Signal once Done closed = %v, want ErrNotStarted", err)
	}
	if e.c.Capabilities().Has(workflow.CapCron) {
		if _, err := e.c.Schedule(ctx, workflow.Schedule{Workflow: wf, Cron: "* * * * *", Location: time.UTC}); !errors.Is(err, workflow.ErrNotStarted) {
			e.t.Fatalf("Schedule once Done closed = %v, want ErrNotStarted", err)
		}
	}
}
