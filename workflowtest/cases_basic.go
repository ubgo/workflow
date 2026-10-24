package workflowtest

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

var lifecycleCases = []testCase{
	{"Enqueue before Start is ErrNotStarted", func(e *env) {
		var c counter
		e.register(oneStep(e.name("w"), c.echo()))
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := e.c.Enqueue(ctx, workflow.Job{Workflow: e.name("w")}); !errors.Is(err, workflow.ErrNotStarted) {
			e.t.Fatalf("Enqueue before Start: err = %v, want ErrNotStarted", err)
		}
	}},
	{"Register after Start and a second Start are ErrAlreadyStarted", func(e *env) {
		var c counter
		e.up(oneStep(e.name("w"), c.echo()))
		if err := e.c.Register(oneStep(e.name("late"), c.echo())); !errors.Is(err, workflow.ErrAlreadyStarted) {
			e.t.Errorf("Register after Start: err = %v, want ErrAlreadyStarted", err)
		}
		if err := e.tryStart(); !errors.Is(err, workflow.ErrAlreadyStarted) {
			e.t.Errorf("second Start: err = %v, want ErrAlreadyStarted", err)
		}
	}},
	{"an invalid workflow is ErrInvalid", func(e *env) {
		if err := e.c.Register(workflow.Workflow{Name: e.name("w")}); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("Register of a workflow with no steps: err = %v, want ErrInvalid", err)
		}
	}},
	{"an unknown workflow is ErrNotFound", func(e *env) {
		var c counter
		e.up(oneStep(e.name("w"), c.echo()))
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := e.c.Enqueue(ctx, workflow.Job{Workflow: e.name("never-registered")}); !errors.Is(err, workflow.ErrNotFound) {
			e.t.Fatalf("Enqueue of an unknown workflow: err = %v, want ErrNotFound", err)
		}
	}},
	{"an unknown run is ErrNotFound", func(e *env) {
		var c counter
		e.up(oneStep(e.name("w"), c.echo()))
		// UUID-shaped, so an engine that validates the ID format still
		// has to look it up.
		ref := workflow.RunRef{RunID: "00000000-0000-4000-8000-000000000000"}
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := e.c.Status(ctx, ref); !errors.Is(err, workflow.ErrNotFound) {
			e.t.Errorf("Status of an unknown run: err = %v, want ErrNotFound", err)
		}
		if e.c.Capabilities().Has(workflow.CapCancel) {
			if err := e.c.Cancel(ctx, ref); !errors.Is(err, workflow.ErrNotFound) {
				e.t.Errorf("Cancel of an unknown run: err = %v, want ErrNotFound", err)
			}
		}
	}},
	{"Capabilities never change", func(e *env) {
		before := e.c.Capabilities()
		var c counter
		e.up(oneStep(e.name("w"), c.echo()))
		if after := e.c.Capabilities(); after != before {
			e.t.Fatalf("Capabilities changed across Start: %s, then %s", before, after)
		}
	}},
}

var runCases = []testCase{
	{"Enqueue runs the workflow once and it succeeds", func(e *env) {
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		payload := []byte(`{"hello":"world"}`)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, Payload: payload})
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("step ran %d times, want 1", got)
		}
		if got := c.input(); !bytes.Equal(got, payload) {
			e.t.Fatalf("first step input = %q, want the payload %q", got, payload)
		}
	}},
	{"Status passes only through valid states to a terminal one", func(e *env) {
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		var seen []workflow.Status
		e.eventually("a terminal status", 0, func() bool {
			st := e.status(ref)
			if len(seen) == 0 || seen[len(seen)-1] != st {
				seen = append(seen, st)
			}
			return st.IsTerminal()
		})
		if last := seen[len(seen)-1]; last != workflow.StatusSucceeded {
			e.t.Fatalf("statuses %v ended %q, want succeeded", seen, last)
		}
		e.quiet()
		if st := e.status(ref); st != workflow.StatusSucceeded {
			e.t.Fatalf("terminal status changed from succeeded to %q", st)
		}
	}},
	{"DAG steps start only after all their parents, with their outputs", func(e *env) {
		// a -> b, a -> c, (b, c) -> d.
		var mu sync.Mutex
		var seq int
		started, ended := map[string]int{}, map[string]int{}
		inputs := map[string][]byte{}
		step := func(name string) workflow.Handler {
			return func(_ context.Context, in []byte) ([]byte, error) {
				mu.Lock()
				seq++
				started[name] = seq
				inputs[name] = append([]byte(nil), in...)
				mu.Unlock()
				out := []byte(`"` + name + `-out"`)
				mu.Lock()
				seq++
				ended[name] = seq
				mu.Unlock()
				return out, nil
			}
		}
		wf := workflow.Workflow{Name: e.name("w"), Steps: []workflow.Step{
			{Name: "a", Run: step("a")},
			{Name: "b", After: []string{"a"}, Run: step("b")},
			{Name: "c", After: []string{"a"}, Run: step("c")},
			{Name: "d", After: []string{"b", "c"}, Run: step("d")},
		}}
		e.up(wf)
		payload := []byte(`"payload"`)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name, Payload: payload}), workflow.StatusSucceeded, 0)
		mu.Lock()
		defer mu.Unlock()
		for _, s := range wf.Steps {
			if _, ok := started[s.Name]; !ok {
				e.t.Fatalf("step %s never ran", s.Name)
			}
			for _, p := range s.After {
				if started[s.Name] < ended[p] {
					e.t.Errorf("step %s started before its parent %s ended", s.Name, p)
				}
			}
		}
		outs := map[string][]byte{"a": []byte(`"a-out"`), "b": []byte(`"b-out"`), "c": []byte(`"c-out"`)}
		for _, s := range wf.Steps {
			if want := workflow.StepInput(payload, s.After, outs); !bytes.Equal(inputs[s.Name], want) {
				e.t.Errorf("step %s input = %s, want %s (workflow.StepInput)", s.Name, inputs[s.Name], want)
			}
		}
	}},
}

var idempotencyCases = []testCase{
	{"the same key while in flight returns the same run", func(e *env) {
		e.need(workflow.CapIdempotencyKey)
		g := newGate(e.t)
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			return in, g.wait(ctx)
		})
		e.up(wf)
		key := e.name("key")
		first := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		ctx, cancel := e.callCtx()
		defer cancel()
		second, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		switch {
		case errors.Is(err, workflow.ErrDuplicate):
		case err != nil:
			e.t.Fatalf("second Enqueue with the same key: err = %v, want nil (same RunRef) or ErrDuplicate", err)
		case second != first:
			e.t.Fatalf("second Enqueue with the same key returned %+v, want the first run %+v", second, first)
		}
		g.open()
		e.waitStatus(first, workflow.StatusSucceeded, 0)
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("step ran %d times for one key, want 1", got)
		}
	}},
	{"the key is released once its run is terminal", func(e *env) {
		e.need(workflow.CapIdempotencyKey)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		key := e.name("key")
		first := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		e.waitStatus(first, workflow.StatusSucceeded, 0)
		second := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		if second == first {
			e.t.Fatalf("a finished run's key still deduplicated: got the old run %+v", first)
		}
		e.waitStatus(second, workflow.StatusSucceeded, 0)
		if got := c.count(); got != 2 {
			e.t.Fatalf("step ran %d times, want 2", got)
		}
	}},
	{"IdempotencyOff ignores the key", func(e *env) {
		// RequiresJob counts any key, whatever the policy, so a driver
		// without the capability refuses the job before reading the policy.
		e.need(workflow.CapIdempotencyKey)
		g := newGate(e.t)
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			return in, g.wait(ctx)
		})
		wf.Idempotency = workflow.IdempotencyOff
		e.up(wf)
		key := e.name("key")
		first := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		second := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		if first == second {
			e.t.Fatalf("IdempotencyOff deduplicated the key: both enqueues returned %+v", first)
		}
		g.open()
		e.waitStatus(first, workflow.StatusSucceeded, 0)
		e.waitStatus(second, workflow.StatusSucceeded, 0)
	}},
	{"IdempotencyRequired refuses a job without a key", func(e *env) {
		e.need(workflow.CapIdempotencyKey)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Idempotency = workflow.IdempotencyRequired
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name}); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("Enqueue without a key: err = %v, want ErrInvalid", err)
		}
	}},
}

var delayedCases = []testCase{
	{"RunAt does not run before its time and runs after", func(e *env) {
		e.need(workflow.CapDelayedStart)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		u := e.tm.unit
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, RunAt: e.now().Add(u)})
		if st := e.status(ref); st != workflow.StatusScheduled {
			e.t.Errorf("status of a delayed run = %q, want scheduled", st)
		}
		if e.clk != nil {
			e.clk.Advance(u / 2)
			e.quiet()
		} else {
			time.Sleep(u / 2)
		}
		if got := c.count(); got != 0 {
			e.t.Fatalf("delayed step ran %d times before RunAt", got)
		}
		if st := e.status(ref); st.IsTerminal() || st == workflow.StatusRunning {
			e.t.Fatalf("status before RunAt = %q, want it not started", st)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, u/4)
		if got := c.count(); got != 1 {
			e.t.Fatalf("delayed step ran %d times, want 1", got)
		}
	}},
}

var retryCases = []testCase{
	{"retry counts follow the step, workflow and RetriesNone rules", func(e *env) {
		var own, inherits, none counter
		wfOwn := oneStep(e.name("own"), own.fail(errBoom))
		wfOwn.Retry = workflow.RetryPolicy{Max: 5}
		wfOwn.Steps[0].Retry = &workflow.RetryPolicy{Max: 2}
		wfInherits := oneStep(e.name("inherits"), inherits.fail(errBoom))
		wfInherits.Retry = workflow.RetryPolicy{Max: 1}
		wfNone := oneStep(e.name("none"), none.fail(errBoom))
		wfNone.Retry = workflow.RetryPolicy{Max: 3}
		wfNone.Steps[0].Retry = workflow.RetriesNone()
		e.up(wfOwn, wfInherits, wfNone)
		for _, tc := range []struct {
			wf   workflow.Workflow
			c    *counter
			want int
		}{{wfOwn, &own, 3}, {wfInherits, &inherits, 2}, {wfNone, &none, 1}} {
			ref := e.enqueue(workflow.Job{Workflow: tc.wf.Name})
			e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
			e.quiet()
			if got := tc.c.count(); got != tc.want {
				e.t.Errorf("%s: %d attempts, want %d", tc.wf.Name, got, tc.want)
			}
		}
	}},
	{"StepInfo names the workflow, step, run, attempt and labels", func(e *env) {
		var mu sync.Mutex
		var infos []workflow.StepInfo
		var oks []bool
		wf := workflow.Workflow{Name: e.name("w"), Retry: workflow.RetryPolicy{Max: 1}}
		wf.Steps = []workflow.Step{{Name: "only", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := workflow.StepInfoFrom(ctx)
			mu.Lock()
			defer mu.Unlock()
			infos = append(infos, info)
			oks = append(oks, ok)
			if len(infos) == 1 {
				return nil, errBoom
			}
			return in, nil
		}}}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, Labels: map[string]string{"tenant": "t1"}})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		mu.Lock()
		defer mu.Unlock()
		if len(infos) != 2 {
			e.t.Fatalf("step ran %d times, want 2 (fail, then succeed)", len(infos))
		}
		for i, info := range infos {
			if !oks[i] {
				e.t.Fatalf("attempt %d: no StepInfo on the handler's ctx", i+1)
			}
			if info.Workflow != wf.Name || info.Step != "only" {
				e.t.Errorf("attempt %d: StepInfo names %s/%s, want %s/only", i+1, info.Workflow, info.Step, wf.Name)
			}
			if info.RunID != ref.RunID {
				e.t.Errorf("attempt %d: StepInfo.RunID = %q, want the enqueued run %q", i+1, info.RunID, ref.RunID)
			}
			if info.Attempt != i+1 {
				e.t.Errorf("attempt %d: StepInfo.Attempt = %d", i+1, info.Attempt)
			}
			if info.Labels["tenant"] != "t1" {
				e.t.Errorf("attempt %d: StepInfo.Labels = %v, want tenant=t1", i+1, info.Labels)
			}
		}
	}},
	{"Permanent stops retries", func(e *env) {
		var c counter
		wf := oneStep(e.name("w"), c.fail(workflow.Permanent(errBoom)))
		wf.Retry = workflow.RetryPolicy{Max: 3}
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusFailed, e.tm.unit)
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("a Permanent error was tried %d times, want 1", got)
		}
	}},
	{"a flaky step succeeds within its retries", func(e *env) {
		var c counter
		wf := oneStep(e.name("w"), func(_ context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if c.count() < 3 {
				return nil, errBoom
			}
			return in, nil
		})
		wf.Retry = workflow.RetryPolicy{Max: 2}
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusSucceeded, e.tm.unit)
		if got := c.count(); got != 3 {
			e.t.Fatalf("%d attempts, want 3", got)
		}
	}},
}

// blockUntilDone is a handler that counts, then waits for ctx to end and
// records that it did.
func blockUntilDone(c *counter, ended *counter) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		c.hit(in)
		<-ctx.Done()
		ended.hit(nil)
		return nil, ctx.Err()
	}
}

var timeoutCases = []testCase{
	{"a step timeout fails the step", func(e *env) {
		var c, ended counter
		wf := oneStep(e.name("step"), blockUntilDone(&c, &ended))
		wf.Steps[0].Timeout = e.tm.unit
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusFailed, e.tm.unit)
		if c.count() != 1 {
			e.t.Errorf("step ran %d times, want 1 (no retries)", c.count())
		}
		if ended.count() != 1 {
			e.t.Errorf("the step's context was not cancelled by the timeout")
		}
	}},
	{"the workflow Timeout applies to steps without their own", func(e *env) {
		var c, ended counter
		wf := oneStep(e.name("wf"), blockUntilDone(&c, &ended))
		wf.Timeout = e.tm.unit
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusFailed, e.tm.unit)
		if ended.count() != 1 {
			e.t.Errorf("the step's context was not cancelled by the workflow timeout")
		}
	}},
}

var onFailureCases = []testCase{
	{"OnFailure runs once when the run fails, with the failing step's input", func(e *env) {
		var c, hook counter
		wf := oneStep(e.name("w"), c.fail(errBoom))
		wf.Retry = workflow.RetryPolicy{Max: 2}
		wf.OnFailure = hook.echo()
		e.up(wf)
		payload := []byte(`"input"`)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name, Payload: payload}), workflow.StatusFailed, e.tm.unit)
		e.eventually("OnFailure to run", 0, func() bool { return hook.count() > 0 })
		e.quiet()
		if got := hook.count(); got != 1 {
			e.t.Fatalf("OnFailure ran %d times, want 1", got)
		}
		if got := hook.input(); !bytes.Equal(got, payload) {
			e.t.Fatalf("OnFailure input = %q, want the failing step's input %q", got, payload)
		}
	}},
	{"OnFailure does not run when the run succeeds", func(e *env) {
		var c, hook counter
		wf := oneStep(e.name("w"), c.echo())
		wf.OnFailure = hook.echo()
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusSucceeded, 0)
		e.quiet()
		if got := hook.count(); got != 0 {
			e.t.Fatalf("OnFailure ran %d times after a success", got)
		}
	}},
}
