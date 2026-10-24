package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// asOpt returns the client as T, skipping the subtest when it does not
// implement T: the interfaces below carry no capability, so not
// implementing one is a driver's honest answer, not a defect.
func asOpt[T any](e *env, what string) T {
	e.t.Helper()
	v, ok := e.c.(T)
	if !ok {
		e.t.Skipf("the client does not implement %s", what)
	}
	return v
}

// skipUnsupported skips the subtest when err is the driver's refusal of
// an interface it implements only to forward (workflow.Forward).
func (e *env) skipUnsupported(err error, what string) {
	e.t.Helper()
	if errors.Is(err, workflow.ErrUnsupported) {
		e.t.Skipf("%s: %v", what, err)
	}
}

// runError returns RunInfo.Error when the driver lists runs, else "".
func (e *env) runError(ref workflow.RunRef) string {
	if !e.c.Capabilities().Has(workflow.CapRunQuery) {
		return ""
	}
	return e.runInfo(ref).Error
}

var snoozeCases = []testCase{
	{"a snoozed step runs again as the same attempt and spends none", func(e *env) {
		e.need(workflow.CapSnooze)
		var s stepInfos
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if c.count() == 1 {
				return nil, workflow.Snooze(e.tm.unit)
			}
			return s.handler()(ctx, in)
		})
		wf.Retry = *workflow.RetriesNone() // a snooze counted as a failure would fail the run
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := s.last().Attempt; got != 1 {
			e.t.Errorf("the step ran again as attempt %d, want 1", got)
		}
		if e.c.Capabilities().Has(workflow.CapRunQuery) {
			info := e.runInfo(ref)
			if info.Attempts != 1 {
				e.t.Errorf("RunInfo.Attempts = %d, want 1 (a snooze is not an attempt)", info.Attempts)
			}
			if len(info.Errors) != 0 {
				e.t.Errorf("RunInfo.Errors = %+v, want none for a snooze", info.Errors)
			}
		}
	}},
	{"a snoozed run waits, then runs", func(e *env) {
		e.need(workflow.CapSnooze)
		var c counter
		wf := oneStep(e.name("w"), func(_ context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if c.count() == 1 {
				return nil, workflow.Snooze(time.Hour)
			}
			return in, nil
		})
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusWaiting, 0)
		e.quiet()
		if c.count() != 1 {
			e.t.Fatalf("the snoozed step ran %d times before its hour passed, want 1", c.count())
		}
		e.needFake("ending an hour-long snooze")
		e.clk.Advance(time.Hour)
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
	}},
}

var heartbeatCases = []testCase{
	{"an attempt that stops beating fails with ErrHeartbeatTimeout", func(e *env) {
		e.need(workflow.CapHeartbeat)
		wf := oneStep(e.name("w"), func(ctx context.Context, _ []byte) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
		wf.Steps[0].HeartbeatTimeout = 2 * e.tm.unit
		wf.Retry = *workflow.RetriesNone()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		if msg := e.runError(ref); msg != "" && !strings.Contains(msg, workflow.ErrHeartbeatTimeout.Error()) {
			e.t.Errorf("RunInfo.Error = %q, want it to name %q", msg, workflow.ErrHeartbeatTimeout)
		}
	}},
	{"an attempt that keeps beating outlives its HeartbeatTimeout", func(e *env) {
		e.need(workflow.CapHeartbeat)
		hb := 3 * e.tm.unit
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			start := e.now()
			for e.now().Sub(start) < 3*hb {
				if err := workflow.Heartbeat(ctx, nil); err != nil {
					return nil, err
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(e.tm.poll):
				}
			}
			return in, nil
		})
		wf.Steps[0].HeartbeatTimeout = hb
		wf.Retry = *workflow.RetriesNone()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
	}},
	{"Heartbeat outside a step is a no-op", func(e *env) {
		if err := workflow.Heartbeat(context.Background(), nil); err != nil {
			e.t.Fatalf("Heartbeat outside a step = %v, want nil", err)
		}
	}},
}

var tenantCases = []testCase{
	{"Job.Tenant reaches the step and the run listing", func(e *env) {
		e.need(workflow.CapTenant)
		var s stepInfos
		wf := oneStep(e.name("w"), s.handler())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, Tenant: "acme"})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := s.last().Tenant; got != "acme" {
			e.t.Errorf("StepInfo.Tenant = %q, want acme", got)
		}
		if !e.c.Capabilities().Has(workflow.CapRunQuery) {
			return
		}
		if got := e.runInfo(ref).Tenant; got != "acme" {
			e.t.Errorf("RunInfo.Tenant = %q, want acme", got)
		}
		ctx, cancel := e.callCtx()
		defer cancel()
		q := as[workflow.RunQuerier](e, "workflow.RunQuerier")
		page, err := q.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, Tenant: "acme"})
		if err != nil || len(page.Runs) != 1 {
			e.t.Errorf("Runs(Tenant acme) = %d runs, %v; want 1", len(page.Runs), err)
		}
		page, err = q.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, Tenant: "other"})
		if err != nil || len(page.Runs) != 0 {
			e.t.Errorf("Runs(Tenant other) = %d runs, %v; want 0", len(page.Runs), err)
		}
	}},
	{"a driver without CapTenant refuses Job.Tenant", func(e *env) {
		e.lacks(workflow.CapTenant)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Tenant: "acme"})
		assertUnsupported(e.t, err, "Job.Tenant", workflow.CapTenant)
	}},
	{"a schedule's Tenant is the tenant of every run it fires", func(e *env) {
		e.need(workflow.CapTenant | workflow.CapCron)
		e.needFake("firing a tick")
		var s stepInfos
		wf := oneStep(e.name("w"), s.handler())
		e.up(wf)
		id := e.schedule(workflow.Schedule{Workflow: wf.Name, Cron: everyMinute, Location: time.UTC, Tenant: "acme"})
		e.advanceMinutes(1)
		e.eventually("the fired run", 0, func() bool { return s.last().Step != "" })
		if got := s.last().Tenant; got != "acme" {
			e.t.Errorf("fired run StepInfo.Tenant = %q, want acme", got)
		}
		if e.c.Capabilities().Has(workflow.CapScheduleList) {
			if got := e.scheduleInfo(id).Tenant; got != "acme" {
				e.t.Errorf("ScheduleInfo.Tenant = %q, want acme", got)
			}
		}
		if !e.c.Capabilities().Has(workflow.CapScheduleTrigger) {
			return
		}
		ref := e.mustTrigger(id)
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if e.c.Capabilities().Has(workflow.CapRunQuery) {
			if got := e.runInfo(ref).Tenant; got != "acme" {
				e.t.Errorf("triggered run RunInfo.Tenant = %q, want acme", got)
			}
		}
	}},
	{"a driver without CapTenant refuses Schedule.Tenant", func(e *env) {
		e.need(workflow.CapCron)
		e.lacks(workflow.CapTenant)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Cron: everyMinute, Location: time.UTC, Tenant: "acme"})
		assertUnsupported(e.t, err, "Schedule.Tenant", workflow.CapTenant)
	}},
	{"an invalid tenant is refused", func(e *env) {
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Tenant: "a b"}); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("Enqueue with tenant %q = %v, want ErrInvalid", "a b", err)
		}
	}},
}

var runTimeoutCases = []testCase{
	{"RunTimeout fails a run that runs too long and runs OnTimeout, not OnFailure", func(e *env) {
		e.need(workflow.CapRunTimeout)
		var onTimeout, onFailure counter
		wf := oneStep(e.name("w"), func(ctx context.Context, _ []byte) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
		wf.RunTimeout = 2 * e.tm.unit
		wf.OnTimeout = onTimeout.echo()
		wf.OnFailure = onFailure.echo()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		e.eventually("OnTimeout to run", e.tm.unit, func() bool { return onTimeout.count() == 1 })
		e.quiet()
		if onFailure.count() != 0 {
			e.t.Errorf("OnFailure ran %d times for a timeout, want 0 (OnTimeout replaces it)", onFailure.count())
		}
		if msg := e.runError(ref); msg != "" && !strings.Contains(msg, workflow.ErrRunTimeout.Error()) {
			e.t.Errorf("RunInfo.Error = %q, want it to name %q", msg, workflow.ErrRunTimeout)
		}
	}},
	{"a step timeout is a timeout: OnTimeout runs", func(e *env) {
		e.need(workflow.CapRunTimeout)
		var onTimeout, onFailure counter
		wf := oneStep(e.name("w"), func(ctx context.Context, _ []byte) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
		wf.Steps[0].Timeout = e.tm.unit
		wf.Retry = *workflow.RetriesNone()
		wf.OnTimeout = onTimeout.echo()
		wf.OnFailure = onFailure.echo()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		e.eventually("OnTimeout to run", e.tm.unit, func() bool { return onTimeout.count() == 1 })
		if onFailure.count() != 0 {
			e.t.Errorf("OnFailure ran for a step timeout while OnTimeout is set")
		}
	}},
	{"an ordinary failure still runs OnFailure", func(e *env) {
		e.need(workflow.CapRunTimeout)
		var c, onTimeout, onFailure counter
		wf := oneStep(e.name("w"), c.fail(workflow.Permanent(errBoom)))
		wf.OnTimeout = onTimeout.echo()
		wf.OnFailure = onFailure.echo()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		e.eventually("OnFailure to run", e.tm.unit, func() bool { return onFailure.count() == 1 })
		if onTimeout.count() != 0 {
			e.t.Errorf("OnTimeout ran for an error that is not a timeout")
		}
	}},
}

var nodeCases = []testCase{
	{"each attempt names its node, and Nodes lists it", func(e *env) {
		e.need(workflow.CapNodeIdentity)
		var s stepInfos
		wf := oneStep(e.name("w"), s.handler())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		node := s.last().Node
		if node == "" {
			e.t.Fatal("StepInfo.Node is empty on a driver with CapNodeIdentity")
		}
		if e.c.Capabilities().Has(workflow.CapRunQuery) {
			if got := e.runInfo(ref).AttemptedBy; !slices.Contains(got, node) {
				e.t.Errorf("RunInfo.AttemptedBy = %v, want it to hold %q", got, node)
			}
		}
		lister := as[workflow.NodeLister](e, "workflow.NodeLister") // CapNodeIdentity implies it
		e.eventually("Nodes to list "+node, e.tm.unit, func() bool {
			ctx, cancel := e.callCtx()
			defer cancel()
			nodes, err := lister.Nodes(ctx)
			if err != nil {
				e.t.Fatalf("Nodes: %v", err)
			}
			return slices.ContainsFunc(nodes, func(n workflow.NodeInfo) bool { return n.ID == node })
		})
	}},
	{"a failed attempt names its node in the error history", func(e *env) {
		e.need(workflow.CapNodeIdentity | workflow.CapErrorHistory)
		var c counter
		wf := oneStep(e.name("w"), c.fail(workflow.Permanent(errBoom)))
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		errs := e.runInfo(ref).Errors
		if len(errs) == 0 || errs[0].Node == "" {
			e.t.Fatalf("RunInfo.Errors = %+v, want the attempt with its Node", errs)
		}
	}},
}

var keyFIFOCases = []testCase{
	{"runs of one key start in enqueue order", func(e *env) {
		e.need(workflow.CapKeyFIFO)
		const n = 5
		g := newGate(e.t)
		var mu sync.Mutex
		var order []string
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			mu.Lock()
			order = append(order, string(in))
			first := len(order) == 1
			mu.Unlock()
			if first {
				if err := g.wait(ctx); err != nil {
					return nil, err
				}
			}
			return in, nil
		})
		wf.Concurrency = &workflow.Concurrency{Key: e.h.keyFromLabel("k"), MaxRuns: 1, FIFO: true}
		e.up(wf)
		labels := map[string]string{"k": "one"}
		refs := []workflow.RunRef{e.enqueue(workflow.Job{Workflow: wf.Name, Payload: []byte(`0`), Labels: labels})}
		e.eventually("the first run to start", 0, func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 1 })
		for i := 1; i < n; i++ {
			refs = append(refs, e.enqueue(workflow.Job{Workflow: wf.Name, Payload: []byte(strconv.Itoa(i)), Labels: labels}))
		}
		e.quiet()
		g.open()
		for _, ref := range refs {
			e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		}
		mu.Lock()
		defer mu.Unlock()
		for i, got := range order {
			if got != strconv.Itoa(i) {
				e.t.Fatalf("start order = %v, want 0..%d in enqueue order", order, n-1)
			}
		}
	}},
}

var dueQueryCases = []testCase{
	{"DueFrom and DueTo list the delayed runs due in a window", func(e *env) {
		e.need(workflow.CapDueQuery | workflow.CapDelayedStart | workflow.CapRunQuery)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		now := e.now().Truncate(time.Second)
		soon, later := now.Add(time.Hour), now.Add(3*time.Hour)
		a := e.enqueue(workflow.Job{Workflow: wf.Name, RunAt: soon})
		e.enqueue(workflow.Job{Workflow: wf.Name, RunAt: later})
		e.enqueue(workflow.Job{Workflow: wf.Name}) // now: no RunAt, never matches
		ctx, cancel := e.callCtx()
		defer cancel()
		page, err := as[workflow.RunQuerier](e, "workflow.RunQuerier").Runs(ctx, workflow.RunQuery{
			Workflow: wf.Name, DueFrom: now.Add(30 * time.Minute), DueTo: now.Add(2 * time.Hour)})
		if err != nil {
			e.t.Fatalf("Runs(Due): %v", err)
		}
		if len(page.Runs) != 1 || page.Runs[0].Ref.ScheduledID != a.ScheduledID {
			e.t.Fatalf("Runs(Due window) = %+v, want only the run due in an hour", page.Runs)
		}
		if got := page.Runs[0].RunAt; got.Sub(soon).Abs() > time.Millisecond {
			e.t.Errorf("RunInfo.RunAt = %s, want %s", got, soon)
		}
	}},
	{"a driver without CapDueQuery refuses DueFrom", func(e *env) {
		e.need(workflow.CapRunQuery)
		e.lacks(workflow.CapDueQuery)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := as[workflow.RunQuerier](e, "workflow.RunQuerier").Runs(ctx, workflow.RunQuery{DueFrom: e.now()})
		assertUnsupported(e.t, err, "RunQuery.Due", workflow.CapDueQuery)
	}},
}

var resultCases = []testCase{
	{"a one-step run's result is its handler's output", func(e *env) {
		e.need(workflow.CapResult)
		wf := oneStep(e.name("w"), func(context.Context, []byte) ([]byte, error) { return []byte(`{"ok":true}`), nil })
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		ctx, cancel := e.callCtx()
		defer cancel()
		out, err := workflow.AwaitResult(ctx, e.c, ref, workflow.AwaitOptions{Poll: e.tm.poll, Sleep: e.sleeper()})
		if err != nil {
			e.t.Fatalf("AwaitResult: %v", err)
		}
		if string(out) != `{"ok":true}` {
			e.t.Errorf("result = %s, want {\"ok\":true}", out)
		}
		if e.c.Capabilities().Has(workflow.CapRunQuery) {
			if got := e.runInfo(ref).Output; string(got) != `{"ok":true}` {
				e.t.Errorf("RunInfo.Output = %s, want the result", got)
			}
		}
	}},
	{"several final steps give a JSON object keyed by step", func(e *env) {
		e.need(workflow.CapResult)
		wf := workflow.Workflow{Name: e.name("w"), Steps: []workflow.Step{
			{Name: "x", Run: func(context.Context, []byte) ([]byte, error) { return []byte(`1`), nil }},
			{Name: "y", Run: func(context.Context, []byte) ([]byte, error) { return []byte(`2`), nil }},
		}}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		ctx, cancel := e.callCtx()
		defer cancel()
		out, err := as[workflow.ResultReader](e, "workflow.ResultReader").Result(ctx, ref)
		if err != nil {
			e.t.Fatalf("Result: %v", err)
		}
		var got map[string]int
		if err := json.Unmarshal(out, &got); err != nil || got["x"] != 1 || got["y"] != 2 {
			e.t.Errorf("result = %s, want {\"x\":1,\"y\":2}", out)
		}
	}},
	{"a failed run has no result", func(e *env) {
		e.need(workflow.CapResult)
		var c counter
		wf := oneStep(e.name("w"), c.fail(workflow.Permanent(errBoom)))
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		ctx, cancel := e.callCtx()
		defer cancel()
		if _, err := as[workflow.ResultReader](e, "workflow.ResultReader").Result(ctx, ref); !errors.Is(err, workflow.ErrWrongState) {
			e.t.Errorf("Result of a failed run = %v, want ErrWrongState", err)
		}
		var rf *workflow.RunFailedError
		if _, err := workflow.AwaitResult(ctx, e.c, ref, workflow.AwaitOptions{Poll: e.tm.poll}); !errors.As(err, &rf) || rf.Status != workflow.StatusFailed {
			e.t.Errorf("AwaitResult of a failed run = %v, want a RunFailedError with StatusFailed", err)
		}
	}},
}

// sleeper is an AwaitOptions.Sleep that moves a fake clock by one unit per
// poll, so a run waiting on the clock still progresses.
func (e *env) sleeper() func(ctx context.Context, d time.Duration) error {
	return func(ctx context.Context, d time.Duration) error {
		if e.clk != nil {
			e.clk.Advance(e.tm.unit)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.tm.poll):
			return nil
		}
	}
}

var rescheduleCases = []testCase{
	{"Reschedule moves a delayed run earlier", func(e *env) {
		e.need(workflow.CapDelayedStart)
		resch := asOpt[workflow.RunRescheduler](e, "workflow.RunRescheduler")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, RunAt: e.now().Add(24 * time.Hour)})
		ctx, cancel := e.callCtx()
		defer cancel()
		at := e.now().Add(e.tm.unit)
		got, err := resch.Reschedule(ctx, ref, at)
		e.skipUnsupported(err, "Reschedule")
		if err != nil {
			e.t.Fatalf("Reschedule: %v", err)
		}
		e.waitStatus(got, workflow.StatusSucceeded, e.tm.unit)
		if e.c.Capabilities().Has(workflow.CapDueQuery) {
			if ra := e.runInfo(got).RunAt; ra.Sub(at).Abs() > time.Millisecond {
				e.t.Errorf("RunInfo.RunAt = %s, want the new time %s", ra, at)
			}
		}
	}},
	{"Reschedule of a run that is not scheduled is refused", func(e *env) {
		resch := asOpt[workflow.RunRescheduler](e, "workflow.RunRescheduler")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := resch.Reschedule(ctx, ref, e.now().Add(time.Hour))
		e.skipUnsupported(err, "Reschedule")
		if !errors.Is(err, workflow.ErrWrongState) {
			e.t.Fatalf("Reschedule of a finished run = %v, want ErrWrongState", err)
		}
	}},
}

// cancelStepRun starts a run whose one step blocks until cancelled, and
// returns it once the step is running.
func (e *env) cancelStepRun(onCancel func(ctx context.Context) ([]byte, error)) (workflow.StepCanceller, workflow.RunRef) {
	e.t.Helper()
	sc := asOpt[workflow.StepCanceller](e, "workflow.StepCanceller")
	var c counter
	wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
		c.hit(in)
		<-ctx.Done()
		if errors.Is(context.Cause(ctx), workflow.ErrStepCancelled) {
			return onCancel(ctx)
		}
		return nil, ctx.Err()
	})
	wf.Retry = workflow.RetryPolicy{Max: 3}
	e.up(wf)
	ref := e.enqueue(workflow.Job{Workflow: wf.Name})
	e.eventually("the step to start", 0, func() bool { return c.count() > 0 })
	return sc, ref
}

var cancelStepCases = []testCase{
	{"a cancelled step that returns a fallback lets the run succeed", func(e *env) {
		sc, ref := e.cancelStepRun(func(context.Context) ([]byte, error) { return []byte(`"fallback"`), nil })
		ctx, cancel := e.callCtx()
		defer cancel()
		err := sc.CancelStep(ctx, ref, "only")
		e.skipUnsupported(err, "CancelStep")
		if err != nil {
			e.t.Fatalf("CancelStep: %v", err)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
	}},
	{"a cancelled step that returns an error fails the run without retry", func(e *env) {
		sc, ref := e.cancelStepRun(func(ctx context.Context) ([]byte, error) { return nil, context.Cause(ctx) })
		ctx, cancel := e.callCtx()
		defer cancel()
		err := sc.CancelStep(ctx, ref, "only")
		e.skipUnsupported(err, "CancelStep")
		if err != nil {
			e.t.Fatalf("CancelStep: %v", err)
		}
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		if msg := e.runError(ref); msg != "" && !strings.Contains(msg, workflow.ErrStepCancelled.Error()) {
			e.t.Errorf("RunInfo.Error = %q, want it to name %q", msg, workflow.ErrStepCancelled)
		}
	}},
	{"CancelStep of an unknown step or a finished run is refused", func(e *env) {
		sc := asOpt[workflow.StepCanceller](e, "workflow.StepCanceller")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		ctx, cancel := e.callCtx()
		defer cancel()
		err := sc.CancelStep(ctx, ref, "only")
		e.skipUnsupported(err, "CancelStep")
		if !errors.Is(err, workflow.ErrWrongState) {
			e.t.Errorf("CancelStep of a finished run = %v, want ErrWrongState", err)
		}
		if err := sc.CancelStep(ctx, ref, "nope"); !errors.Is(err, workflow.ErrNotFound) && !errors.Is(err, workflow.ErrWrongState) {
			e.t.Errorf("CancelStep of an unknown step = %v, want ErrNotFound", err)
		}
	}},
}

var progressCases = []testCase{
	{"progress reported by a step is kept with the run", func(e *env) {
		e.need(workflow.CapRunQuery)
		g := newGate(e.t)
		reported := make(chan error, 1)
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			reported <- workflow.ReportProgress(ctx, workflow.Progress{Percent: 50, Message: "half"})
			return in, g.wait(ctx)
		})
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		var err error
		select {
		case err = <-reported:
		case <-time.After(e.tm.wait):
			e.t.Fatal("the step never reported")
		}
		e.skipUnsupported(err, "ReportProgress")
		if err != nil {
			e.t.Fatalf("ReportProgress: %v", err)
		}
		e.eventually("RunInfo.Progress", 0, func() bool {
			p := e.runInfo(ref).Progress
			return p != nil && p.Percent == 50 && p.Message == "half" && p.Step == "only"
		})
		g.open()
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
	}},
	{"ReportProgress refuses a percent outside 0..100", func(e *env) {
		if err := workflow.ReportProgress(context.Background(), workflow.Progress{Percent: 101}); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("ReportProgress(101) = %v, want ErrInvalid", err)
		}
	}},
}

var lifecycleEventCases = []testCase{
	{"a run's lifecycle arrives in order", func(e *env) {
		ln := asOpt[workflow.LifecycleNotifier](e, "workflow.LifecycleNotifier")
		var c counter
		wf := oneStep(e.name("w"), func(_ context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if c.count() == 1 {
				return nil, errBoom
			}
			return in, nil
		})
		wf.Retry = workflow.RetryPolicy{Max: 1}
		var mu sync.Mutex
		var kinds []workflow.LifecycleKind
		err := ln.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) {
			if ev.Workflow != wf.Name {
				return
			}
			mu.Lock()
			kinds = append(kinds, ev.Kind)
			mu.Unlock()
		})
		e.skipUnsupported(err, "OnLifecycle")
		if err != nil {
			e.t.Fatalf("OnLifecycle: %v", err)
		}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		want := []workflow.LifecycleKind{workflow.LifecycleInserted, workflow.LifecycleStarted, workflow.LifecycleRetrying,
			workflow.LifecycleStarted, workflow.LifecycleFinished}
		e.eventually("the finished event", e.tm.unit, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(kinds) >= len(want)
		})
		mu.Lock()
		defer mu.Unlock()
		if !slices.Equal(kinds, want) {
			e.t.Fatalf("lifecycle = %v, want %v", kinds, want)
		}
	}},
	{"OnLifecycle after Start is refused", func(e *env) {
		ln := asOpt[workflow.LifecycleNotifier](e, "workflow.LifecycleNotifier")
		var c counter
		e.up(oneStep(e.name("w"), c.echo()))
		err := ln.OnLifecycle(func(context.Context, workflow.LifecycleEvent) {})
		e.skipUnsupported(err, "OnLifecycle")
		if !errors.Is(err, workflow.ErrAlreadyStarted) {
			e.t.Fatalf("OnLifecycle after Start = %v, want ErrAlreadyStarted", err)
		}
	}},
}

var outcomeCases = []testCase{
	{"Enqueue reports a new run and a duplicate", func(e *env) {
		e.need(workflow.CapIdempotencyKey)
		g := newGate(e.t)
		var c counter
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		e.up(wf)
		key := e.name("key")
		ctx, out := workflow.WithEnqueueOutcome(context.Background())
		first, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		if err != nil {
			e.t.Fatalf("Enqueue: %v", err)
		}
		if !out.Reported || out.Duplicate {
			e.t.Fatalf("first Enqueue outcome = %+v, want Reported and not Duplicate", *out)
		}
		ctx, out = workflow.WithEnqueueOutcome(context.Background())
		second, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
		if err != nil {
			e.t.Fatalf("second Enqueue: %v", err)
		}
		if !out.Reported || !out.Duplicate {
			e.t.Fatalf("duplicate Enqueue outcome = %+v, want Reported and Duplicate", *out)
		}
		if second.RunID != first.RunID {
			e.t.Errorf("duplicate returned %+v, want the holder %+v", second, first)
		}
		g.open()
		e.waitStatus(first, workflow.StatusSucceeded, e.tm.unit)
	}},
}

var runErrorCases = []testCase{
	{"a failed run's error names the run, while RunInfo.Error stays the step's message", func(e *env) {
		var c counter
		wf := oneStep(e.name("w"), c.fail(workflow.Permanent(errBoom)))
		var mu sync.Mutex
		var ends []workflow.RunEvent
		n, notifies := e.c.(workflow.RunNotifier)
		if notifies {
			if err := n.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
				mu.Lock()
				ends = append(ends, ev)
				mu.Unlock()
			}); err != nil {
				e.t.Fatalf("OnRunFinished: %v", err)
			}
		}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		if e.c.Capabilities().Has(workflow.CapRunQuery) {
			msg := e.runInfo(ref).Error
			if !strings.Contains(msg, errBoom.Error()) || strings.Contains(msg, "workflow \""+wf.Name+"\" run ") {
				e.t.Errorf("RunInfo.Error = %q, want the step's message without the RunError context", msg)
			}
		}
		if !notifies {
			return
		}
		e.eventually("the run-finished event", e.tm.unit, func() bool { mu.Lock(); defer mu.Unlock(); return len(ends) > 0 })
		mu.Lock()
		defer mu.Unlock()
		// In-process drivers hand back the error value; an engine rebuilds
		// it from text, where errors.As finds nothing and is not required.
		if re, ok := workflow.RunErrorOf(ends[0].Err); ok {
			id := ref.RunID.String()
			if i := strings.LastIndexByte(id, ':'); i >= 0 {
				id = id[i+1:] // a prefixing wrapper (failover) hands out its own refs
			}
			if !strings.HasSuffix(re.RunID.String(), id) || re.Step != "only" {
				e.t.Errorf("RunError = %+v, want run %s step only", re, ref.RunID)
			}
		}
	}},
}

var reservedLabelCases = []testCase{
	{"a plain Enqueue cannot set a reserved label", func(e *env) {
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		for _, key := range []string{workflow.LabelParentRun, workflow.LabelSchedule, workflow.LabelPrefixBatch + "id", workflow.LabelPrefixOTel + "traceparent"} {
			if _, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Labels: map[string]string{key: "forged"}}); !errors.Is(err, workflow.ErrInvalid) {
				e.t.Errorf("Enqueue with label %q = %v, want ErrInvalid (workflow.CheckJobLabels)", key, err)
			}
		}
		if _, err := e.c.Enqueue(workflow.AllowReservedLabels(ctx, workflow.LabelParentRun), workflow.Job{Workflow: wf.Name,
			Labels: map[string]string{workflow.LabelParentRun: "r"}}); err != nil {
			e.t.Errorf("Enqueue with the key allowed by its owner = %v, want it accepted", err)
		}
	}},
}

// p5Groups are the conformance groups of the p5 contracts, in Run order.
var p5Groups = []struct {
	name  string
	cases []testCase
}{
	{"snooze", snoozeCases},
	{"heartbeat", heartbeatCases},
	{"tenant", tenantCases},
	{"runtimeout", runTimeoutCases},
	{"nodes", nodeCases},
	{"keyfifo", keyFIFOCases},
	{"duequery", dueQueryCases},
	{"result", resultCases},
	{"reschedule", rescheduleCases},
	{"cancelstep", cancelStepCases},
	{"progress", progressCases},
	{"lifecycleevents", lifecycleEventCases},
	{"outcome", outcomeCases},
	{"runerror", runErrorCases},
	{"reservedlabels", reservedLabelCases},
	{"schedseconds", schedSecondsCases},
	{"schedactive", schedActiveCases},
	{"schedkeep", schedKeepCases},
	{"ticktime", tickTimeCases},
}
