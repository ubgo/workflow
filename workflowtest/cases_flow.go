package workflowtest

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

var cancelCases = []testCase{
	{"Cancel stops a running run, and cancelling twice is fine", func(e *env) {
		e.need(workflow.CapCancel)
		var c, ended counter
		wf := oneStep(e.name("w"), blockUntilDone(&c, &ended))
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.eventually("the step to start", 0, func() bool { return c.count() > 0 })
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Cancel(ctx, ref); err != nil {
			e.t.Fatalf("Cancel: %v", err)
		}
		e.waitStatus(ref, workflow.StatusCancelled, 0)
		e.eventually("the step's context to end", 0, func() bool { return ended.count() > 0 })
		if err := e.c.Cancel(ctx, ref); err != nil {
			e.t.Fatalf("second Cancel: %v, want nil", err)
		}
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("a cancelled step ran %d times, want 1 (no retry after cancel)", got)
		}
	}},
	{"Cancel stops a scheduled run before it starts", func(e *env) {
		e.need(workflow.CapCancel | workflow.CapDelayedStart)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, RunAt: e.now().Add(e.tm.unit)})
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Cancel(ctx, ref); err != nil {
			e.t.Fatalf("Cancel of a scheduled run: %v", err)
		}
		e.waitStatus(ref, workflow.StatusCancelled, 0)
		if e.clk != nil {
			e.clk.Advance(2 * e.tm.unit)
			e.quiet()
		} else {
			time.Sleep(e.tm.unit + e.tm.quiet)
		}
		if got := c.count(); got != 0 {
			e.t.Fatalf("a cancelled scheduled run started %d times", got)
		}
		if err := e.c.Cancel(ctx, ref); err != nil {
			e.t.Fatalf("second Cancel: %v, want nil", err)
		}
	}},
	{"Cancel of a finished run is a no-op", func(e *env) {
		e.need(workflow.CapCancel)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Cancel(ctx, ref); err != nil {
			e.t.Fatalf("Cancel of a finished run: %v, want nil", err)
		}
		if st := e.status(ref); st != workflow.StatusSucceeded {
			e.t.Fatalf("Cancel changed a finished run's status to %q", st)
		}
	}},
}

// waitWorkflow is a run that waits for event (with key) and then hands
// the event payload to a step counted by after.
func waitWorkflow(name, event, key string, timeout time.Duration, after *counter) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{
		{Name: "wait", Wait: &workflow.WaitSpec{Event: event, Key: key, Timeout: timeout}},
		{Name: "after", After: []string{"wait"}, Run: after.echo()},
	}}
}

var waitCases = []testCase{
	{"Signal with the matching key resumes the run, another key does not", func(e *env) {
		e.need(workflow.CapWaitForEvent)
		var after counter
		event := e.name("event")
		wf := waitWorkflow(e.name("w"), event, "order-1", 0, &after)
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusWaiting, 0)
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Signal(ctx, workflow.Event{Name: event, Key: "order-2", Payload: []byte(`"wrong"`)}); err != nil {
			e.t.Fatalf("Signal: %v", err)
		}
		e.quiet()
		if st := e.status(ref); st != workflow.StatusWaiting || after.count() != 0 {
			e.t.Fatalf("a Signal with another key moved the run to %q (after ran %d times)", st, after.count())
		}
		payload := []byte(`"approved"`)
		if err := e.c.Signal(ctx, workflow.Event{Name: event, Key: "order-1", Payload: payload}); err != nil {
			e.t.Fatalf("Signal: %v", err)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		if got := after.input(); !bytes.Equal(got, payload) {
			e.t.Fatalf("step after the wait got %q, want the event payload %q", got, payload)
		}
	}},
	{"KeyFrom gives each run its own key, read from the Job", func(e *env) {
		e.need(workflow.CapWaitForEvent)
		var mu sync.Mutex
		got := map[string][]byte{}
		event := e.name("event")
		wf := workflow.Workflow{Name: e.name("w"), Steps: []workflow.Step{
			{Name: "wait", Wait: &workflow.WaitSpec{Event: event, KeyFrom: e.h.waitKeyFromLabel("order")}},
			{Name: "after", After: []string{"wait"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				mu.Lock()
				got[string(info.RunID)] = append([]byte(nil), in...)
				mu.Unlock()
				return in, nil
			}},
		}}
		e.up(wf)
		a := e.enqueue(workflow.Job{Workflow: wf.Name, Payload: []byte(`"pa"`), Labels: map[string]string{"order": "a"}})
		b := e.enqueue(workflow.Job{Workflow: wf.Name, Payload: []byte(`"pb"`), Labels: map[string]string{"order": "b"}})
		e.waitStatus(a, workflow.StatusWaiting, 0)
		e.waitStatus(b, workflow.StatusWaiting, 0)
		ctx, cancel := e.callCtx()
		defer cancel()
		payloadA := []byte(`"for-a"`)
		if err := e.c.Signal(ctx, workflow.Event{Name: event, Key: "a", Payload: payloadA}); err != nil {
			e.t.Fatalf("Signal: %v", err)
		}
		e.waitStatus(a, workflow.StatusSucceeded, 0)
		e.quiet()
		if st := e.status(b); st != workflow.StatusWaiting {
			e.t.Fatalf("run b moved to %q on a Signal keyed for run a", st)
		}
		if err := e.c.Signal(ctx, workflow.Event{Name: event, Key: "b", Payload: []byte(`"for-b"`)}); err != nil {
			e.t.Fatalf("Signal: %v", err)
		}
		e.waitStatus(b, workflow.StatusSucceeded, 0)
		mu.Lock()
		defer mu.Unlock()
		if len(got) != 2 {
			e.t.Fatalf("the step after the wait ran for %d runs, want 2 (StepInfo.RunID must tell them apart)", len(got))
		}
		for _, tc := range []struct {
			ref  workflow.RunRef
			want string
		}{{a, `"for-a"`}, {b, `"for-b"`}} {
			if in := got[string(tc.ref.RunID)]; string(in) != tc.want {
				e.t.Errorf("run %s: step after the wait got %q, want its event payload %s", tc.ref.RunID, in, tc.want)
			}
		}
	}},
	{"a Signal before the wait step is kept from the run's enqueue", func(e *env) {
		// Signals are kept from the run's enqueue time (Hatchet's
		// look-back), so one sent while an earlier step runs is not lost,
		// and one sent before the enqueue does not count.
		e.need(workflow.CapWaitForEvent)
		g := newGate(e.t)
		var first, after counter
		event := e.name("event")
		wf := workflow.Workflow{Name: e.name("w"), Steps: []workflow.Step{
			{Name: "first", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				first.hit(in)
				return in, g.wait(ctx)
			}},
			{Name: "wait", After: []string{"first"}, Wait: &workflow.WaitSpec{Event: event, KeyFrom: e.h.waitKeyFromLabel("order")}},
			{Name: "after", After: []string{"wait"}, Run: after.echo()},
		}}
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		// Sent before run b exists: it must not resume b.
		if err := e.c.Signal(ctx, workflow.Event{Name: event, Key: "b", Payload: []byte(`"stale"`)}); err != nil {
			e.t.Fatalf("Signal: %v", err)
		}
		if e.clk != nil {
			// Keep the stale signal strictly before the enqueue, so the
			// case does not hang on whether equal times count.
			e.clk.Advance(time.Second)
		}
		a := e.enqueue(workflow.Job{Workflow: wf.Name, Labels: map[string]string{"order": "a"}})
		b := e.enqueue(workflow.Job{Workflow: wf.Name, Labels: map[string]string{"order": "b"}})
		e.eventually("both first steps to start", 0, func() bool { return first.count() == 2 })
		payload := []byte(`"early"`)
		if err := e.c.Signal(ctx, workflow.Event{Name: event, Key: "a", Payload: payload}); err != nil {
			e.t.Fatalf("Signal: %v", err)
		}
		g.open()
		e.waitStatus(a, workflow.StatusSucceeded, 0)
		if got := after.input(); !bytes.Equal(got, payload) {
			e.t.Fatalf("step after the wait got %q, want the early event's payload %q", got, payload)
		}
		e.waitStatus(b, workflow.StatusWaiting, 0)
		e.quiet()
		if st := e.status(b); st != workflow.StatusWaiting {
			e.t.Fatalf("a Signal sent before run b was enqueued moved it to %q", st)
		}
		if got := after.count(); got != 1 {
			e.t.Fatalf("the step after the wait ran %d times, want 1 (only run a)", got)
		}
	}},
	{"a wait times out without a Signal", func(e *env) {
		e.need(workflow.CapWaitForEvent)
		var after counter
		wf := waitWorkflow(e.name("w"), e.name("event"), "", e.tm.unit, &after)
		e.up(wf)
		e.waitStatus(e.enqueue(workflow.Job{Workflow: wf.Name}), workflow.StatusFailed, e.tm.unit/4)
		if got := after.count(); got != 0 {
			e.t.Fatalf("the step after a timed-out wait ran %d times", got)
		}
	}},
	{"a Signal nobody waits for is not an error", func(e *env) {
		e.need(workflow.CapWaitForEvent)
		var c counter
		e.up(oneStep(e.name("w"), c.echo()))
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Signal(ctx, workflow.Event{Name: e.name("nobody")}); err != nil {
			e.t.Fatalf("Signal with no waiter: %v, want nil", err)
		}
	}},
}

var sleepCases = []testCase{
	{"a sleep step wakes after its duration", func(e *env) {
		e.need(workflow.CapDurableSleep)
		var after counter
		u := e.tm.unit
		wf := workflow.Workflow{Name: e.name("w"), Steps: []workflow.Step{
			{Name: "sleep", Sleep: u},
			{Name: "after", After: []string{"sleep"}, Run: after.echo()},
		}}
		e.up(wf)
		payload := []byte(`"p"`)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, Payload: payload})
		e.waitStatus(ref, workflow.StatusWaiting, 0)
		if e.clk != nil {
			e.clk.Advance(u / 2)
			e.quiet()
		} else {
			time.Sleep(u / 2)
		}
		if got := after.count(); got != 0 {
			e.t.Fatalf("the step after a sleep ran %d times before the sleep ended", got)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, u/4)
		if got := after.count(); got != 1 {
			e.t.Fatalf("the step after a sleep ran %d times, want 1", got)
		}
	}},
}

var durableCases = []testCase{
	{"a waiting run survives a restart", func(e *env) {
		e.need(workflow.CapDurable | workflow.CapWaitForEvent)
		if e.restart == nil {
			e.t.Skip("Instance.Restart is nil")
		}
		var after counter
		event := e.name("event")
		wf := waitWorkflow(e.name("w"), event, "k", 0, &after)
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusWaiting, 0)
		e.doRestart()
		if st := e.status(ref); st != workflow.StatusWaiting {
			e.t.Fatalf("status after restart = %q, want waiting", st)
		}
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Signal(ctx, workflow.Event{Name: event, Key: "k", Payload: []byte(`"x"`)}); err != nil {
			e.t.Fatalf("Signal after restart: %v", err)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
	}},
	{"a step cut off by a restart runs again on the new client", func(e *env) {
		// The kill-a-worker case: the old client's Start context is
		// cancelled while the step runs, which a durable driver must treat
		// as a lost worker, not a cancelled run. The new client picks the
		// run up (at once, or once a lease expires on its clock) and runs
		// the step again.
		e.need(workflow.CapDurable)
		if e.restart == nil {
			e.t.Skip("Instance.Restart is nil")
		}
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if c.count() == 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return in, nil
		})
		// A cut-off attempt may count against the retries; one retry keeps
		// the case about recovery, not about attempt accounting.
		wf.Retry = workflow.RetryPolicy{Max: 1}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.eventually("the step to start", 0, func() bool { return c.count() > 0 })
		e.doRestart()
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := c.count(); got < 2 {
			e.t.Fatalf("the step ran %d times, want it run again after the restart", got)
		}
	}},
	{"a step cut off by a crash runs again on the new client", func(e *env) {
		e.need(workflow.CapDurable)
		if e.crash == nil {
			e.t.Skip("Instance.Crash is nil")
		}
		g := newGate(e.t)
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if c.count() == 1 {
				// The crashed worker's attempt: it never finishes, as if
				// its process were gone. Only the subtest's end frees it.
				_ = g.wait(context.Background())
				return nil, errBoom
			}
			return in, nil
		})
		// A crashed attempt may count against the retries; one retry keeps
		// the case about recovery, not about attempt accounting.
		wf.Retry = workflow.RetryPolicy{Max: 1}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.eventually("the step to start", 0, func() bool { return c.count() > 0 })
		e.doCrash()
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := c.count(); got < 2 {
			e.t.Fatalf("the step ran %d times, want it run again after the crash", got)
		}
	}},
	{"a scheduled run survives a restart", func(e *env) {
		e.need(workflow.CapDurable | workflow.CapDelayedStart)
		if e.restart == nil {
			e.t.Skip("Instance.Restart is nil")
		}
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, RunAt: e.now().Add(e.tm.unit)})
		e.doRestart()
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit/4)
		if got := c.count(); got != 1 {
			e.t.Fatalf("the scheduled step ran %d times after a restart, want 1", got)
		}
	}},
}

var capsCases = []testCase{
	{"Start refuses a workflow that needs a missing capability", func(e *env) {
		have := e.c.Capabilities()
		var c counter
		base := oneStep("", c.echo())
		candidates := []struct {
			cap  workflow.Caps
			edit func(*workflow.Workflow)
		}{
			{workflow.CapCron, func(w *workflow.Workflow) { w.Cron = []string{"0 0 1 1 *"} }},
			{workflow.CapConcurrencyKey, func(w *workflow.Workflow) {
				w.Concurrency = &workflow.Concurrency{Key: e.h.keyFromLabel("k"), MaxRuns: 1}
			}},
			{workflow.CapRateLimit, func(w *workflow.Workflow) {
				w.RateLimits = []workflow.RateLimit{{Key: e.h.keyFromLabel("k"), Limit: 1, Per: time.Second}}
			}},
			{workflow.CapIdempotencyKey, func(w *workflow.Workflow) { w.Idempotency = workflow.IdempotencyRequired }},
			{workflow.CapWaitForEvent, func(w *workflow.Workflow) {
				w.Steps = []workflow.Step{{Name: "wait", Wait: &workflow.WaitSpec{Event: "e"}}}
			}},
			{workflow.CapDurableSleep, func(w *workflow.Workflow) {
				w.Steps = []workflow.Step{{Name: "sleep", Sleep: time.Hour}}
			}},
			{workflow.CapRetryJitter, func(w *workflow.Workflow) {
				w.Retry = workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: time.Second, Jitter: 0.5}}
			}},
			{workflow.CapRetryDelay, func(w *workflow.Workflow) {
				w.Retry = workflow.RetryPolicy{Max: 1, DelayFor: workflow.RetryAfterDelay}
			}},
			{workflow.CapCronZone, func(w *workflow.Workflow) {
				w.Cron = []string{"0 0 1 1 *"}
				w.CronLocation = time.FixedZone("UTC+5", 5*60*60)
			}},
			{workflow.CapMisfireSkip, func(w *workflow.Workflow) {
				w.Cron = []string{"0 0 1 1 *"}
				w.Misfire = workflow.Misfire{Policy: workflow.MisfireSkip}
			}},
			{workflow.CapMisfireFireOnce, func(w *workflow.Workflow) {
				w.Cron = []string{"0 0 1 1 *"}
				w.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce}
			}},
			{workflow.CapMisfireFireAll, func(w *workflow.Workflow) {
				w.Cron = []string{"0 0 1 1 *"}
				w.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 1}
			}},
		}
		tested := 0
		for _, cand := range candidates {
			if have.Has(cand.cap) {
				continue
			}
			tested++
			fresh := newEnv(e.t, e.h, e.nonce)
			wf := base
			wf.Name = fresh.name(cand.cap.String())
			cand.edit(&wf)
			// Register may refuse already; Start must at the latest.
			err := fresh.c.Register(wf)
			if err == nil {
				err = fresh.tryStart()
			}
			assertUnsupported(e.t, err, "", cand.cap)
		}
		if tested == 0 {
			e.t.Skip("driver has every capability Start checks")
		}
	}},
	{"Enqueue refuses Job fields it cannot honour", func(e *env) {
		have := e.c.Capabilities()
		if have.Has(workflow.CapDelayedStart | workflow.CapIdempotencyKey) {
			e.t.Skip("driver has CapDelayedStart and CapIdempotencyKey")
		}
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		if !have.Has(workflow.CapDelayedStart) {
			_, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, RunAt: e.now().Add(time.Hour)})
			assertUnsupported(e.t, err, "RunAt", 0)
		}
		if !have.Has(workflow.CapIdempotencyKey) {
			_, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, IdempotencyKey: e.name("key")})
			assertUnsupported(e.t, err, "IdempotencyKey", 0)
		}
		e.quiet()
		if got := c.count(); got != 0 {
			e.t.Fatalf("a refused job ran %d times", got)
		}
	}},
	{"Schedule and Cancel refuse without their capability", func(e *env) {
		have := e.c.Capabilities()
		if have.Has(workflow.CapCron | workflow.CapCancel) {
			e.t.Skip("driver has CapCron and CapCancel")
		}
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		if !have.Has(workflow.CapCron) {
			_, err := e.c.Schedule(ctx, workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: "* * * * *", Location: time.UTC})
			assertUnsupported(e.t, err, "", 0)
		}
		if !have.Has(workflow.CapCancel) {
			ref := e.enqueue(workflow.Job{Workflow: wf.Name})
			err := e.c.Cancel(ctx, ref)
			assertUnsupported(e.t, err, "", 0)
		}
	}},
}
