package workflowtest

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/ubgo/workflow"
)

// farCron fires once a day at 03:00 UTC, so a case that triggers a
// schedule by hand never sees a cron tick of it.
const farCron = "0 3 * * *"

// blockFirst returns a handler that counts its calls and holds the first
// one until g opens (or its context ends); later calls return at once.
func blockFirst(c *counter, g *gate) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		c.hit(in)
		if c.count() == 1 {
			if err := g.wait(ctx); err != nil {
				return nil, err
			}
		}
		return in, nil
	}
}

// runInfo returns ref's RunInfo through RunQuerier, or fails.
func (e *env) runInfo(ref workflow.RunRef) workflow.RunInfo {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	page, err := as[workflow.RunQuerier](e, "workflow.RunQuerier").Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		e.t.Fatalf("Runs(Ref %+v): %v", ref, err)
	}
	if len(page.Runs) != 1 {
		e.t.Fatalf("Runs(Ref %+v) = %d runs, want 1", ref, len(page.Runs))
	}
	return page.Runs[0]
}

// scheduleInfo returns the listed schedule id, or fails.
func (e *env) scheduleInfo(id workflow.ScheduleID) workflow.ScheduleInfo {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	list, err := as[workflow.ScheduleLister](e, "workflow.ScheduleLister").Schedules(ctx)
	if err != nil {
		e.t.Fatalf("Schedules: %v", err)
	}
	for _, info := range list {
		if info.ID == id {
			return info
		}
	}
	e.t.Fatalf("Schedules does not list %q", id)
	return workflow.ScheduleInfo{}
}

// waitWorkflowIdle waits until no run of wf is unfinished, so a tick
// that follows decides overlap from a terminal run. A durable driver
// commits a run's end after its handler returns, so a fixed sleep is not
// enough. It asks RunQuerier when the driver has it, else the schedules'
// last-run status, else waits Quiet (an in-process driver ends the run
// before its handler's return is seen).
func (e *env) waitWorkflowIdle(wf string) {
	e.t.Helper()
	caps := e.c.Capabilities()
	switch {
	case caps.Has(workflow.CapRunQuery):
		q := as[workflow.RunQuerier](e, "workflow.RunQuerier")
		open := []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued, workflow.StatusRunning,
			workflow.StatusWaiting, workflow.StatusRetrying}
		e.eventually("the runs of "+wf+" to finish", 0, func() bool {
			ctx, cancel := e.callCtx()
			defer cancel()
			page, err := q.Runs(ctx, workflow.RunQuery{Workflow: wf, Statuses: open})
			return err == nil && len(page.Runs) == 0
		})
	case caps.Has(workflow.CapScheduleList | workflow.CapScheduleHistory):
		e.eventually("the last runs of "+wf+" to finish", 0, func() bool {
			ctx, cancel := e.callCtx()
			defer cancel()
			list, err := as[workflow.ScheduleLister](e, "workflow.ScheduleLister").Schedules(ctx)
			if err != nil {
				return false
			}
			for _, info := range list {
				if info.Workflow == wf && !info.Last.Run.IsZero() && !info.Last.Status.IsTerminal() {
					return false
				}
			}
			return true
		})
	default:
		e.quiet()
	}
}

// trigger calls TriggerSchedule and returns its result.
func (e *env) trigger(id workflow.ScheduleID) (workflow.RunRef, error) {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	return as[workflow.ScheduleTriggerer](e, "workflow.ScheduleTriggerer").TriggerSchedule(ctx, id)
}

// mustTrigger triggers id or fails.
func (e *env) mustTrigger(id workflow.ScheduleID) workflow.RunRef {
	e.t.Helper()
	ref, err := e.trigger(id)
	if err != nil {
		e.t.Fatalf("TriggerSchedule(%s): %v", id, err)
	}
	if ref.IsZero() {
		e.t.Fatalf("TriggerSchedule(%s) returned an empty RunRef", id)
	}
	return ref
}

var overlapCases = []testCase{
	{"OverlapSkip skips a tick while the previous run is unfinished", func(e *env) {
		e.need(workflow.CapCron | workflow.CapOverlapSkip)
		e.needFake("placing ticks while a run is held")
		var c counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		var log tickLog
		if e.c.Capabilities().Has(workflow.CapTickEvents) {
			if err := as[workflow.TickNotifier](e, "workflow.TickNotifier").OnTick(log.observe); err != nil {
				e.t.Fatalf("OnTick: %v", err)
			}
		}
		e.up(wf)
		e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: everyMinute, Location: time.UTC, Overlap: workflow.OverlapSkip})
		tick(e, &c, 1, 1) // the first run starts and is held
		e.advanceMinutes(2)
		if got := c.count(); got != 1 {
			e.t.Fatalf("two ticks during an unfinished run started %d runs, want 1 (OverlapSkip)", got)
		}
		g.open()
		e.waitWorkflowIdle(wf.Name)
		tick(e, &c, 1, 2) // the previous run is done: the next tick fires
		if e.c.Capabilities().Has(workflow.CapTickEvents) && log.count(workflow.TickSkipped, workflow.TickReasonOverlap) < 1 {
			e.t.Errorf("ticks = %+v, want skips with reason %q", log.ticks, workflow.TickReasonOverlap)
		}
	}},
	{"the default policy fires while the previous run is unfinished", func(e *env) {
		needCronFiring(e)
		e.needFake("placing ticks while a run is held")
		var c counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		e.up(wf)
		e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: everyMinute, Location: time.UTC})
		tick(e, &c, 1, 1)
		tick(e, &c, 1, 2)
	}},
	{"Workflow.CronOverlap skips a declared cron's tick", func(e *env) {
		e.need(workflow.CapCron | workflow.CapOverlapSkip)
		e.needFake("placing ticks while a run is held")
		var c counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		wf.Cron = []string{everyMinute}
		wf.CronOverlap = workflow.OverlapSkip
		e.up(wf)
		tick(e, &c, 1, 1)
		e.advanceMinutes(1)
		if got := c.count(); got != 1 {
			e.t.Fatalf("a tick during an unfinished run started %d runs, want 1 (CronOverlap skip)", got)
		}
		g.open()
		e.waitWorkflowIdle(wf.Name)
		tick(e, &c, 1, 2)
	}},
	{"without CapOverlapSkip OverlapSkip is refused", func(e *env) {
		e.need(workflow.CapCron)
		e.lacks(workflow.CapOverlapSkip)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Cron: everyMinute, Location: time.UTC, Overlap: workflow.OverlapSkip})
		assertUnsupported(e.t, err, "Schedule.Overlap", workflow.CapOverlapSkip)
	}},
	{"an unknown overlap policy is ErrInvalid", func(e *env) {
		e.need(workflow.CapCron)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Cron = []string{everyMinute}
		wf.CronOverlap = "sometimes"
		if err := e.c.Register(wf); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("Register with CronOverlap %q: %v, want ErrInvalid", wf.CronOverlap, err)
		}
	}},
}

var triggerCases = []testCase{
	{"TriggerSchedule runs the schedule now with its input and labels", func(e *env) {
		e.need(workflow.CapCron | workflow.CapScheduleTrigger)
		var s stepInfos
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			return s.handler()(ctx, in)
		})
		e.up(wf)
		name := e.name("s")
		id := e.schedule(workflow.Schedule{Name: name, Workflow: wf.Name, Cron: farCron, Location: time.UTC,
			Input: []byte(`"in"`), Labels: map[string]string{"team": "ops"}})
		ref := e.mustTrigger(id)
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		if got := string(c.input()); got != `"in"` {
			e.t.Errorf("the triggered run's input = %s, want the schedule's", got)
		}
		// LabelSchedule must be the ID the caller holds, so it can be
		// handed back to PauseSchedule or TriggerSchedule.
		l := s.last().Labels
		if l["team"] != "ops" || l[workflow.LabelTrigger] != workflow.LabelTriggerManual || l[workflow.LabelSchedule] != string(id) || l[workflow.LabelScheduleName] != name {
			e.t.Errorf("the triggered run's labels = %v, want the schedule's, %s=%s, %s=%s and %s=%s", l, workflow.LabelSchedule, id, workflow.LabelScheduleName, name, workflow.LabelTrigger, workflow.LabelTriggerManual)
		}
	}},
	{"TriggerSchedule obeys OverlapSkip", func(e *env) {
		e.need(workflow.CapCron | workflow.CapScheduleTrigger | workflow.CapOverlapSkip)
		var c counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		e.up(wf)
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: farCron, Location: time.UTC, Overlap: workflow.OverlapSkip})
		first := e.mustTrigger(id)
		e.eventually("the first run to start", 0, func() bool { return c.count() == 1 })
		if _, err := e.trigger(id); !errors.Is(err, workflow.ErrOverlap) {
			e.t.Fatalf("a trigger while the previous run is unfinished: %v, want ErrOverlap", err)
		}
		g.open()
		e.waitStatus(first, workflow.StatusSucceeded, 0)
		second := e.mustTrigger(id)
		e.waitStatus(second, workflow.StatusSucceeded, 0)
	}},
	{"TriggerSchedule fires a paused schedule", func(e *env) {
		e.need(workflow.CapCron | workflow.CapScheduleTrigger | workflow.CapSchedulePause)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: farCron, Location: time.UTC})
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := as[workflow.SchedulePauser](e, "workflow.SchedulePauser").PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
			e.t.Fatalf("PauseSchedule: %v", err)
		}
		e.waitStatus(e.mustTrigger(id), workflow.StatusSucceeded, 0)
	}},
	{"TriggerSchedule of an unknown schedule is ErrNotFound", func(e *env) {
		e.need(workflow.CapCron | workflow.CapScheduleTrigger)
		e.up(oneStep(e.name("w"), (&counter{}).echo()))
		if _, err := e.trigger(workflow.ScheduleID(e.name("missing"))); !errors.Is(err, workflow.ErrNotFound) {
			e.t.Fatalf("TriggerSchedule of an unknown id: %v, want ErrNotFound", err)
		}
	}},
}

var scheduleHistoryCases = []testCase{
	{"Schedules reports the last trigger, its run and outcome", func(e *env) {
		e.need(workflow.CapCron | workflow.CapScheduleList | workflow.CapScheduleHistory | workflow.CapScheduleTrigger)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: farCron, Location: time.UTC})
		if last := e.scheduleInfo(id).Last; last != (workflow.ScheduleLast{}) {
			e.t.Fatalf("Last before any fire = %+v, want zero", last)
		}
		before := e.now()
		ref := e.mustTrigger(id)
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		last := e.scheduleInfo(id).Last
		if last.Run.RunID != ref.RunID {
			e.t.Errorf("Last.Run = %+v, want the triggered run %+v", last.Run, ref)
		}
		if last.Fire.IsZero() || last.Fire.Before(before.Add(-time.Minute)) {
			e.t.Errorf("Last.Fire = %v, want the trigger's time (about %v)", last.Fire, before)
		}
		if last.Status != workflow.StatusSucceeded {
			e.t.Errorf("Last.Status = %q, want succeeded", last.Status)
		}
	}},
	{"Schedules reports the last cron fire", func(e *env) {
		needCronFiring(e)
		e.need(workflow.CapScheduleList | workflow.CapScheduleHistory)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: everyMinute, Location: time.UTC})
		tick(e, &c, 1, 1)
		var last workflow.ScheduleLast
		e.eventually("the fire to be recorded and finish", 0, func() bool {
			last = e.scheduleInfo(id).Last
			return last.Status == workflow.StatusSucceeded
		})
		if last.Run.IsZero() || last.Fire.IsZero() {
			e.t.Errorf("Last = %+v, want the fire's time and run", last)
		}
		if last.Fire.Second() != 0 || last.Fire.Nanosecond() != 0 {
			e.t.Errorf("Last.Fire = %v, want the tick's minute boundary", last.Fire)
		}
	}},
}

var enqueueOnlyCases = []testCase{
	{"an enqueue-only client enqueues and a worker client runs it", func(e *env) {
		e.need(workflow.CapEnqueueOnly)
		if e.eonly == nil {
			e.t.Skip("Instance.EnqueueOnly is nil")
		}
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		eo := e.eonly(e.t)
		ctx, cancel := e.callCtx()
		defer cancel()
		// Never started and the workflow never registered on it: a web
		// node knows names and payloads, not handlers.
		ref, err := eo.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"web"`)})
		if err != nil {
			e.t.Fatalf("Enqueue on an enqueue-only client: %v", err)
		}
		e.eventually("the worker to run it", e.tm.unit, func() bool {
			st, err := eo.Status(ctx, ref)
			return err == nil && st == workflow.StatusSucceeded
		})
		if got := string(c.input()); got != `"web"` {
			e.t.Errorf("the worker ran input %s, want the enqueued one", got)
		}
		if err := eo.Start(ctx); err != nil {
			e.t.Errorf("Start on an enqueue-only client: %v, want nil (it starts nothing)", err)
		}
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Errorf("the run executed %d times, want 1", got)
		}
	}},
}

var errorHistoryCases = []testCase{
	{"RunInfo.Errors records each failed attempt, oldest first", func(e *env) {
		e.need(workflow.CapErrorHistory | workflow.CapRunQuery)
		var c counter
		wf := oneStep(e.name("w"), func(_ context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if c.count() <= 2 {
				return nil, errBoom
			}
			return in, nil
		})
		wf.Retry = workflow.RetryPolicy{Max: 2}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		errs := e.runInfo(ref).Errors
		if len(errs) != 2 {
			e.t.Fatalf("Errors = %+v, want 2 entries", errs)
		}
		for i, ae := range errs {
			if ae.Step != "only" || ae.Attempt != i+1 || !strings.Contains(ae.Error, errBoom.Error()) || ae.At.IsZero() {
				e.t.Errorf("Errors[%d] = %+v, want step only, attempt %d, the error and a time", i, ae, i+1)
			}
		}
		if errs[1].At.Before(errs[0].At) {
			e.t.Errorf("Errors are not oldest first: %v then %v", errs[0].At, errs[1].At)
		}
	}},
	{"a failed run's history ends with its final error", func(e *env) {
		e.need(workflow.CapErrorHistory | workflow.CapRunQuery)
		var c counter
		wf := oneStep(e.name("w"), c.fail(errBoom))
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		errs := e.runInfo(ref).Errors
		if len(errs) != 1 || errs[0].Attempt != 1 || !strings.Contains(errs[0].Error, errBoom.Error()) {
			e.t.Fatalf("Errors = %+v, want the one failed attempt", errs)
		}
	}},
}

var shutdownRequeueCases = []testCase{
	{"a step cut short by a clean stop runs again without spending an attempt", func(e *env) {
		e.need(workflow.CapDurable | workflow.CapShutdownRequeue)
		if e.restart == nil {
			e.t.Skip("Instance.Restart is nil")
		}
		var s stepInfos
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			if c.count() == 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return s.handler()(ctx, in)
		})
		// No retries: had the cut-off attempt counted, the run would fail.
		wf.Retry = *workflow.RetriesNone()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.eventually("the step to start", 0, func() bool { return c.count() > 0 })
		e.doRestart()
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := s.last().Attempt; got != 1 {
			e.t.Errorf("the step ran again as attempt %d, want 1 (the stop must not spend an attempt)", got)
		}
		if e.c.Capabilities().Has(workflow.CapRunQuery) {
			info := e.runInfo(ref)
			if info.Attempts != 1 {
				e.t.Errorf("RunInfo.Attempts = %d, want 1", info.Attempts)
			}
			if e.c.Capabilities().Has(workflow.CapErrorHistory) && len(info.Errors) != 0 {
				e.t.Errorf("RunInfo.Errors = %+v, want none for a requeued attempt", info.Errors)
			}
		}
	}},
}

// forcer returns the client as a RunForcer.
func (e *env) forcer() workflow.RunForcer { return as[workflow.RunForcer](e, "workflow.RunForcer") }

var forceCases = []testCase{
	{"ForceComplete ends a running run as succeeded and continues its chain", func(e *env) {
		e.need(workflow.CapForceRun)
		var c, next counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		after := oneStep(e.name("after"), next.echo())
		e.up(wf, after)
		job := workflow.Job{Workflow: wf.Name}
		if e.c.Capabilities().Has(workflow.CapChain) {
			job.Chain = []workflow.Job{{Workflow: after.Name}}
		}
		ref := e.enqueue(job)
		e.eventually("the step to start", 0, func() bool { return c.count() == 1 })
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.forcer().ForceComplete(ctx, ref); err != nil {
			e.t.Fatalf("ForceComplete: %v", err)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		if len(job.Chain) > 0 {
			e.eventually("the chain to continue", 0, func() bool { return next.count() == 1 })
		}
	}},
	{"ForceComplete accepts a failed run", func(e *env) {
		e.need(workflow.CapForceRun)
		var c counter
		wf := oneStep(e.name("w"), c.fail(errBoom))
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.forcer().ForceComplete(ctx, ref); err != nil {
			e.t.Fatalf("ForceComplete of a failed run: %v", err)
		}
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
	}},
	{"ForceFail ends a running run as failed without OnFailure", func(e *env) {
		e.need(workflow.CapForceRun)
		var c, onFail counter
		g := newGate(e.t)
		wf := oneStep(e.name("w"), blockFirst(&c, g))
		wf.OnFailure = onFail.echo()
		wf.Retry = workflow.RetryPolicy{Max: 3}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.eventually("the step to start", 0, func() bool { return c.count() == 1 })
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.forcer().ForceFail(ctx, ref, "operator says stop"); err != nil {
			e.t.Fatalf("ForceFail: %v", err)
		}
		e.waitStatus(ref, workflow.StatusFailed, 0)
		g.open()
		e.quiet()
		if got := onFail.count(); got != 0 {
			e.t.Errorf("OnFailure ran %d times after ForceFail, want 0", got)
		}
		if got := c.count(); got != 1 {
			e.t.Errorf("the step ran %d times, want 1 (no retry after ForceFail)", got)
		}
		if e.c.Capabilities().Has(workflow.CapRunQuery) {
			if msg := e.runInfo(ref).Error; !strings.Contains(msg, "operator says stop") {
				e.t.Errorf("RunInfo.Error = %q, want the reason", msg)
			}
		}
	}},
	{"forcing a finished run is ErrWrongState and an unknown run ErrNotFound", func(e *env) {
		e.need(workflow.CapForceRun)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		ctx, cancel := e.callCtx()
		defer cancel()
		f := e.forcer()
		if err := f.ForceComplete(ctx, ref); !errors.Is(err, workflow.ErrWrongState) {
			e.t.Errorf("ForceComplete of a succeeded run: %v, want ErrWrongState", err)
		}
		if err := f.ForceFail(ctx, ref, "x"); !errors.Is(err, workflow.ErrWrongState) {
			e.t.Errorf("ForceFail of a succeeded run: %v, want ErrWrongState", err)
		}
		missing := workflow.RunRef{RunID: workflow.RunID(e.name("missing"))}
		if err := f.ForceComplete(ctx, missing); !errors.Is(err, workflow.ErrNotFound) {
			e.t.Errorf("ForceComplete of an unknown run: %v, want ErrNotFound", err)
		}
		if err := f.ForceFail(ctx, missing, "x"); !errors.Is(err, workflow.ErrNotFound) {
			e.t.Errorf("ForceFail of an unknown run: %v, want ErrNotFound", err)
		}
	}},
}
