package workflowtest

import (
	"context"
	"errors"
	"time"

	"github.com/ubgo/workflow"
)

// The schedule-shape cases: the seconds dialect (Schedule.Seconds), the
// active period (Schedule.Active, Workflow.CronActive), the per-schedule
// run history limit (Schedule.KeepRuns) and the tick time every fired
// run carries (LabelTickAt), each with its refusal on a driver that lacks
// it.

// minuteAfter is the minute boundary n minutes after the one at or
// before t.
func minuteAfter(t time.Time, n int) time.Time {
	return t.Truncate(time.Minute).Add(time.Duration(n) * time.Minute)
}

// scheduleRefusal asks for a Schedule that needs missing on a driver that
// has cron but lacks missing, and checks the refusal names it.
func scheduleRefusal(e *env, missing workflow.Caps, edit func(*workflow.Schedule)) {
	e.need(workflow.CapCron)
	e.lacks(missing)
	var c counter
	wf := oneStep(e.name("w"), c.echo())
	e.up(wf)
	s := workflow.Schedule{Workflow: wf.Name, Cron: everyMinute, Location: time.UTC}
	edit(&s)
	ctx, cancel := e.callCtx()
	defer cancel()
	_, err := e.c.Schedule(ctx, s)
	assertUnsupported(e.t, err, "", missing)
}

var schedSecondsCases = []testCase{
	{"a seconds schedule fires at its second, once a minute", func(e *env) {
		needCronFiring(e)
		e.need(workflow.CapCronSeconds)
		e.needFake("firing at a chosen second")
		var s stepInfos
		var c counter
		wf := oneStep(e.name("w"), func(ctx_ context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			return s.handler()(ctx_, in)
		})
		e.up(wf)
		e.schedule(workflow.Schedule{Workflow: wf.Name, Cron: "30 * * * * *", Seconds: true, Location: time.UTC})
		e.advanceMinutes(1)
		e.eventually("the seconds tick to fire", 0, func() bool { return c.count() >= 1 })
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("one minute fired %d runs, want 1", got)
		}
		if at, ok := s.last().TickAt(); e.c.Capabilities().Has(workflow.CapTickTime) && (!ok || at.Second() != 30) {
			e.t.Errorf("TickAt = %v %v, want a tick at second 30", at, ok)
		}
	}},
	{"six fields without Seconds are refused as invalid", func(e *env) {
		e.need(workflow.CapCron)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Cron: "30 * * * * *", Location: time.UTC})
		if !errors.Is(err, workflow.ErrInvalid) && !errors.Is(err, workflow.ErrUnsupported) {
			e.t.Fatalf("err = %v, want ErrInvalid (six fields are never guessed to be seconds)", err)
		}
	}},
	{"without CapCronSeconds Seconds is refused", func(e *env) {
		scheduleRefusal(e, workflow.CapCronSeconds, func(s *workflow.Schedule) { s.Cron, s.Seconds = "30 * * * * *", true })
	}},
}

var schedActiveCases = []testCase{
	{"ticks before Active.From do not fire; the tick at From does", func(e *env) {
		needCronFiring(e)
		e.need(workflow.CapScheduleActive)
		e.needFake("a start instant")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		from := minuteAfter(e.now(), 2)
		e.schedule(workflow.Schedule{Workflow: wf.Name, Cron: everyMinute, Location: time.UTC, Active: workflow.Active{From: from}})
		e.blockUntilTimers(1)
		e.clk.Advance(time.Minute)
		e.quiet()
		if got := c.count(); got != 0 {
			e.t.Fatalf("a tick before From fired %d runs", got)
		}
		tick(e, &c, 1, 1)
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("the tick at From fired %d runs, want 1", got)
		}
	}},
	{"no tick fires at or after Active.Until", func(e *env) {
		needCronFiring(e)
		e.need(workflow.CapScheduleActive)
		e.needFake("an end instant")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		until := minuteAfter(e.now(), 2)
		e.schedule(workflow.Schedule{Workflow: wf.Name, Cron: everyMinute, Location: time.UTC, Active: workflow.Active{Until: until}})
		tick(e, &c, 1, 1)
		// The tick at Until is outside the period; the driver may arm no
		// timer at all once the period has ended.
		e.clk.Advance(3 * time.Minute)
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("ticks fired %d runs, want 1 (only the tick before Until)", got)
		}
	}},
	{"a declared cron honours CronActive", func(e *env) {
		needCronFiring(e)
		e.need(workflow.CapScheduleActive)
		e.needFake("a start instant")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Cron = []string{everyMinute}
		wf.CronActive = workflow.Active{From: minuteAfter(e.now(), 2)}
		e.up(wf)
		e.blockUntilTimers(1)
		e.clk.Advance(time.Minute)
		e.quiet()
		if got := c.count(); got != 0 {
			e.t.Fatalf("a tick before CronActive.From fired %d runs", got)
		}
		tick(e, &c, 1, 1)
	}},
	{"Active with Until before From is invalid", func(e *env) {
		e.need(workflow.CapCron)
		now := e.now()
		err := workflow.ValidateSchedule(workflow.Schedule{Workflow: "w", Cron: everyMinute, Location: time.UTC, Active: workflow.Active{From: now, Until: now}})
		if !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("err = %v, want ErrInvalid", err)
		}
	}},
	{"without CapScheduleActive Active is refused", func(e *env) {
		scheduleRefusal(e, workflow.CapScheduleActive, func(s *workflow.Schedule) { s.Active = workflow.Active{From: e.now().Add(time.Hour)} })
	}},
}

var schedKeepCases = []testCase{
	{"KeepRuns keeps only the newest finished runs of the schedule", func(e *env) {
		needCronFiring(e)
		e.need(workflow.CapScheduleKeep | workflow.CapRunQuery)
		e.needFake("four ticks in a row")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		id := e.schedule(workflow.Schedule{Workflow: wf.Name, Cron: everyMinute, Location: time.UTC, KeepRuns: 2})
		other := e.enqueue(workflow.Job{Workflow: wf.Name}) // not the schedule's: never pruned by it
		e.waitStatus(other, workflow.StatusSucceeded, 0)
		for n := 2; n <= 5; n++ {
			tick(e, &c, 1, n)
		}
		q := as[workflow.RunQuerier](e, "workflow.RunQuerier")
		var fired []workflow.RunInfo
		e.eventually("the schedule's history to shrink to 2", 0, func() bool {
			ctx, cancel := e.callCtx()
			defer cancel()
			page, err := q.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, Labels: map[string]string{workflow.LabelSchedule: string(id)}})
			if err != nil {
				e.t.Fatalf("Runs: %v", err)
			}
			fired = page.Runs
			return len(fired) == 2
		})
		for _, r := range fired {
			if r.Status != workflow.StatusSucceeded {
				e.t.Errorf("kept run %s is %s, want the two newest finished runs", r.Ref.RunID, r.Status)
			}
		}
		if st := e.status(other); st != workflow.StatusSucceeded {
			e.t.Errorf("a run the schedule did not fire was pruned: status %s", st)
		}
	}},
	{"without CapScheduleKeep KeepRuns is refused", func(e *env) {
		scheduleRefusal(e, workflow.CapScheduleKeep, func(s *workflow.Schedule) { s.KeepRuns = 3 })
	}},
}

var tickTimeCases = []testCase{
	{"a fired run carries the tick it stands for", func(e *env) {
		needCronFiring(e)
		e.need(workflow.CapTickTime)
		var s stepInfos
		var c counter
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			c.hit(in)
			return s.handler()(ctx, in)
		})
		e.up(wf)
		want := minuteAfter(e.now(), 1)
		e.schedule(workflow.Schedule{Workflow: wf.Name, Cron: everyMinute, Location: time.UTC})
		tick(e, &c, 1, 1)
		at, ok := s.last().TickAt()
		if !ok {
			e.t.Fatalf("the fired run has no %s label: %v", workflow.LabelTickAt, s.last().Labels)
		}
		if e.clk != nil && !at.Equal(want) {
			e.t.Errorf("TickAt = %v, want the tick %v", at, want)
		}
		if at.Second() != 0 || at.Nanosecond() != 0 {
			e.t.Errorf("TickAt = %v, want a minute boundary, not the wall time the run started", at)
		}
	}},
	{"a run nobody scheduled carries no tick", func(e *env) {
		e.need(workflow.CapTickTime)
		var s stepInfos
		wf := oneStep(e.name("w"), s.handler())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
		if at, ok := s.last().TickAt(); ok {
			e.t.Errorf("an enqueued run has TickAt %v", at)
		}
	}},
}
