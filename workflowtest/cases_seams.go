package workflowtest

import (
	"context"
	"time"

	"github.com/ubgo/workflow"
)

// failOnce returns a handler that counts every attempt and fails the
// first with err, so the case can watch when the retry comes.
func (c *counter) failOnce(err error) workflow.Handler {
	return func(_ context.Context, in []byte) ([]byte, error) {
		c.hit(in)
		if c.count() == 1 {
			return nil, err
		}
		return in, nil
	}
}

// assertNoRetryBefore lets d pass (on the fake clock, or in real time)
// and fails if the step was tried more than once by then.
func assertNoRetryBefore(e *env, c *counter, d time.Duration, why string) {
	e.t.Helper()
	e.eventually("the first attempt", 0, func() bool { return c.count() >= 1 })
	e.quiet() // let the driver arm the retry before the clock moves
	if e.clk != nil {
		e.clk.Advance(d)
		e.quiet()
	} else {
		time.Sleep(d)
	}
	if got := c.count(); got != 1 {
		e.t.Fatalf("%d attempts before %v passed, want 1 (%s)", got, d, why)
	}
}

var retrySeamCases = []testCase{
	{"a RetryAfter delay replaces the backoff under DelayFor", func(e *env) {
		e.need(workflow.CapRetryDelay)
		u := e.tm.unit
		var c counter
		wf := oneStep(e.name("w"), c.failOnce(workflow.RetryAfter(errBoom, u)))
		// No Backoff: a driver that ignored DelayFor would retry at once.
		wf.Retry = workflow.RetryPolicy{Max: 1, DelayFor: workflow.RetryAfterDelay}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		assertNoRetryBefore(e, &c, u/2, "the retry must wait for RetryAfter")
		e.waitStatus(ref, workflow.StatusSucceeded, u/4)
		if got := c.count(); got != 2 {
			e.t.Fatalf("%d attempts, want 2", got)
		}
	}},
	{"Backoff.Jitter never shortens a delay below (1-Jitter)", func(e *env) {
		e.need(workflow.CapRetryJitter)
		u := e.tm.unit
		var c counter
		wf := oneStep(e.name("w"), c.failOnce(errBoom))
		wf.Retry = workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: u, Jitter: 0.5}}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		assertNoRetryBefore(e, &c, u/4, "a jittered delay stays within [Initial/2, Initial]")
		e.waitStatus(ref, workflow.StatusSucceeded, u/4)
		if got := c.count(); got != 2 {
			e.t.Fatalf("%d attempts, want 2", got)
		}
	}},
}

// misfireHourly is the expression of the misfire cases: hourly, so a
// jump of the fake clock misses a known number of ticks.
const misfireHourly = "0 * * * *"

// misfireCase schedules an hourly cron under m, jumps the fake clock past
// three ticks so the newest is 30 minutes late, and asserts that exactly
// want runs fire. Then it moves to the next tick on time and asserts it
// fires once, so no policy may stop the schedule for good.
func misfireCase(m workflow.Misfire, want int) func(e *env) {
	return func(e *env) {
		e.need(workflow.RequiresSchedule(workflow.Schedule{Location: time.UTC, Misfire: m}))
		if e.clk == nil {
			e.t.Skip("missing cron ticks on purpose needs a fake clock")
		}
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Schedule(ctx, workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: misfireHourly, Location: time.UTC, Misfire: m})
		if err != nil {
			e.t.Fatalf("Schedule: %v", err)
		}
		e.blockUntilTimers(1)
		first := e.clk.Now().UTC().Truncate(time.Hour).Add(time.Hour)
		e.clk.Set(first.Add(2*time.Hour + 30*time.Minute)) // ticks first, +1h, +2h missed
		if want > 0 {
			e.eventually("the missed ticks to fire", 0, func() bool { return c.count() >= want })
		}
		e.quiet()
		if got := c.count(); got != want {
			e.t.Fatalf("policy %+v fired %d runs for 3 missed ticks (newest 30m late), want %d", m, got, want)
		}
		e.clk.Set(first.Add(3 * time.Hour)) // the next tick, on time
		e.eventually("the on-time tick to fire", 0, func() bool { return c.count() >= want+1 })
		e.quiet()
		if got := c.count(); got != want+1 {
			e.t.Fatalf("policy %+v: the on-time tick brought the count to %d, want %d", m, got, want+1)
		}
	}
}

var misfireCases = []testCase{
	{"MisfireSkip fires no missed tick", misfireCase(workflow.Misfire{Policy: workflow.MisfireSkip}, 0)},
	{"MisfireFireOnce fires the newest missed tick once", misfireCase(workflow.Misfire{Policy: workflow.MisfireFireOnce}, 1)},
	{"MisfireFireOnce drops a tick older than Within", misfireCase(workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: 10 * time.Minute}, 0)},
	{"MisfireFireAll fires every missed tick", misfireCase(workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 10}, 3)},
	{"MisfireFireAll fires at most Max", misfireCase(workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 2}, 2)},
	{"MisfireFireAll drops ticks older than Within", misfireCase(workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 10, Within: 100 * time.Minute}, 2)},
	{"a declared Cron runs in its CronLocation, or Start refuses the zone", func(e *env) {
		e.need(workflow.CapCron)
		if e.clk == nil {
			e.t.Skip("checking the zone needs a fake clock to jump to a chosen hour")
		}
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Cron = []string{"0 9 * * *"}
		wf.CronLocation = time.FixedZone("UTC+5", 5*60*60)
		err := e.c.Register(wf)
		if err == nil {
			err = e.tryStart()
		}
		if !e.c.Capabilities().Has(workflow.CapCronZone) {
			assertUnsupported(e.t, err, "", workflow.CapCronZone)
			return
		}
		if err != nil {
			e.t.Fatalf("Register/Start with a CronLocation: %v", err)
		}
		// 09:00 in UTC+5 is 04:00 UTC; a driver that ignored the zone
		// would fire at 09:00 UTC instead.
		e.blockUntilTimers(1)
		now := e.clk.Now().UTC()
		fire := time.Date(now.Year(), now.Month(), now.Day(), 4, 0, 0, 0, time.UTC)
		for !fire.After(now.Add(time.Minute)) {
			fire = fire.AddDate(0, 0, 1)
		}
		e.clk.Set(fire.Add(-30 * time.Second))
		e.quiet()
		if got := c.count(); got != 0 {
			e.t.Fatalf("the declared cron fired %d times before 09:00 in its zone", got)
		}
		e.clk.Advance(time.Minute)
		e.eventually("the declared cron to fire at 09:00 in its zone", 0, func() bool { return c.count() == 1 })
	}},
	{"Schedule refuses a zone or misfire policy the driver lacks", func(e *env) {
		e.need(workflow.CapCron)
		have := e.c.Capabilities()
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		tested := 0
		for _, cand := range []struct {
			cap   workflow.Caps
			field string
			edit  func(*workflow.Schedule)
		}{
			{workflow.CapCronZone, "Location", func(s *workflow.Schedule) { s.Location = time.FixedZone("UTC+5", 5*60*60) }},
			{workflow.CapMisfireSkip, "Misfire", func(s *workflow.Schedule) { s.Misfire = workflow.Misfire{Policy: workflow.MisfireSkip} }},
			{workflow.CapMisfireFireOnce, "Misfire", func(s *workflow.Schedule) { s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce} }},
			{workflow.CapMisfireFireAll, "Misfire", func(s *workflow.Schedule) { s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 1} }},
		} {
			if have.Has(cand.cap) {
				continue
			}
			tested++
			s := workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: misfireHourly, Location: time.UTC}
			cand.edit(&s)
			_, err := e.c.Schedule(ctx, s)
			assertUnsupported(e.t, err, cand.field, 0)
		}
		if tested == 0 {
			e.t.Skip("driver has CapCronZone and every misfire capability")
		}
	}},
}
