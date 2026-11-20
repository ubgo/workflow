// Command schedule-tools shows the helpers around a schedule: the
// extended "L", "W" and "#" day fields, hashed spread with "H", English
// descriptions, exclusion calendars, backfilling missed ticks, a limited
// and a one-shot schedule pruned once ended, a run at startup once per
// day across nodes, and jitter on the fire time. Everything runs on the
// in-memory driver on a fake clock, so the output is the same on every
// run.
//
// It is the program behind docs/schedules/expressions.md and
// docs/schedules/backfill.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "schedule-tools:", err)
		os.Exit(1)
	}
}

// monday is where every listing and fake clock starts: Monday 12 October
// 2026, 00:00 UTC.
var monday = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		return err
	}
	steps := []func(context.Context, *time.Location) error{extended, hashed, describe, calendars, backfill, limited, startup, jitter}
	for _, step := range steps {
		if err := step(ctx, berlin); err != nil {
			return err
		}
	}
	return nil
}

// recorder is a one-step workflow that records the tick of every run and
// the clock time its step started.
type recorder struct {
	clk   clock.Clock
	mu    sync.Mutex
	ticks []time.Time
	ran   []time.Time
}

func (r *recorder) workflow(name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "run", Run: r.step}}}
}

// snippet:tickat
func (r *recorder) step(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	day, _ := info.TickAt() // the tick this run is for, not the time it started
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ticks = append(r.ticks, day)
	// snippet:end
	if r.clk != nil {
		r.ran = append(r.ran, r.clk.Now())
	}
	return nil, nil
}

// wait polls until the recorder ran n times and returns the ticks, sorted.
func (r *recorder) wait(n int) ([]time.Time, error) {
	for range 5000 {
		r.mu.Lock()
		got := slices.Clone(r.ticks)
		r.mu.Unlock()
		if len(got) >= n {
			slices.SortFunc(got, func(a, b time.Time) int { return a.Compare(b) })
			return got, nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil, fmt.Errorf("ran fewer than %d times", n)
}

// fakeClient starts a memory client on a fake clock at start.
func fakeClient(ctx context.Context, start time.Time, wfs []workflow.Workflow, opts ...memory.Option) (*memory.Client, *clock.Fake, error) {
	fc := clock.NewFake(start)
	c := memory.New(append([]memory.Option{memory.WithClock(fc)}, opts...)...)
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return nil, nil, err
		}
	}
	return c, fc, c.Start(ctx)
}

// stepTo waits until n timers are armed (the schedules are asleep), then
// moves the fake clock to at.
func stepTo(ctx context.Context, fc *clock.Fake, n int, at time.Time) error {
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := fc.BlockUntil(wait, n); err != nil {
		return err
	}
	fc.Set(at)
	return nil
}

// extended lists the fires of the extended day fields and runs one on a
// driver built with the extended parser.
func extended(ctx context.Context, _ *time.Location) error {
	fmt.Println("extended:")
	for _, expr := range []string{"0 9 L * *", "0 9 15W * *", "0 18 * * FRIL", "0 9 * * MON#1"} {
		// snippet:extended
		s, err := cron.ParseExtended(expr) // L, L-n, nW, LW, d#n and dL on top of the five fields
		if err != nil {
			return err
		}
		next := s.Between(monday, monday.AddDate(0, 3, 0), time.UTC, 3)
		// snippet:end
		var days []string
		for _, t := range next {
			days = append(days, t.Format("Mon Jan 2 15:04"))
		}
		fmt.Printf("  %-14s %v\n", expr, days)
	}
	rec := &recorder{}
	// snippet:extended-driver
	c, fc, err := fakeClient(ctx, monday, []workflow.Workflow{rec.workflow("month-end")},
		memory.WithCronParser(cron.Extended)) // opt-in: this driver now reads L, W and #
	if err != nil {
		return err
	}
	defer c.Stop()
	_, err = c.Schedule(ctx, workflow.Schedule{Name: "month-end", Workflow: "month-end", Cron: "0 9 L * *", Location: time.UTC})
	// snippet:end
	if err != nil {
		return err
	}
	if err := stepTo(ctx, fc, 1, time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC)); err != nil {
		return err
	}
	ticks, err := rec.wait(1)
	if err != nil {
		return err
	}
	_, refused := memory.New().Schedule(ctx, workflow.Schedule{Workflow: "month-end", Cron: "0 9 L * *", Location: time.UTC})
	fmt.Printf("  memory with cron.Extended fired for %s; the default parser refuses L: %t\n", ticks[0].Format("Jan 2 15:04"), refused != nil)
	return nil
}

// hashed spreads one expression over tenants.
func hashed(context.Context, *time.Location) error {
	fmt.Println("hashed:")
	for _, tenant := range []string{"acme", "globex", "initech"} {
		// snippet:hash
		expr, err := cron.Hash("H H(0-5) * * *", tenant+"/nightly-export") // once a night, 00:00-05:59
		if err != nil {
			return err
		}
		// snippet:end
		fmt.Printf("  %-8s %s\n", tenant, expr)
	}
	return nil
}

// describe prints the English form of a few expressions.
func describe(context.Context, *time.Location) error {
	fmt.Println("describe:")
	for _, expr := range []string{"0 9 * * MON-FRI", "*/15 * * * *", "5 * * * *", "0 0 L * *", "30 8 * * MON#1", "@every 1h30m"} {
		// snippet:describe
		text, err := cron.Describe(expr) // "0 9 * * MON-FRI" is "At 09:00, Monday through Friday"
		if err != nil {
			return err
		}
		// snippet:end
		fmt.Printf("  %-16s %s\n", expr, text)
	}
	text, err := cron.DescribeSeconds("*/10 * * * * *")
	if err != nil {
		return err
	}
	fmt.Printf("  %-16s %s\n", "*/10 * * * * *", text)
	return nil
}

// calendars skips holidays and weekends on a daily schedule and prints
// what the tick observer saw for each day.
func calendars(ctx context.Context, berlin *time.Location) error {
	fmt.Println("calendars:")
	// snippet:calendar
	holidays, err := workflow.ExcludeDates(berlin, "2026-12-24", "2026-12-25", "2026-12-26")
	if err != nil {
		return err
	}
	weekends, err := workflow.ExcludeWeekdays(berlin, time.Saturday, time.Sunday)
	if err != nil {
		return err
	}
	wf := workflow.Workflow{
		Name:         "standup-digest",
		Steps:        []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
		Cron:         []string{"0 9 * * *"},
		CronLocation: berlin,
		ScheduleWhen: workflow.SkipExcluded(workflow.AnyExcluded(holidays, weekends)),
	}
	// snippet:end
	start := time.Date(2026, 12, 22, 0, 0, 0, 0, berlin)
	fc := clock.NewFake(start)
	c := memory.New(memory.WithClock(fc))
	ticks := make(chan workflow.Tick, 16)
	if err := c.OnTick(func(_ context.Context, t workflow.Tick) { ticks <- t }); err != nil {
		return err
	}
	if err := c.Register(wf); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	for day := range 7 {
		if err := stepTo(ctx, fc, 1, start.AddDate(0, 0, day).Add(9*time.Hour)); err != nil {
			return err
		}
		select {
		case t := <-ticks:
			if t.Reason != "" {
				fmt.Printf("  %s %s (%s)\n", t.At.In(berlin).Format("Mon Jan 2"), t.Outcome, t.Reason)
			} else {
				fmt.Printf("  %s %s\n", t.At.In(berlin).Format("Mon Jan 2"), t.Outcome)
			}
		case <-time.After(5 * time.Second):
			return errors.New("no tick reported")
		}
	}
	return nil
}

// backfill replays three days the scheduler missed, twice.
func backfill(ctx context.Context, berlin *time.Location) error {
	fmt.Println("backfill:")
	rec := &recorder{}
	c := memory.New()
	if err := c.Register(rec.workflow("daily-report")); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	report := workflow.Schedule{Name: "daily-report", Workflow: "daily-report", Cron: "0 2 * * *", Location: berlin}
	// snippet:backfill
	spec := workflow.BackfillSpec{
		Schedule: &report, // the schedule as declared: workflow, cron, zone, input, labels
		From:     time.Date(2026, 10, 1, 0, 0, 0, 0, berlin),
		To:       time.Date(2026, 10, 4, 0, 0, 0, 0, berlin), // [From, To): Oct 1, 2 and 3
	}
	ticks, err := workflow.BackfillPlan(spec) // what would run, nothing enqueued
	if err != nil {
		return err
	}
	refs, err := workflow.Backfill(ctx, c, spec) // one run per tick, keyed by schedule and tick
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Printf("  plan: %d ticks, first %s\n", len(ticks), ticks[0].Format("Jan 2 15:04 MST"))
	ran, err := rec.wait(3)
	if err != nil {
		return err
	}
	for _, t := range ran {
		fmt.Printf("  run for %s\n", t.In(berlin).Format("Jan 2 15:04"))
	}
	again, err := workflow.Backfill(ctx, c, spec)
	if err != nil {
		return err
	}
	fmt.Printf("  started %d; a repeat returns the same runs: %t\n", len(refs), slices.Equal(again, refs))
	return nil
}

// limited runs a schedule three times, a one-shot once, and prunes both.
func limited(ctx context.Context, _ *time.Location) error {
	fmt.Println("limited:")
	start := monday.Add(10 * time.Hour)
	poll, once := &recorder{}, &recorder{}
	c, fc, err := fakeClient(ctx, start, []workflow.Workflow{poll.workflow("poll-export"), once.workflow("send-reminder")})
	if err != nil {
		return err
	}
	defer c.Stop()
	// snippet:limit
	s, err := workflow.LimitRuns(workflow.Schedule{Workflow: "poll-export", Cron: "*/15 * * * *", Location: time.UTC},
		3, fc.Now(), nil) // the next three ticks from now, then the Active period ends
	if err != nil {
		return err
	}
	if _, err := c.Schedule(ctx, s); err != nil {
		return err
	}
	reminder, err := workflow.OneShot(workflow.Schedule{Workflow: "send-reminder", Location: time.UTC},
		start.Add(20*time.Minute)) // fires once, at 10:20
	if err != nil {
		return err
	}
	if _, err := c.Schedule(ctx, reminder); err != nil {
		return err
	}
	// snippet:end
	fmt.Printf("  limited: %s, active %s to %s\n", s.Cron, s.Active.From.Format("15:04:05"), s.Active.Until.Format("15:04:05"))
	fmt.Printf("  one-shot: %s\n", reminder.Cron)
	// Walk the clock through the ticks; armed is how many schedules
	// still sleep on a timer at that point.
	for _, step := range []struct {
		at    time.Duration
		armed int
	}{{15 * time.Minute, 2}, {20 * time.Minute, 2}, {30 * time.Minute, 1}, {45 * time.Minute, 1}} {
		if err := stepTo(ctx, fc, step.armed, start.Add(step.at)); err != nil {
			return err
		}
	}
	fc.Set(start.Add(time.Hour)) // nothing is armed any more: both periods ended
	polls, err := poll.wait(3)
	if err != nil {
		return err
	}
	reminders, err := once.wait(1)
	if err != nil {
		return err
	}
	fmt.Printf("  poll-export ran %d times (last %s), send-reminder %d time\n", len(polls), polls[len(polls)-1].Format("15:04"), len(reminders))
	// snippet:prune
	pruned, err := workflow.PruneEndedSchedules(ctx, c, fc.Now()) // Active ended + MisfireGrace
	if err != nil {
		return err
	}
	// snippet:end
	left, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("  pruned %d schedules, %d left\n", len(pruned), len(left))
	return nil
}

// startup boots three "nodes" on one client: per node each starts a run,
// once per day only the first does.
func startup(ctx context.Context, berlin *time.Location) error {
	fmt.Println("startup:")
	rec := &recorder{}
	c, fc, err := fakeClient(ctx, monday.Add(7*time.Hour), []workflow.Workflow{rec.workflow("warm-cache")})
	if err != nil {
		return err
	}
	defer c.Stop()
	warm := workflow.Schedule{Name: "warm-cache", Workflow: "warm-cache", Cron: "0 * * * *", Location: berlin}
	var refs []workflow.RunRef
	for range 3 { // three replicas booting in one rollout
		// snippet:startup
		ref, err := workflow.FireAtStartup(ctx, c, warm,
			workflow.StartupOncePer(workflow.PeriodDay, berlin), // one run per Berlin day across the fleet
			workflow.StartupClock(fc))
		if err != nil {
			return err
		}
		// snippet:end
		refs = append(refs, ref)
	}
	if _, err := rec.wait(1); err != nil {
		return err
	}
	fmt.Printf("  three boots, once per day: %d run\n", len(slices.Compact(refs)))
	return nil
}

// jitter spreads a 09:00 fire over five minutes.
func jitter(ctx context.Context, _ *time.Location) error {
	fmt.Println("jitter:")
	start := monday.Add(8 * time.Hour)
	fc := clock.NewFake(start)
	rec := &recorder{clk: fc}
	// snippet:jitter
	c := memory.New(memory.WithClock(fc), memory.WithMiddleware(
		middleware.FireJitter(5*time.Minute, middleware.WithFireJitterClock(fc)), // each fired run waits tick + [0, 5m)
	))
	if err := middleware.CheckFireJitter(c); err != nil { // needs CapSnooze and CapTickTime
		return err
	}
	// snippet:end
	if err := c.Register(rec.workflow("nightly-sync")); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "nightly-sync", Cron: "0 9 * * *", Location: time.UTC}); err != nil {
		return err
	}
	tick := monday.Add(9 * time.Hour)
	if err := stepTo(ctx, fc, 1, tick); err != nil {
		return err
	}
	// The run fired and its step snoozed: the schedule and the snooze
	// each wait on a timer. The deadline is tick + the run's jitter.
	var fired workflow.RunID
	for range 5000 {
		page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "nightly-sync", Limit: 1})
		if err != nil {
			return err
		}
		if len(page.Runs) == 1 {
			fired = page.Runs[0].Ref.RunID
			break
		}
		time.Sleep(time.Millisecond)
	}
	wait := middleware.RunIDJitter(workflow.StepInfo{RunID: fired}, 5*time.Minute)
	if err := stepTo(ctx, fc, 2, tick.Add(wait)); err != nil {
		return err
	}
	if _, err := rec.wait(1); err != nil {
		return err
	}
	fmt.Printf("  tick %s, step started %s\n", tick.Format("15:04:05"), rec.ran[0].Format("15:04:05"))
	return nil
}
