package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

// LatestOnlyConfig configures LatestOnly.
type LatestOnlyConfig struct {
	// Schedules looks up the schedule that fired a run (its
	// LabelSchedule), to compute the tick after the run's own. Usually
	// the client itself (CapScheduleList). Required unless Next is set.
	Schedules workflow.ScheduleLister

	// Next, when set, returns the tick that follows tick for the run
	// info describes, in place of the lookup through Schedules; ok false
	// means unknown, and the step runs. For a schedule kept outside the
	// client or a custom calendar.
	Next func(ctx context.Context, info workflow.StepInfo, tick time.Time) (next time.Time, ok bool, err error)

	// Parser is the cron dialect of the schedules when the drivers use a
	// custom one (their WithCronParser option); nil means the default.
	Parser cron.Parser

	// When is the workflow's ScheduleWhen, when it has one: a due tick
	// the condition rejects fired no run, so it does not make a run
	// stale. LatestOnly asks it, on the step's context, about each tick
	// after the run's own that is already due, as the scheduler did when
	// the tick came; set it to the same function as Workflow.ScheduleWhen.
	// Nil means every tick passes. Not used with Next.
	When func(ctx context.Context, tick workflow.Tick) bool

	// OnSkip, when set, is told about every attempt LatestOnly skips.
	OnSkip func(ctx context.Context, info workflow.StepInfo, tick, newer time.Time)

	// Clock is the now the newer tick is compared with; nil means
	// clock.Real. Use the driver's clock.
	Clock clock.Clock
}

// LatestOnly skips the work of a scheduled run that is no longer the most
// recent one (WP-10): when a tick after the run's own tick (LabelTickAt)
// that the schedule really fires is already due, a newer run exists or is
// about to, so the step returns its input unchanged without calling the
// handler. Use it for work where only the latest state matters, such as
// refreshing a cache or a dashboard: after a misfire catch-up fires three
// missed hours, or when a backlog delays a run past its successor's tick,
// only the newest one does the work.
//
// The newer tick is found through the schedule's own rules, as listed by
// Schedules: the cron in its Location, its Active period and Windows
// (NextRuns), and When for the workflow's ScheduleWhen. A paused
// schedule fires nothing, and with OverlapSkip no tick fires while this
// run is unfinished, so neither counts a predicted tick. A fire the
// driver recorded after the run's tick (ScheduleInfo.Last, with
// CapScheduleHistory) makes the run stale whatever the rules say.
//
// Runs always do the work when they were not fired by a schedule (no tick
// label), when someone asked for them (LabelTrigger: a Backfill, a manual
// trigger, a FireAtStartup run), and when their schedule is gone
// (unscheduled since) or its next tick is unknown.
//
// It needs the tick time on the run's labels, which drivers with
// CapTickTime set (memory, sqlstore, Redis, Hatchet), and the schedule,
// read through Schedules (CapScheduleList) or given by Next. Apply it to
// the steps whose work should be skipped by wrapping their handler
// (Step{Run: latest(refresh)}), or to every step with a driver's
// WithMiddleware option; steps downstream of a skipped step still run and
// receive its input.
func LatestOnly(cfg LatestOnlyConfig) workflow.Middleware {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if cfg.Schedules == nil && cfg.Next == nil {
				return nil, workflow.Permanent(fmt.Errorf("%w: LatestOnly needs Schedules or Next", workflow.ErrInvalid))
			}
			info, ok := workflow.StepInfoFrom(ctx)
			if !ok || info.Labels[workflow.LabelTrigger] != "" {
				return next(ctx, in)
			}
			tick, ok := info.TickAt()
			if !ok {
				return next(ctx, in)
			}
			now := clk.Now()
			newer, ok, err := latestNewer(ctx, cfg, info, tick, now)
			if err != nil {
				return nil, fmt.Errorf("middleware: latest only: %w", err)
			}
			if !ok || newer.IsZero() || newer.After(now) {
				return next(ctx, in)
			}
			if cfg.OnSkip != nil {
				cfg.OnSkip(ctx, info, tick, newer)
			}
			return in, nil
		}
	}
}

// latestScan bounds how many due ticks after a run's own LatestOnly asks
// When about: a condition that rejects every one of thousands of ticks
// (a seconds cron, a long backlog) ends the search with "unknown", so
// the run does its work instead of looping.
const latestScan = 1000

// latestNewer returns the first tick after tick, for the run info
// describes, that makes the run stale: the next tick the schedule really
// fires, or, when that is not due by now, a later fire the driver
// recorded. ok false means unknown.
func latestNewer(ctx context.Context, cfg LatestOnlyConfig, info workflow.StepInfo, tick, now time.Time) (time.Time, bool, error) {
	if cfg.Next != nil {
		return cfg.Next(ctx, info, tick)
	}
	id := info.Labels[workflow.LabelSchedule]
	if id == "" {
		return time.Time{}, false, nil
	}
	list, err := cfg.Schedules.Schedules(ctx)
	if err != nil {
		return time.Time{}, false, err
	}
	for _, s := range list {
		if string(s.ID) != id {
			continue
		}
		next, ok, err := nextFire(ctx, cfg, s, tick, now)
		if err != nil || (ok && !next.After(now)) {
			return next, ok, err
		}
		if last := s.Last; last.Fire.After(tick) && last.Run.RunID != info.RunID {
			return last.Fire, true, nil
		}
		return next, ok, nil
	}
	return time.Time{}, false, nil
}

// nextFire is the first tick of s after tick that s fires by its own
// rules: none while it is paused or while OverlapSkip holds ticks back
// for this unfinished run; otherwise the next tick inside its Active
// period and Windows (workflow.NextRuns) that cfg.When accepts, asked
// only about ticks already due by now. ok false means unknown.
func nextFire(ctx context.Context, cfg LatestOnlyConfig, s workflow.ScheduleInfo, tick, now time.Time) (time.Time, bool, error) {
	if s.Paused || s.Overlap.Blocks(workflow.StatusRunning) {
		return time.Time{}, false, nil
	}
	sched, err := workflow.ParseCron(s.Cron, s.Seconds, cfg.Parser)
	if err != nil {
		return time.Time{}, false, err
	}
	loc := s.Location
	if loc == nil {
		loc = time.UTC
	}
	after := tick
	for range latestScan {
		runs := workflow.NextRuns(workflow.NextRunsQuery{Schedule: sched, Location: loc, After: after, Count: 1, Windows: s.Windows, Active: s.Active})
		if len(runs) == 0 {
			return time.Time{}, false, nil
		}
		at := runs[0].UTC
		if at.After(now) || cfg.When == nil || cfg.When(ctx, latestTick(s, at, loc)) {
			return at, true, nil
		}
		after = at
	}
	return time.Time{}, false, nil
}

// latestTick is the Tick the scheduler built for s at at, as When sees it.
func latestTick(s workflow.ScheduleInfo, at time.Time, loc *time.Location) workflow.Tick {
	return workflow.Tick{ScheduleID: s.ID, Name: s.Name, Workflow: s.Workflow, Declared: s.Declared, At: at, Location: loc, Outcome: workflow.TickFired, Labels: s.Labels}
}
