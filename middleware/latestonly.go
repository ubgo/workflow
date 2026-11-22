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
//
//ds:def id=mw-latestonlyconfig-vv2859xc owner=@khanakia stability=stable
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

	// DefaultMisfire is the misfire policy the driver applies to a
	// schedule whose Misfire is empty, so LatestOnly knows which missed
	// ticks fire. Zero means MisfireFireOnce, the default of the memory,
	// SQL and Redis drivers (a missed tick fires once). Set
	// Misfire{Policy: MisfireSkip} for a driver that never catches up
	// (Hatchet).
	DefaultMisfire workflow.Misfire

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
// Schedules: the cron in its Location, its Active period and Windows,
// When for the workflow's ScheduleWhen, and its misfire policy. A due
// tick counts only when the policy fires it on a catch-up from the run's
// tick to now (Misfire.Due; DefaultMisfire for a schedule without one):
// whether the scheduler fired every tick on time or was down and catches
// up now, that tick or a newer one fires, while a tick the policy drops
// (MisfireSkip past MisfireGrace, a Within deadline) may never have
// fired. A paused schedule fires nothing, and with OverlapSkip no tick
// fires while this run is unfinished, so neither counts a predicted
// tick. A fire the driver recorded after the run's tick
// (ScheduleInfo.Last, with CapScheduleHistory) makes the run stale
// whatever the rules say, so on such a driver a run delayed past ticks
// that fired on time skips under every policy.
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
//
//ds:def id=mw-latestonly-g8qwm9je owner=@khanakia stability=stable
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

// latestNewer returns the tick after tick, for the run info describes,
// that makes the run stale: the oldest due tick the schedule fires by its
// own rules (nextFire), else a newer fire the driver recorded. ok false
// means unknown or none.
//
//ds:def id=mw-latestnewer-myhc2v57 owner=@khanakia stability=stable
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
		if err != nil || ok {
			return next, ok, err
		}
		if last := s.Last; last.Fire.After(tick) && last.Run.RunID != info.RunID {
			return last.Fire, true, nil // a newer run exists, whatever the rules say
		}
		return time.Time{}, false, nil
	}
	return time.Time{}, false, nil
}

// nextFire is the oldest due tick of s after tick (at or before now) that
// fires or will fire, when no newer fire is recorded. None while s is
// paused or while OverlapSkip holds ticks back for this unfinished run.
// Otherwise the ticks are the ones s's misfire policy (or
// cfg.DefaultMisfire for the driver default) fires on a catch-up from
// tick to now (Misfire.Due, over the ticks inside Active): whatever the
// scheduler did in between (fired every tick on time, or was down and
// catches up now), that tick or a newer one fires, while a tick the
// policy drops may never have fired. A tick outside Windows or rejected
// by cfg.When fires nothing. ok false means none.
//
//ds:def id=mw-nextfire-725thfze owner=@khanakia stability=stable
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
	policy := s.Misfire
	if policy.IsZero() {
		policy = cfg.DefaultMisfire
	}
	if policy.IsZero() {
		policy = workflow.Misfire{Policy: workflow.MisfireFireOnce}
	}
	for _, at := range policy.Due(activeTicks{sched, s.Active}, tick, now, loc) {
		if !s.Windows.IsZero() && !s.Windows.Allows(at, loc) {
			continue
		}
		if cfg.When != nil && !cfg.When(ctx, latestTick(s, at, loc)) {
			continue
		}
		return at, true, nil
	}
	return time.Time{}, false, nil
}

// activeTicks is a schedule's ticks limited to its Active period, as the
// drivers' misfire plans see them: a tick outside it is no tick at all.
type activeTicks struct {
	src workflow.TickSource
	act workflow.Active
}

func (a activeTicks) Between(from, to time.Time, loc *time.Location, limit int) []time.Time {
	if !a.act.From.IsZero() && from.Before(a.act.From) {
		from = a.act.From.Add(-time.Nanosecond)
	}
	if !a.act.Until.IsZero() && !to.Before(a.act.Until) {
		to = a.act.Until.Add(-time.Nanosecond)
	}
	if !to.After(from) {
		return nil
	}
	return a.src.Between(from, to, loc, limit)
}

// latestTick is the Tick the scheduler built for s at at, as When sees it.
func latestTick(s workflow.ScheduleInfo, at time.Time, loc *time.Location) workflow.Tick {
	return workflow.Tick{ScheduleID: s.ID, Name: s.Name, Workflow: s.Workflow, Declared: s.Declared, At: at, Location: loc, Outcome: workflow.TickFired, Labels: s.Labels}
}
