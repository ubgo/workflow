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

	// OnSkip, when set, is told about every attempt LatestOnly skips.
	OnSkip func(ctx context.Context, info workflow.StepInfo, tick, newer time.Time)

	// Clock is the now the newer tick is compared with; nil means
	// clock.Real. Use the driver's clock.
	Clock clock.Clock
}

// LatestOnly skips the work of a scheduled run that is no longer the most
// recent one (WP-10): when the tick after the run's own tick (LabelTickAt)
// is already due, a newer run exists or is about to, so the step returns
// its input unchanged without calling the handler. Use it for work where
// only the latest state matters, such as refreshing a cache or a
// dashboard: after a misfire catch-up fires three missed hours, or when a
// backlog delays a run past its successor's tick, only the newest one
// does the work. Runs not fired by a schedule (no tick label) always run.
//
// It needs the tick time on the run's labels, which drivers with
// CapTickTime set (memory, sqlstore, Redis, Hatchet), and the schedule's
// expression, read through Schedules (CapScheduleList) or given by Next.
// A run whose schedule is gone (unscheduled since) runs. Apply it to the
// steps whose work should be skipped (Workflow.Use or a step wrapper);
// steps downstream of a skipped step still run and receive its input.
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
			if !ok {
				return next(ctx, in)
			}
			tick, ok := info.TickAt()
			if !ok {
				return next(ctx, in)
			}
			newer, ok, err := latestNext(ctx, cfg, info, tick)
			if err != nil {
				return nil, fmt.Errorf("middleware: latest only: %w", err)
			}
			if !ok || newer.IsZero() || newer.After(clk.Now()) {
				return next(ctx, in)
			}
			if cfg.OnSkip != nil {
				cfg.OnSkip(ctx, info, tick, newer)
			}
			return in, nil
		}
	}
}

// latestNext returns the tick after tick for the run info describes.
func latestNext(ctx context.Context, cfg LatestOnlyConfig, info workflow.StepInfo, tick time.Time) (time.Time, bool, error) {
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
		sched, err := workflow.ParseCron(s.Cron, s.Seconds, cfg.Parser)
		if err != nil {
			return time.Time{}, false, err
		}
		loc := s.Location
		if loc == nil {
			loc = time.UTC
		}
		return sched.Next(tick, loc), true, nil
	}
	return time.Time{}, false, nil
}
