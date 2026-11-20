package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/ubgo/workflow/clock"
)

// LabelTriggerStartup is the LabelTrigger value of a run FireAtStartup
// started, so it is told apart from a cron fire, a manual trigger and a
// backfill.
const LabelTriggerStartup = "startup"

// StartupOption tunes FireAtStartup.
type StartupOption func(*startupOptions)

type startupOptions struct {
	once   bool
	period Period
	loc    *time.Location
	clk    clock.Clock
}

// StartupOncePer makes FireAtStartup start the run once per period p on
// loc's wall clock across every node that boots, instead of once per
// node: the run's idempotency key (and LabelRunKey) is built from the
// schedule and PeriodKey(now, p, loc), so ten replicas booting in one
// rollout start one run, and a node that restarts the same day starts
// none (while the first run is unfinished, or, on a client with
// CapRunQuery, while the driver still keeps it). It needs
// CapIdempotencyKey; FireAtStartup refuses without it. loc is required
// (time.Local is refused, as for PeriodKey).
func StartupOncePer(p Period, loc *time.Location) StartupOption {
	return func(o *startupOptions) { o.once, o.period, o.loc = true, p, loc }
}

// StartupClock sets the clock FireAtStartup reads now from (the tick
// label and the period); the default is clock.Real.
func StartupClock(c clock.Clock) StartupOption {
	return func(o *startupOptions) {
		if c != nil {
			o.clk = c
		}
	}
}

// FireAtStartup enqueues one run of s.Workflow now, as if s had ticked,
// for a schedule that must also run when the app boots (warm a cache,
// catch up after a deploy) and not only at its next tick. Call it after
// Start. The run gets s.Input, s.Tenant and s.Labels plus the labels a
// manual trigger gets, with the trigger marked as a startup:
// LabelScheduleName (s.Name, else s.Cron), LabelTickAt set to now (the
// step's StepInfo.TickAt), and LabelTrigger set to LabelTriggerStartup.
// It carries no LabelSchedule, since s need not be registered yet.
//
// By default every node that calls it starts a run (per node, as a boot
// hook is). StartupOncePer starts one per period across the fleet. It
// validates s as Client.Schedule would and refuses (ErrInvalid) a bad
// one. It runs on every driver; StartupOncePer needs CapIdempotencyKey.
func FireAtStartup(ctx context.Context, c Client, s Schedule, opts ...StartupOption) (RunRef, error) {
	o := startupOptions{clk: clock.Real{}}
	for _, opt := range opts {
		opt(&o)
	}
	if err := ValidateSchedule(s); err != nil {
		return RunRef{}, fmt.Errorf("FireAtStartup: %w", err)
	}
	now := o.clk.Now()
	name := s.Name
	if name == "" {
		name = s.Cron
	}
	labels := FiredLabelsAt(s.Labels, "", name, now)
	delete(labels, LabelSchedule)
	labels[LabelTrigger] = LabelTriggerStartup
	job := Job{Workflow: s.Workflow, Payload: s.Input, Labels: labels, Tenant: s.Tenant}
	ctx = AllowReservedLabels(ctx, LabelScheduleName, LabelTickAt, LabelTrigger, LabelRunKey)
	if !o.once {
		return c.Enqueue(ctx, job)
	}
	if !c.Capabilities().Has(CapIdempotencyKey) {
		return RunRef{}, &UnsupportedError{Field: "StartupOncePer", Missing: CapIdempotencyKey}
	}
	period, err := PeriodKey(now, o.period, o.loc)
	if err != nil {
		return RunRef{}, fmt.Errorf("FireAtStartup: %w", err)
	}
	scope := fmt.Sprintf("%s\x00%s\x00%s\x00%t", s.Workflow, name, s.Location, s.Seconds)
	key := scheduleRunKey("startup", scope, period)
	job.IdempotencyKey = key
	labels[LabelRunKey] = key
	return enqueueUnlessKeyed(ctx, c, job, key)
}
