package middleware

import (
	"context"
	"hash/fnv"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// FireJitterNeeds is what FireJitter needs from the driver: the tick
// label on fired runs (CapTickTime) and a snooze that spends no attempt
// (CapSnooze). CheckFireJitter tests a client against it.
var FireJitterNeeds = workflow.CapsOf(workflow.CapTickTime, workflow.CapSnooze)

// JitterFunc picks a run's jitter in [0, window) from its StepInfo. It must
// be deterministic: every attempt and every node asks it again and must
// get the same answer, or a run would snooze to a moving target.
type JitterFunc func(info workflow.StepInfo, window time.Duration) time.Duration

// RunIDJitter is the default JitterFunc: FNV-1a of the run ID, modulo
// window, so runs fired by one tick spread evenly across the window while
// each run's own delay never changes.
func RunIDJitter(info workflow.StepInfo, window time.Duration) time.Duration {
	h := fnv.New64a()
	h.Write([]byte(info.RunID))
	if window <= 0 {
		return 0
	}
	return time.Duration(h.Sum64() % uint64(window))
}

// FireJitterOption tunes FireJitter.
type FireJitterOption func(*fireJitter)

// WithJitterFunc replaces RunIDJitter. Nil is ignored.
func WithJitterFunc(f JitterFunc) FireJitterOption {
	return func(j *fireJitter) {
		if f != nil {
			j.pick = f
		}
	}
}

// WithFireJitterClock sets the clock now is read from; the default is
// clock.Real. Use the driver's clock (a clock.Fake in tests).
func WithFireJitterClock(c clock.Clock) FireJitterOption {
	return func(j *fireJitter) {
		if c != nil {
			j.clk = c
		}
	}
}

type fireJitter struct {
	window time.Duration
	pick   JitterFunc
	clk    clock.Clock
}

// FireJitter spreads the runs a schedule fires over a window instead of
// starting them all on the tick: a run fired for tick T waits until
// T + jitter, where the jitter is drawn from the run (RunIDJitter by
// default) in [0, window). A thousand tenants' "0 9 * * *" schedules then
// reach the database and the upstream API across the window instead of in one
// second, while each schedule keeps its expression.
//
// How it decides, step by step, with no state: a step of a run a cron
// tick fired (it carries the tick label, workflow.TickTime, which drivers
// with CapTickTime set, and no workflow.LabelTrigger) returns
// workflow.Snooze for the time left while the clock is before
// T + jitter, and runs otherwise.
// So it holds only the run's first steps (its later steps start after
// the deadline and pass straight through), a retry or another node
// reaches the same deadline, and a run that starts late (a misfire
// catch-up, a busy queue) is not delayed further. Plain enqueues and
// runs started by hand (TriggerSchedule, Backfill, FireAtStartup: they
// carry LabelTrigger) are never touched. A window of zero or less turns
// it off.
//
// It needs CapSnooze and CapTickTime (memory, sqlstore, redis); check a
// client with CheckFireJitter where the client is built. On a driver
// without CapTickTime nothing is delayed; on one without CapSnooze the
// snooze is an ordinary error that spends an attempt, so do not use it
// there.
func FireJitter(window time.Duration, opts ...FireJitterOption) workflow.Middleware {
	j := &fireJitter{window: window, pick: RunIDJitter, clk: clock.Real{}}
	for _, o := range opts {
		o(j)
	}
	return func(next workflow.Handler) workflow.Handler {
		if j.window <= 0 {
			return next
		}
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := workflow.StepInfoFrom(ctx)
			if !ok {
				return next(ctx, in)
			}
			tick, ok := workflow.TickTime(info.Labels)
			if _, triggered := info.Labels[workflow.LabelTrigger]; !ok || triggered {
				return next(ctx, in)
			}
			jitter := min(max(j.pick(info, j.window), 0), j.window)
			if wait := tick.Add(jitter).Sub(j.clk.Now()); wait > 0 {
				return nil, workflow.Snooze(wait)
			}
			return next(ctx, in)
		}
	}
}

// CheckFireJitter refuses a client that lacks FireJitterNeeds with a
// *workflow.UnsupportedError naming what is missing.
func CheckFireJitter(c workflow.Client) error {
	if missing := c.Capabilities().Missing(FireJitterNeeds); !missing.IsZero() {
		return &workflow.UnsupportedError{Field: "FireJitter", Missing: missing}
	}
	return nil
}
