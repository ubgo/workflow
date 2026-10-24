package workflow

import (
	"context"
	"slices"
)

// InEnvironment returns a Workflow.ScheduleWhen that fires ticks only when
// current is one of allowed ("production"), for schedules that must not
// run in staging or on a laptop. The environment names are the app's: the
// library has no notion of one and reads nothing from the process, so the
// app passes its own value (a config field, an env var it already reads).
// With no allowed names it never fires. Skipped ticks are reported as
// TickSkipped with TickReasonWhen.
//
// The decision is made once, here: the returned function ignores the tick.
// Combine it with other conditions through AllWhen.
func InEnvironment(current string, allowed ...string) func(context.Context, Tick) bool {
	ok := slices.Contains(allowed, current)
	return func(context.Context, Tick) bool { return ok }
}

// AllWhen returns a Workflow.ScheduleWhen that fires a tick only when every
// condition does, asking them in order and stopping at the first false.
// Nil conditions are ignored; with none it always fires.
func AllWhen(conds ...func(context.Context, Tick) bool) func(context.Context, Tick) bool {
	return func(ctx context.Context, t Tick) bool {
		for _, c := range conds {
			if c != nil && !c(ctx, t) {
				return false
			}
		}
		return true
	}
}
