package workflow

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

// DefaultMilestonePoll is how long a Milestone step snoozes between looks
// at its run data while the run is not in the wanted state.
const DefaultMilestonePoll = 5 * time.Second

// MilestoneOptions tunes Milestone.
type MilestoneOptions struct {
	// Poll is the snooze between looks while the run is not in the
	// wanted state; zero means DefaultMilestonePoll. Ignored with Skip.
	Poll time.Duration

	// Skip, when true, makes the step return its input unchanged at once
	// when the run is not in the wanted state, instead of waiting for it.
	Skip bool
}

// Milestone enables a step only while its run is in a given state (WC-18):
// the state is run data (SetRunData) under name, and the step runs h only
// while that value equals want. Otherwise it waits: it returns
// Snooze(Poll), so the step is put back without spending an attempt and
// looks again later, which needs a driver with CapSnooze (on a driver
// without it every look spends an attempt, so set a generous retry
// policy or use Skip). With MilestoneOptions.Skip it passes its input
// through unchanged instead of waiting, as When does.
//
// Another step of the same run moves the state with SetRunData, so a
// milestone gates work on what the run has reached ("ship only while the
// order is still 'paid', not after it was 'refunded'"). It reads run data,
// so it needs the Journal plugin. want is compared with the stored value
// after a JSON round trip into a value of want's type, with
// reflect.DeepEqual. A stored value that does not decode into want's type
// is a Permanent error.
func Milestone[T any](name string, want T, h Handler, opts MilestoneOptions) Handler {
	poll := opts.Poll
	if poll <= 0 {
		poll = DefaultMilestonePoll
	}
	return func(ctx context.Context, in []byte) ([]byte, error) {
		if name == "" || h == nil {
			return nil, Permanent(invalidf("Milestone needs a run data name and a handler"))
		}
		got, ok, err := RunData[T](ctx, name)
		if err != nil {
			return nil, fmt.Errorf("workflow: milestone %q: %w", name, err)
		}
		if ok && reflect.DeepEqual(got, want) {
			return h(ctx, in)
		}
		if opts.Skip {
			return in, nil
		}
		return nil, Snooze(poll)
	}
}
