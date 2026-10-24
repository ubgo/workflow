package batch

import (
	"time"

	"github.com/ubgo/workflow"
)

// effects are what the Manager must do after a state change. They are
// computed inside Store.Update and carried out after it commits, so the
// decision is atomic and the side effects (enqueues, cancels) run outside
// the store's lock.
type effects struct {
	catch      bool // enqueue the Catch workflow
	then       bool // enqueue the Then workflow
	finally    bool // enqueue the Finally workflow
	cancelRest bool // cancel every job that has not finished

	// recorded is set when recordFinish applied a status (not a repeat);
	// record turns it into a Progress callback for that job.
	recorded bool
	// progress lists the jobs whose end decided a Progress callback.
	progress []int
}

// recordFinish applies one job's terminal status to st. It is the whole
// completion decision:
//
//   - A job's first terminal status wins; a repeat (a hook that fires
//     twice) changes nothing, so no job is counted twice.
//   - The first StatusFailed job fires Catch, once, whether or not
//     AllowFailures is set. Without AllowFailures it also
//     cancels the batch, so the jobs still running are cancelled.
//   - A job cancelled on its own (StatusCancelled) is not a failure: it
//     fires no Catch, but it still stops Then.
//   - A discarded job (StatusDiscarded) counts as State.DiscardedAs says,
//     a failure by default; see counted.
//   - Once every job is terminal the batch finishes, once: Then fires only
//     when every job succeeded and the batch was not cancelled; Finally
//     always fires.
func recordFinish(st *State, idx int, status workflow.Status, now time.Time) effects {
	var eff effects
	if idx < 0 || idx >= len(st.Jobs) || !status.IsTerminal() || st.Jobs[idx].Status.IsTerminal() {
		return eff
	}
	st.Jobs[idx].Status = status
	eff.recorded = true
	if st.counted(status) == workflow.StatusFailed && !st.CatchFired {
		st.CatchFired = true
		eff.catch = true
		if !st.AllowFailures && !st.Cancelled {
			st.Cancelled, st.CancelledAt, st.FailCancelled = true, now, true
			eff.cancelRest = true
		}
	}
	finish(st, now, &eff)
	return eff
}

// markCancelled cancels the batch: Then will not fire, and the jobs still
// running must be cancelled. Cancelling a finished or already cancelled
// batch changes nothing.
func markCancelled(st *State, now time.Time) effects {
	if st.Finished || st.Cancelled {
		return effects{}
	}
	st.Cancelled, st.CancelledAt = true, now
	return effects{cancelRest: true}
}

// finish latches Finished when every job is terminal and sets the Then and
// Finally effects; it is a no-op once Finished is set, which makes both
// callbacks fire at most once.
func finish(st *State, now time.Time, eff *effects) {
	if st.Finished {
		return
	}
	allOK := true
	for _, j := range st.Jobs {
		if !j.Status.IsTerminal() {
			return
		}
		allOK = allOK && st.counted(j.Status) == workflow.StatusSucceeded
	}
	st.Finished, st.FinishedAt = true, now
	eff.then = allOK && !st.Cancelled
	eff.finally = true
}

// counted is the status a job's end counts as in every batch decision: a
// StatusDiscarded end counts as st.DiscardedAs (StatusFailed when empty),
// every other status as itself.
func (st *State) counted(s workflow.Status) workflow.Status {
	if s != workflow.StatusDiscarded {
		return s
	}
	if st.DiscardedAs == "" {
		return workflow.StatusFailed
	}
	return st.DiscardedAs
}
