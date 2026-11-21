// Command run-transitions reads the run state machine as data:
// workflow.Transitions, Status.TransitionsFrom and Status.CanTransitionTo.
// It lists the moves out of one status, asks which moves exist at all,
// keeps only the moves a given driver can report (its capabilities cover
// the move's Needs), and guards a status change the way a driver of your
// own would before it stores one.
//
// It is the program behind docs/jobs/run-states.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "run-transitions:", err)
		os.Exit(1)
	}
}

// snippet:guard
// errIllegalMove is what checkMove wraps, so a caller tests for it with
// errors.Is instead of matching the message.
var errIllegalMove = errors.New("illegal status change")

// checkMove is the guard a driver of your own runs before it stores a
// status change: the move must be in the machine, and the driver must
// declare every capability one of the matching transitions needs.
func checkMove(caps workflow.Caps, from, to workflow.Status) error {
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("%w: no move from %s to %s", errIllegalMove, from, to)
	}
	for _, t := range from.TransitionsFrom() {
		if t.To == to && caps.Has(t.Needs) {
			return nil
		}
	}
	return fmt.Errorf("%w: %s to %s needs capabilities this driver lacks", errIllegalMove, from, to)
}

// snippet:end

// run is main without the process exit, so the test drives the same code
// and checks every line it writes to w.
func run(ctx context.Context, w io.Writer) error {
	_ = ctx // the state machine is data: nothing here talks to a driver

	// snippet:from
	for _, t := range workflow.StatusRetrying.TransitionsFrom() {
		fmt.Fprintf(w, "%s -> %s: %s (needs %s)\n", t.From, t.To, t.Trigger, t.Needs)
	}
	// snippet:end

	// snippet:can
	for _, move := range [][2]workflow.Status{ // each pair is a from -> to move
		{workflow.StatusFailed, workflow.StatusQueued},       // Retry re-opens a failed run
		{workflow.StatusDiscarded, workflow.StatusQueued},    // a discarded run is never re-opened
		{workflow.StatusScheduled, workflow.StatusSucceeded}, // ForceComplete
		{workflow.StatusSucceeded, workflow.StatusFailed},    // terminal: no way out
	} {
		fmt.Fprintf(w, "%s -> %s allowed: %v\n", move[0], move[1], move[0].CanTransitionTo(move[1]))
	}
	// snippet:end

	// snippet:caps
	// reportable counts the moves in the machine whose Needs caps covers.
	reportable := func(caps workflow.Caps) (n int) {
		for _, t := range workflow.Transitions() {
			if caps.Has(t.Needs) {
				n++
			}
		}
		return n
	}
	caps := memory.New().Capabilities()
	noRunStates := caps.Without(workflow.CapRunStates) // as on a driver without retrying and discarded
	fmt.Fprintf(w, "the machine has %d moves; memory reports %d, without CapRunStates %d\n",
		len(workflow.Transitions()), reportable(caps), reportable(noRunStates))
	// snippet:end

	// snippet:check
	fmt.Fprintln(w, "memory, running -> retrying:", checkMove(caps, workflow.StatusRunning, workflow.StatusRetrying))
	fmt.Fprintln(w, "no CapRunStates, running -> retrying:", checkMove(noRunStates, workflow.StatusRunning, workflow.StatusRetrying))
	fmt.Fprintln(w, "memory, cancelled -> queued:", checkMove(caps, workflow.StatusCancelled, workflow.StatusQueued))
	// snippet:end
	return nil
}
