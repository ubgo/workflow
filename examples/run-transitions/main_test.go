package main

import (
	"bytes"
	"context"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `retrying -> running: the backoff elapsed and a worker claimed the step (needs RunStates)
retrying -> queued: the backoff elapsed (needs RunStates)
retrying -> waiting: parallel steps changed which status describes the run (needs RunStates)
retrying -> cancelled: Cancel or CancelAll (needs Cancel)
retrying -> failed: the run went past Workflow.RunTimeout (needs RunTimeout)
retrying -> failed: ForceFail (needs ForceRun)
retrying -> succeeded: ForceComplete (needs ForceRun)
failed -> queued allowed: true
discarded -> queued allowed: false
scheduled -> succeeded allowed: true
succeeded -> failed allowed: false
the machine has 40 moves; memory reports 40, without CapRunStates 32
memory, running -> retrying: <nil>
no CapRunStates, running -> retrying: illegal status change: running to retrying needs capabilities this driver lacks
memory, cancelled -> queued: illegal status change: no move from cancelled to queued
`

// TestRun runs the program and checks every line it writes: the moves out
// of retrying, the four CanTransitionTo answers, the move counts per
// capability set and the guard's three verdicts.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
