package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: each
// failed attempt is kept with its error and only the panic carries a
// stack, a failed run force-completed reads succeeded, a stuck run
// force-failed reads failed with the operator's reason and without its
// OnFailure handler, and a finished run refuses a force.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- error history
  attempt 1 of "send": smtp 451: try again later (stack: false)
  attempt 2 of "send": smtp 451: try again later (stack: false)
  attempt 3 of "send": workflow/middleware: step panicked: nil template (stack: true)
-- force complete a failed run
  send-email: succeeded
-- force fail a stuck run
  import-feed: failed, "workflow: run ended by an operator: feed provider is down until Monday", OnFailure ran 0 times
-- a finished run cannot be forced
  ErrWrongState: true
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
