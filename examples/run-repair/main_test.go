package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: each
// failed attempt is kept with its error and only the panic carries a
// stack, a failed run force-completed reads succeeded, a stuck run
// force-failed reads failed with the operator's reason and without its
// OnFailure handler, and a finished run refuses a force.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
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
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
