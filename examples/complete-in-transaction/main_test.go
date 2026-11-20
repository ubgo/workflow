package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: a step
// completed in the app's committed transaction hands its output to the next
// step and is not retried for the error it returned after the commit; a
// rolled-back transaction leaves no payment and the step's own error
// applies; CompleteTx outside a step is refused.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- commit
email got {"order":"o1","paid":4200}
run: succeeded
charge attempts: 1
-- roll back
run: failed
payments stored: 1
-- outside a step
refused with ErrInvalid: true
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
