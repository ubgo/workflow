package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: a run
// waiting on a promise until an API call resolves it, a second outcome
// refused, a run started and signalled in one call, an update answered by
// the run, and a loop continued as three runs.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `promise:
  before approval the run has ended: false
  after approval: "approved by finance"
  a second, different outcome is refused: true
signal with start and update:
  started and signalled: "socks added, total 3 items"
  update answered: "hat added, total 3 items"
continue as new:
  rounds run as separate runs: 3
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
