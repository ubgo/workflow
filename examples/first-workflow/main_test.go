package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks the lines the guide shows: a retry
// that succeeds, the joined parents' outputs, and a permanent failure that
// runs OnFailure once without retrying.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `charge A-1: attempt 1 failed
charge A-1: attempt 2 charged 30
notify: receipt for A-1, parcel for A-1
run order: succeeded (status succeeded, err <nil>)
on failure: {"id":"A-2","amount":0}
run order: failed (status failed, err memory: step "charge" failed after 1 attempt(s): order A-2: nothing to charge)
`
	if got := out; got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
