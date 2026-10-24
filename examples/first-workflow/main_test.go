package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks the lines the guide shows: a retry
// that succeeds, the joined parents' outputs, and a permanent failure that
// runs OnFailure once without retrying.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `charge A-1: attempt 1 failed
charge A-1: attempt 2 charged 30
notify: receipt for A-1, parcel for A-1
run order: succeeded (status succeeded, err <nil>)
on failure: {"id":"A-2","amount":0}
run order: failed (status failed, err memory: step "charge" failed after 1 attempt(s): order A-2: nothing to charge)
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
