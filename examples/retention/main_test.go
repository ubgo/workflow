package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks the lines the guide shows.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `now: deleted 3, held undelivered 0, idempotency 0, batch 0
a day later: deleted 2
a week later: deleted 1
archived: email failed "fail" err=sqlstore: step "send" failed after 1 attempt(s): mailbox does not exist, email succeeded "a" send="sent", email succeeded "b" send="sent", report succeeded "r1" build="r1", report succeeded "r2" build="r2", report succeeded "r3" build="r3"
no observer, Undelivered false: deleted 0, held undelivered 1
Undelivered true: deleted 1
archive failed: deleted 0, error is the hook's: true
the run is kept: succeeded
`
	if got := out; got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
