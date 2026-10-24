package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun checks every line the guide shows: a delayed job, a retry
// backoff and a week-long sleep all finish on a clock nobody advanced, a
// run waiting for an event is counted as waiting until it is signalled,
// and a paused workflow makes RunAll stop with ErrRunAllStalled.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `succeeded=2 failed=0 waiting=1 sends=2 clock=Oct 14 09:00
after the signal: succeeded=1 waiting=0
paused: stalled=true succeeded=0
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
