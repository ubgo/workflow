package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: of each
// burst of three, the first two runs are cancelled and only the last one
// runs, in one process and across two clients sharing the SQLite file.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- Debounce a burst
job 1: cancelled
job 2: cancelled
ran with {"change":3}
-- A store shared by every process
ran with {"change":3}
job 1 (from process A): cancelled
job 2 (from process B): cancelled
job 3 (from process A): succeeded
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
