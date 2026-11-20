package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `old worker up; old web node enqueues one run
new web node enqueues one run; the old worker leaves it alone
  the v2 run on a fleet of v1 workers: queued
new worker up: runs the v2 run and drains the v1 run
run versions: [1 1 2]
journal (sorted; the two runs interleave):
  new: charge (run v1)
  new: charge (run v2)
  new: email with the fraud score (run v2)
  new: fraud check (run v2)
  new: no fraud check for a run the old code started (run v1)
  new: plain email for a run the old code started (run v1)
  old: charge (run v1)
  old: email (run v1)
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
