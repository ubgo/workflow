package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: a
// folder value written by one workflow and read by another, per-workflow
// and global counters, a run whose folder has no plan, and two SQLite
// clients updating one total without losing a write.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `memory:
  order: pro plan, 40 spent so far
  order: pro plan, 100 spent so far
  globex order failed: true
  runs: signup 1, order 2, all 3
sqlite, two clients:
  acme spent 100 over 10 orders; runs counted 11
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
