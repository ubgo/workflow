package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line: a payload search, a
// suspended run held between its steps and resumed, a detour that holds a
// run, resumes it and resets it from the step that failed, a schedule fire that
// replaces the running run, and a duplicate that replaces a scheduled
// job's arguments.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `search:
  1 run(s) for order o-1 with rush: {"order":{"id":"o-1"},"rush":true}
suspend:
  after fetch, while suspended: load not run, run finished: false
  resumed, load got "feed.csv"
detour:
  held, resumed, failed in transform, reset from transform: succeeded; fetch ran 1 time(s)
replace:
  first run cancelled, second running
unique replace:
  1 scheduled digest, sent with ["c1","c2","c3"]
`
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
}
