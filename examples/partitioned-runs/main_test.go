package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: a
// status per day with the failed day's error, a retry that starts only
// the failed day, and a set enqueued twice while running that starts each
// tenant once.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `backfill:
  2026-11-01 succeeded
  2026-11-02 failed: memory: step "load" failed after 1 attempt(s): source down for 2026-11-02
  2026-11-03 succeeded
retry failed:
  retried: [2026-11-02]
  2026-11-01 succeeded
  2026-11-02 succeeded
  2026-11-03 succeeded
per tenant, enqueued twice while running:
  same runs: true
  acme succeeded
  globex succeeded
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
