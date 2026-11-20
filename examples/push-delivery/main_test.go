package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: a
// delivery, a retried one, one refused for good, and a forged request.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `deliveries:
  ann@example.com: succeeded after 1 attempt(s), result {"sent_to":"ann@example.com"}
  flaky@example.com: succeeded after 2 attempt(s), result {"sent_to":"flaky@example.com"}
  bounced@example.com: failed after 1 attempt(s): memory: step "deliver" failed after 1 attempt(s): push: endpoint answered 422: the job failed for good
forged request:
  unsigned: 401
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
