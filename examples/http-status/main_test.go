package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: the
// closed gate, 202 with a Location, the owner check, the live event
// stream with the streamed chunks, and the result.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `closed by default:
  no session: 401
  a workflow not listed: 404
start:
  202, Location /api/jobs/runs/<id>
status while running:
  200, status running, done false
another user:
  403
events:
  status running
  chunk id=1 "Three "
  chunk id=2 "orders "
  chunk id=3 "shipped."
  status succeeded (finished)
  end succeeded {"summary":"Three orders shipped."}
status when done:
  200, status succeeded, done true, result {"summary":"Three orders shipped."}
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
