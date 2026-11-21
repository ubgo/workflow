package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: a
// signal sent to one run by its ID resumes only that run, a child's
// escalation resumes its parent's wait, and the migration's handler
// hears both slow shards between its steps; ApproveRun, RejectRun,
// SignalChoiceRun and UpdateRun answer only the run they name.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `signal a run by its ID:
  second order: paid "card 4242"
  first order: waiting
escalation to the parent:
  parent took: "receipt over limit: 1200"
handlers between steps:
  {"heard":["shard 1 is slow","shard 3 is slow"],"shards":3}
approvals, races and updates by run ID:
  refund 1: "approved=true by ana"
  refund 2: "approved=false by bo"
  dispatch took: "next day"
  cart answered: "added socks"
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
