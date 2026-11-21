package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks its output: the edited payload is
// the one sent, a finished run cannot be edited, Admin.EditRun changes the
// priority of a scheduled run, and the HTTP route lets support move a run
// (200) but not change its payload (403), lets a lead change it (200), and
// answers 409 for a run that already ran.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `sent to {"to":"new@example.com"}
editing it after it ran: true
admin edit: priority 20 status scheduled
POST /runs/{id}/edit: support moves it 200 support readdresses it 403 lead readdresses it 200 a finished run 409
after the HTTP edits: due in 24h0m0s more, priority 20 same ref true
`
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
}
