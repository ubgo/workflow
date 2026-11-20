package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks its output: the edited payload is
// the one sent, and a finished run cannot be edited.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := "sent to {\"to\":\"new@example.com\"}\nediting it after it ran: true\n"
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
}
