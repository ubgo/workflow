package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program (SQLite with the in-process bus, or Postgres
// with pgnotify when PG_DSN is set) and checks that the job started long
// before the worker's next poll, so it was woken up, not polled.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := "started before the next poll (5s): true\n"
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
