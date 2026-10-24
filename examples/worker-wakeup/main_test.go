package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program (SQLite with the in-process bus, or Postgres
// with pgnotify when PG_DSN is set) and checks that the job started long
// before the worker's next poll, so it was woken up, not polled.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := "started before the next poll (5s): true\n"
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
