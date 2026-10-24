package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: of each
// burst of three, the first two runs are cancelled and only the last one
// runs, in one process and across two clients sharing the SQLite file.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- Debounce a burst
job 1: cancelled
job 2: cancelled
ran with {"change":3}
-- A store shared by every process
ran with {"change":3}
job 1 (from process A): cancelled
job 2 (from process B): cancelled
job 3 (from process A): succeeded
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
