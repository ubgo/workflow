package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `resize finished
drained and stopped
enqueue after Drain: workflow: client not started: the client was stopped
backfill 2026-01-01
backfill 2026-01-02
backfill 2026-01-03
idle for a minute: stopped
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
