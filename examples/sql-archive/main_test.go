package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// prune moved all three runs, the hot tables no longer know the first, and
// the archive pages them back newest first with their step outputs.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `pruned 3 runs from the hot tables
status of run_000001: not found (it left the queue)
page 1: run_000003 succeeded "INV-3" outputs render="pdf:INV-3" send="sent"
page 1: run_000002 succeeded "INV-2" outputs render="pdf:INV-2" send="sent"
page 2: run_000001 succeeded "INV-1" outputs render="pdf:INV-1" send="sent"
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
