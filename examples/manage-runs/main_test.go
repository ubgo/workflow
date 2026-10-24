package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `failed: run-2 at step "ship": memory: step "ship" failed after 1 attempt(s): warehouse API: 503
retried run-2: succeeded; charge ran 1 time(s), ship 2
deleted: true then Status is ErrNotFound: true
cancelled waiting runs: 2
while paused: queued
after Resume: succeeded
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
