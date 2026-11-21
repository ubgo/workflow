package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `default poll: 1s
clock 1970-01-01 00:00:00, enqueue: workflow: client not started
not started yet: true
clock synced, started; the job ran at 2026-11-21 09:30:00
check failed: workflow: start when: database unreachable
gave up: context deadline exceeded
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
