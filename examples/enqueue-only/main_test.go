package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// enqueue-only client enqueues and reads status without Start, and the
// step cut short by worker A's clean stop runs on worker B as attempt 1
// again, so the run succeeds with one attempt counted and no error
// recorded although its policy allows no retry.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `web enqueued true status queued
worker A runs attempt 1
worker A stopped
worker B runs attempt 1
run succeeded, attempts counted 1, errors recorded 0
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
