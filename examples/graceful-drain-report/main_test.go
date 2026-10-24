package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README shows.
const want = `memory: detailed=true finished steps=0 runs=0, cut off 1
  cut: thumbnail/resize attempt 1 -> cancelled
memory: drain error is a deadline: true
memory: cut run status: cancelled
sql: worker A runs attempt 1
sql: detailed=true finished steps=0 runs=0, cut off 1
  cut: thumbnail/resize attempt 1 -> requeued
sql: worker B runs attempt 1
sql: run finished: succeeded
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
