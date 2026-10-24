package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, as the README shows it. The fake clock makes it exact.
const want = `enqueued: scheduled id set true, run id set false, status scheduled
cancelled before it started: cancelled
run-finished: cancelled, cancelled true
reminder sent to ada@example.com at Tue 09:00
after it ran: succeeded, terminal true
past RunAt: run id set true
reminder sent to bob@example.com at Tue 09:00
cancelled while running: cancelled
run-finished: cancelled, cancelled true
unknown run: true
`

// TestRun runs the program and checks every line it prints, so a change
// in the library's behaviour fails here before the guide goes stale.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output differs from the README.\ngot:\n%s\nwant:\n%s", got, want)
	}
}
