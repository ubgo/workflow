package main

import (
	"bytes"
	"context"
	"testing"
)

// want is every line the guide shows. The run IDs are fixed because the
// memory driver numbers runs from one counter per client that its step
// attempts share, and the program runs one thing at a time.
const want = `sync after 7s: retrying
sync after 7.5s: succeeded, attempts 2
level=ERROR msg="memory: OnFailure handler failed" workflow=import run=run-6 error="pager is down"
middleware calls: 4
job on queue video: ErrInvalid = true
welcome on queue emails: succeeded
workflow on unserved queue at Start: ErrInvalid = true
default retention: 24h0m0s, at most 10000 runs
run-2: forgotten
run-5: succeeded
run-8: succeeded
stopped by itself after 30s idle
enqueue after the stop: ErrNotStarted = true
schedule heap false: 3 schedules, 3 pending timer(s), 3 fired at 09:05
schedule heap true: 3 schedules, 1 pending timer(s), 3 fired at 09:05
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
