package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guides show: the
// bucket filling and leaking on a fake clock, Observe, the refusals, the
// period keys in Berlin and one enqueue per customer per month.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `bucket:
  call 1: throttled=false retry after 0s
  call 2: throttled=false retry after 0s
  call 3: throttled=false retry after 0s
  call 4: throttled=false retry after 0s
  call 5: throttled=true retry after 500ms
  after 500ms: <nil>
  another key has its own bucket: <nil>
  after Observe(1), a call of cost 3 waits 0s
  cost above capacity: ErrInvalid=true permanent=true
  TakeOrSnooze on a full bucket: snooze=true for 500ms
period:
  day     2026-11-01
  week    2026-W44
  month   2026-11
  quarter 2026-Q4
  year    2026
  time.Local refused: true
  key period/customer-42/2026-11
  second enqueue in the month returned the first run: true
  a job that already has a key is refused: true
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
