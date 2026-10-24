package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: a job
// without a key is refused, jobs fail over while the primary is down, the
// second failover in a row starts a cool-off that skips the primary until
// the fake clock moves past it, and only DefaultClassifier fails over on an
// unmarked error.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- Fail over to a second backend
no key: true true
order-1 -> primary
-- the primary goes down
  event: failed_over email
order-2 -> secondary
  event: failed_over email
  event: cool_off_started email
order-3 -> secondary
  event: primary_skipped email
order-4 -> secondary
order-2 on the secondary: succeeded
-- the primary is back and the cool-off ends
  event: cool_off_ended email
order-5 -> primary
capabilities include Cron: true
-- Decide what counts as an outage
default classifier: failed over to secondary
strict classifier: returned "unexpected reply"
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
