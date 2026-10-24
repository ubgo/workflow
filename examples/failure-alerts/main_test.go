package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// first final failure alerts, two more within the dedup window are
// reported but not alerted, the next one after the window alerts with
// "and 2 more", and every failed attempt reaches the reporter with its
// labels and without the payload.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- first failure
  ALERT charge/capture: memory: step "capture" failed after 2 attempt(s): card declined (and 0 more)
  report charge step=capture final=false attempt=1 tenant=acme: card declined
  report charge step=capture final=false attempt=2 tenant=acme: card declined
  report charge step=capture final=true attempt=0 tenant=acme: memory: step "capture" failed after 2 attempt(s): card declined
-- two more within 10 minutes: reported, not alerted
  report charge step=capture final=false attempt=1 tenant=acme: card declined
  report charge step=capture final=false attempt=1 tenant=acme: card declined
  report charge step=capture final=false attempt=2 tenant=acme: card declined
  report charge step=capture final=false attempt=2 tenant=acme: card declined
  report charge step=capture final=true attempt=0 tenant=acme: memory: step "capture" failed after 2 attempt(s): card declined
  report charge step=capture final=true attempt=0 tenant=acme: memory: step "capture" failed after 2 attempt(s): card declined
-- 10 minutes later
  ALERT charge/capture: memory: step "capture" failed after 2 attempt(s): card declined (and 2 more)
  report charge step=capture final=false attempt=1 tenant=acme: card declined
  report charge step=capture final=false attempt=2 tenant=acme: card declined
  report charge step=capture final=true attempt=0 tenant=acme: memory: step "capture" failed after 2 attempt(s): card declined
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
