package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: the
// refund logs each line once although its body ran four times, every
// side effect ran once, the recorded card error took the store credit
// branch on every wake, the drifting code failed with the
// nondeterminism error, and both histories were forgotten.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `refund:
  order A-1 loaded: 40 EUR
  in the return window the run is waiting, holding no worker
  three days later it waits for the approver
  approved by dana
  card refund refused (card expired), paying store credit
  result: A-1: 40 EUR as store credit credit-7, approved by dana, closed 2026-03-06T09:15:00Z
  the body ran 4 times; load-order ran 1, refund-card 1, store-credit 1
nondeterminism:
  the run ended failed: memory: step "orchestrate" failed after 1 attempt(s): workflow: replay: call 0 is step store-credit, but the history has step refund-card
  position 0: recorded "step refund-card", the code called "step store-credit"; permanent true
  matches ErrNondeterminism: true
journal:
  runs holding history after both runs finished: 0
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
