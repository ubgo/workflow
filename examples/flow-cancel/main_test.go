package main

import (
	"bytes"
	"context"
	"testing"
)

// want is the program's whole output, as the README and the guides show
// it. The run listing is newest first, so the children print 3, 2, 1.
const want = `cancel every child of a step:
  album succeeded, 3 renders running
  cancelled 3 children
  child 3 of step render-all: cancelled
  child 2 of step render-all: cancelled
  child 1 of step render-all: cancelled
  cancel again: 0 children (only unfinished ones count)
withdraw several steps:
  quote succeeded with "cached feed-eu + cached feed-us"
  withdraw again: ErrWrongState true, both steps named true
deliver only to runs waiting now:
  before any run waits: resumed 0
  the run started later is still waiting: the first nudge was not kept
  while it waits: resumed 1
  run succeeded
wait for all of, or any of:
  A-1 (order.paid + order.packed): shipped; child succeeded
  A-2 (order.paid): cancelled, 1 child withdrawn; child cancelled
`

// TestRun runs the program and checks every line it prints, so a change
// in the library's behaviour fails here before the guides go stale.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output differs from the README.\ngot:\n%s\nwant:\n%s", got, want)
	}
}
