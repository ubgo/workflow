package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, as the README shows it. The fake clock makes the times exact.
const want = `wait and sleep:
  both orders waiting for payment
  paid A-2 at Mon 09:00: A-2 waiting (sleeping), A-1 waiting
  ship A-2 (paid 40) at Tue 09:00
  A-2 succeeded, A-1 still waiting
  ship A-1 (paid 40) at Wed 09:00
give up after a timeout:
  on failure: escalate "expense 77" at Sat 09:00
  run failed, deadline exceeded true
steps that depend on other steps:
  quote/quote attempt 1: price 120, stock 7
step timeout:
  run failed: memory: step "write" failed after 1 attempt(s): workflow: step attempt timed out: memory: step "write" attempt 1 after 30s: context deadline exceeded
refusals:
  memory: workflow: WaitSpec.KeyFrom.Expr unsupported by this driver: the memory driver evaluates only KeyFunc keys; workflow order, step await-payment
  hatchet: workflow: WaitSpec.KeyFrom.Expr unsupported by this driver: workflow order step await-payment: Hatchet has no API that evaluates a CEL expression to a string for the driver; use workflow.KeyFunc
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
