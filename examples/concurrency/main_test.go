package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, as the README shows it. The fake clock makes the rate window exact.
const want = `per key:
  acme-1 running, acme-2 queued, globex-1 running
  acme-1 finished, started: acme-2
drop newest:
  second sync: error <nil>, status cancelled
cancel in progress:
  started preview-2; preview-1 cancelled, preview-2 running
rate limit:
  started mail-1 and mail-2; mail-3 queued
  a minute later, started mail-3
priority:
  password-reset, normal, backfill
refusals:
  memory, KeyExpr: workflow: Workflow.Concurrency.Key.Expr unsupported by this driver: the memory driver evaluates only KeyFunc keys; workflow report
  hatchet, KeyFunc: workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow report: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression | unsupported: true
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
