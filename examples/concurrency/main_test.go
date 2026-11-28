package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
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
several keys:
  started acme-1 and widgets-1; acme-2 queued, widgets-2 queued
  acme-1 finished, started: acme-2
  widgets-1 finished, started: widgets-2
  hatchet: true | missing: ConcurrencyLimits
shared scope:
  started sync-acme; check-acme queued; started check-widgets
  sync-acme finished, started: check-acme
  invalid: true
  workflow: invalid spec: workflow "check": ConcurrencyLimits[0] names Scope "account" with MaxRuns 2, but workflow "sync" ConcurrencyLimits[0] names it with MaxRuns 1: workflows sharing a scope must declare the same MaxRuns
release while waiting:
  whole_run: acme-1 retrying in an hour; acme-2 queued
    an hour later: acme-1 succeeded, then acme-2 succeeded
  while_running: acme-1 retrying in an hour; acme-2 succeeded at once
    an hour later: acme-1 succeeded
refusals:
  memory, KeyExpr: workflow: Workflow.Concurrency.Key.Expr unsupported by this driver: the memory driver evaluates only KeyFunc keys; workflow report
  hatchet, KeyFunc: workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow report: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression | unsupported: true
`

// TestRun runs the program and checks every line it prints, so a change
// in the library's behaviour fails here before the guide goes stale.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if got := out; got != want {
		t.Errorf("output differs from the README.\ngot:\n%s\nwant:\n%s", got, want)
	}
}
