package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: the
// any-of audit on each orders version, the all-of report only once both
// datasets changed, a retried step announcing its version again starting
// nothing, and the lineage of both versions and of the report's run.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `load-orders 2026-11-20T09:
  attempt 1 started: audit
  attempt 2 started: nothing
load-fx 2026-11-20T09:
  attempt 1 started: revenue
load-orders 2026-11-20T10:
  attempt 1 started: audit
  attempt 2 started: nothing
lineage:
  warehouse.orders 2026-11-20T10 produced by load-orders/write
  warehouse.orders 2026-11-20T09 produced by load-orders/write
  revenue-report run read warehouse.orders@2026-11-20T09, warehouse.fx_rates@2026-11-20T09
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
