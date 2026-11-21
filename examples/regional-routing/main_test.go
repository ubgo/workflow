package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: each
// job lands in the region its label names, its status is read from that
// region, an unknown region is refused, a signal reaches a run in the
// region that is not the default, a schedule is placed by the same
// router, a run listing covers every region, and a router by tenant
// refuses a tenant it has no region for.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- Route each job to its region
us customer's invoice -> us
status read from us: succeeded
invoice without a label -> eu
unknown region refused: true true
-- Signal a run in another region
approval in us: succeeded
-- Place a schedule with the same router
schedule -> us
-- List runs across regions
invoices: eu 1, us 1
-- Route by tenant
acme -> eu
globex -> us
initech: refused (true)
-- Keep an event in its region
order 42: us succeeded, eu waiting
unknown order refused: true
-- Name the region that fires declared Cron
Cron without a schedule region refused: true
with WithScheduleRegion("eu"): <nil>
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
