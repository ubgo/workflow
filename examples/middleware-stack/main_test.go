package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, the one the guides show: the
// clock is fake and the run IDs are left out, so it never varies.
const want = `a before
b before
handler
b after
a after
-- step middleware
level=WARN msg="workflow step failed" workflow=charge step=run attempt=1 duration=0s permanent=false error="payments api down"
run charge: succeeded, 2 handler call(s)
level=ERROR msg="workflow step panicked" panic="nil map"
level=ERROR msg="workflow step failed" workflow=panics step=run attempt=1 duration=0s permanent=false error="workflow/middleware: step panicked: nil map"
run panics: failed (memory: step "run" failed after 1 attempt(s): workflow/middleware: step panicked: nil map), 1 handler call(s)
level=ERROR msg="workflow step failed" workflow=validate step=run attempt=1 duration=0s permanent=true error="field amount: bad input"
run validate: failed (memory: step "run" failed after 1 attempt(s): field amount: bad input), 1 handler call(s)
level=ERROR msg="workflow step failed" workflow=slow step=run attempt=1 duration=0s permanent=false error="context deadline exceeded"
run slow: failed (memory: step "run" failed after 1 attempt(s): context deadline exceeded), 1 handler call(s)
run charge: succeeded, 0 handler call(s)
-- throttle
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="payments api down"
run call-api: failed (memory: step "run" failed after 1 attempt(s): payments api down), 1 handler call(s)
circuit "call-api" open for 5m0s
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="payments api down"
run call-api: failed (memory: step "run" failed after 1 attempt(s): payments api down), 1 handler call(s)
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="workflow/middleware: step throttled after repeated errors (key \"call-api\")"
run call-api: failed (memory: step "run" failed after 1 attempt(s): workflow/middleware: step throttled after repeated errors (key "call-api")), 0 handler call(s)
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="payments api down"
run call-api: failed (memory: step "run" failed after 1 attempt(s): payments api down), 1 handler call(s)
-- enqueue middleware
enqueue charge tier="bulk" queue="bulk" err=<nil>
run charge: succeeded, 0 handler call(s)
enqueue charge tier="" queue="" err=workflow: duplicate idempotency key: unique key "o-7" is held
same order again: true
unique twice: true workflow: invalid spec: Unique: label "ubgo.workflow/unique-key" uses the reserved prefix "ubgo.workflow/"
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
