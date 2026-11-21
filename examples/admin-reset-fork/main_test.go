package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun checks every line the guide shows: what each client supports,
// the resets and the fork, the signal, the step cancel, the forced ends
// and the edited delayed run, with the refusals in between.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// want is the program's whole output, as docs/admin/operations.md shows it.
const want = `-- what a wrapper supports
memory client                has ResetRun true  Supports(OpResetRun) true  admin reset_run true
wrapper over memory          has ResetRun true  Supports(OpResetRun) true  admin reset_run true
wrapper over a plain client  has ResetRun true  Supports(OpResetRun) false admin reset_run false
wrapper over a plain client: nodes supported=false reason="the client does not implement the interface this operation needs"
-- reset and fork
reset run-2 from transform: same run true, now succeeded
reset a succeeded run without the opt-in: true
reset with Succeeded: succeeded, redo label "true"
fork run-20: new run true, succeeded, fork-of run-2 from step publish, ticket OPS-7
fork an unfinished run: true
-- signal the runs waiting now
resumed: 1
resumed again: 0 (zero is not an error)
an event without a name: true
-- withdraw a step, force an end
cancel step await of run-7: run failed at step await
forced apac: succeeded
forced latam: failed, error workflow: run ended by an operator: region retired
a reason over 1024 bytes: true
-- edit and move a delayed run
edited and moved delayed-26: scheduled, priority 5, due 11:00:00
reschedule to the zero time: true
`
