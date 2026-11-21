package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// per-workflow bound lets two exports wait and refuses the third with
// ErrQueueFull, an unbounded workflow passes, the tenant at its quota is
// refused with ErrQuotaExceeded, a job without a tenant is not counted,
// and a count that fails fails the enqueue.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `globex export 1: accepted
globex export 2: accepted
globex export 3: refused, queue full (workflow/middleware: queue is full: 2 of 2 for workflow "export")
globex report: accepted
acme report: refused, quota (workflow/middleware: tenant quota exceeded: 5 of 5 for workflow "report")
report without a tenant: accepted
globex report, table down: failed (workflow/middleware: Quota count: uploads table unreachable)
`
	if got := out.String(); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
}
