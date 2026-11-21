package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// bulk job is routed to its own workflow, the default rule labels the
// plain job, a label chosen at the enqueue site wins over the table, and
// the caller's labels map is left as it was.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `job invoice map[tier:bulk] -> run of invoice-bulk, cost-center=batch
job invoice map[] -> run of invoice, cost-center=web
job invoice map[cost-center:sales] -> run of invoice, cost-center=sales
the caller's labels are not changed: map[tier:bulk]
`
	if got := out.String(); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
}
