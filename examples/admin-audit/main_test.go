package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: the
// audited writes (a refused one included) and the run's trace link.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `actions:
  vic POST /admin/runs/<run>/retry: 403
  ann POST /admin/runs/<run>/retry: 200
  ann GET /admin/runs/<run>: 200
  ann DELETE /admin/runs/no-such-run: 404
audit log:
  09:30:00 vic runs.retry <run> allowed=false 403 admin: not allowed: runs.retry
  09:30:00 ann runs.retry <run> allowed=true 200 ok
  09:30:00 ann runs.delete no-such-run allowed=true 404 not found
run detail:
  status succeeded
  trace_url https://tracing.example.com/trace/4bf92f3577b34da6a3ce929d0e0e4736
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
