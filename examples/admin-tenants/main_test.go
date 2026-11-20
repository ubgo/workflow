package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: a
// customer lists only its tenant's runs, gets 404 for another tenant's
// run and 403 for another tenant's filter and for a system-wide route,
// its bulk cancel leaves the other tenant's run alone, and an operator
// given AllTenants sees every tenant and the queues.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- A customer sees its own tenant
acme GET /runs: 200, tenants acme,acme
acme GET globex's run: 404
acme GET /runs?tenant=globex: 403
acme GET /queues: 403
acme GET /capabilities: tenant acme
-- A bulk action stays in the tenant
acme POST /runs/cancel: 200 {"cancelled":2}
globex's run: waiting
-- An operator sees every tenant
ops GET /runs: 200, tenants globex,acme,acme
ops GET /queues: 200
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
