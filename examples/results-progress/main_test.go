package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks its whole output: progress events and the latest progress, a result awaited and decoded, the result of two final steps, a failed run, and runs of one tenant, one of them fired by a tenant's schedule.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `progress:
   33% exported 1000 of 3000 orders (step export, at 09:00)
   67% exported 2000 of 3000 orders (step export, at 09:00)
  100% exported 3000 of 3000 orders (step export, at 09:00)
  latest: 100% exported 3000 of 3000 orders
  outside a step: unsupported true
result:
  {"file":"orders.csv","rows":3000}
  decoded: orders.csv, 3000 rows
  checkout, final steps [email invoice]: {"email":"sent","invoice":"inv_7"}
  pay: ended failed: memory: step "charge" failed after 1 attempt(s): card declined
  Result of a failed run: ErrWrongState true
tenants:
  acme checkout succeeded (fired by acme-nightly)
  acme checkout succeeded
  acme export-orders succeeded
  tenant with a space: ErrInvalid true
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
