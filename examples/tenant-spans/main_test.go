package main

import (
	"bytes"
	"context"
	"testing"
)

// want is the program's whole output, as the README and the guide show it.
const want = `default:
  workflow.enqueue send-invoice (producer) workflow.tenant: (not recorded)
  workflow.step send-invoice/send (consumer) workflow.tenant: (not recorded)
WithTenant:
  workflow.enqueue send-invoice (producer) workflow.tenant: acme
  workflow.step send-invoice/send (consumer) workflow.tenant: acme
`

// TestRun runs the program and checks every line: without otel.WithTenant
// no span carries the tenant, with it both the enqueue and the step span
// do.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
