package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, as the README and the guide show it.
const want = `loaded "order": reserve (run) await-payment (wait) ship (run)
both orders waiting for payment
A-1: "shipped A-1, paid 40"
A-2: "shipped A-2, paid 25"
refused: workflow: invalid spec: dsl: steps[1].handler: no handler is registered as "ship-order" (registered: reserve-stock, ship-parcel)
round trip equal: true
`

// TestRun runs the program and checks every line it prints, so a change
// in the library's behaviour fails here before the guide goes stale.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if out != want {
		t.Errorf("output =\n%s\nwant\n%s", out, want)
	}
}
