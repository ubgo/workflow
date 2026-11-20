package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: the
// paid order confirmed in three runs, the declined one compensated in
// four (payment.failed, then order.cancelled, then the release), the
// inventory released once, and each order's trail as the tree of events
// its causation labels draw.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `order 1001 (card ok): 3 runs succeeded, 0 failed
order 1002 (card declined): 4 runs succeeded, 0 failed
inventory: reserved 2, released 1
trail order-1001 (3 runs):
  order.placed o1001 -> charge succeeded, reserve succeeded
    payment.captured payment.captured/o1001 -> confirm-order succeeded
trail order-1002 (4 runs):
  order.placed o1002 -> charge succeeded, reserve succeeded
    payment.failed payment.failed/o1002 -> cancel-order succeeded
      order.cancelled order.cancelled/payment.failed/o1002 -> release succeeded
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
