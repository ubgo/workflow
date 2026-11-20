package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: entry,
// exit and transition actions in their order, a payment the guard refuses
// rejected, deep history returning a held order to paid and in-transit,
// the done event of the parallel state completing the order, the
// payment timer cancelling a second order when the fake clock passes 30
// minutes, and both instances' logs.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `order o-1:
> start, total 120
  states: [cart]
> checkout
  exit cart
  enter fulfilment
  states: [awaiting packing]
> pay 50
  rejected: no transition takes it in the current state
> pay 120
  exit awaiting
  action: record payment of 120
  enter paid
  states: [paid packing]
> ship
  states: [paid in-transit]
  email: order o-1 is on its way
> hold
  exit fulfilment
  enter on-hold
  states: [on-hold]
> resume
  exit on-hold
  enter fulfilment
  enter paid
  states: [paid in-transit]
> deliver
  exit fulfilment
  enter completed
  states: [completed]
status of o-1: states [completed], done true, paid 120 of 120
  1 09:00 (start)         [] -> [cart]
  2 09:00 checkout        [cart] -> [awaiting packing]
  3 09:00 pay             [awaiting packing] -> [paid packing]
  4 09:00 ship            [paid packing] -> [paid in-transit]
  5 09:00 hold            [paid in-transit] -> [on-hold]
  6 09:00 resume          [on-hold] -> [paid in-transit]
  8 09:00 deliver         [paid in-transit] -> [completed]
order o-2:
> start, total 80
  states: [cart]
> checkout
  exit cart
  enter fulfilment
  states: [awaiting packing]
> 30 minutes pass without a payment
  exit awaiting
  exit fulfilment
  enter cancelled
status of o-2: states [cancelled], done true, paid 0 of 80
  1 09:00 (start)         [] -> [cart]
  2 09:00 checkout        [cart] -> [awaiting packing]
  3 09:30 payment-timeout [awaiting packing] -> [cancelled]
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
