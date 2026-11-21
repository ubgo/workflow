package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guides show: trigger
// rules with an exit handler, a saga, a multi-choice, a race, a poll, an
// approval with a signed link, a group, an early end and a recursion limit,
// a saga across steps, a local merge, a race deadline, a sleep until a
// time, a typed enqueue and result, and a shape guard.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `rules:
  card "ok": "cart released"
    ran: charge, receipt, cleanup
  card "declined": failed: workflow: steps failed before the exit handler: step "charge": card declined
    ran: charge, tell-customer, cleanup
saga:
  failed: workflow: saga compensated: saga "trip": no rooms
    ran: book car, book flight, book hotel, cancel flight, cancel car
multi-choice:
  {"email":"sent by email","sms":"sent by sms"}
race:
  "offer closed: too expensive"
poll:
  "s3://lake/2026-11-20/" after 3 looks
approval:
  "refund paid, approved by ana@example.com"
  the same link two days later: workflow: invalid approval token: expired
group:
  {"clean":"120 clean rows","count":"3 rejected"}
end run:
  status succeeded, ran: lookup
recursion:
  depth 0, depth 1, depth 2, depth 3 stopped: limit reached
saga across steps:
  failed: trip compensated: hotel: no rooms
    ran: cancel flight-booking-1, cancel car-booking-1
local merge:
  audit after email-receipt-1, sms-receipt-1
race deadline:
  nobody answered: "listing expired"
sleep until:
  "renewed"
    renewed on time: true
typed:
  pen costs 10.00
shape guard:
  shape v1 ee4f66355fdd42d5, v2 2572197af55e2410
  run from the old build failed: was enqueued with steps ee4f66355fdd42d5, now 2572197af55e2410 (step "fetch")
`
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
}
