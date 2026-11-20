package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guides show: trigger
// rules with an exit handler, a saga, a multi-choice, a race, a poll, an
// approval with a signed link, a group, an early end and a recursion limit.
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
`
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
}
