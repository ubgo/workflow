# flow-patterns

Control-flow building blocks made of ordinary steps, on the in-memory driver: trigger rules over a tolerated card decline with an exit handler, a saga that cancels a flight and a car when the hotel is full, a multi-choice over notification channels, a race decided by the customer's first reply, a polling sensor, a human approval with a signed link, a step group, an early successful end, and a recursion limit. Then a saga across three parallel booking steps whose undo step cancels what went through, a local merge that reads only the branches a multi-choice took, a race whose default branch a delayed deadline job answers, a durable sleep until a time, a typed enqueue and result, and a shape guard that catches a renamed step during a rolling deploy.

```sh
cd examples
go run ./flow-patterns
```

```text
rules:
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
```

The guides that copy this program are [trigger rules](../../docs/workflows/trigger-rules.md), [sagas](../../docs/workflows/sagas.md), [multi-choice and races](../../docs/workflows/multi-choice-and-race.md), [polling and approvals](../../docs/workflows/polling-and-approval.md) and [groups and guards](../../docs/workflows/groups-and-guards.md). `go test ./flow-patterns` checks every line of the output.
