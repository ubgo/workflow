# flow-patterns

Control-flow building blocks made of ordinary steps, on the in-memory driver: trigger rules over a tolerated card decline with an exit handler, a saga that cancels a flight and a car when the hotel is full, a multi-choice over notification channels, a race decided by the customer's first reply, a polling sensor, a human approval with a signed link, a step group, an early successful end, and a recursion limit.

```sh
cd examples
go run ./flow-patterns
```

The guides that copy this program are [trigger rules](../../docs/workflows/trigger-rules.md), [sagas](../../docs/workflows/sagas.md), [multi-choice and races](../../docs/workflows/multi-choice-and-race.md), [polling and approvals](../../docs/workflows/polling-and-approval.md) and [groups and guards](../../docs/workflows/groups-and-guards.md). `go test ./flow-patterns` checks every line of the output.
