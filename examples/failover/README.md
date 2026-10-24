# failover

Join two clients with `failover.New`: a job without an idempotency key is refused with `ErrKeyRequired`; while the primary refuses enqueues with an error marked `workflow.Unavailable`, jobs go to the secondary and their references say so; two failovers in a row start a one-minute cool-off (ended at once with a fake clock), reported by `WithObserver`; and an unmarked error fails over under `DefaultClassifier` but not under `StrictClassifier`. Both backends are the memory driver.

```sh
cd examples
go run ./failover
```

```text
-- Fail over to a second backend
no key: true true
order-1 -> primary
-- the primary goes down
  event: failed_over email
order-2 -> secondary
  event: failed_over email
  event: cool_off_started email
order-3 -> secondary
  event: primary_skipped email
order-4 -> secondary
order-2 on the secondary: succeeded
-- the primary is back and the cool-off ends
  event: cool_off_ended email
order-5 -> primary
capabilities include Cron: true
-- Decide what counts as an outage
default classifier: failed over to secondary
strict classifier: returned "unexpected reply"
```

In production the primary is typically a Hatchet client and the secondary a `sqlstore` client; the program's code does not change.

The guide [Failover](../../docs/reliability/failover.md) explains each step. `main_test.go` runs the same program and checks every line above.
