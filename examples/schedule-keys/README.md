# schedule-keys

A schedule with an idempotency key: a manual `TriggerSchedule` and a job enqueued by hand under the same key join the run that is still running, a schedule with `OnHeld: HeldRefuse` refuses the trigger with `ErrDuplicate` instead, and `ExpandScheduleKey` computes the per-day key a daily schedule fires with. It runs on the in-memory driver.

```sh
cd examples
go run ./schedule-keys
```

```text
second trigger returned the running run: true duplicate: true
a hand enqueue under the same key joined it: true
a refusing trigger while the key is held: true
the key of a daily digest fired at 09:00: digest:2026-11-28
the run: succeeded
```

The guide is [Idempotency keys on schedules](../../docs/schedules/schedule-keys.md).
