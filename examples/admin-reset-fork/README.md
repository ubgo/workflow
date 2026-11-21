# admin-reset-fork

Acting on runs from Go through the admin module, on the memory driver with a fake clock. First it asks which client supports a reset: the memory client, an app's wrapper that embeds `workflow.Forward` over it, and the same wrapper over a client without the operation. Both wrappers have a `ResetRun` method, but `workflow.Supports` (through the wrapper's `workflow.OpProber`) and `Admin.Supports` answer for the client underneath. Then `Admin.ResetRun` re-runs a failed run in place from a step, refuses a succeeded one without `ResetOptions.Succeeded` and redoes it with the opt-in, `Admin.ForkRun` starts a new run with its own idempotency key and labels, `Admin.SignalWaiting` resumes only the run waiting now, `Admin.CancelStep` withdraws a parked step, `ForceComplete` and `ForceFail` end runs from outside, and `EditRun` and `Reschedule` change a delayed run before it starts.

```sh
cd examples
go run ./admin-reset-fork
```

```text
-- what a wrapper supports
memory client                has ResetRun true  Supports(OpResetRun) true  admin reset_run true
wrapper over memory          has ResetRun true  Supports(OpResetRun) true  admin reset_run true
wrapper over a plain client  has ResetRun true  Supports(OpResetRun) false admin reset_run false
wrapper over a plain client: nodes supported=false reason="the client does not implement the interface this operation needs"
-- reset and fork
reset run-2 from transform: same run true, now succeeded
reset a succeeded run without the opt-in: true
reset with Succeeded: succeeded, redo label "true"
fork run-20: new run true, succeeded, fork-of run-2 from step publish, ticket OPS-7
fork an unfinished run: true
-- signal the runs waiting now
resumed: 1
resumed again: 0 (zero is not an error)
an event without a name: true
-- withdraw a step, force an end
cancel step await of run-7: run failed at step await
forced apac: succeeded
forced latam: failed, error workflow: run ended by an operator: region retired
a reason over 1024 bytes: true
-- edit and move a delayed run
edited and moved delayed-26: scheduled, priority 5, due 11:00:00
reschedule to the zero time: true
```

The guide that copies this program is [Admin operations](../../docs/admin/operations.md). `go test ./admin-reset-fork` checks every line.
