# start-gate

A client whose `Start` waits for a readiness check. A device boots with its clock at 1970 (a fake clock stands in for its real-time clock): `workflow.StartWhen` with `workflow.ClockAfter` holds `Start` back, `Enqueue` returns `workflow.ErrNotStarted` meanwhile, and once the network sync sets the clock the gate opens and a job runs with the right time. Two more gates show a `ReadyFunc` whose error ends the wait and a ctx deadline that gives up on a check that never passes.

```sh
cd examples
go run ./start-gate
```

```text
default poll: 1s
clock 1970-01-01 00:00:00, enqueue: workflow: client not started
not started yet: true
clock synced, started; the job ran at 2026-11-21 09:30:00
check failed: workflow: start when: database unreachable
gave up: context deadline exceeded
```

## The code

- `gate`: `workflow.StartWhen` with `workflow.ClockAfter`, `StartWhenOptions.Poll` and `StartWhenOptions.Clock`.
- `before`: `Enqueue` before the gate opens returns `workflow.ErrNotStarted`.
- `ready-func`: a `workflow.ReadyFunc` that combines the clock check with a database check; its error ends the wait.
- `deadline`: a ctx deadline for a check that never passes.

`main_test.go` runs the program and checks every line. The guide [docs/operations/start-gate.md](../../docs/operations/start-gate.md) copies its code from the regions marked `snippet:`.
