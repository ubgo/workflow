# graceful-drain-report

What a drain tells you when a step does not finish inside the grace period. `workflow.ShutdownHookReport(client, grace, onReport)` returns a plain `func(context.Context) error` that any phased shutdown manager can register; it drains within the grace and hands a `DrainReport` to `onReport`: steps and runs finished during the drain, and every attempt cut off with its run id, workflow, step, attempt number and disposition (`cancelled` in memory, `requeued` on a durable driver, `engine` on Hatchet).

```sh
cd examples
go run ./graceful-drain-report
```

```text
memory: finished steps=0 runs=0, clean=false, cut off 1 attempt(s) in 1 run(s)
  cut: thumbnail/resize attempt 1 -> cancelled: the run is cancelled
memory: drain error is a deadline: true
memory: cut run status: cancelled
sql: worker A runs attempt 1
sql: drain report true, requeue on a clean stop true
sql: finished steps=0 runs=0, clean=false, cut off 1 attempt(s) in 1 run(s)
  cut: thumbnail/resize attempt 1 -> requeued: another worker runs it as the same attempt
sql: drain error is the deadline: true same as DrainOutcome: true
sql: worker B runs attempt 1
sql: run finished: succeeded
```

The SQL worker's cut-off attempt was handed back without spending an attempt (`CapShutdownRequeue`), so worker B ran it as attempt 1 again. The memory client is drained through `ShutdownHookReport`; the SQL worker is drained by hand, the way the hook does it inside.

## The code

- `hook`: `workflow.ShutdownHookReport`, the one-line hook a shutdown manager registers.
- `report`: the `onReport` callback: `DrainReport` (`FinishedSteps`, `FinishedRuns`, `Clean`, `CutRuns`, `Cut`), `DrainCut` and the three `DrainDisposition` values.
- `caps`: `CapDrainReport` and `CapShutdownRequeue` asked from the client.
- `drain-with-report`: `DrainContext`, the `ReportingDrainer` type assertion, `DrainWithReport` and `DrainOutcome`.

`main_test.go` runs the program and checks every line. The guide [docs/operations/deploy.md](../../docs/operations/deploy.md) copies its code from the regions marked `snippet:`.
