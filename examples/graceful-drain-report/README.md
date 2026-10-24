# graceful-drain-report

What a drain tells you when a step does not finish inside the grace period. `workflow.ShutdownHookReport(client, grace, onReport)` returns a plain `func(context.Context) error` that any phased shutdown manager can register; it drains within the grace and hands a `DrainReport` to `onReport`: steps and runs finished during the drain, and every attempt cut off with its run id, workflow, step, attempt number and disposition (`cancelled` in memory, `requeued` on a durable driver, `engine` on Hatchet).

```sh
cd examples
go run ./graceful-drain-report
```

```text
memory: detailed=true finished steps=0 runs=0, cut off 1
  cut: thumbnail/resize attempt 1 -> cancelled
memory: drain error is a deadline: true
memory: cut run status: cancelled
sql: worker A runs attempt 1
sql: detailed=true finished steps=0 runs=0, cut off 1
  cut: thumbnail/resize attempt 1 -> requeued
sql: worker B runs attempt 1
sql: run finished: succeeded
```

The SQL worker's cut-off attempt was handed back without spending an attempt, so worker B ran it as attempt 1 again. `go test ./graceful-drain-report` checks every line.
