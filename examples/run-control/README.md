# run-control

Operator tools on the in-memory driver: finding runs by a field of their JSON payload (`RunQuery.Payload`), suspending one run between its steps and resuming it (`workflow.Suspension`), a detour that holds an import, lets it go on and resets it from its failed transform (`SuspendRun`, `ResumeRun`, `RunResetter`), a schedule whose new tick replaces the running run (`workflow.GuardOverlap`), and a duplicate enqueue that replaces a scheduled job's arguments (`UniqueConfig.Replace`).

```sh
cd examples
go run ./run-control
```

```text
search:
  1 run(s) for order o-1 with rush: {"order":{"id":"o-1"},"rush":true}
suspend:
  after fetch, while suspended: load not run, run finished: false
  resumed, load got "feed.csv"
detour:
  held, resumed, failed in transform, reset from transform: succeeded; fetch ran 1 time(s)
replace:
  first run cancelled, second running
unique replace:
  1 scheduled digest, sent with ["c1","c2","c3"]
```

The guides that copy this program are [manage runs](../../docs/jobs/manage-runs.md) (search inside payloads, suspend one run, detours), [overlap](../../docs/schedules/overlap.md) (replace or buffer one) and [unique jobs](../../docs/reliability/unique-jobs.md) (replace a scheduled duplicate). `go test ./run-control` checks every line.
