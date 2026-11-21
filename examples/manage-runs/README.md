# manage-runs

The operator's view of runs on the in-memory driver: list failed runs with their error and the step that failed, re-run one from that step (the step that already succeeded does not run again), forget a finished run, cancel every waiting run of a workflow at once, pause a workflow, and reset a failed run from an earlier step. Each operation is reached the way an app reaches it, through a capability check (or `workflow.Supports` for an operation with no bit) and a type assertion on the `workflow.Client` it holds.

```sh
cd examples
go run ./manage-runs
```

```text
failed: run-2 at step "ship": memory: step "ship" failed after 1 attempt(s): warehouse API: 503
retried run-2: succeeded; charge ran 1 time(s), ship 2
retry a succeeded run: ErrWrongState true
deleted: true then Status is ErrNotFound: true
cancelled waiting runs: 2
while paused: queued
after Resume: succeeded
steps that run again: [charge ship]
reset run-16 from charge: succeeded, same ref true; charge ran 1 more time(s)
```

`charge` ran once although the run ran twice: `Retry` resumes a failed run at the step that failed, keeping the outputs of the steps before it. A second `Retry` of the run that succeeded is refused with `ErrWrongState`. The two delayed runs were `scheduled`, so the default `CancelAll` (runs not yet started) took both. The last run is reset from `charge`, not from `ship` where it failed, so `workflow.StepsFrom` lists both steps and `charge` runs once more.

The guide that copies this program is [manage runs](../../docs/jobs/manage-runs.md). `go test ./manage-runs` checks every line.
