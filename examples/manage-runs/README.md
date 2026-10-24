# manage-runs

The operator's view of runs on the in-memory driver: list failed runs with their error and the step that failed, re-run one from that step (the step that already succeeded does not run again), forget a finished run, cancel every waiting run of a workflow at once, and pause a workflow.

```sh
cd examples
go run ./manage-runs
```

```text
failed: run-2 at step "ship": memory: step "ship" failed after 1 attempt(s): warehouse API: 503
retried run-2: succeeded; charge ran 1 time(s), ship 2
deleted: true then Status is ErrNotFound: true
cancelled waiting runs: 2
while paused: queued
after Resume: succeeded
```

`charge` ran once although the run ran twice: `Retry` resumes a failed run at the step that failed, keeping the outputs of the steps before it. The two delayed runs were `scheduled`, so the default `CancelAll` (runs not yet started) took both.

The guide that copies this program is [manage runs](../../docs/jobs/manage-runs.md). `go test ./manage-runs` checks every line.
