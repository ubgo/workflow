# failover-manage

Managing runs through a failover client while they sit on two backends: two jobs went to the primary, two to the secondary while the primary refused enqueues. `Runs` merges both backends into one list, newest first, and prefixes every `Ref` with the backend that holds the run; `Retry` and `Delete` go to the backend the ref names; `RunQuery.Ref` with a prefixed ref asks only that backend and returns zero or one run; `CancelAll` cancels on both and returns the total (one delayed run on each here); `Pause` holds the workflow on both, and `Paused` lists what either holds. Both backends are the memory driver on one fake clock.

```sh
cd examples
go run ./failover-manage
```

```text
-- every run, from both backends, newest first
  o-4 on secondary succeeded
  o-3 on secondary failed memory: step "bill" failed after 1 attempt(s): billing API: 500
  o-2 on primary   failed memory: step "bill" failed after 1 attempt(s): billing API: 500
  o-1 on primary   succeeded
-- retry the failed runs, each on its own backend
  o-3 on secondary: retried, succeeded
  o-2 on primary: retried, succeeded
-- one run by its ref
  o-3 on secondary: succeeded
-- forget a finished run
  o-4 on secondary succeeded
  o-3 on secondary succeeded
  o-2 on primary   succeeded
-- cancel every waiting run on both backends
  cancelled: 2
-- pause on both backends
  paused: invoice
```

Each call needs its capability on both backends; without it the failover client refuses with an `*workflow.UnsupportedError` before touching either one.

## The code

- `list`: one merged list of both backends' runs.
- `retry`: the failed runs, each retried on its own backend.
- `one`: a single run by its prefixed ref.
- `delete`, `cancel-all`, `pause`: forgetting a run, cancelling waiting runs, holding a workflow.

The guide [Failover](../../docs/reliability/failover.md#query-retry-cancel-and-pause-runs) copies its code from the regions marked `snippet:`. `main_test.go` runs the same program and checks every line above.
