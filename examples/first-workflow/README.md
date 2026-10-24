# first-workflow

A four-step order workflow on the in-memory driver. `charge` fails once and is retried by the workflow's retry policy; `receipt` and `ship` start in parallel once it succeeds; `notify` waits for both and receives their outputs as one JSON object keyed by step name. A second order with nothing to charge fails with a `workflow.Permanent` error, is not retried, and runs the workflow's `OnFailure` handler once. A run-finished observer reports each run's end.

```sh
cd examples
go run ./first-workflow
```

```text
charge A-1: attempt 1 failed
charge A-1: attempt 2 charged 30
notify: receipt for A-1, parcel for A-1
run order: succeeded (status succeeded, err <nil>)
on failure: {"id":"A-2","amount":0}
run order: failed (status failed, err memory: step "charge" failed after 1 attempt(s): order A-2: nothing to charge)
```

## What to look at

- `workflow.Define` turns typed functions into handlers; a step's input is the payload (first step), its one parent's output, or an object keyed by parent name (several parents), as `workflow.StepInput` builds it.
- `workflow.StepInfoFrom(ctx)` gives the handler its attempt number.
- `Workflow.Retry` is the default for every step; `workflow.Permanent` skips the remaining attempts.
- `OnRunFinished` is registered before `Start`; the memory driver delivers every terminal run to it.

The walkthrough is [docs/start/first-workflow.md](../../docs/start/first-workflow.md). `main_test.go` runs the program and checks every line above.
