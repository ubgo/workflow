# custom-driver

`Tiny`, in `driver.go`, is a complete `workflow.Client` in under 300 lines: steps run as a DAG on one goroutine per run, with retries, timeouts, `OnFailure`, `StepInfo` and `Status`, every timer read from an injected `clock.Clock`. It declares no capability, so it refuses delayed starts, idempotency keys, cron, cancel, waits and sleeps with an `UnsupportedError` instead of ignoring them. `main_test.go` holds it to the conformance suite every shipped driver runs.

```sh
cd examples
go run ./custom-driver
```

```text
render got "42 rows"
tiny-1: succeeded
delayed start refused: true
workflow: Job.RunAt unsupported by this driver (missing DelayedStart): job for workflow report
```

```sh
go test ./custom-driver -run TestConformance -v
```

20 subtests pass and 44 are skipped, each naming the capability it needs (`driver lacks IdempotencyKey`, `driver lacks Cancel`, and so on). The walkthrough is [docs/extending/writing-a-driver.md](../../docs/extending/writing-a-driver.md).
