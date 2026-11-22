# custom-driver

`Tiny`, in `driver.go`, is a complete `workflow.Client` in about 300 lines: steps run as a DAG on one goroutine per run, with retries, timeouts, `OnFailure`, `StepInfo` and `Status`, every timer read from an injected `clock.Clock`. A timed-out attempt keeps the core's step-timeout handshake: its context is cancelled with [`workflow.TimeoutError`](ds:block?id=timeouterror-nqztem89), and step middleware can claim the timeout with [`workflow.ClaimTimeout`](ds:block?id=claimtimeout-62xdkgef). It declares no capability, so it refuses delayed starts, idempotency keys, cron, cancel, waits and sleeps with an `UnsupportedError` instead of ignoring them. `main_test.go` holds it to the conformance suite every shipped driver runs.

```sh
cd examples
go run ./custom-driver
```

```text
render got "42 rows"
tiny-1: succeeded
delayed start refused: true
workflow: Job.RunAt unsupported by this driver (missing DelayedStart): job for workflow report
reset failed: true succeeded: false succeeded with Succeeded: true
fork running: false cancelled: true redo of succeeded: true
fork without a step: workflow: invalid spec: ForkRun needs a step
chain after the attempt: [email audit] changed: true
delayed chain job: workflow: Job.RunAt unsupported by this driver (missing DelayedStart): job for workflow later
last missed tick: 12:00:00
trigger labels: 5 tick at 2026-01-05T12:30:00Z trigger manual
key enterprise weight 4 stride 262144
key free weight 1 stride 1048576
CancelAll cancels: [scheduled queued]
pause a queue needs: PauseQueue a workflow needs: PauseWorkflow
slots in use 3 of 4
too many slots: workflow: invalid spec: slot pool: 5 slots asked of 4
heartbeat export row 4000
progress export 40% "4,000 of 10,000 rows" at 09:00:00
without a reporter: workflow: ReportProgress unsupported by this driver: the driver keeps no progress, or ctx is not a step's
```

The lines after the refusal come from `helpers.go`: the core helpers a driver calls for the capabilities `Tiny` does not declare (`ResetStatusOK`, `ForkStatusOK`, `ForkRedo`, `CheckFork`, `NewChainEdits`, `LastTick`, `TriggeredLabelsAt`, `FairWeight`, `FairStride`, `CancelStatuses`, `RequiresPause`, `NewSlotPool`, and `WithHeartbeater` and `WithProgressReporter` on a step's context), each printing what it decides.

```sh
go test ./custom-driver -run TestConformance -v
```

43 cases pass and 198 are skipped, each naming the capability it needs (`driver lacks IdempotencyKey`, `driver lacks Cancel`, and so on), across the 70 groups of [`workflowtest.GroupNames()`](ds:block?id=workflowtest-allgroups-uk4d9m6p). `TestStepTimeoutHandshake` runs the `attemptcause` and `toleratedtimeout` groups again with `StepTimeoutHandshake` set on the [`workflowtest.Harness`](ds:block?id=workflowtest-harness-7uqe9srm), so their step-timeout cases run on `Tiny` too:

```sh
go test ./custom-driver -run TestStepTimeoutHandshake -v | grep -E '^(--- |    --- |ok)'
```

```text
--- PASS: TestStepTimeoutHandshake (0.01s)
    --- PASS: TestStepTimeoutHandshake/attemptcause (0.00s)
    --- PASS: TestStepTimeoutHandshake/toleratedtimeout (0.01s)
ok  	github.com/ubgo/workflow/examples/custom-driver	0.017s
```

The walkthrough is [docs/extending/writing-a-driver.md](../../docs/extending/writing-a-driver.md).
