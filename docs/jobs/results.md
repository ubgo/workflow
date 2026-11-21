# Results and awaiting a run

Most jobs are fire-and-forget, but some callers need the answer: a CLI that kicks off an export and prints the file name, a request handler that runs a quote calculation in the background and waits a few seconds for it, an admin page that shows what a run produced. A run that succeeds keeps its result, and `workflow.AwaitResult` waits for a run and returns it.

## Before you start

- Keeping results needs `workflow.CapResult`: the client implements `workflow.ResultReader` and fills `RunInfo.Output`. The memory, Redis, SQL and Hatchet drivers have it.
- `workflow.Await` (wait for the end, no result) polls `Client.Status`, so it works on every driver.
- The example is [`examples/results-progress`](../../examples/results-progress). It runs on the memory driver, so its output never varies. Run it with `go run ./results-progress` from `examples/`.

## What a run's result is

A run's result is the output of its final steps, the steps no other step lists in `After` (`workflow.Sinks(wf)` names them, in declaration order). `workflow.RunOutput(wf, outputs)` is the rule every driver with `CapResult` applies:

- **One final step:** its output, unchanged. A one-step job's result is simply what its handler returned.
- **Several final steps:** a JSON object keyed by step name, built the way `workflow.StepInput` builds a joining step's input.

The checkout workflow below ends in two steps, so its result is an object with both outputs:

<!-- snippet: examples/results-progress/main.go#sinks -->
```go
checkout := workflow.Workflow{
	Name: "checkout",
	Steps: []workflow.Step{
		{Name: "charge", Run: ok(`"ch_1"`)},
		{Name: "email", After: []string{"charge"}, Run: ok(`"sent"`)},    // a final step
		{Name: "invoice", After: []string{"charge"}, Run: ok(`"inv_7"`)}, // a final step
	},
}
```

```text
  checkout, final steps [email invoice]: {"email":"sent","invoice":"inv_7"}
```

The result is kept with the run until the run is deleted or removed by retention. `ResultReader.Result(ctx, ref)` returns it for a run in `StatusSucceeded`; any other status returns `workflow.ErrWrongState`, and an unknown run `workflow.ErrNotFound`, so a caller can tell "not done yet" from "done with an empty result". `ResultReader` is an optional interface, so reach it with a type assertion; `RunInfo.Output` carries the same bytes in a `Runs` listing, and `RunOutput` over the step outputs gives them again:

<!-- snippet: examples/results-progress/main.go#read-result -->
```go
reader, isReader := workflow.Client(c).(workflow.ResultReader) // CapResult
if !isReader {
	return errors.New("this driver keeps no results")
}
raw2, err := reader.Result(ctx, ref2) // ErrWrongState unless the run succeeded
if err != nil {
	return err
}
fmt.Printf("  checkout, final steps %v: %s\n", workflow.Sinks(checkout), raw2)

listed, err := c.Runs(ctx, workflow.RunQuery{Ref: ref2})
if err != nil {
	return err
}
fmt.Printf("  RunInfo.Output is the same: %t\n", bytes.Equal(listed.Runs[0].Output, raw2))

// RunOutput is the rule the driver applied to the step outputs.
rule := workflow.RunOutput(checkout, map[string][]byte{
	"charge": []byte(`"ch_1"`), "email": []byte(`"sent"`), "invoice": []byte(`"inv_7"`),
})
fmt.Printf("  RunOutput gives the same: %t\n", bytes.Equal(rule, raw2))
```

```text
  checkout, final steps [email invoice]: {"email":"sent","invoice":"inv_7"}
  RunInfo.Output is the same: true
  RunOutput gives the same: true
```

`workflow.Client(c)` in the assertion turns the concrete `*memory.Client` into the interface first, as app code that holds a `workflow.Client` already has it. A wrapper (`workflow.Use`, middleware, failover) implements `ResultReader` whatever it wraps and refuses with an `UnsupportedError` when the client inside lacks `CapResult`, so on a wrapped client check `Capabilities().Has(workflow.CapResult)` as well.

## Wait for a result

`workflow.AwaitResult(ctx, client, ref, opts)` blocks until the run ends and returns its result. Give `ctx` a deadline: it is how long the caller is willing to wait. Decode the bytes with `workflow.DecodePayload` (see [typed payloads](payloads.md)):

<!-- snippet: examples/results-progress/main.go#await -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export-orders", Tenant: "acme"})
if err != nil {
	return err
}
ctx, cancel := context.WithTimeout(ctx, time.Minute) // how long the caller will wait
defer cancel()
raw, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 10 * time.Millisecond}) // how often to look
if err != nil {
	return err
}
res, err := workflow.DecodePayload[ExportResult](raw)
if err != nil {
	return err
}
```

It polls `Client.Status` every `AwaitOptions.Poll` (zero means `workflow.DefaultAwaitPoll`, 500 ms). `AwaitOptions.Sleep` replaces the real timer, for a test that drives the wait with a fake clock. When the run succeeds it calls `Result`; a client without `ResultReader` returns an `UnsupportedError` naming `CapResult` there, after the wait.

`workflow.Await(ctx, client, ref, opts)` is the same wait without the result: it returns the run's terminal status, or `ctx`'s error with the last status seen.

## When the run fails

A run that ends in any status but succeeded makes `AwaitResult` return a `*workflow.RunFailedError` with the run's `Ref`, its `Status` and, when the client can list runs (`RunQuerier`), `Err`, the run's error message:

<!-- snippet: examples/results-progress/main.go#failed -->
```go
_, err = workflow.AwaitResult(ctx, c, ref3, workflow.AwaitOptions{Poll: 10 * time.Millisecond}) // how often to look
var failed *workflow.RunFailedError
if errors.As(err, &failed) {
	fmt.Printf("  pay: ended %s: %s\n", failed.Status, failed.Err)
}
```

```text
  pay: ended failed: memory: step "charge" failed after 1 attempt(s): card declined
```

A cancelled or discarded run's `RunFailedError` matches `workflow.ErrCancelled`, so `errors.Is(err, workflow.ErrCancelled)` tells "someone stopped it" from "it failed".

## Try it

```sh
cd examples
go run ./results-progress
```

The result part of the output:

```text
result:
  {"file":"orders.csv","rows":3000}
  decoded: orders.csv, 3000 rows
  checkout, final steps [email invoice]: {"email":"sent","invoice":"inv_7"}
  RunInfo.Output is the same: true
  RunOutput gives the same: true
  pay: ended failed: memory: step "charge" failed after 1 attempt(s): card declined
  Result of a failed run: ErrWrongState true
```

`go test ./results-progress` checks every line.

## Pitfalls

- **Never await inside a step.** `Await` holds the calling goroutine for the whole wait; in a step that is a worker held idle, and once every worker is awaiting, none is left to run the runs being awaited. A step that needs another run's result starts it as a child and waits with `workflow.AwaitChild`, which holds no worker (see [child runs](../workflows/children.md)).
- **Results are stored with the run.** A step that returns a large blob makes every run's record that large. Return a reference (a file name, a blob key) and keep the data elsewhere, or use the [claim check](../reliability/claim-check.md) for step outputs.
- **A result outlives nothing.** Once retention prunes or `Delete` removes a run, `Result` returns `ErrNotFound`. Copy what you need when the run finishes, for instance from the [run-finished hook](../observability/run-finished.md).
- **`AwaitResult` without a deadline waits forever** on a run that never ends (a wait step whose event never comes). Always pass a `ctx` with a timeout.
- **Polling is not free.** Each poll is a `Status` call to the backend; on a busy durable driver, keep `Poll` at a second or more for long runs.

## Related

- [Typed payloads](payloads.md): `DecodePayload`.
- [Cancel and status](cancel-and-status.md): `Status` and the terminal statuses.
- [Progress and heartbeats](progress.md): what a long run reports before its result.
- [Manage runs](manage-runs.md): `RunInfo.Output` in listings.
- Example: [examples/results-progress](../../examples/results-progress/).
