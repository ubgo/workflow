# Enqueue a job

A job is one request to run a registered workflow. You enqueue one whenever your app has work that should happen outside the current request: send an email after sign-up, rebuild a report after an import, call a slow API. [`Client.Enqueue`](ds:block?id=client-enqueue-mafr75se) returns at once with a reference to the run; the workflow's steps run on the client's workers.

## Before you start

- A client with the workflow registered and started. The [quickstart](../start/quickstart.md) builds one on the memory driver; every driver takes the same `Job`.
- The example for this page is [`examples/quickstart`](../../examples/quickstart) for a plain enqueue and [`examples/concurrency`](../../examples/concurrency) for priorities. Run them with `go run ./quickstart` and `go run ./concurrency` from `examples/`.

## Enqueue a job

[`workflow.Job`](ds:block?id=job-j5g728nc) names the workflow and carries its input. `Payload` is the input of the workflow's first steps, as bytes; `workflow.Define` decodes it from JSON into a struct for you (see [steps](../workflows/steps.md#typed-handlers-with-define)). `Labels` carry app metadata such as the tenant or where the job came from, explicitly, so a middleware can require one instead of trusting a context value.

<!-- snippet: examples/quickstart/main.go#start -->
```go
ctx, cancel := context.WithCancel(ctx)
defer cancel() // cancelling the Start context stops the workers
if err := c.Start(ctx); err != nil {
	return err
}
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "greet", Payload: []byte(`{"Name":"Ada"}`)})
if err != nil {
	return err
}
```

`Enqueue` returns a [`workflow.RunRef`](ds:block?id=runref-yjg7d4x5). Keep it if you want to read the run's status or cancel it later ([cancel and status](cancel-and-status.md)). A run that starts now has `RunRef.RunID` set; a run delayed with `RunAt` has only `ScheduledID` until it starts ([delays](delays.md)), except a delayed start with an idempotency key on Hatchet, which has its `RunID` at once ([`Enqueue`](ds:block?id=hatchet-client-enqueue-ym8nfcz9)).

Other `Job` fields, each on its own page:

| Field | What it does | Page |
|---|---|---|
| `RunAt` | Starts the run later | [delays](delays.md) |
| `IdempotencyKey` | One run per key while it is unfinished | [idempotency](idempotency.md) |
| `Priority` | Orders runs waiting for a worker | [below](#priority) |

`Enqueue` before `Start` returns `workflow.ErrNotStarted` ([`Client`](ds:block?id=client-enqueue-mafr75se); the memory driver's [`usableLocked`](ds:block?id=memory-client-usablelocked-ky48yta5)), so a forgotten `Start` fails loudly instead of queueing work nobody runs. An unregistered workflow returns an error matching `workflow.ErrNotFound` ([memory](ds:block?id=memory-client-enqueuelocked-z3x2cytk), [SQL](ds:block?id=sql-client-enqueuereg-pzaxh9eg), [Redis](ds:block?id=redis-client-enqueue-f4veqdur), [Hatchet](ds:block?id=hatchet-client-lookup-ch832bcf)), except on an enqueue-only client (`CapEnqueueOnly`), which enqueues any name for the workers that register it.

## Priority

[`Job.Priority`](ds:block?id=job-j5g728nc) orders runs that are waiting for a worker: higher goes first, and runs of equal priority go first come, first served (the memory driver's [`admitLocked`](ds:block?id=memory-client-admitlocked-g322u8vq)). Zero is the default. It does not pre-empt a step that is already running.

The example runs one worker, keeps it busy, enqueues three jobs and prints the order they run in:

<!-- snippet: examples/concurrency/main.go#priority -->
```go
for _, j := range []workflow.Job{
	{Workflow: "task", Payload: []byte("backfill"), Priority: -10},
	{Workflow: "task", Payload: []byte("normal")}, // Priority 0 is the default
	{Workflow: "task", Payload: []byte("password-reset"), Priority: 10},
} {
	if _, err := c.Enqueue(ctx, j); err != nil {
		return err
	}
}
```

## Try it

```sh
cd examples
go run ./concurrency
```

The priority part of the output:

```text
priority:
  password-reset, normal, backfill
```

## Pitfalls

- **Hatchet has three priority levels and none below its default.** The Hatchet driver maps 0 to the engine default (its lowest level), 1 to medium and 2 or more to high, and refuses a negative `Priority` with an error matching `workflow.ErrUnsupported` (field `Job.Priority`, [`runPriority`](ds:block?id=hatchet-runpriority-3dcz9qpp)). The example's `-10` works on the memory, SQL and Redis drivers only; on Hatchet keep priorities at 0 and above.
- **Priority is not a queue per tenant.** It orders the waiting runs; a flood of high-priority jobs still delays the others. To stop one tenant from crowding out the rest, cap it with [concurrency per key](concurrency.md#limit-runs-per-key).
- **Labels are visible to every step and observer.** A run's labels reach each step as [`StepInfo.Labels`](ds:block?id=stepinfo-kun3n4c3) and each run-finished observer as [`RunEvent.Labels`](ds:block?id=runevent-upvhdx59), so do not put secrets in them. The library writes its own labels under three prefixes (`ubgo.workflow/`, `workflow.batch.` and `otel.`, listed by [`workflow.ReservedLabelPrefixes`](ds:block?id=reservedlabelprefixes-s6pnbs6s)); keep app labels out of them: every driver's `Enqueue` refuses one with `workflow.ErrInvalid` ([`CheckJobLabels`](ds:block?id=checkjoblabels-p6m28b9h); the source labels set by `workflow.WithSource` are the exception), and `Client.Schedule` refuses them too ([`ValidateSchedule`](ds:block?id=validateschedule-knj437ed)).

## Related

- [Run a job later](delays.md), [Idempotency keys](idempotency.md), [Cancel and status](cancel-and-status.md)
- [Workflows and steps](../workflows/steps.md)
- Example: [`examples/concurrency`](../../examples/concurrency)
