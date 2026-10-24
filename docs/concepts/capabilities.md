# Capabilities

A driver declares what it can really do, as a set of capabilities, and the library compares that set with what each workflow, schedule and job asks for. A workflow that needs something the driver lacks is refused when the client starts; a field the driver cannot honour is refused with an error naming it. Nothing is silently dropped or emulated badly, so the same spec either runs the same way on every driver that accepts it, or fails on the spot. This page is how to read those declarations and check against them before anything runs.

## Before you start

- You know the [client](client.md) lifecycle: `Register`, then `Start`.

## What a driver declares

`Client.Capabilities()` returns a `workflow.Caps`, a bit set that never changes over the life of a client. `Has(want)` reports whether it holds every capability in `want`; `Missing(required)` returns the ones it lacks (zero means none); `String()` joins the names with `|`.

| Capability | What it promises |
|---|---|
| `CapDurable` | Runs, waits and sleeps survive a process restart |
| `CapDelayedStart` | `Job.RunAt` is honoured |
| `CapWaitForEvent` | Wait steps suspend without holding a worker until a `Signal` or their deadline |
| `CapDurableSleep` | Sleep steps suspend without holding a worker |
| `CapConcurrencyKey` | `Workflow.Concurrency` is enforced per key |
| `CapRateLimit` | `Workflow.RateLimits` are enforced |
| `CapIdempotencyKey` | `Job.IdempotencyKey` dedups enqueues |
| `CapCron` | `Workflow.Cron` and `Client.Schedule` fire |
| `CapCancel` | `Client.Cancel` stops a run |
| `CapRunFinished` | The client implements `RunNotifier` with guaranteed delivery: every terminal run, at least once |
| `CapRetryJitter` | `Backoff.Jitter` spreads retry delays |
| `CapRetryDelay` | `RetryPolicy.DelayFor` sets a retry's delay from the error (`RetryAfter`) |
| `CapMisfireSkip`, `CapMisfireFireOnce`, `CapMisfireFireAll` | The matching `Misfire` policy is honoured |
| `CapCronZone` | Crons may run in a zone other than UTC, evaluated in that zone across DST |
| `CapQueue`, `CapQueueOverride` | `Workflow.Queue` and `Job.Queue` route runs to named queues with per-queue worker limits; with the override a run may leave its workflow's queue ([named queues](../jobs/queues.md)) |
| `CapQueueLimitGlobal` | `QueueConfig.Limit` caps a queue's in-flight step attempts across every client sharing the store ([named queues](../jobs/queues.md#cap-a-queue-across-every-worker)) |
| `CapJobRetry`, `CapRetryUntil`, `CapRetryMaxErrors` | `Job.Retry`, `RetryPolicy.Until` and `RetryPolicy.MaxErrors` are honoured ([retries](../jobs/retries.md#limits-beyond-max)) |
| `CapDiscard` | A step returning `Discard(err)` ends its run without failing it (`discarded` with `CapRunStates`, else `cancelled`); without it `Discard` acts as `Permanent` |
| `CapDiscardExhausted` | `RetryPolicy.DiscardOnExhaust` ends a run whose retries ran out `discarded` instead of `failed` ([retries](../jobs/retries.md#quiet-giving-up-discardonexhaust)) |
| `CapRunQuery`, `CapRetryRun`, `CapDeleteRun`, `CapCancelAll` | The client implements `RunQuerier`, `RunRetrier`, `RunDeleter` and `BulkCanceller` ([manage runs](../jobs/manage-runs.md)) |
| `CapPauseWorkflow`, `CapPauseQueue` | The client implements `Pauser` for workflows and everything, and for queues |
| `CapScheduleWhen`, `CapScheduleWindow` | `Workflow.ScheduleWhen` is asked at every tick; `Schedule.Windows` and `Workflow.CronWindows` are honoured |
| `CapTickEvents`, `CapScheduleList`, `CapSchedulePause` | The client implements `TickNotifier`, `ScheduleLister` and `SchedulePauser` |
| `CapDrain` | The client implements `Drainer` (`Drain`, `Done`) |
| `CapDrainReport` | The client implements `ReportingDrainer`: `DrainWithReport` says which attempts a drain cut off ([stop gracefully](client.md#stop-gracefully)) |
| `CapRunStates` | Runs report `retrying` while a step waits on its backoff and `discarded` after `Discard` ([run states](../jobs/run-states.md)) |
| `CapChain` | `Job.Chain` runs jobs one after another, and steps edit the rest with `PrependToChain` and `AppendToChain` ([chains](../workflows/chains.md)) |
| `CapWorkerScale` | The client implements `WorkerScaler`, so a `BalanceLoop` can move workers between queues ([worker balancing](../operations/balancing.md)) |
| `CapClosure` | The client implements `ClosureEnqueuer`: in-process Go functions run as jobs ([closures](../jobs/closures.md)) |
| `CapLabelPrefix` | `RunQuery.LabelPrefixes` matches runs by a label value prefix in `Runs` and `CancelAll` ([search by label](../jobs/manage-runs.md#search-by-label)) |

None of the drivers needs a server to answer, so you can print them all:

<!-- snippet: examples/capabilities/main.go#declare -->
```go
sqlc, err := sqlstore.New(db, sqlb.SQLiteDialect{})
if err != nil {
	return err
}
drivers := []struct {
	name string
	c    workflow.Client
}{
	{"memory", memory.New()},
	{"sqlstore", sqlc},
	{"hatchet", hatchet.New(hatchet.Config{})}, // no connection before Start
}
for _, d := range drivers {
	caps := d.c.Capabilities()
	fmt.Fprintf(out, "%-8s durable=%-5v %s\n", d.name, caps.Has(workflow.CapDurable), caps)
}
```

```text
memory   durable=false DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|RunFinished|RetryJitter|RetryDelay|MisfireSkip|MisfireFireOnce|MisfireFireAll|CronZone|Queue|QueueOverride|JobRetry|RetryUntil|RetryMaxErrors|Discard|RunQuery|RetryRun|DeleteRun|CancelAll|PauseWorkflow|PauseQueue|SchedulePause|ScheduleWhen|ScheduleWindow|TickEvents|ScheduleList|Drain|RunStates|Chain|WorkerScale|Closure|LabelPrefix|OverlapSkip|ScheduleTrigger|ScheduleHistory|ErrorHistory|ForceRun|Snooze|Heartbeat|Tenant|RunTimeout|NodeIdentity|KeyFIFO|DueQuery|Result|CronSeconds|ScheduleActive|ScheduleKeep|TickTime|QueueLimitGlobal|DiscardExhausted|DrainReport
sqlstore durable=true  Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|RunFinished|RetryJitter|RetryDelay|MisfireSkip|MisfireFireOnce|MisfireFireAll|CronZone|Queue|QueueOverride|JobRetry|RetryUntil|RetryMaxErrors|Discard|RunQuery|RetryRun|DeleteRun|CancelAll|PauseWorkflow|PauseQueue|SchedulePause|ScheduleWhen|ScheduleWindow|TickEvents|ScheduleList|Drain|RunStates|Chain|WorkerScale|LabelPrefix|OverlapSkip|ScheduleTrigger|ScheduleHistory|EnqueueOnly|ErrorHistory|ShutdownRequeue|ForceRun|Snooze|Heartbeat|Tenant|RunTimeout|NodeIdentity|KeyFIFO|DueQuery|Result|CronSeconds|ScheduleActive|ScheduleKeep|TickTime|QueueLimitGlobal|DiscardExhausted|DrainReport
hatchet  durable=true  Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|Queue|RunQuery|RetryRun|CancelAll|ScheduleList|Drain|ScheduleTrigger|ScheduleHistory|EnqueueOnly|Tenant|KeyFIFO|DueQuery|Result|CronSeconds|TickTime|DrainReport
```

Each declared capability is proven by the [conformance suite](../extending/writing-a-driver.md#run-the-conformance-suite) against the real backend; the Hatchet driver's against a live engine. Durability is never inferred from a workflow: the memory driver runs a workflow that waits a week, and losing it on exit is the app's choice of driver. The full matrix, with what each driver refuses beyond capabilities, is in the [capability reference](../reference/capabilities.md#capability-matrix).

## Check a workflow before you run it

`workflow.Requires(wf)` computes what a workflow needs as written: `CapCron` for a `Cron`, `CapCronZone` for a `CronLocation` other than UTC, the misfire capability of its policy, `CapConcurrencyKey`, `CapRateLimit`, `CapIdempotencyKey` for `IdempotencyRequired`, `CapWaitForEvent` and `CapDurableSleep` for wait and sleep steps, and the retry capabilities of each step's effective policy (a workflow default every step overrides asks for nothing).

<!-- snippet: examples/capabilities/main.go#requires -->
```go
digest := workflow.Workflow{
	Name:         "digest",
	Cron:         []string{"0 9 * * *"},
	CronLocation: kolkata,
	Misfire:      workflow.Misfire{Policy: workflow.MisfireSkip},
	Retry:        workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Second, Factor: 2, Jitter: 0.2}},
	Steps: []workflow.Step{{Name: "send", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		return nil, nil
	}}},
}
need := workflow.Requires(digest)
fmt.Fprintln(out, "digest needs:", need)
for _, d := range drivers {
	if missing := d.c.Capabilities().Missing(need); missing != 0 {
		fmt.Fprintf(out, "  %s lacks %s\n", d.name, missing)
	}
}
```

```text
digest needs: Cron|RetryJitter|MisfireSkip|CronZone
  hatchet lacks RetryJitter|MisfireSkip|CronZone
```

`workflow.CheckCaps(have, wf)` is the same check as an error: nil, or an `UnsupportedError` naming the workflow. Drivers call it from `Start` (the Hatchet driver already at `Register`), so a deploy that registers `digest` on Hatchet fails at start-up, before any run. `workflow.RequiresJob(job)` is the per-job counterpart: `CapDelayedStart` for a `RunAt`, `CapIdempotencyKey` for a key (even when the workflow's policy is `IdempotencyOff`, because a key is the caller's request and is never dropped without an error). Drivers check it in `Enqueue`.

<!-- snippet: examples/capabilities/main.go#job -->
```go
job := workflow.Job{Workflow: "digest", RunAt: time.Date(2026, 12, 1, 9, 0, 0, 0, kolkata), IdempotencyKey: "digest-2026-12-01"}
fmt.Fprintln(out, "job needs:", workflow.RequiresJob(job))
```

```text
job needs: DelayedStart|IdempotencyKey
```

## Check a schedule

A schedule made at run time goes through the same check. `workflow.RequiresSchedule(s)` is `CapCron`, plus `CapCronZone` for a `Location` other than UTC and the capability of its misfire policy; `workflow.CheckScheduleCaps(have, s)` returns an `UnsupportedError` whose `Field` names the cause (`Schedule`, `Schedule.Location` or `Schedule.Misfire`). Drivers call it from `Client.Schedule`, after `ValidateSchedule`.

<!-- snippet: examples/capabilities/main.go#schedule -->
```go
nightly := workflow.Schedule{Name: "acme-nightly", Workflow: "digest", Cron: "0 2 * * *", Location: kolkata}
fmt.Fprintln(out, "schedule needs:", workflow.RequiresSchedule(nightly))
fmt.Fprintln(out, "on hatchet:", workflow.CheckScheduleCaps(hatchet.New(hatchet.Config{}).Capabilities(), nightly))
fmt.Fprintln(out, "on memory: ", workflow.CheckScheduleCaps(memory.New().Capabilities(), nightly))
```

```text
schedule needs: Cron|CronZone
on hatchet: workflow: Schedule.Location unsupported by this driver (missing CronZone): schedule for workflow digest
on memory:  <nil>
```

## Refusals

A refusal is a `*workflow.UnsupportedError`: `Field` is the spec field or feature, `Missing` the capabilities that would be needed (zero when the refusal is not about a capability), `Reason` a detail such as the workflow name. It matches `workflow.ErrUnsupported` under `errors.Is`, and its message alone tells an operator what to change. `workflow.Unsupported(field, reason)` builds one without a capability, for a driver refusing a field it cannot express.

<!-- snippet: examples/capabilities/main.go#refusal -->
```go
err = workflow.CheckCaps(hatchet.New(hatchet.Config{}).Capabilities(), digest)
if ue, ok := errors.AsType[*workflow.UnsupportedError](err); ok {
	fmt.Fprintf(out, "refused: field=%s missing=%s reason=%q\n", ue.Field, ue.Missing, ue.Reason)
}
fmt.Fprintln(out, "is ErrUnsupported:", errors.Is(err, workflow.ErrUnsupported))

// The Hatchet driver also refuses at Register what its engine cannot
// express, naming the field: here a Go key function, where Hatchet
// takes CEL expressions (workflow.KeyExpr).
perTenant := workflow.Workflow{
	Name:        "sync",
	Concurrency: &workflow.Concurrency{Key: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] }), MaxRuns: 1},
	Steps:       digest.Steps,
}
fmt.Fprintln(out, "hatchet Register:", hatchet.New(hatchet.Config{}).Register(perTenant))
```

```text
refused: field=Workflow missing=RetryJitter|MisfireSkip|CronZone reason="workflow digest"
is ErrUnsupported: true
hatchet Register: workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow sync: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression
```

Where each driver refuses:

| Driver | At `Register` | At `Start` | At `Enqueue` and `Schedule` |
|---|---|---|---|
| memory | invalid specs, bad cron expressions | the capability check (it passes, since only `CapDurable` is missing and nothing requires it), an expression key (`KeyExpr`) on concurrency, rate limits or a wait | invalid schedules |
| sqlstore | invalid specs, bad cron expressions | the capability check (it has every capability but `CapClosure`), an expression key (`KeyExpr`) | invalid schedules |
| hatchet | what Hatchet cannot express (a Go key function on concurrency, since keys are CEL; a non-UTC `CronLocation`; any `Misfire`; cron macros and `@every`; a backoff whose `Initial` is not `Factor` seconds; `Backoff.Jitter`; `DelayFor`; a rate-limit window other than a second, minute, hour, day or week), then the capability check | `Config` tunings past the SDK's limits | a negative `Job.Priority`, `IdempotencyKey` with `RunAt`, a `Schedule.Location` other than UTC, any `Schedule.Misfire` |

The Hatchet list is the driver's own documentation (`hatchet/doc.go`), and the [Hatchet guide](../drivers/hatchet.md#what-it-can-do-and-what-it-refuses) explains each.

## Try it

```sh
cd examples
go run ./capabilities
```

The program prints every block above in order, then the error examples of [errors](errors.md) and [labels](labels.md).

## Pitfalls

- A capability check is not the only refusal. `Requires` sees capabilities; a driver may also refuse a field it cannot express (a Go key function on Hatchet, a `KeyExpr` on the memory, SQL and Redis drivers). Register on the real driver in a test to catch both.
- `CapRunFinished` is about the guarantee, not the method. The Hatchet client implements `OnRunFinished` but does not declare `CapRunFinished`, because its delivery is best-effort; code that needs every end must check the capability, not only the type assertion.
- `Caps.Missing` takes the required set, not the available one: `have.Missing(need)`.
- A wrapper declares what it really guarantees. `failover.Client` declares the intersection of its two backends, so a workflow that runs on each alone may be refused by the pair.
- In app tests, `workflowtest.NewFake(workflowtest.FakeLike(prod))` refuses what the production driver's capabilities refuse, so a test cannot pass with a field production would reject ([Behave like your production driver](../testing/fake.md#behave-like-your-production-driver)).

## Related

- [Capability reference](../reference/capabilities.md#capability-matrix)
- [Choose a driver](../choosing/drivers.md#memory-sql-redis-or-hatchet)
- [Errors](errors.md#sentinel-errors)
- Example: [capabilities](../../examples/capabilities/)
