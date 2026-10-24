# The Hatchet driver

The Hatchet driver is a `workflow.Client` whose runs, DAG steps, durable waits and sleeps, retries, timeouts, crons, concurrency keys and idempotency keys are [Hatchet](https://github.com/hatchet-dev/hatchet)'s own, through its current Go SDK (`sdks/go`). Use it when you already run a Hatchet engine, or want its dashboard and its scale, and keep the same workflow code you run on the [memory](memory.md) and [SQL](sqlstore.md) drivers. It is a separate module, so an app that never uses Hatchet never downloads the SDK.

What Hatchet cannot express is refused with an error that names the field, never dropped: workflows at `Register`, schedules at `Schedule`, jobs at `Enqueue`.

## Before you start

- `go get github.com/ubgo/workflow/hatchet@latest` (the module needs Go 1.26).
- A Hatchet engine and an API token. The SDK reads `HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT` and `HATCHET_CLIENT_TLS_STRATEGY` from the environment; a local `hatchet-lite` works.

## Create the client

`hatchet.New(cfg)` returns a client; nothing connects until `Start`. `Register` only checks and records definitions. `Start` checks the Config tunings, builds the SDK client (unless `Config.Client` was given), declares every workflow, registers the worker and returns once the engine lists it. The worker stops when `Start`'s context is cancelled.

<!-- snippet: examples/hatchet-worker/main.go#start -->
```go
err := c.Register(workflow.Workflow{
	Name: "example-greet",
	Steps: []workflow.Step{{Name: "say", Run: workflow.Define(func(ctx context.Context, in struct{ Name string }) (string, error) {
		return "hello " + in.Name, nil
	})}},
})
if err != nil {
	return err
}
ctx, cancel := context.WithCancel(ctx)
defer cancel()                       // cancelling Start's context stops the worker
if err := c.Start(ctx); err != nil { // connects, declares, waits until the engine lists the worker
	return err
}
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "example-greet", Payload: []byte(`{"Name":"Ada"}`)})
if err != nil {
	return err
}
```

The zero `Config{}` builds the SDK client from the `HATCHET_CLIENT_*` variables at `Start`. To pass your own options, set `Config.ClientOptions` (token, host and port, namespace, TLS), or build the SDK client yourself and set `Config.Client`.

## Configuration

Every field is optional; a zero value means the default.

<!-- snippet: examples/hatchet-worker/main.go#new -->
```go
c := hatchet.New(hatchet.Config{
	// Client: an SDK client you built; nil builds one from ClientOptions
	// and the HATCHET_CLIENT_* environment variables at Start.
	Namespace:          os.Getenv("HATCHET_CLIENT_NAMESPACE"), // must equal the SDK's namespace
	WorkerName:         "hatchet-worker-example",              // its name in the dashboard
	Slots:              10,                                    // steps this worker runs at once
	StepTimeout:        10 * time.Minute,                      // for steps that set no Timeout (default 24h)
	IdempotencyMaxHold: 6 * time.Hour,                         // a key's longest hold (default 24h)
	RegisterTimeout:    time.Minute,                           // Start's wait for the worker (default 30s)
	VisibilityLag:      5 * time.Second,                       // how far the read side may trail a write
	Middleware:         nil,                                   // step middleware, outermost first
	Logger:             logger,                                // background errors (nil discards)
	Clock:              clock.Real{},                          // local tickers; nil means clock.Real

	GroupChunkRuns:           t.GroupChunkRuns, // tunings: see docs/operations/tuning.md
	GroupChunkBytes:          t.GroupChunkBytes,
	BatchCoordinator:         t.BatchCoordinator,
	BatchCoordinatorTimeout:  t.BatchCoordinatorTimeout,
	BatchCoordinatorRetry:    t.BatchCoordinatorRetry,
	BatchCoordinatorMaxInput: t.BatchCoordinatorMaxInput,
})
```

| Field | What it does | Default |
|---|---|---|
| `Namespace` | Must equal the namespace the SDK client uses (option or `HATCHET_CLIENT_NAMESPACE`), so idempotency keys are scoped by the name the engine knows each workflow by | empty |
| `WorkerName` | The worker's name in Hatchet's dashboard; the driver adds `-` and eight characters per process | `ubgo-workflow` |
| `Slots` | How many steps this worker runs at once | the SDK's default |
| `StepTimeout` | Execution timeout of a step whose `Workflow.Timeout` and `Step.Timeout` are zero. Hatchet always applies one, and its server default kills long jobs minutes in, so the driver stands in a day for "none" | 24h |
| `ScheduleTimeout` | How long a step may wait in the engine's queue before a worker takes it (held by a rate limit, a concurrency limit, busy or absent workers); past it the engine fails the run with no attempt and no error message. The engine's own default is five minutes, which failed a run a rate limit held longer, so every step declares this instead (`DefaultScheduleTimeout`); set it above the longest wait you expect | 24h |
| `IdempotencyMaxHold` | Caps how long a key stays held when its run never finishes; normally a key is released when the run ends | 24h |
| `RegisterTimeout` | How long `Start` waits for the engine to list the worker; raise it for an engine that starts slowly | 30s |
| `VisibilityLag` | How long the engine's read side may trail a write: `Status` retries a 404 this long before `ErrNotFound` | 5s |
| `FailedSettle` | How long after a failed attempt a FAILED read of a run with retries left still counts as between attempts (the run reads queued or running): the engine shows FAILED for a moment between a failed attempt and its retry | 5s (`hatchet.DefaultFailedSettle`) |
| `Middleware` | Step middleware around every step handler, first outermost | none |
| `Logger` | Errors of background work (run-finished polls, observer panics, worker stop) and of `BatchSource` given no logger | discard |
| `Recycle` | Stop this client's workers at a limit (finished handler attempts, time since `Start`, heap size) so a supervisor starts a fresh process; up to `Slots` attempts the engine already handed out may start past `MaxJobs` ([Worker recycling](../operations/recycling.md)) | never |
| `Clock` | What is local to the process: poll tickers, the delivered-run window, `FinishedAt` of a delayed start cancelled before it fired. Times compared with the engine's own timestamps stay on the wall clock | `clock.Real` |
| `Queues` | The named queues this process serves, one Hatchet worker each with `Workers` as its slots (`Slots` when zero); empty means one worker for everything | none |
| `API` | The engine's REST API (`hatchet.NewAPI(token, serverURL)`), which turns on workflow pause and schedule pause | nil (pause refused) |
| `RunFinishedPoll`, `RunFinishedWindow`, `RunFinishedCatchUp` | The run-end notifier's poll interval, how old an unfinished run may be and still be watched, and how far back `Start` replays ends | 1s, 24h, none |
| `GroupChunkRuns`, `GroupChunkBytes`, `BatchCoordinator`, `BatchCoordinatorTimeout`, `BatchCoordinatorRetry`, `BatchCoordinatorMaxInput` | Batches: [Tuning › Hatchet tunings](../operations/tuning.md#hatchet-tunings) | see there |

Set `Step.Timeout` near each step's real duration: it is also how long a step on a dead worker waits before the engine reclaims it (see [Known engine quirks](#known-engine-quirks)).

## What it can do and what it refuses

The driver declares `CapDurable`, `CapDelayedStart`, `CapWaitForEvent`, `CapDurableSleep`, `CapConcurrencyKey`, `CapRateLimit`, `CapIdempotencyKey`, `CapCron` and `CapCancel`, each proven by the shared conformance suite against a real engine, and `CapQueue`, `CapRunQuery`, `CapRetryRun`, `CapCancelAll`, `CapScheduleList`, `CapDrain`, `CapScheduleTrigger`, `CapScheduleHistory` and `CapEnqueueOnly`, proven by the driver's live tests (see [Queues, run management and drain](#queues-run-management-and-drain)), `CapTenant`, `CapTickTime`, `CapCronSeconds`, `CapResult`, `CapDueQuery` and `CapKeyFIFO`, proven live (see [Tenants, results and reschedule](#tenants-results-and-reschedule)), and, only when `Config.API` is set, `CapPauseWorkflow` and `CapSchedulePause`. It does not declare `CapSnooze`, `CapHeartbeat`, `CapRunTimeout`, `CapScheduleActive`, `CapScheduleKeep`, `CapNodeIdentity`, `CapRunFinished`, `CapCronZone`, `CapRetryJitter`, `CapRetryDelay`, any misfire capability, `CapQueueOverride`, `CapJobRetry`, `CapRetryUntil`, `CapRetryMaxErrors`, `CapDiscard`, `CapDeleteRun`, `CapPauseQueue`, `CapScheduleWhen`, `CapScheduleWindow`, `CapTickEvents`, `CapOverlapSkip` (refused by field name), `CapErrorHistory`, `CapShutdownRequeue` or `CapForceRun`.

Refusals happen before anything connects. Each error wraps `workflow.ErrUnsupported` and names the field and, where one applies, the missing capability:

<!-- snippet: examples/hatchet-worker/main.go#refusals -->
```go
berlin, err := time.LoadLocation("Europe/Berlin")
if err != nil {
	return err
}
refused := []workflow.Workflow{
	// Hatchet evaluates crons in UTC only.
	{Name: "berlin-report", Cron: []string{"0 9 * * *"}, CronLocation: berlin, Steps: []workflow.Step{{Name: "only", Run: noop}}},
	// Hatchet's concurrency keys are CEL expressions, not Go functions.
	{Name: "per-tenant", Concurrency: &workflow.Concurrency{
		Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] }),
		MaxRuns: 1,
	}, Steps: []workflow.Step{{Name: "only", Run: noop}}},
	// Hatchet waits Factor^n seconds and never adds jitter.
	{Name: "jittered", Retry: workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{
		Initial: 2 * time.Second, Factor: 2, Jitter: 0.2,
	}}, Steps: []workflow.Step{{Name: "only", Run: noop}}},
}
for _, wf := range refused {
	err := c.Register(wf) // refused here, before anything connects
	fmt.Fprintf(out, "%s: ErrUnsupported=%v\n  %v\n", wf.Name, errors.Is(err, workflow.ErrUnsupported), err)
}
```

The full list, from `hatchet/doc.go`:

| Refused | Where | Use instead |
|---|---|---|
| `Concurrency.Key` as a Go function (`workflow.KeyFunc`) | `Register` | `workflow.KeyExpr` with a CEL expression |
| `WaitSpec.KeyFrom` as an expression | `Register` | `workflow.KeyFunc` |
| `Workflow.CronLocation` or `Schedule.Location` other than UTC | `Register`, `Schedule` | Write the expression in UTC, or another driver |
| Any `Workflow.Misfire` or `Schedule.Misfire` policy | `Register`, `Schedule` | None: a tick missed while the engine was down is dropped |
| Cron macros and `@every` | `Register`, `Schedule` | A 5-field expression (checked with `cron.Parse`; there is no other parser option) |
| A retry `Backoff` whose `Initial` is not `Factor` seconds | `Register` | `Initial: 2*time.Second, Factor: 2` and so on (Hatchet waits Factor^n seconds) |
| `Backoff.Jitter`, a `RetryPolicy.DelayFor` such as `RetryAfterDelay` | `Register` | None: the engine never takes a retry delay from the error |
| A negative `Job.Priority` | `Enqueue` | Zero or more |
| `Job.IdempotencyKey` together with `Job.RunAt` | `Enqueue` | One or the other |
| A `RateLimit.Per` other than a second, minute, hour, day or week | `Register` | One of those |
| `RateLimits` on a workflow with more than one first step | `Register` | One first step (Hatchet limits tasks, not runs) |
| A keyless `RateLimit` | `Register` | ``workflow.KeyExpr(`"all"`)`` for one bucket |
| `GroupChunkRuns` over 1000, `GroupChunkBytes` over 2 MiB, a `BatchCoordinatorRetry` Hatchet cannot express | `Start` (the chunk sizes wrap `workflow.ErrInvalid`) | Values within the limits |
| `RetryPolicy.Until`, `RetryPolicy.MaxErrors` | `Register` | `Max` only: Hatchet fixes a task's retries when it is declared |
| `Job.Retry` | `Enqueue` | The step's or workflow's `Retry` |
| `Job.Queue` other than the workflow's queue | `Enqueue` | Set `Workflow.Queue`; a run goes to whichever worker registered its workflow |
| `QueueConfig.Priority` | `Start` | Separate workers (or processes) per queue, sized with `Workers` |
| A non-zero `QueueConfig.Limit` | `Start` | `Workers` per queue worker, or `Workflow.Concurrency` for a limit across workers: the engine has no count for a whole queue (no `CapQueueLimitGlobal`) |
| `RetryPolicy.DiscardOnExhaust` | `Register` | None: the engine has no discarded status, so a run out of retries is `failed` (no `CapDiscardExhausted`) |
| `Workflow.ScheduleWhen`, `Workflow.CronWindows`, `Schedule.Windows` | `Register`, `Schedule` | Check the condition inside the step; the engine fires every tick itself |
| A `RunQuery` or `CancelAll` filter naming `StatusRetrying` or `StatusDiscarded` | `Runs`, `CancelAll` | None: a run in backoff reads `queued` and a discarded run `failed` (no `CapRunStates`); see [Run states](../jobs/run-states.md) |
| `Job.Chain`, and `PrependToChain`/`AppendToChain` in a step | `Enqueue`, `RunGroup`, `SpawnGroup`; the step call | Enqueue the follow-up run from the step; see [Chains](../workflows/chains.md#hatchet-use-child-runs) |
| Worker balancing | Not offered: the client does not implement `WorkerScaler`, so it cannot be a `BalanceLoop`'s `Scaler` | More worker processes for the busy queue: the SDK fixes a worker's slots when it is created |
| `workflow.EnqueueClosure` | `EnqueueClosure` | A registered workflow: a Go closure cannot be sent to the engine |
| `Workflow.CronActive`, `Schedule.Active`, `Workflow.CronKeepRuns`, `Schedule.KeepRuns` | `Register`, `Schedule` | Check the period in the step; prune runs with your own job |
| `Workflow.RunTimeout`, `Workflow.OnTimeout`, `Step.HeartbeatTimeout` | `Register` | `Step.Timeout` per step: the engine has one execution deadline per task |
| A non-zero `QueueConfig.Weight` | `Start` | Separate workers per queue |
| A payload over `Config.PayloadLimit` (1.5 MiB by default) | `Enqueue` | A [claim check](../reliability/claim-check.md) for big payloads |

An engine call that fails because the engine could not be reached (a gRPC Unavailable status, a network error, an HTTP 502, 503 or 504) returns an error matching `workflow.ErrUnavailable`, so a [failover client](../reliability/failover.md) can tell an outage from a refusal; your own cancellation or deadline never does.

Rate limits are Hatchet's fixed windows, not aligned to the clock: up to twice the limit can start in one window-long span across a refill, and the next run of a full bucket may start up to about 30 seconds after the window ends. A held run stays queued; it is never dropped. Each (workflow, limit) has its own buckets, as on the other drivers: the engine keys a dynamic bucket by the key's value alone, so the driver prefixes the value with the limit's namespace, workflow and position, and two workflows whose keys give the same value no longer share a bucket.

Run ends: `OnRunFinished` works, best-effort, by polling the engine's run list; Hatchet has no run-end callback, so `CapRunFinished` is not claimed. A restarted client replays only ends of runs created within `Config.RunFinishedCatchUp`. See [Run-finished hook](../observability/run-finished.md).

## Queues, run management and drain

The driver covers most of the queue and run management toolbox natively: named queues with their own workers, a run list with filters, retrying a failed run from the step that failed, cancelling by filter, listing schedules, and draining a worker before a deploy. Use them as on any driver; check `c.Capabilities()` first when the same code also runs elsewhere.

### Step 1: serve queues with their own workers

`Workflow.Queue` names a workflow's queue, and `Config.Queues` lists the queues this process serves. `Start` runs one Hatchet worker per listed queue, with that queue's `Workers` as its slots; a workflow whose queue is not listed gets no worker in this process, so its runs wait for a process that serves it. Without `Config.Queues`, one worker serves everything, as before.

<!-- snippet: examples/hatchet-manage/main.go#queues -->
```go
c := hatchet.New(hatchet.Config{
	WorkerName: "hatchet-manage-example",
	API:        api, // turns on CapPauseWorkflow and CapSchedulePause
	// One Hatchet worker per queue, each with its own slots. A workflow
	// whose queue is not listed gets no worker in this process.
	Queues: []workflow.QueueConfig{{Name: "reports", Workers: 2}},
})
err = c.Register(workflow.Workflow{
	Name:  report,
	Queue: "reports", // every run of this workflow goes to the "reports" worker
	Steps: []workflow.Step{
		{Name: "fetch", Run: noop},
		{Name: "render", After: []string{"fetch"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("renderer offline") // the first run fails here
			}
			return in, nil
		}},
	},
})
```

### Step 2: list runs

`c.Runs` filters by workflow, queue, labels, statuses and enqueue time, newest first, a page at a time (`Limit`, then `Cursor` from `RunPage.Next`). Each run carries its failed step, error, attempts, payload and labels.

<!-- snippet: examples/hatchet-manage/main.go#runs -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{
	Workflow: report,
	Labels:   map[string]string{"tenant": "acme"},
	Statuses: []workflow.Status{workflow.StatusFailed},
})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(out, "failed run: queue=%s step=%s error=%q\n", r.Queue, r.FailedStep, r.Error)
}
```

`c.CancelAll(ctx, q)` takes the same filters (without `Limit` and `Cursor`) and cancels the matches; with no `Statuses` it cancels only the runs not started yet: delayed starts and queued runs.

### Step 3: retry a failed run

`c.Retry` replays the failed (and any cancelled) step tasks through Hatchet's replay. The steps that succeeded keep their outputs and do not run again, and the run keeps its `RunID`.

<!-- snippet: examples/hatchet-manage/main.go#retry -->
```go
// Replays the failed step; "fetch" succeeded and does not run again.
if _, err := c.Retry(ctx, ref); err != nil {
	return err
}
```

### Step 4: list schedules

A named `Schedule` is one cron trigger per workflow and name, and its ID (`named:` and an encoding of the workflow and name) stays the same when the definition changes: the engine cannot edit a trigger in place, so the driver replaces the trigger behind the same ID, keeping a pause when `Config.API` is set. Calling it on every boot is safe. `c.Schedules` lists every trigger, declared ones included, with its next fire in UTC.

<!-- snippet: examples/hatchet-manage/main.go#schedules -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{Workflow: report, Name: "nightly", Cron: "0 2 * * *", Location: time.UTC})
if err != nil {
	return err
}
again, err := c.Schedule(ctx, workflow.Schedule{Workflow: report, Name: "nightly", Cron: "0 2 * * *", Location: time.UTC})
if err != nil {
	return err
}
fmt.Fprintln(out, "same name, same definition, same ID:", again == id)
list, err := c.Schedules(ctx)
if err != nil {
	return err
}
for _, s := range list {
	if s.Workflow == report {
		fmt.Fprintf(out, "schedule %s: %s, next fire in UTC at %s\n", s.Name, s.Cron, s.Next.Format("15:04"))
	}
}
```

### Step 5: drain before a deploy

`c.Drain` pauses this process's workers on the engine, so no new step reaches them, waits for the steps running here, then stops the workers. `c.Done()` closes once they have stopped. Runs it did not take stay queued for other workers. `c.DrainWithReport` (`CapDrainReport`) also lists the handlers cut off at the deadline, each with the disposition `engine`: whether the engine requeues them, and when, is its decision, which this driver cannot promise, and it is not told when a run ends, so `FinishedRuns` counts only the runs whose handler returned on this client and that the engine reports finished within about two seconds of the drain (a multi-step run still in flight is not counted).

<!-- snippet: examples/hatchet-manage/main.go#drain -->
```go
dctx, stop := context.WithTimeout(ctx, drainTimeout)
defer stop()
// Stops taking new steps, waits for the running ones, then stops the
// workers. Unfinished runs stay on the engine for another worker.
if err := c.Drain(dctx); err != nil {
	return err
}
<-c.Done()
```

### Step 6: pause a workflow or a schedule

Pause needs `Config.API`: SDK v0.107.1 has no call for the engine's workflow pause or cron enable, so the driver calls the REST API itself. `hatchet.NewAPI(token, serverURL)` reads the tenant (and, when `serverURL` is empty, the address) from the same token the SDK uses:

<!-- snippet: examples/hatchet-pause/main.go#api -->
```go
// The tenant and, with no URL given, the address come from the token.
api, err := hatchet.NewAPI(os.Getenv(envToken), os.Getenv("HATCHET_CLIENT_SERVER_URL"))
if err != nil {
	return err
}
c := hatchet.New(hatchet.Config{
	WorkerName: "hatchet-pause-example",
	API:        api, // turns on CapPauseWorkflow and CapSchedulePause
})
```

Without it the driver declares neither capability and refuses both calls before it connects:

<!-- snippet: examples/hatchet-pause/main.go#refusals -->
```go
c := hatchet.New(hatchet.Config{}) // no API
caps := c.Capabilities()
fmt.Fprintf(out, "without API: PauseWorkflow=%v SchedulePause=%v\n", caps.Has(workflow.CapPauseWorkflow), caps.Has(workflow.CapSchedulePause))

err := c.Pause(ctx, workflow.PauseWorkflow("reports"))
fmt.Fprintf(out, "Pause: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))
err = c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: "any"})
fmt.Fprintf(out, "PauseSchedule: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

// NewAPI reads the tenant from the token, so a value that is not one
// is refused at once.
_, err = hatchet.NewAPI("not-a-token", "")
fmt.Fprintf(out, "NewAPI with a bad token: ErrInvalid=%v\n", errors.Is(err, workflow.ErrInvalid))
```

With it, after `Start`, pause a workflow and resume it:

<!-- snippet: examples/hatchet-pause/main.go#pause-workflow -->
```go
if err := c.Pause(ctx, workflow.PauseWorkflow(report)); err != nil {
	return err
}
held, err := c.Enqueue(ctx, workflow.Job{Workflow: report}) // accepted, held by the engine
if err != nil {
	return err
}
paused, err := c.Paused(ctx)
if err != nil {
	return err
}
fmt.Fprintln(out, "workflow listed as paused:", slices.Contains(paused, workflow.PauseWorkflow(report)))
```

<!-- snippet: examples/hatchet-pause/main.go#resume-workflow -->
```go
if err := c.Resume(ctx, workflow.PauseWorkflow(report)); err != nil {
	return err
}
```

and pause a schedule by the ID `Schedule` returned or `Schedules` listed:

<!-- snippet: examples/hatchet-pause/main.go#pause-schedule -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{Workflow: report, Name: "nightly", Cron: "0 2 * * *", Location: time.UTC})
if err != nil {
	return err
}
defer func() { _ = c.Unschedule(context.WithoutCancel(ctx), id) }()
// Disables the cron trigger; a tick that falls due meanwhile is dropped.
if err := c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
	return err
}
```

The program [hatchet-pause](../../examples/hatchet-pause/) runs the refusals with no engine and, when `HATCHET_CLIENT_TOKEN` is set, every call above against a real engine.

`Pause(PauseWorkflow(name))` holds the workflow on the engine, so every worker obeys it: queued steps are not assigned, running ones finish, `Enqueue` still works, and crons and delayed starts that come due wait too. `Paused` lists paused workflows; `Resume(PauseAll())` clears them all. `PauseSchedule` disables a cron trigger (declared ones too); a tick that falls due while it is disabled is dropped, not caught up.

### Step 7: run a schedule now, read its last fire, enqueue from web nodes

`TriggerSchedule(ctx, id)` (`CapScheduleTrigger`) reads a cron trigger, declared or made by `Schedule`, and enqueues its workflow now with the trigger's input and labels plus the trigger labels (`workflow.TriggeredLabels`); it works on a disabled trigger too and leaves the cron alone ([Trigger a schedule now](../schedules/overlap.md#trigger-a-schedule-now)).

`ScheduleInfo.Last` (`CapScheduleHistory`) is not stored by the driver: `Schedules` reads it from the engine's run list, as the newest run the engine fired for the trigger, or that `TriggerSchedule` started for it, within `Config.ScheduleHistoryWindow` (a week by default, `hatchet.DefaultScheduleHistoryWindow`).

`Config.EnqueueOnly` (`CapEnqueueOnly`) makes a client for web nodes: `Enqueue`, `Status`, `Cancel`, `Signal`, `Schedule` and the listings work without `Start` and without a worker, connecting on first use; `Start` starts nothing and `OnRunFinished` is refused ([Web nodes that only enqueue](../operations/deploy.md#web-nodes-that-only-enqueue)).

The driver refuses `OverlapSkip` by field name, because the engine fires ticks without asking a worker, and it claims no error history, shutdown requeue or force: the engine has no call that completes or fails a run from outside, and a worker stopped by its `Start` context spends the attempts it cut short, so use `Drain` for a clean deploy.

### What Hatchet cannot do here

<!-- snippet: examples/hatchet-manage/main.go#refusals -->
```go
c := hatchet.New(hatchet.Config{})
// Hatchet fixes a task's retries when it is declared: no deadline.
err := c.Register(workflow.Workflow{Name: "until", Retry: workflow.RetryPolicy{Max: 3, Until: time.Hour},
	Steps: []workflow.Step{{Name: "only", Run: noop}}})
fmt.Fprintf(out, "Retry.Until: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

// ... and no per-dispatch policy either.
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "any", Retry: &workflow.RetryPolicy{Max: 5}})
fmt.Fprintf(out, "Job.Retry: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

// Each queue is its own Hatchet worker: queues cannot be ordered.
err = hatchet.New(hatchet.Config{Queues: []workflow.QueueConfig{{Name: "high", Priority: 10}}}).Start(ctx)
fmt.Fprintf(out, "QueueConfig.Priority: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

// The driver says what it cannot do, so an app can check first.
for _, cp := range []workflow.Caps{workflow.CapPauseWorkflow, workflow.CapDeleteRun, workflow.CapDiscard} {
	fmt.Fprintf(out, "%v: %v\n", cp, c.Capabilities().Has(cp))
}
```

- **Pause without `Config.API`**, `PauseAll()` and `PauseSchedule` of `All`: refused. The engine stores a pause per workflow and per trigger, with nothing that would also hold ones made later; pause each one. `CapPauseQueue` is not claimed: a queue is not something the engine pauses.
- **Discard**: `workflow.Discard(err)` fails the run, as `Permanent` does (no `CapDiscard`), and `RetryPolicy.DiscardOnExhaust` is refused (no `CapDiscardExhausted`).
- **Delete a run**: the engine has no run delete (no `CapDeleteRun`).
- **Tick events, `ScheduleWhen`, windows**: the engine fires crons without the driver.
- **Label prefix search**: `RunQuery.LabelPrefixes` is refused in `Runs` and `CancelAll` with an `UnsupportedError` (no `CapLabelPrefix`); the engine matches run metadata by exact value only.

### Try it

```sh
cd examples
go run ./hatchet-manage
```

Without an engine it prints the refusals:

```text
Retry.Until: ErrUnsupported=true
Job.Retry: ErrUnsupported=true
QueueConfig.Priority: ErrUnsupported=true
PauseWorkflow: false
DeleteRun: false
Discard: false
HATCHET_CLIENT_TOKEN is not set: the live run is skipped
```

With `HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT` and `HATCHET_CLIENT_TLS_STRATEGY` set for a running engine, the last line becomes:

```text
failed run: queue=reports step=render error="renderer offline"
after retry: succeeded, render attempts=2
same name, same definition, same ID: true
schedule nightly: 0 2 * * *, next fire in UTC at 02:00
workflow listed as paused: true
after resume: succeeded
drained
```

### Pitfalls

- **`ScheduleInfo.Last` comes from the engine's run list.** A trigger whose last fire is older than `Config.ScheduleHistoryWindow`, or whose runs the engine's own retention removed, reads as never fired. `Schedules` costs two run-list calls and one run read per trigger, so listing many triggers is slow.
- **A run held longer than `Config.ScheduleTimeout` is failed by the engine without an attempt** and with an empty error. Set it above the longest rate-limit or backlog wait you expect; the `OnFailure` task keeps the engine's five minutes.
- **An engine UUID stored for a named trigger before this release changes once.** The first `Schedule` call replaces the unmarked trigger and returns the stable `named:` ID; re-read IDs from `Schedule` or `Schedules` rather than keeping the old one. Engine-UUID IDs still work for triggers made without a name.
- **An enqueue-only client needs a worker to have registered the workflow once.** The engine refuses a workflow no worker has registered. A job of a workflow the web node did not `Register` gets only `CheckJob`, so `IdempotencyRequired` and queue rules apply only if the web node registers it too; `OnRunFinished` is refused there, so observe run ends on a worker.

- A workflow reaches the engine only when some worker registers it: `Enqueue` of a workflow whose queue no process has ever served fails.
- `Retry` does not take the idempotency key again on the engine. It refuses with `ErrDuplicate` when another unfinished run holds the key, but an `Enqueue` with that key while the retried run is unfinished starts a second run.
- A named schedule whose definition changed is replaced on the engine (Hatchet cannot edit a trigger in place), behind the same `named:` ID.
- `Runs` reads each listed run in full (one engine call per run), and its cursor is an offset: runs created while you page can make a later page repeat one. `EnqueuedAt` is when the engine created the run; for a delayed start that fired, its fire time.
- `Schedules` lists every trigger of the namespace, including ones made in the dashboard or by other apps; set `Config.Namespace` to keep apps apart.
- `OnRunFinished` polls: a failure retried within a poll interval may be reported only as the final end.

## Tenants, results and reschedule

Batch features the engine can honour, each proven against a live engine:

- **Tenants** (`CapTenant`): `Job.Tenant` travels as run metadata and reaches `StepInfo.Tenant`, `RunInfo.Tenant` and `RunEvent.Tenant`; `RunQuery.Tenant` filters on the engine ([Tenants](../jobs/tenants.md)).
- **Tick time** (`CapTickTime`): every run the engine's cron fires carries `workflow.LabelTickAt`, read from the fire time the engine stamps on it (whole seconds); `TriggerSchedule` sets it to the trigger time.
- **Seconds** (`CapCronSeconds`): a six-field expression with `Schedule.Seconds` or `Workflow.CronSeconds` is sent as is; the engine's scheduler reads six fields as seconds first.
- **Results** (`CapResult`): `Client.Result` and `RunInfo.Output` are the final steps' outputs, which the engine keeps with the run. A client that never registered the workflow rebuilds its steps from the run's shape, so a result with several final steps has its keys in name order there ([Results](../jobs/results.md)).
- **Due-time queries and reschedule** (`CapDueQuery`, `RunRescheduler`): `RunQuery.DueFrom`/`DueTo` filter on `RunInfo.RunAt`, applied to each listed page, so a page can be short. `Reschedule` creates the delayed start again at the new time with the same token and deletes the old one, so the `RunRef` is unchanged; a start due within 15 seconds is refused with `ErrWrongState`, because the engine loads a scheduled run into its ticker about five seconds before it is due and fires it at the loaded time whatever a later change says.
- **FIFO per key** (`CapKeyFIFO`): the engine queues a key's waiting runs in insert order; `Priority` still goes first.
- **Nodes and health** (`NodeLister`, `HealthChecker`): `Config.NodeID` is `StepInfo.Node` and a label on every worker the client registers; `Nodes` lists the nodes whose workers the engine shows active. `CapNodeIdentity` is not claimed: the engine returns no per-attempt worker, so `RunInfo.AttemptedBy` stays empty. `Health` is live until a recycle, and ready while the client is started, the engine answers and its workers are active ([Nodes and health](../operations/nodes.md)).
- **Payload limit** (`PayloadLimiter`): `Enqueue` refuses a payload over `Config.PayloadLimit` (`hatchet.DefaultPayloadLimit`, 1.5 MiB, sized for the engine's default 4 MiB gRPC message) with a `*workflow.PayloadTooLargeError` before it reaches the engine.
- **Outcomes and errors**: `Enqueue` reports a new run or a duplicate key through `workflow.WithEnqueueOutcome`, and a failed run's `RunEvent.Err` is a `*workflow.RunError`.

Not offered, and why: snooze (the engine counts every failed attempt, so `workflow.Snooze` is an ordinary error), heartbeats (the engine's timeout refresh adds to one execution deadline and cannot fail an attempt for a missed beat; `workflow.Heartbeat` is a no-op), run timeouts, active periods and `KeepRuns`, cancelling one step (the engine's cancel ends the task as cancelled, where the contract lets the handler's return decide), lifecycle events (the engine has no insert or start callback) and progress (`ReportProgress` returns an `UnsupportedError`).

## Known engine quirks

Checked on hatchet-lite v0.110.5 with SDK v0.107.1, each with the driver's workaround:

- Scheduled runs store their trigger time without a zone, so a local time fires hours off: `Enqueue` sends `RunAt` in UTC.
- A scheduled-run record is deleted when it fires: the run carries a token in its metadata and the `ScheduledID` encodes it, so `Status` and `Cancel` find the fired run.
- A run is missing from the REST read side for about a second after it is created: `Status` retries a 404 for up to `VisibilityLag`.
- Between a failed attempt and its retry the engine reports the run FAILED for a moment. The driver treats FAILED as final only when it marked the failure non-retryable, the step used its retries, or the failure is older than `FailedSettle`; until then the run reads queued or running. A failure the driver ends without retry is stored with the prefix `[ubgo.workflow: not retried] `, which `RunInfo.Error` and `RunEvent.Err` strip but the Hatchet dashboard shows.
- The SDK panics when a worker is stopped before it registers: `Start` returns only once the engine lists the worker.
- Delete endpoints answer 204, which the SDK reports as an error: a 204 is treated as success.
- A DAG wait condition exposes neither the event payload nor which condition fired: wait steps run as durable tasks.
- A durable wait filtered by a CEL expression never resumed once a non-matching event of the same name arrived in its look-back window: the correlation key is part of the event key instead, and events count from the run's enqueue time.
- A step whose worker process dies is reclaimed only when the step's execution timeout expires, then retried if it has a retry left. With the default `StepTimeout` that is a day, so set `Step.Timeout` near the step's real duration. Reclaim through missed worker heartbeats is not tested.
- The engine rejects a rate limit without a key even when it has a key expression: every limit carries a key naming it.
- Runs are listed by creation time only: the run-end notifier tracks unfinished runs between polls.
- A run's stored input nests the trigger input under `input`: `Runs` reads the payload from there.
- The run details' task events come back empty on hatchet-lite v0.110.5, so the worker that ran each attempt is not known.
- The engine loads a scheduled run into its ticker about five seconds before it is due and fires it at the loaded time: `Reschedule` refuses a start due within 15 seconds.
- Workflow pause takes a mandatory queue TTL, after which the engine cancels the held runs, and an empty or unparsable TTL silently becomes five minutes. `Pause` sends a hundred years. Anyone pausing through the dashboard or their own REST calls must set a long TTL too.

## Reach the Hatchet SDK

`Client.Native()` returns the SDK client, the one escape hatch for what the workflow API does not cover (the run list, workers, the dashboard's data). It is nil before `Start` unless `Config.Client` was given. Do not start a second worker on it.

<!-- snippet: examples/hatchet-worker/main.go#native -->
```go
sdk := c.Native() // the Hatchet SDK client; nil before Start unless Config.Client was set
workers, err := sdk.Workers().List(ctx)
if err != nil {
	return err
}
```

## Try it

The refusals need no engine:

```sh
cd examples
go run ./hatchet-worker
```

```text
capabilities: Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|Queue|RunQuery|RetryRun|CancelAll|ScheduleList|Drain|ScheduleTrigger|ScheduleHistory|EnqueueOnly|Tenant|KeyFIFO|DueQuery|Result|CronSeconds|TickTime|DrainReport
berlin-report: ErrUnsupported=true
  workflow: Workflow.CronLocation unsupported by this driver (missing CronZone): workflow berlin-report: Hatchet evaluates crons in UTC only, not Europe/Berlin; shifting the expression would break across DST
per-tenant: ErrUnsupported=true
  workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow per-tenant: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression
jittered: ErrUnsupported=true
  workflow "jittered" step "only": workflow: Retry.Backoff.Jitter unsupported by this driver (missing RetryJitter): Hatchet waits exactly Factor^n seconds and has no jitter
GroupChunkRuns 5000: ErrInvalid=true
  workflow: invalid spec: hatchet driver: GroupChunkRuns 5000 is over the SDK's bulk limit of 1000
HATCHET_CLIENT_TOKEN is not set: the live run is skipped
```

With `HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT` and `HATCHET_CLIENT_TLS_STRATEGY` set for a running engine, the last line becomes:

```text
live run: succeeded
worker listed through Native: true
```

## Pitfalls

- `Config.Namespace` must match the SDK's namespace, or idempotency keys are scoped by a name the engine does not use.
- Without `Step.Timeout`, a step on a crashed worker waits up to `StepTimeout` (a day by default) before it is retried.
- A tick missed while the engine was down is dropped: Hatchet never catches up, and no misfire policy is accepted.
- `OnRunFinished` is best-effort. Use the memory or SQL driver when every run end must reach an observer.
- `Clock` does not make the engine's own timers fake: steps, sleeps and crons run on the engine's time.

## Related

- [Choose a driver](../choosing/drivers.md) and [Capabilities](../concepts/capabilities.md)
- [Tuning › Hatchet tunings](../operations/tuning.md#hatchet-tunings)
- [Native batch groups](../batches/native-groups.md)
- Example: [hatchet-worker](../../examples/hatchet-worker/)
- Example: [hatchet-manage](../../examples/hatchet-manage/) (queues, run list, retry, schedules, drain)
- Example: [hatchet-pause](../../examples/hatchet-pause/) (workflow and schedule pause through `Config.API`)
- [Run-finished hook](../observability/run-finished.md)
