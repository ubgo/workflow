# The Hatchet driver

The Hatchet driver is a `workflow.Client` whose runs, DAG steps, durable waits and sleeps, retries, timeouts, crons, concurrency keys and idempotency keys are [Hatchet](https://github.com/hatchet-dev/hatchet)'s own, through its current Go SDK (`sdks/go`). Use it when you already run a Hatchet engine, or want its dashboard and its scale, and keep the same workflow code you run on the [memory](memory.md) and [SQL](sqlstore.md) drivers. It is a separate module, so an app that never uses Hatchet never downloads the SDK.

What Hatchet cannot express is refused with an error that names the field, never dropped: workflows at [`Register`](ds:block?id=hatchet-checksupported-733uwgsp), schedules at [`Schedule`](ds:block?id=hatchet-client-schedule-ug2ype7f), jobs at [`Enqueue`](ds:block?id=hatchet-client-prepare-z4xnsdmz).

## Before you start

- `go get github.com/ubgo/workflow/hatchet@latest` (the module needs Go 1.26).
- A Hatchet engine and an API token. The SDK reads `HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT` and `HATCHET_CLIENT_TLS_STRATEGY` from the environment; a local `hatchet-lite` works.

## Create the client

[`hatchet.New(cfg)`](ds:block?id=hatchet-new-4dhudaw3) returns a client; nothing connects until `Start`. [`Register`](ds:block?id=hatchet-client-register-zg59b2j4) only checks and records definitions. [`Start`](ds:block?id=hatchet-client-start-mjwjna3p) checks the Config tunings, builds the SDK client (unless `Config.Client` was given), declares every workflow, registers the worker and returns once the engine lists it ([`registerWait`](ds:block?id=hatchet-client-registerwait-rum23crp)). The worker stops when `Start`'s context is cancelled.

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

The zero `Config{}` builds the SDK client from the `HATCHET_CLIENT_*` variables at `Start`. To pass your own options, set `Config.ClientOptions` (token, host and port, namespace, TLS), or build the SDK client yourself and set `Config.Client`. A client for a reporting service that reads a month of schedule history, from [examples/hatchet-schedules](../../examples/hatchet-schedules/):

<!-- snippet: examples/hatchet-schedules/main.go#new -->
```go
c := hatchet.New(hatchet.Config{
	// The zero Config builds the SDK client from the HATCHET_CLIENT_*
	// variables at Start; ClientOptions or Client replace that.
	Namespace:             os.Getenv("HATCHET_CLIENT_NAMESPACE"), // must equal the SDK's
	WorkerName:            "reports-worker",
	ScheduleHistoryWindow: historyWindow, // how far back ScheduleInfo.Last looks (default a week)
})
```

[`hatchet.New`](ds:block?id=hatchet-new-4dhudaw3) never fails and never connects: a bad `Config` is reported by `Start` ([`checkConfig`](ds:block?id=hatchet-checkconfig-wx933eq2)), and a workflow the engine cannot honour by `Register` ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)). Every zero field takes a default the package exports, so an app can log or compare against them:

<!-- snippet: examples/hatchet-schedules/main.go#defaults -->
```go
retry := hatchet.DefaultBatchCoordinatorRetry()
fmt.Fprintln(w, "StepTimeout:", hatchet.DefaultStepTimeout)
fmt.Fprintln(w, "ScheduleTimeout:", hatchet.DefaultScheduleTimeout)
fmt.Fprintln(w, "RegisterTimeout:", hatchet.DefaultRegisterTimeout)
fmt.Fprintln(w, "RunFinishedPoll:", hatchet.DefaultRunFinishedPoll)
fmt.Fprintln(w, "ScheduleHistoryWindow:", hatchet.DefaultScheduleHistoryWindow)
fmt.Fprintln(w, "PayloadLimit:", hatchet.DefaultPayloadLimit, "bytes")
fmt.Fprintln(w, "GroupChunkRuns:", hatchet.DefaultGroupChunkRuns, "GroupChunkBytes:", hatchet.DefaultGroupChunkBytes)
fmt.Fprintf(w, "BatchCoordinatorRetry: %d retries, %s doubling, capped at %s\n", retry.Max, retry.Backoff.Initial, retry.Backoff.Max)
```

```text
StepTimeout: 24h0m0s
ScheduleTimeout: 24h0m0s
RegisterTimeout: 30s
RunFinishedPoll: 1s
ScheduleHistoryWindow: 168h0m0s
PayloadLimit: 1572864 bytes
GroupChunkRuns: 1000 GroupChunkBytes: 2097152
BatchCoordinatorRetry: 10 retries, 2s doubling, capped at 2m0s
```

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
| [`Namespace`](ds:block?id=hatchet-enginename-q6x5kbtp) | Must equal the namespace the SDK client uses (option or `HATCHET_CLIENT_NAMESPACE`), so idempotency keys are scoped by the name the engine knows each workflow by | empty |
| `WorkerName` | The worker's name in Hatchet's dashboard; the driver adds `-` and eight characters per process ([`startWorker`](ds:block?id=hatchet-client-startworker-kd28sgmy)) | [`ubgo-workflow`](ds:block?id=hatchet-defaultworkername-25wkdr2z) |
| `Slots` | How many steps this worker runs at once | the SDK's default |
| `StepTimeout` | Execution timeout of a step whose `Workflow.Timeout` and `Step.Timeout` are zero ([`stepTimeout`](ds:block?id=hatchet-steptimeout-jkmqcsw5)), and of a wait step without a `Timeout` or `Deadline` ([`waitTimeout`](ds:block?id=hatchet-waittimeout-cwq36qj9)). Hatchet always applies one, and its server default kills long jobs minutes in, so the driver stands in a day for "none". A step with its own timeout is timed by the worker ([`callTimed`](ds:block?id=hatchet-client-calltimed-xjgb6ju6)), and the engine's timeout is 5 seconds later ([`engineTimeoutMargin`](ds:block?id=hatchet-enginetimeoutmargin-nwx47exf)) | 24h ([`DefaultStepTimeout`](ds:block?id=hatchet-defaultsteptimeout-j3gvwcvw)) |
| `ScheduleTimeout` | How long a step may wait in the engine's queue before a worker takes it (held by a rate limit, a concurrency limit, busy or absent workers); past it the engine fails the run with no attempt and no error message. The engine's own default is five minutes, which failed a run a rate limit held longer, so every step declares this instead (`DefaultScheduleTimeout`); set it above the longest wait you expect | 24h ([`DefaultScheduleTimeout`](ds:block?id=hatchet-defaultscheduletimeout-pw5yeefh)) |
| `IdempotencyMaxHold` | Caps how long a key stays held when its run never finishes; normally a key is released when the run ends. A delayed start with a key further ahead than this is refused, since its key would lapse first ([`checkDelayedKey`](ds:block?id=hatchet-checkdelayedkey-8s6en9y6)) | 24h ([`DefaultIdempotencyMaxHold`](ds:block?id=hatchet-defaultidempotencymaxhold-9vxvg6ux)) |
| `RegisterTimeout` | How long `Start` waits for the engine to list the worker; raise it for an engine that starts slowly ([`registerWait`](ds:block?id=hatchet-client-registerwait-rum23crp)) | 30s ([`DefaultRegisterTimeout`](ds:block?id=hatchet-defaultregistertimeout-3vqg4k5w)) |
| `VisibilityLag` | How long the engine's read side may trail a write: [`Status`](ds:block?id=hatchet-rundetailslagged-fm92bjc9) retries a 404 this long before `ErrNotFound` | 5s ([`DefaultVisibilityLag`](ds:block?id=hatchet-defaultvisibilitylag-n7hzhafh)) |
| `FailedSettle` | How long after a failed attempt a FAILED read of a run with retries left still counts as between attempts (the run reads queued or running): the engine shows FAILED for a moment between a failed attempt and its retry ([`failureFinal`](ds:block?id=hatchet-failurefinal-b3phakuj)) | 5s ([`hatchet.DefaultFailedSettle`](ds:block?id=hatchet-defaultfailedsettle-9m779yka)) |
| `Middleware` | Step middleware around every step handler, first outermost | none |
| `Logger` | Errors of background work (run-finished polls, observer panics, worker stop) and of `BatchSource` given no logger | discard |
| `Recycle` | Stop this client's workers at a limit (finished handler attempts, time since `Start`, heap size) so a supervisor starts a fresh process; up to `Slots` attempts the engine already handed out may start past `MaxJobs` ([Worker recycling](../operations/recycling.md)) | never |
| [`Clock`](ds:block?id=hatchet-new-4dhudaw3) | What is local to the process: poll tickers, the delivered-run window, `FinishedAt` of a delayed start cancelled before it fired ([`cancelledBeforeFire`](ds:block?id=hatchet-client-cancelledbeforefire-gjetapqk)). Times compared with the engine's own timestamps stay on the wall clock | `clock.Real` |
| `Queues` | The named queues this process serves ([`workerPools`](ds:block?id=hatchet-workerpools-2t59fpep)), one Hatchet worker each with `Workers` as its slots (`Slots` when zero); empty means one worker for everything | none |
| `API` | The engine's REST API ([`hatchet.NewAPI(token, serverURL)`](ds:block?id=hatchet-newapi-ngnn5g33)), which turns on workflow pause and schedule pause ([`capabilities`](ds:block?id=hatchet-client-capabilities-3ms5edtr)) | nil (pause refused) |
| `RunFinishedPoll`, `RunFinishedWindow`, `RunFinishedCatchUp` | The run-end notifier's poll interval, how old an unfinished run may be and still be watched, and how far back `Start` replays ends ([`OnRunFinished`](ds:block?id=hatchet-client-onrunfinished-ymfhyxkr)) | 1s ([`DefaultRunFinishedPoll`](ds:block?id=hatchet-defaultrunfinishedpoll-47nap6m3)), 24h ([`DefaultRunFinishedWindow`](ds:block?id=hatchet-defaultrunfinishedwindow-eees2d36)), none |
| `GroupChunkRuns`, `GroupChunkBytes`, `BatchCoordinator`, `BatchCoordinatorTimeout`, `BatchCoordinatorRetry`, `BatchCoordinatorMaxInput` | Batches: [Tuning › Hatchet tunings](../operations/tuning.md#hatchet-tunings) | see there; the coordinator's timeout is 5 minutes ([`DefaultBatchCoordinatorTimeout`](ds:block?id=hatchet-defaultbatchcoordinatortimeout-76ab65pt)) |

Set `Step.Timeout` near each step's real duration: it is also how long, plus 5 seconds ([`engineTimeout`](ds:block?id=hatchet-enginetimeout-cuw8khf9)), a step on a dead worker waits before the engine reclaims it (see [Limitations](#limitations)).

## What it can do and what it refuses

The driver declares ([`caps`](ds:block?id=hatchet-caps-d6x9fv8z)) `CapDurable`, `CapDelayedStart`, `CapWaitForEvent`, `CapDurableSleep`, `CapConcurrencyKey`, `CapRateLimit`, `CapIdempotencyKey`, `CapCron` and `CapCancel`, each proven by the shared conformance suite against a real engine, and `CapQueue`, `CapRunQuery`, `CapRetryRun`, `CapCancelAll`, `CapScheduleList`, `CapDrain`, `CapScheduleTrigger`, `CapScheduleHistory` and `CapEnqueueOnly`, proven by the driver's live tests (see [Queues, run management and drain](#queues-run-management-and-drain)), `CapTenant`, `CapTickTime`, `CapCronSeconds`, `CapResult`, `CapDueQuery` and `CapKeyFIFO`, proven live (see [Tenants, results and reschedule](#tenants-results-and-reschedule)), and, only when `Config.API` is set, `CapPauseWorkflow` and `CapSchedulePause` ([`capabilities`](ds:block?id=hatchet-client-capabilities-3ms5edtr)). It does not declare `CapSnooze`, `CapHeartbeat`, `CapRunTimeout`, `CapScheduleActive`, `CapScheduleKeep`, `CapNodeIdentity`, `CapRunFinished`, `CapCronZone`, `CapRetryJitter`, `CapRetryDelay`, any misfire capability, `CapQueueOverride`, `CapJobRetry`, `CapRetryUntil`, `CapRetryMaxErrors`, `CapDiscard`, `CapDeleteRun`, `CapPauseQueue`, `CapScheduleWhen`, `CapScheduleWindow`, `CapTickEvents`, `CapOverlapSkip` (refused by field name), `CapErrorHistory`, `CapShutdownRequeue` or `CapForceRun`.

Refusals happen before anything connects. Each error wraps `workflow.ErrUnsupported` and names the field and, where one applies, the missing capability ([`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)):

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
	fmt.Printf("%s: ErrUnsupported=%v\n  %v\n", wf.Name, errors.Is(err, workflow.ErrUnsupported), err)
}
```

Every refusal, and everything the driver does differently from the other drivers, is in one table under [Limitations](#limitations).

An engine call that fails because the engine could not be reached (a gRPC Unavailable status, a network error, an HTTP 502, 503 or 504) returns an error matching `workflow.ErrUnavailable` ([`engineErr`](ds:block?id=hatchet-engineerr-ungnu8e7)), so a [failover client](../reliability/failover.md) can tell an outage from a refusal; your own cancellation or deadline never does.

Rate limits are Hatchet's fixed windows ([`rateLimitWindows`](ds:block?id=hatchet-ratelimitwindows-5mdddvs7)), not aligned to the clock: up to twice the limit can start in one window-long span across a refill, and the next run of a full bucket may start up to about 30 seconds after the window ends. A held run stays queued; it is never dropped. Each (workflow, limit) has its own buckets, as on the other drivers: the engine keys a dynamic bucket by the key's value alone, so the driver prefixes the value with the limit's namespace, workflow and position ([`scopedRateLimitExpr`](ds:block?id=hatchet-scopedratelimitexpr-7864cad6)), and two workflows whose keys give the same value no longer share a bucket.

Run ends: [`OnRunFinished`](ds:block?id=hatchet-client-onrunfinished-ymfhyxkr) works, best-effort, by polling the engine's run list ([`poll`](ds:block?id=hatchet-notifier-poll-ge6vy3sb)); Hatchet has no run-end callback, so `CapRunFinished` is not claimed. A restarted client replays only ends of runs created within `Config.RunFinishedCatchUp`. See [Run-finished hook](../observability/run-finished.md).

## Queues, run management and drain

The driver covers most of the queue and run management toolbox natively: named queues with their own workers, a run list with filters, retrying a failed run from the step that failed, cancelling by filter, listing schedules, and draining a worker before a deploy ([`caps`](ds:block?id=hatchet-caps-d6x9fv8z)). Use them as on any driver; check `c.Capabilities()` first when the same code also runs elsewhere.

### Step 1: serve queues with their own workers

`Workflow.Queue` names a workflow's queue, and `Config.Queues` lists the queues this process serves. `Start` runs one Hatchet worker per listed queue ([`workerPools`](ds:block?id=hatchet-workerpools-2t59fpep)), with that queue's `Workers` as its slots; a workflow whose queue is not listed gets no worker in this process, so its runs wait for a process that serves it. Without `Config.Queues`, one worker serves everything, as before.

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

[`c.Runs`](ds:block?id=hatchet-client-runs-tp4ns8fc) filters by workflow, queue, labels, statuses and enqueue time, newest first, a page at a time (`Limit`, then `Cursor` from `RunPage.Next`). Each run carries its failed step, error, attempts, payload and labels.

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
	fmt.Printf("failed run: queue=%s step=%s error=%q\n", r.Queue, r.FailedStep, r.Error)
}
```

[`c.CancelAll(ctx, q)`](ds:block?id=hatchet-client-cancelall-np3kt3p8) takes the same filters (without `Limit` and `Cursor`) and cancels the matches; with no `Statuses` it cancels only the runs not started yet: delayed starts and queued runs.

### Step 3: retry a failed run

[`c.Retry`](ds:block?id=hatchet-client-retry-jgtn26c3) replays the failed (and any cancelled) step tasks through Hatchet's replay ([`replayTasks`](ds:block?id=hatchet-replaytasks-uc6typ7h)). The steps that succeeded keep their outputs and do not run again, and the run keeps its `RunID`.

<!-- snippet: examples/hatchet-manage/main.go#retry -->
```go
// Replays the failed step; "fetch" succeeded and does not run again.
if _, err := c.Retry(ctx, ref); err != nil {
	return err
}
```

### Step 4: list schedules

A named `Schedule` is one cron trigger per workflow and name, and its ID ([`namedScheduleID`](ds:block?id=hatchet-namedscheduleid-cbs2f42h): `named:` and an encoding of the workflow and name) stays the same when the definition changes: the engine cannot edit a trigger in place, so the driver replaces the trigger behind the same ID, keeping a pause when `Config.API` is set ([`replaceNamedCron`](ds:block?id=hatchet-client-replacenamedcron-3yexpvd5)). Calling it on every boot is safe. [`c.Schedules`](ds:block?id=hatchet-client-schedules-r5btud4q) lists every trigger, declared ones included, with its next fire in UTC.

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
fmt.Println("same name, same definition, same ID:", again == id)
list, err := c.Schedules(ctx)
if err != nil {
	return err
}
for _, s := range list {
	if s.Workflow == report {
		fmt.Printf("schedule %s: %s, next fire in UTC at %s\n", s.Name, s.Cron, s.Next.Format("15:04"))
	}
}
```

### Step 5: drain before a deploy

[`c.Drain`](ds:block?id=hatchet-client-drain-b9ubn36x) pauses this process's workers on the engine, so no new step reaches them, waits for the steps running here, then stops the workers. `c.Done()` closes once they have stopped. Runs it did not take stay queued for other workers. `c.DrainWithReport` (`CapDrainReport`) also lists the handlers cut off at the deadline, each with the disposition `engine`: whether the engine requeues them, and when, is its decision, which this driver cannot promise, and it is not told when a run ends, so `FinishedRuns` counts only the runs whose handler returned on this client and that the engine reports finished within about two seconds of the drain ([`drainRunSettle`](ds:block?id=hatchet-drainrunsettle-92prtznt), [`settleFinishedRuns`](ds:block?id=hatchet-client-settlefinishedruns-93ccuyj8)) (a multi-step run still in flight is not counted).

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

Pause needs `Config.API`: SDK v0.107.1 has no call for the engine's workflow pause or cron enable, so the driver calls the REST API itself. [`hatchet.NewAPI(token, serverURL)`](ds:block?id=hatchet-newapi-ngnn5g33) reads the tenant (and, when `serverURL` is empty, the address) from the same token the SDK uses:

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

Without it the driver declares neither capability ([`capabilities`](ds:block?id=hatchet-client-capabilities-3ms5edtr)) and refuses both calls before it connects:

<!-- snippet: examples/hatchet-pause/main.go#refusals -->
```go
c := hatchet.New(hatchet.Config{}) // no API
caps := c.Capabilities()
fmt.Printf("without API: PauseWorkflow=%v SchedulePause=%v\n", caps.Has(workflow.CapPauseWorkflow), caps.Has(workflow.CapSchedulePause))

err := c.Pause(ctx, workflow.PauseWorkflow("reports"))
fmt.Printf("Pause: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))
err = c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: "any"})
fmt.Printf("PauseSchedule: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

// NewAPI reads the tenant from the token, so a value that is not one
// is refused at once.
_, err = hatchet.NewAPI("not-a-token", "")
fmt.Printf("NewAPI with a bad token: ErrInvalid=%v\n", errors.Is(err, workflow.ErrInvalid))
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
fmt.Println("workflow listed as paused:", slices.Contains(paused, workflow.PauseWorkflow(report)))
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

[`Pause(PauseWorkflow(name))`](ds:block?id=hatchet-client-pause-g7q4ssw8) holds the workflow on the engine, so every worker obeys it: queued steps are not assigned, running ones finish, `Enqueue` still works, and crons and delayed starts that come due wait too. [`Paused`](ds:block?id=hatchet-client-paused-c4qesxnc) lists paused workflows; [`Resume(PauseAll())`](ds:block?id=hatchet-client-resume-3tkbjrce) clears them all. [`PauseSchedule`](ds:block?id=hatchet-client-pauseschedule-m3skypt5) disables a cron trigger (declared ones too); a tick that falls due while it is disabled is dropped, not caught up.

### Step 7: run a schedule now, read its last fire, enqueue from web nodes

[`TriggerSchedule(ctx, id)`](ds:block?id=hatchet-client-triggerschedule-yt9g9ejr) (`CapScheduleTrigger`) reads a cron trigger, declared or made by `Schedule`, and enqueues its workflow now with the trigger's input and labels plus the trigger labels (`workflow.TriggeredLabels`); it works on a disabled trigger too and leaves the cron alone ([Trigger a schedule now](../schedules/overlap.md#trigger-a-schedule-now)):

<!-- snippet: examples/hatchet-schedules/main.go#trigger -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{Workflow: report, Name: "nightly", Cron: "0 2 * * *", Location: time.UTC})
if err != nil {
	return err
}
ref, err := c.TriggerSchedule(ctx, id) // runs it now; the cron is left alone
if err != nil {
	return err
}
```

`ScheduleInfo.Last` (`CapScheduleHistory`) is not stored by the driver: `Schedules` reads it from the engine's run list ([`scheduleLast`](ds:block?id=hatchet-client-schedulelast-ag8zzmyn)), as the newest run the engine fired for the trigger, or that `TriggerSchedule` started for it, within `Config.ScheduleHistoryWindow` (a week by default, [`hatchet.DefaultScheduleHistoryWindow`](ds:block?id=hatchet-defaultschedulehistorywindow-sspufpjq)). Raise the window for a cron that fires less than weekly, or its last fire reads as never:

<!-- snippet: examples/hatchet-schedules/main.go#last -->
```go
infos, err := c.Schedules(ctx) // Last comes from the engine's run list
if err != nil {
	return err
}
for _, s := range infos {
	if s.ID == id {
		fmt.Fprintf(w, "schedule %s: last run %s, next fire %s\n", s.Name, s.Last.Status, s.Next.Format("15:04 MST"))
	}
}
```

```text
schedule nightly: last run succeeded, next fire 02:00 UTC
```

`Config.EnqueueOnly` (`CapEnqueueOnly`) makes a client for web nodes: `Enqueue`, `Status`, `Cancel`, `Signal`, `Schedule` and the listings work without `Start` and without a worker, connecting on first use ([`lookup`](ds:block?id=hatchet-client-lookup-ch832bcf)); `Start` starts nothing ([`startEnqueueOnly`](ds:block?id=hatchet-client-startenqueueonly-ddszquvp)) and [`OnRunFinished`](ds:block?id=hatchet-client-onrunfinished-ymfhyxkr) is refused ([Web nodes that only enqueue](../operations/deploy.md#web-nodes-that-only-enqueue)); a run recorder takes `EnqueuePlugin` there ([Record runs from web nodes](#record-runs-from-web-nodes)):

<!-- snippet: examples/hatchet-schedules/main.go#enqueue-only -->
```go
web := hatchet.New(hatchet.Config{EnqueueOnly: true}) // connects on first use
err := web.OnRunFinished(func(context.Context, workflow.RunEvent) {})
fmt.Fprintln(w, "OnRunFinished on a web node: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))
fmt.Fprintln(w, "Start on a web node starts nothing:", web.Start(ctx) == nil)
```

```text
OnRunFinished on a web node: ErrUnsupported = true
Start on a web node starts nothing: true
```

A job the web node enqueues runs on any worker that registered its workflow; the program's live part enqueues one and reads `succeeded`.

The driver refuses `OverlapSkip` by field name, because the engine fires ticks without asking a worker ([`overlapReason`](ds:block?id=hatchet-overlapreason-4xfzeess)), and it claims no error history, shutdown requeue or force ([`caps`](ds:block?id=hatchet-caps-d6x9fv8z)): the engine has no call that completes or fails a run from outside, and a worker stopped by its `Start` context spends the attempts it cut short, so use `Drain` for a clean deploy.

<!-- snippet: examples/hatchet-schedules/main.go#overlap -->
```go
err = c.Register(workflow.Workflow{
	Name:        "digest",
	Cron:        []string{"*/5 * * * *"},
	CronOverlap: workflow.OverlapSkip, // the engine fires ticks without asking a worker
	Steps:       []workflow.Step{{Name: "send", Run: noop}},
})
fmt.Fprintln(w, "OverlapSkip: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))
```

### Step 8: cancel a delayed start

[`Cancel`](ds:block?id=hatchet-client-cancelref-55mg6gda) of a delayed start deletes the engine's scheduled run. The engine loads a scheduled run into its ticker up to five seconds before it is due and fires it at the loaded time even after the delete, so a cancel within 15 seconds ([`dueGuard`](ds:block?id=hatchet-dueguard-kj9nm3dg)) of the due time watches for the run: none by the deadline, `Cancel` returns nil; one fired, it cancels that run (unless it already ended) and returns [`hatchet.ErrCancelTooLate`](ds:block?id=hatchet-errcanceltoolate-s9c8yhrj), which also matches `workflow.ErrWrongState` ([`Cancel`](ds:block?id=hatchet-client-cancel-mvr63tkp)). Handle it where a cancel must mean "did not run":

<!-- snippet: examples/hatchet-schedules/main.go#cancel-too-late -->
```go
err = c.Cancel(ctx, delayed)
switch {
case errors.Is(err, hatchet.ErrCancelTooLate):
	// The engine had loaded the start and fired it; Cancel cancelled
	// the run unless it had already ended. Check what it did.
	fmt.Fprintln(w, "the delayed start fired before the cancel reached the engine")
case err != nil:
	return err
default:
	fmt.Fprintln(w, "delayed start cancelled before it fired")
}
```

A start cancelled well ahead of its due time, as here (five minutes), never fires: `delayed start cancelled before it fired`. The measured odds of a late cancel are under [Pitfalls](#pitfalls-1).

### Step 9: hold an idempotency key across a delayed start

The engine's scheduled runs take no idempotency key, so a delayed start that carries one (`Job.RunAt` with `Job.IdempotencyKey`; [`workflow.Spawn`](ds:block?id=spawn-jrs3a39f) sets a key on every child, so a delayed child is one too) is not a scheduled run on this driver. [`Enqueue`](ds:block?id=hatchet-client-enqueue-537mqp9q) starts it at once as a run of the workflow's delayed variant, `<name>__wf_delayed`, which `Start` declares next to every workflow without a `Concurrency` that can take a key ([`hasStartGate`](ds:block?id=hatchet-hasstartgate-jam5t2ur), [`declareDelayed`](ds:block?id=hatchet-client-declaredelayed-vjsnjb2h)). The variant's first task is a start gate, a durable task that sleeps on the engine until `RunAt` and wakes about a second late at most ([`startGateFunc`](ds:block?id=hatchet-startgatefunc-u8ju3mwr)), so the engine holds the key from the `Enqueue` on: a second `Enqueue` with the key, delayed or not, returns the same `RunRef`, and the run reads `StatusScheduled` until the gate opens. Its `RunRef` holds the `RunID`, not a `ScheduledID`, and `Cancel` frees the key before the steps run.

[`checkDelayedKey`](ds:block?id=hatchet-checkdelayedkey-8s6en9y6) refuses the pair (field `Job.IdempotencyKey`, `ErrUnsupported`) on a workflow with a `Concurrency`, whose slots the variant would not share; on a client that did not register the workflow, such as a web node's enqueue-only client, which cannot tell whether the workers declared a variant; and when `RunAt` is further ahead than `Config.IdempotencyMaxHold` (a day by default, [`DefaultIdempotencyMaxHold`](ds:block?id=hatchet-defaultidempotencymaxhold-9vxvg6ux)), the longest the engine holds a key. [`Reschedule`](ds:block?id=hatchet-errreschedulegated-9akd6fmr) refuses to move a waiting one, since a durable sleep cannot be moved: cancel it and enqueue it again. From [examples/hatchet-schedules](../../examples/hatchet-schedules/), on a started client:

<!-- snippet: examples/hatchet-schedules/main.go#delayed-key -->
```go
// A key with RunAt: the run starts at once on the workflow's delayed
// variant, whose start gate sleeps until RunAt, so the engine holds
// the key from now on.
job := workflow.Job{Workflow: report, RunAt: time.Now().Add(delayedBy), IdempotencyKey: "invoice-42"}
keyed, err := c.Enqueue(ctx, job)
if err != nil {
	return err
}
again, err := c.Enqueue(ctx, job) // the key is held: the same run comes back
if err != nil {
	return err
}
fmt.Fprintln(w, "keyed delayed start has a RunID:", !keyed.RunID.IsZero(), "same run for the key:", again == keyed)
if err := waitFor(ctx, c, keyed, workflow.StatusScheduled); err != nil {
	return err
}
_, err = c.Reschedule(ctx, keyed, time.Now().Add(2*delayedBy)) // its gate is a durable sleep
fmt.Fprintln(w, "Reschedule of a keyed start: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))

far := job
far.IdempotencyKey = "invoice-43"
far.RunAt = time.Now().Add(hatchet.DefaultIdempotencyMaxHold + delayedBy) // the key would lapse first
_, err = c.Enqueue(ctx, far)
fmt.Fprintln(w, "RunAt past IdempotencyMaxHold: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))

if err := c.Cancel(ctx, keyed); err != nil { // frees the key; the step never runs
	return err
}
```

Against hatchet-lite v0.110.5 it prints:

```text
keyed delayed start has a RunID: true same run for the key: true
Reschedule of a keyed start: ErrUnsupported = true
RunAt past IdempotencyMaxHold: ErrUnsupported = true
```

The variant's name and its gate's step name are the driver's own, so [`Register`](ds:block?id=hatchet-checksupported-733uwgsp) refuses a workflow that uses either with `ErrInvalid`, before anything connects:

<!-- snippet: examples/hatchet-schedules/main.go#delayed-names -->
```go
reserved := []struct {
	why string
	wf  workflow.Workflow
}{
	{"a name ending in __wf_delayed", workflow.Workflow{Name: "invoice__wf_delayed", Steps: []workflow.Step{{Name: "send", Run: noop}}}},
	{"a step named wf__start", workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{Name: "wf__start", Run: noop}}}},
}
for _, r := range reserved {
	err := c.Register(r.wf) // the driver's delayed variants and their start gate use these names
	fmt.Fprintf(w, "%s: ErrInvalid = %v\n", r.why, errors.Is(err, workflow.ErrInvalid))
}
```

```text
a name ending in __wf_delayed: ErrInvalid = true
a step named wf__start: ErrInvalid = true
```

The key itself holds only until the first of the run's first tasks ends, and the gate is that task, so after `RunAt` the steps run without it; see the idempotency rows under [Limitations](#limitations).

### What Hatchet cannot do here

<!-- snippet: examples/hatchet-manage/main.go#refusals -->
```go
c := hatchet.New(hatchet.Config{})
// Hatchet fixes a task's retries when it is declared: no deadline.
err := c.Register(workflow.Workflow{Name: "until", Retry: workflow.RetryPolicy{Max: 3, Until: time.Hour},
	Steps: []workflow.Step{{Name: "only", Run: noop}}})
fmt.Printf("Retry.Until: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

// ... and no per-dispatch policy either.
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "any", Retry: &workflow.RetryPolicy{Max: 5}})
fmt.Printf("Job.Retry: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

// Each queue is its own Hatchet worker: queues cannot be ordered.
err = hatchet.New(hatchet.Config{Queues: []workflow.QueueConfig{{Name: "high", Priority: 10}}}).Start(ctx)
fmt.Printf("QueueConfig.Priority: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

// The driver says what it cannot do, so an app can check first.
for _, cp := range []workflow.Caps{workflow.CapPauseWorkflow, workflow.CapDeleteRun, workflow.CapDiscard} {
	fmt.Printf("%v: %v\n", cp, c.Capabilities().Has(cp))
}
```

Pause without `Config.API`, discard, run delete, tick events and label-prefix search are not offered either ([`capabilities`](ds:block?id=hatchet-client-capabilities-3ms5edtr)), on top of the refusals above ([`checkRetry`](ds:block?id=hatchet-checkretry-ccamdfbn), [`Enqueue`](ds:block?id=hatchet-client-prepare-z4xnsdmz), [`checkQueues`](ds:block?id=hatchet-checkqueues-xebdznbs)); the reasons and what to use instead are under [Limitations](#limitations).

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

- **`ScheduleInfo.Last` comes from the engine's run list** ([`scheduleLast`](ds:block?id=hatchet-client-schedulelast-ag8zzmyn)). A trigger whose last fire is older than `Config.ScheduleHistoryWindow`, or whose runs the engine's own retention removed, reads as never fired. `Schedules` costs two run-list calls and one run read per trigger, so listing many triggers is slow.
- **A run held longer than `Config.ScheduleTimeout` is failed by the engine without an attempt** and with an empty error ([`DefaultScheduleTimeout`](ds:block?id=hatchet-defaultscheduletimeout-pw5yeefh)). Set it above the longest rate-limit or backlog wait you expect; the `OnFailure` task keeps the engine's five minutes.
- **An engine UUID stored for a named trigger before this release changes once.** The first `Schedule` call replaces the unmarked trigger and returns the stable `named:` ID ([`replaceNamedCron`](ds:block?id=hatchet-client-replacenamedcron-3yexpvd5)); re-read IDs from `Schedule` or `Schedules` rather than keeping the old one. Engine-UUID IDs still work for triggers made without a name.
- **An enqueue-only client needs a worker to have registered the workflow once.** The engine refuses a workflow no worker has registered. A job of a workflow the web node did not `Register` gets only `CheckJob` ([`enqueueOnlyLookup`](ds:block?id=hatchet-client-enqueueonlylookup-btua7kah)), so `IdempotencyRequired` and queue rules apply only if the web node registers it too; `OnRunFinished` is refused there, so observe run ends on a worker.

- A workflow reaches the engine only when some worker registers it ([`workerPools`](ds:block?id=hatchet-workerpools-2t59fpep)): `Enqueue` of a workflow whose queue no process has ever served fails.
- [`Retry`](ds:block?id=hatchet-client-retry-jgtn26c3) does not take the idempotency key again on the engine. It refuses with `ErrDuplicate` ([`checkKeyFree`](ds:block?id=hatchet-client-checkkeyfree-p52jhfhq)) when another unfinished run holds the key, but an `Enqueue` with that key while the retried run is unfinished starts a second run.
- A named schedule whose definition changed is replaced on the engine (Hatchet cannot edit a trigger in place), behind the same `named:` ID ([`replaceNamedCron`](ds:block?id=hatchet-client-replacenamedcron-3yexpvd5)).
- [`Runs`](ds:block?id=hatchet-client-runs-tp4ns8fc) reads each listed run in full (one engine call per run), and its cursor is an offset: runs created while you page can make a later page repeat one. `EnqueuedAt` is when the engine created the run; for a delayed start that fired, its fire time.
- [`Schedules`](ds:block?id=hatchet-client-schedules-r5btud4q) lists every trigger of the namespace, including ones made in the dashboard or by other apps; set `Config.Namespace` to keep apps apart.
- `OnRunFinished` polls ([`poll`](ds:block?id=hatchet-notifier-poll-ge6vy3sb)): a failure retried within a poll interval may be reported only as the final end.

## Tenants, results and reschedule

Batch features the engine can honour ([`caps`](ds:block?id=hatchet-caps-d6x9fv8z)), each proven against a live engine:

- **Tenants** (`CapTenant`): `Job.Tenant` travels as run metadata ([`prepare`](ds:block?id=hatchet-client-prepare-z4xnsdmz)) and reaches `StepInfo.Tenant`, `RunInfo.Tenant` and `RunEvent.Tenant`; `RunQuery.Tenant` filters on the engine ([Tenants](../jobs/tenants.md)).
- **Tick time** (`CapTickTime`): every run the engine's cron fires carries `workflow.LabelTickAt`, read from the fire time the engine stamps on it (whole seconds, [`fireTime`](ds:block?id=hatchet-firetime-mu3jaxcd)); `TriggerSchedule` sets it to the trigger time.
- **Seconds** (`CapCronSeconds`): a six-field expression with `Schedule.Seconds` or `Workflow.CronSeconds` is sent as is ([`checkCron`](ds:block?id=hatchet-checkcron-s2s37tr5)); the engine's scheduler reads six fields as seconds first.
- **Results** (`CapResult`): [`Client.Result`](ds:block?id=hatchet-client-result-vs7rxkce) and `RunInfo.Output` are the final steps' outputs, which the engine keeps with the run. A client that never registered the workflow rebuilds its steps from the run's shape ([`shapeWorkflow`](ds:block?id=hatchet-shapeworkflow-ugsy7nxq)), so a result with several final steps has its keys in name order there ([Results](../jobs/results.md)).
- **Due-time queries and reschedule** (`CapDueQuery`, `RunRescheduler`): `RunQuery.DueFrom`/`DueTo` filter on `RunInfo.RunAt`, applied to each listed page, so a page can be short. [`Reschedule`](ds:block?id=hatchet-client-reschedule-3nfqbter) creates the delayed start again at the new time with the same token and deletes the old one, so the `RunRef` is unchanged; moved to now or earlier, it deletes the start and starts the run at once with the same input, metadata and token ([`startNow`](ds:block?id=hatchet-client-startnow-upqvz45e)), so the ref it returns also carries the `RunID` (the engine fires a scheduled run only from its ticker, too late for `Reschedule` to return the run's ID); a start due within 15 seconds ([`dueGuard`](ds:block?id=hatchet-dueguard-kj9nm3dg)) is refused with `ErrWrongState`, because the engine loads a scheduled run into its ticker about five seconds before it is due and fires it at the loaded time whatever a later change says.
- **FIFO per key** (`CapKeyFIFO`): the engine queues a key's waiting runs in insert order ([`concurrencyStrategy`](ds:block?id=hatchet-concurrencystrategy-hzvwerpw)); `Priority` still goes first.
- **Round robin** (`ConcurrencyRoundRobin`): maps onto the engine's group round-robin ([`concurrencyStrategy`](ds:block?id=hatchet-concurrencystrategy-hzvwerpw)), which serves keys in equal turns. `Concurrency.Weight` is refused (no `CapKeyWeights`, [`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)), and `CapRoundRobin` is not declared until a live test measures the engine's turns the way the conformance suite does.
- **Nodes and health** (`NodeLister`, `HealthChecker`): `Config.NodeID` is `StepInfo.Node` and a label on every worker the client registers; [`Nodes`](ds:block?id=hatchet-client-nodes-52bhc2hy) lists the nodes whose workers the engine shows active. `CapNodeIdentity` is not claimed: the engine returns no per-attempt worker, so `RunInfo.AttemptedBy` stays empty. [`Health`](ds:block?id=hatchet-client-health-zg8x2yqe) is live until a recycle, and ready while the client is started, the engine answers and its workers are active ([Nodes and health](../operations/nodes.md)).
- **Payload limit** (`PayloadLimiter`): `Enqueue` refuses a payload over `Config.PayloadLimit` ([`hatchet.DefaultPayloadLimit`](ds:block?id=hatchet-defaultpayloadlimit-zv2xjvqt), 1.5 MiB, sized for the engine's default 4 MiB gRPC message) with a `*workflow.PayloadTooLargeError` before it reaches the engine.
- **Outcomes and errors**: [`Enqueue`](ds:block?id=hatchet-client-enqueue-537mqp9q) reports a new run or a duplicate key through `workflow.WithEnqueueOutcome`, and a failed run's `RunEvent.Err` is a `*workflow.RunError` ([`runError`](ds:block?id=hatchet-runerror-uu2gg4db)).

Not offered ([`caps`](ds:block?id=hatchet-caps-d6x9fv8z)): snooze, heartbeats, run timeouts, active periods and `KeepRuns`, cancelling one step, lifecycle events and progress; see [Limitations](#limitations) for why and what to use instead.

## Limitations

Everything the Hatchet driver refuses, and everything it does differently from the memory, SQL and Redis drivers, in one table. A refusal is an error wrapping `workflow.ErrUnsupported` (an `*workflow.UnsupportedError` naming the field and, where one applies, the missing capability: [`unsupported`](ds:block?id=hatchet-unsupported-z4dbbpq3)) at the call named, before anything runs; nothing is dropped silently. Checked on hatchet-lite v0.110.5 with SDK v0.107.1. The engine quirks the driver works around without changing what you get are under [Known engine quirks](#known-engine-quirks).

| Feature | On Hatchet | Why (engine fact) | Instead |
|---|---|---|---|
| [`Workflow.RunTimeout`](ds:block?id=hatchet-checksupported-733uwgsp), `Workflow.OnTimeout` | Refused at `Register` (no `CapRunTimeout`) | The engine times out tasks, not runs, and has no hook that runs on a timeout | `Step.Timeout` on each step; a run deadline on the [memory, SQL or Redis driver](../jobs/timeouts.md) |
| [`Step.Timeout`](ds:block?id=hatchet-client-calltimed-xjgb6ju6) (and [`Workflow.Timeout`](ds:block?id=hatchet-enginetimeout-cuw8khf9)) | Honoured by the worker: the attempt's context is cancelled with `workflow.ErrStepTimeout` (`context.Cause` matches it), the attempt fails with it, and a step tolerated by `WithRules` records it as `FailureTimeout`. The engine's own task timeout is set to `Step.Timeout` rounded up to whole seconds plus 5 seconds, and ends only a handler that ignores its context, with the engine's error text | The engine fails a task at its execution timeout with its own message | Return when the step's context ends ([Timeouts](../jobs/timeouts.md), [trigger rules](../workflows/trigger-rules.md#send-a-missed-deadline-to-a-fallback)) |
| A step whose worker process died | Retried only when the step's execution timeout expires: [`Step.Timeout`](ds:block?id=hatchet-enginetimeout-cuw8khf9) plus 5 seconds, or [`Config.StepTimeout`](ds:block?id=hatchet-defaultsteptimeout-j3gvwcvw) (a day by default) for a step without one | The engine reclaims a task at its timeout; reclaim through missed worker heartbeats is not tested | Set `Step.Timeout` near the step's real duration |
| A wait step's execution timeout | The driver declares it ([`waitTimeout`](ds:block?id=hatchet-waittimeout-cwq36qj9)) as long as the wait can last plus 5 seconds: the wait's `Timeout`, or the time left to its `Deadline` when the client starts, rounded up to whole seconds. `Step.Timeout` (else `Workflow.Timeout`) can raise it but never lowers it, so a short `Workflow.Timeout` meant for the run steps leaves the waits alone. A wait ends by its event, or by its `Timeout` or `Deadline` with `workflow.ErrWaitTimeout`, as on the other drivers. A wait with neither (wait forever) gets `Config.StepTimeout` (a day by default) or a longer `Step.Timeout`, after which the engine ends it with its own error text. A sleep step is not affected: its sleep is a condition the engine holds before the step's task starts | A durable task's event wait counts against its execution timeout, and the engine always applies one (seen live on hatchet-lite v0.110.5; `TestLongWaitLive` proves 30-second, `Deadline` and year-long waits and a 9-second sleep under a 3-second `Config.StepTimeout`) | A wait that may last longer than a day takes a `Timeout` or `Deadline`, or a `Step.Timeout` or `Config.StepTimeout` longer than the wait ([Signals](../workflows/signals.md), [Timeouts](../jobs/timeouts.md)) |
| [`Step.HeartbeatTimeout`](ds:block?id=hatchet-checksupported-733uwgsp); `workflow.Heartbeat` | Refused at `Register` (no `CapHeartbeat`); `Heartbeat` is a no-op | A task has one execution deadline, which the engine's timeout refresh only extends, so a missed beat cannot fail the attempt while `Step.Timeout` holds | `Step.Timeout`; heartbeats on the [memory, SQL or Redis driver](../jobs/progress.md#send-heartbeats) |
| A retry [`Backoff`](ds:block?id=hatchet-retrybackoff-vrbwvu97) whose `Initial` is not `Factor` seconds | Refused at `Register` | The engine waits Factor^n seconds before retry n | `Backoff{Initial: 2 * time.Second, Factor: 2}` and so on ([Retries](../jobs/retries.md)) |
| [`Backoff.Jitter`](ds:block?id=hatchet-retrybackoff-vrbwvu97), [`RetryPolicy.DelayFor`](ds:block?id=hatchet-checkretry-ccamdfbn) (`workflow.RetryAfterDelay`) | Refused at `Register` (no `CapRetryJitter`, `CapRetryDelay`) | The engine waits exactly the backoff and never takes a delay from the error | A plain backoff, or another driver |
| [`RetryPolicy.Until`](ds:block?id=hatchet-checkretry-ccamdfbn), `RetryPolicy.MaxErrors`, [`Job.Retry`](ds:block?id=hatchet-caps-d6x9fv8z) | Refused at `Register` and `Enqueue` (no `CapRetryUntil`, `CapRetryMaxErrors`, `CapJobRetry`) | A task's retries are fixed when its workflow is declared | `RetryPolicy.Max` on the step or workflow |
| [`RetryPolicy.DiscardOnExhaust`](ds:block?id=hatchet-checkretry-ccamdfbn); [`workflow.Discard`](ds:block?id=hatchet-caps-d6x9fv8z) | `DiscardOnExhaust` refused at `Register`; a `Discard` fails the run, as `Permanent` does (no `CapDiscardExhausted`, `CapDiscard`) | The engine has no discarded status | Treat a failed run as discarded ([Run states](../jobs/run-states.md)) |
| [`workflow.Snooze`](ds:block?id=hatchet-caps-d6x9fv8z) | An ordinary error: it counts as a failed attempt (no `CapSnooze`) | The engine counts every failed attempt against the retries | A retry policy, or a delayed child run ([Retries](../jobs/retries.md)) |
| [`Retry`](ds:block?id=hatchet-client-checkkeyfree-p52jhfhq) of a run that had an idempotency key | Refused with `ErrDuplicate` when another unfinished run holds the key; otherwise replayed, but the replayed run does not hold the key again, so an `Enqueue` with it starts a second run | The engine's replay does not take the key again | Avoid enqueueing the same key while a retried run is unfinished ([Manage runs](../jobs/manage-runs.md)) |
| A delayed start with an idempotency key ([`Job.RunAt`](ds:block?id=hatchet-client-enqueue-537mqp9q) and [`Job.IdempotencyKey`](ds:block?id=hatchet-checkdelayedkey-8s6en9y6), which [`Spawn`](ds:block?id=spawn-jrs3a39f) always sets) | Taken: the run starts at once as a run of the workflow's delayed variant ([`<name>__wf_delayed`](ds:block?id=hatchet-client-declaredelayed-vjsnjb2h), its steps behind a start gate, a durable task that sleeps until `RunAt` ([`startGateFunc`](ds:block?id=hatchet-startgatefunc-u8ju3mwr)), about a second late at most), so the engine holds the key from the `Enqueue` until `RunAt`. It reads `StatusScheduled` while it waits, its `RunRef` holds the `RunID` (no `ScheduledID`), a duplicate `Enqueue`, delayed or not, gets the same `RunRef`, and `Cancel`, `CancelAll`, `CancelChildren` and a workflow pause act on it. Refused (field `Job.IdempotencyKey`) on a workflow with a `Concurrency`, with `RunAt` further ahead than `Config.IdempotencyMaxHold`, and on a client that did not register the workflow | The engine's scheduled runs take no idempotency key; the engine keeps concurrency slots per workflow and holds a key at most `IdempotencyMaxHold` | A delayed start without a key, or [unique jobs](../reliability/unique-jobs.md), where it is refused ([Delays](../jobs/delays.md)) |
| How long an idempotency key holds | Until the first of the run's first steps ends ([`Client.prepare`](ds:block?id=hatchet-client-prepare-z4xnsdmz)), not until the run ends: after that an `Enqueue` with the key starts a second run. A keyed delayed start holds its key until `RunAt` (its gate is its first task); the steps then run without it | The engine releases a key when the first task created with the run reaches a terminal state (`ReleaseIdempotencyKeys`), and `IdempotencyMaxHold` caps it | One step, or a short first step, where a duplicate during the later steps would hurt; [unique jobs](../reliability/unique-jobs.md) hold until the run ends ([Idempotency keys](../jobs/idempotency.md)) |
| A step named [`wf__start`](ds:block?id=hatchet-startgatetask-bpsdadyd); a workflow name ending in [`__wf_delayed`](ds:block?id=hatchet-delayedsuffix-s4kw5cfy) | Refused at `Register` (`ErrInvalid`, [`checkSupported`](ds:block?id=hatchet-checksupported-733uwgsp)) | The names of the start gate and of the delayed variants | Another name |
| [`Reschedule`](ds:block?id=hatchet-errreschedulegated-9akd6fmr) of a keyed delayed start | Refused (field `Reschedule`) | It is a started run whose gate sleeps on the engine, and a durable sleep cannot be moved | `Cancel` it, which frees the key, and `Enqueue` it again |
| [`Reschedule`](ds:block?id=hatchet-client-reschedule-3nfqbter) of a delayed start due within 15 seconds ([`dueGuard`](ds:block?id=hatchet-dueguard-kj9nm3dg)) | Refused with `ErrWrongState` | The engine loads a scheduled run about five seconds before it is due and fires it at the loaded time whatever a later change says | Reschedule earlier ([Manage runs](../jobs/manage-runs.md)) |
| [`Cancel`](ds:block?id=hatchet-client-cancelref-55mg6gda) of a delayed start due within 15 seconds | Can block until the due time plus `VisibilityLag`, and returns [`hatchet.ErrCancelTooLate`](ds:block?id=hatchet-errcanceltoolate-s9c8yhrj) (matches `ErrWrongState`) when the start fired anyway; that run is cancelled | Same: a deleted start the engine already loaded still fires | Cancel well ahead of the due time ([Step 8](#step-8-cancel-a-delayed-start)) |
| [`Workflow.CronLocation`](ds:block?id=hatchet-checksupported-733uwgsp), [`Schedule.Location`](ds:block?id=hatchet-checkschedulezone-9z4wzpcs) other than UTC | Refused at `Register`, `Schedule` (no `CapCronZone`) | The engine evaluates crons in UTC only | An expression in UTC ([Time zones](../schedules/time-zones.md)) |
| Any [`Workflow.Misfire`](ds:block?id=hatchet-checksupported-733uwgsp) or [`Schedule.Misfire`](ds:block?id=hatchet-checkschedulemisfire-ucbwxfy2) policy | Refused (no misfire capability) | The engine never catches up a missed tick and has no lateness window: a tick missed while it was down is dropped | [Misfire](../schedules/misfire.md); another driver when a missed tick must run |
| Cron macros and [`@every`](ds:block?id=hatchet-checkcron-s2s37tr5) | Refused at `Register`, `Schedule` | The engine parses five-field (or six-field, seconds first) expressions only | A five-field expression ([Expressions](../schedules/expressions.md)) |
| [`Workflow.ScheduleWhen`](ds:block?id=hatchet-checksupported-733uwgsp), `Workflow.CronWindows`, [`Schedule.Windows`](ds:block?id=hatchet-client-schedule-ug2ype7f); tick events ([`caps`](ds:block?id=hatchet-caps-d6x9fv8z)) | Refused (no `CapScheduleWhen`, `CapScheduleWindow`); `CapTickEvents` not claimed | The engine fires every tick itself, without asking a worker | Check the condition in the step ([Conditions](../schedules/conditions.md)) |
| `OverlapSkip` on [`Workflow.CronOverlap`](ds:block?id=hatchet-checksupported-733uwgsp) or [`Schedule.Overlap`](ds:block?id=hatchet-client-schedule-ug2ype7f) | Refused (no `CapOverlapSkip`) | The engine fires a tick without asking a worker and has no setting that skips one while the previous run is unfinished | A `Concurrency` with `MaxRuns: 1`, or another driver ([Overlap](../schedules/overlap.md)) |
| [`Workflow.CronActive`](ds:block?id=hatchet-checksupported-733uwgsp), [`Schedule.Active`](ds:block?id=hatchet-client-schedule-ug2ype7f), `Workflow.CronKeepRuns`, `Schedule.KeepRuns` | Refused (no `CapScheduleActive`, `CapScheduleKeep`) | A Hatchet cron has no start or end, and the engine keeps runs for its retention with no run delete | Check the period in the step; prune with your own job |
| Workflow and schedule pause without [`Config.API`](ds:block?id=hatchet-client-capabilities-3ms5edtr); [`PauseAll()`](ds:block?id=hatchet-errpauseall-z8c4b762), `PauseSchedule` of `All` | Refused (`CapPauseWorkflow`, `CapSchedulePause` only with `Config.API`) | SDK v0.107.1 has no pause call; the engine stores a pause per workflow and per trigger, with nothing that also holds ones made later | `Config.API` and a pause per workflow or trigger ([Step 6](#step-6-pause-a-workflow-or-a-schedule)) |
| Queue pause | Not offered (no `CapPauseQueue`, [`caps`](ds:block?id=hatchet-caps-d6x9fv8z)) | A worker pause is not durable across nodes | Pause the workflows |
| [`Concurrency.Key`](ds:block?id=hatchet-checksupported-733uwgsp) as a Go function (`workflow.KeyFunc`) | Refused at `Register` | The engine evaluates concurrency keys as CEL | `workflow.KeyExpr` ([Concurrency](../jobs/concurrency.md)) |
| [`Concurrency.Weight`](ds:block?id=hatchet-checksupported-733uwgsp); round-robin turns ([`concurrencyStrategy`](ds:block?id=hatchet-concurrencystrategy-hzvwerpw)) | `Weight` refused (no `CapKeyWeights`); `ConcurrencyRoundRobin` is the engine's group round robin, but `CapRoundRobin` is not claimed | The engine serves keys in equal turns and takes no weight; its turns are not yet measured live | Equal turns, or another driver |
| [`WaitSpec.KeyFrom`](ds:block?id=hatchet-checksupported-733uwgsp) as an expression | Refused at `Register` | The engine has no API that evaluates CEL to a string for the driver | `workflow.KeyFunc` ([Signals](../workflows/signals.md)) |
| `SignalWaiting` (transient signals) | Not implemented (no [`TransientSignaller`](ds:block?id=transientsignaller-cww6hkhu)) | The engine cannot tell which runs are suspended on a wait at one moment | A kept `Signal` ([Signals](../workflows/signals.md)) |
| [`workflow.OnSignals`](ds:block?id=signalsplugin-bindclient-g8nk42vn) | Refused at `Use` | The engine keeps no record of the events a run took that a step could read | [Signals](../workflows/signals.md) on another driver |
| [`RateLimit.Per`](ds:block?id=hatchet-ratelimit-mvwtadc2) other than a second, minute, hour, day or week; a keyless `RateLimit`; [`RateLimits`](ds:block?id=hatchet-checksupported-733uwgsp) on a workflow with more than one first step | Refused at `Register` | Fixed windows only; the engine needs a key on every limit; it limits tasks, not runs | One of those windows; ``workflow.KeyExpr(`"all"`)`` for one bucket; one first step |
| Rate-limit windows ([`rateLimitWindows`](ds:block?id=hatchet-ratelimitwindows-5mdddvs7)) | Up to twice the limit can start in one window-long span across a refill, and the next run of a full bucket starts up to about 30 seconds after the window ends; a held run stays queued | The engine's window is fixed but not aligned to the clock, and refills when it next checks the bucket | Size the limit for that |
| [`Workflow.Version`](ds:block?id=hatchet-caps-d6x9fv8z), `Workflow.MinVersion` | Refused at `Register` (no `CapWorkflowVersion`) | The engine hands a run to any worker that registered the workflow's name | Separate workflow names during a rolling deploy ([Deploy](../operations/deploy.md)) |
| A negative [`Job.Priority`](ds:block?id=hatchet-runpriority-3dcz9qpp) | Refused at `Enqueue` | The engine has no level below its default | Zero or more (three levels) |
| [`Job.Queue`](ds:block?id=hatchet-client-prepare-z4xnsdmz) other than the workflow's | Refused at `Enqueue` (no `CapQueueOverride`) | A run goes to whichever worker registered its workflow | `Workflow.Queue` ([Queues](../jobs/queues.md)) |
| [`QueueConfig.Priority`](ds:block?id=hatchet-checkqueues-xebdznbs), `QueueConfig.Weight`, a non-zero `QueueConfig.Limit` | Refused at `Start` (no `CapQueueLimitGlobal`) | Each queue is its own worker, and the engine has no count of a whole queue's steps | `Workers` per queue, or a `Concurrency` for a limit across workers ([Queues](../jobs/queues.md)) |
| Worker scaling (`WorkerScaler`, `BalanceLoop`) | Not implemented (no `CapWorkerScale`, [`caps`](ds:block?id=hatchet-caps-d6x9fv8z)) | The SDK fixes a worker's slots when it is created | More worker processes for the busy queue ([Balancing](../operations/balancing.md)) |
| [`Job.Chain`](ds:block?id=hatchet-caps-d6x9fv8z), `PrependToChain`, `AppendToChain` | Refused at `Enqueue`, `RunGroup`, `SpawnGroup` and in the step (no `CapChain`) | A run's shape is fixed when its workflow is declared | Child runs ([Chains](../workflows/chains.md#hatchet-use-child-runs)) |
| `workflow.EnqueueClosure` | Refused (no `CapClosure`, [`caps`](ds:block?id=hatchet-caps-d6x9fv8z)) | A Go closure cannot be sent to the engine | A registered workflow or `workflow.NewFunc` ([Closures](../jobs/closures.md)) |
| A payload over [`Config.PayloadLimit`](ds:block?id=hatchet-defaultpayloadlimit-zv2xjvqt) (1.5 MiB by default); a JSON object payload with a top-level [`_wf_payload`](ds:block?id=hatchet-encodeinput-47cxpm7v) key | Refused at `Enqueue` (`*workflow.PayloadTooLargeError`; `Job.Payload`) | The engine's default gRPC message is 4 MiB and stores a JSON payload about 2.34 times over; the driver carries the exact bytes under that key | A [claim check](../reliability/claim-check.md); another key name |
| A label under `wf__` or `hatchet__`, or `traceparent` ([`isReservedLabel`](ds:block?id=hatchet-isreservedlabel-s394qze6)) | Refused at `Enqueue` (field [`Job.Labels`](ds:block?id=hatchet-runmetadata-hrnkbdru)) | The driver's and the engine's own run metadata | Another label name ([Labels](../concepts/labels.md)) |
| Jobs with `RunAt` or an `IdempotencyKey` in a batch | [`RunGroup`](ds:block?id=hatchet-client-rungroup-mjs85udq) and `SpawnGroup` refuse them; a batch with one goes through one `Enqueue` per job | The engine's bulk trigger takes neither | [Native batch groups](../batches/native-groups.md) |
| [`OnRunFinished`](ds:block?id=hatchet-client-onrunfinished-ymfhyxkr) | Works by polling the run list, best effort (no `CapRunFinished`): a run older than `RunFinishedWindow` when it ended, one that ended while no process polled and was created before `RunFinishedCatchUp`, or a delayed start another process cancelled is not reported; refused on an enqueue-only client | The engine has no run-end callback | A run recorder with its Reconciler, or the memory or SQL driver ([Run-finished hook](../observability/run-finished.md)) |
| `RunInfo.Error`, `RunEvent.Err` text | The engine's stored text, which hatchet-lite v0.110.5 hands back as a JSON string, in double quotes; in `RunEvent.Err` a step timeout still matches `ErrStepTimeout` and a wait timeout `ErrWaitTimeout` ([`storedTimeoutKinds`](ds:block?id=hatchet-storedtimeoutkinds-98vv3f3q)) (both also `ErrTimeout`) | The engine stores a task's error as JSON | Match with `errors.Is`, or by substring |
| A `RunQuery` or `CancelAll` filter naming `StatusRetrying` or `StatusDiscarded` ([`checkRunStates`](ds:block?id=hatchet-checkrunstates-d62jh9cp)) | Refused (no `CapRunStates`): a run in backoff reads `queued`, a discarded run `failed` | The engine's statuses are queued, running, completed, failed and cancelled | [Run states](../jobs/run-states.md) |
| `RunQuery.LabelPrefixes`, `RunQuery.Payload` | Refused by [`Runs`](ds:block?id=hatchet-client-runs-tp4ns8fc) and [`CancelAll`](ds:block?id=hatchet-client-cancelall-np3kt3p8) (no `CapLabelPrefix`, `CapPayloadSearch`) | The engine filters metadata by exact value only | Exact labels |
| [`Runs`](ds:block?id=hatchet-client-runs-tp4ns8fc) paging and times | One engine read per listed run; the cursor is an offset, so runs created while you page can repeat; `EnqueuedAt` is when the engine created the run (for a fired delayed start, its fire time) | The engine lists runs by creation time with an offset | Page soon after the query; filter with `From`/`To` |
| `EditRun`, `ResetRun`, `ForkRun` | Not implemented (no [`RunEditor`](ds:block?id=runeditor-ss73q99q), [`RunResetter`](ds:block?id=runresetter-pe26r9jr), [`RunForker`](ds:block?id=runforker-9cpe2f7f)) | A run's input and shape are the engine's once it is created; its replay re-runs failed tasks only | `Cancel` and `Enqueue` again; `Retry` for a failed run ([Edit runs](../jobs/edit-runs.md), [Manage runs](../jobs/manage-runs.md)) |
| Durable stores for the core helpers (`JournalStore` for `Memo` and `Spawn`, `PromiseStore`, `DataStore`, `BucketStore`, `Inbox`, the unique, debounce, aggregate, disable and lineage stores) | None: the helpers use their in-memory defaults, which hold within one process | The engine keeps no app data the driver could use | The SQL or Redis driver's store beside the Hatchet client ([Memoisation](../workflows/memoisation.md)) |
| Run delete, forcing a run's end, cancelling one step | Not offered (no `CapDeleteRun`, `CapForceRun` in [`caps`](ds:block?id=hatchet-caps-d6x9fv8z); no [`StepCanceller`](ds:block?id=stepcanceller-gp5urxfn)) | The engine has no run delete, no call that completes or fails a run from outside, and its cancel ends a task as cancelled where the contract lets the handler decide | The engine's retention; `Cancel` the run |
| Progress (`ReportProgress`), lifecycle events (`OnLifecycle`) | [`ReportProgress`](ds:block?id=reportprogress-qtz35tuj) returns an `UnsupportedError`; no [`LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9) | The driver keeps no progress, and the engine has no insert or start callback | [Progress](../jobs/progress.md) and [lifecycle events](../observability/lifecycle.md) on another driver, or a run recorder |
| Per-attempt node and error history | Not claimed (no `CapNodeIdentity`, `CapErrorHistory` in [`caps`](ds:block?id=hatchet-caps-d6x9fv8z)): `RunInfo.AttemptedBy`, `AttemptError.Node` and `RunInfo.Errors` stay empty | The run details' task events come back empty on hatchet-lite v0.110.5 | A [run recorder](../observability/run-recorder.md) keeps every attempt's error |
| Shutdown requeue | Not claimed (no `CapShutdownRequeue` in [`caps`](ds:block?id=hatchet-caps-d6x9fv8z)): a worker stopped by its `Start` context cancels its running steps, and the engine counts the attempt | The engine has no hand-back for a stopping worker | `Drain` before a deploy ([Step 5](#step-5-drain-before-a-deploy)) |
| [`Config.Clock`](ds:block?id=hatchet-new-4dhudaw3) | Fakes only what is local to the process; steps, sleeps, crons and timeouts run on the engine's time | The engine keeps its own timers | Real time in tests against an engine ([Time](../concepts/time.md)) |
| A workflow no worker has registered | `Enqueue` fails ([`lookup`](ds:block?id=hatchet-client-lookup-ch832bcf)) | Workflows reach the engine only through a worker's registration | Start a worker that registers it once |

## Known engine quirks

Checked on hatchet-lite v0.110.5 with SDK v0.107.1, each with the driver's workaround; what they leave different for your app is in [Limitations](#limitations). Three of them are the app's to tune: the read side's lag (`VisibilityLag`, default [`DefaultVisibilityLag`](ds:block?id=hatchet-defaultvisibilitylag-n7hzhafh)), the moment a failed attempt reads FAILED before its retry (`FailedSettle`, default [`DefaultFailedSettle`](ds:block?id=hatchet-defaultfailedsettle-9m779yka)), and how long a dead worker's step waits before the engine reclaims it (`Step.Timeout`):

<!-- snippet: examples/hatchet-schedules/main.go#quirks -->
```go
slow := hatchet.New(hatchet.Config{
	VisibilityLag: slowEngineLag, // Status retries the read side's 404 this long (default 5s)
	FailedSettle:  slowEngineLag, // a FAILED read with retries left is "between attempts" (default 5s)
})
err = slow.Register(workflow.Workflow{Name: "export", Steps: []workflow.Step{{
	Name:    "upload",
	Run:     noop,
	Timeout: uploadTimeout, // also how long a step on a dead worker waits before the engine reclaims it
}}})
```

The quirks and what the driver does about each:

- Scheduled runs store their trigger time without a zone, so a local time fires hours off: [`Enqueue`](ds:block?id=hatchet-client-enqueue-537mqp9q) sends `RunAt` in UTC.
- A scheduled-run record is deleted when it fires: the run carries a token in its metadata and the `ScheduledID` encodes it ([`scheduledIDFromMetadata`](ds:block?id=hatchet-scheduledidfrommetadata-qs7dg9ac)), so [`Status`](ds:block?id=hatchet-client-status-ykkq4x58) and `Cancel` find the fired run.
- A run is missing from the REST read side for about a second after it is created: `Status` retries a 404 for up to `VisibilityLag` ([`runDetailsLagged`](ds:block?id=hatchet-rundetailslagged-fm92bjc9)).
- Between a failed attempt and its retry the engine reports the run FAILED for a moment. The driver treats FAILED as final ([`failureFinal`](ds:block?id=hatchet-failurefinal-b3phakuj)) only when it marked the failure non-retryable, the step used its retries, or the failure is older than `FailedSettle`; until then the run reads queued or running. A failure the driver ends without retry is stored with the prefix [`[ubgo.workflow: not retried] `](ds:block?id=hatchet-finalmarker-qyzxgwfh), which `RunInfo.Error` and `RunEvent.Err` strip but the Hatchet dashboard shows.
- The SDK panics when a worker is stopped before it registers: `Start` returns only once the engine lists the worker ([`registerWait`](ds:block?id=hatchet-client-registerwait-rum23crp)).
- Delete endpoints answer 204, which the SDK reports as an error: a 204 is treated as success ([`isNoContent`](ds:block?id=hatchet-isnocontent-trj95gub)).
- A DAG wait condition exposes neither the event payload nor which condition fired: wait steps run as durable tasks ([`waitFunc`](ds:block?id=hatchet-client-waitfunc-ed3hwy7u)).
- A durable wait filtered by a CEL expression never resumed once a non-matching event of the same name arrived in its look-back window: the correlation key is part of the event key instead ([`eventKey`](ds:block?id=hatchet-eventkey-p3rh4yc7)), and events count from the run's enqueue time.
- The engine rejects a rate limit without a key even when it has a key expression: every limit carries a key naming it ([`rateLimit`](ds:block?id=hatchet-ratelimit-mvwtadc2)).
- Runs are listed by creation time only: the run-end notifier tracks unfinished runs between polls ([`poll`](ds:block?id=hatchet-notifier-poll-ge6vy3sb)).
- A run's stored input nests the trigger input under `input`: `Runs` reads the payload from there ([`runInput`](ds:block?id=hatchet-runinput-xzafk9na)).
- The engine loads a scheduled run into its ticker up to five seconds before it is due and fires it at the loaded time, whatever a later delete says: `Reschedule` and `Cancel` of a start due within 15 seconds ([`dueGuard`](ds:block?id=hatchet-dueguard-kj9nm3dg)) behave as [Limitations](#limitations) says.
- The engine deletes a scheduled run's record as it fires it, a moment before its read side lists the run, so a delayed start with no record and no listed run could be fired or cancelled. `Status` calls it cancelled only after `VisibilityLag` ([`resolveScheduled`](ds:block?id=hatchet-resolvescheduled-65msmc9t)), or at once when this client's `Cancel` deleted it at least 15 seconds before it was due or watched it past its due time with no run fired. It never decides by the `ScheduledID`'s first `RunAt`, which `Reschedule` can move earlier. `Status` of a delayed start that another process cancelled takes up to `VisibilityLag`.
- Workflow pause takes a mandatory queue TTL, after which the engine cancels the held runs, and an empty or unparsable TTL silently becomes five minutes. `Pause` sends a hundred years ([`pauseQueueTTL`](ds:block?id=hatchet-pausequeuettl-gmqt4v6r)). Anyone pausing through the dashboard or their own REST calls must set a long TTL too.

## Record run history

The engine keeps its runs as long as its own retention says, lists them the way it sees them, and keeps no step timeline. A [run recorder](../observability/run-recorder.md) keeps a history your app owns next to it: `workflow.Use(hatchet.New(cfg), rec.Plugin())`, then register and start through the client `Use` returned. What differs on this driver:

- **Run ends are best effort: run the Reconciler.** The driver implements `OnRunFinished` by polling the engine's run list and does not claim `CapRunFinished`, so a run end can be missed: a run that was older than `Config.RunFinishedWindow` when it ended, a run that ended while no process was polling and was created more than `Config.RunFinishedCatchUp` before the next `Start`, a delayed start cancelled before it fired (only the process that cancelled it reports that). Run [`recorder.NewReconciler(c, store)`](ds:block?id=rec-newreconciler-su363hcv) with `go rc.Run(ctx)` next to the client: each pass reads unfinished records back from the engine and finishes them.
- **The record outlives the engine's retention.** Once the engine prunes a run, the record still answers; an unfinished record whose run the engine no longer knows is flagged stale, never cancelled.
- **The record keeps every attempt's error.** The driver claims no `CapErrorHistory`, so `RunInfo.Errors` is empty here, while the recorder's step middleware stores each failed attempt with its error on the record.
- **Results.** The driver declares `CapResult`, so [`recorder.WithResultReader(c)`](ds:block?id=rec-withresultreader-wrjsqy26) stores a succeeded run's result on the record.
- **Backfill costs engine calls.** The Reconciler lists the engine's runs of the backfill window, and `Runs` reads each listed run in full. On an engine other apps share, scope it with [`recorder.WithBackfillQuery(workflow.RunQuery{Workflow: ...})`](ds:block?id=rec-withbackfillquery-qc3kqct7) or `Tenant`, and keep `RunQuery.LabelPrefixes` out of it: the driver refuses them (no `CapLabelPrefix`).

The recorder's end-to-end suite runs its driver cases against a live engine (with `HATCHET_CLIENT_TOKEN` set), recording into [`recorder.MemoryStore`](ds:block?id=rec-memorystore-smz35v44).

### Record runs from web nodes

A client built with `Config.EnqueueOnly` refuses `OnRunFinished`, so `workflow.Use(web, rec.Plugin())` fails there with an error matching `workflow.ErrUnsupported`. Give the web node the recorder's enqueue half, [`rec.EnqueuePlugin()`](ds:block?id=rec-recorder-enqueueplugin-rn5k8npv), and the worker processes the full plugin, over one store every process reaches:

```go
rec, err := recorder.New(store) // a store every process reaches: sqlrecorder on a shared database
// The web node: it enqueues and runs nothing, so it records only what it accepted.
web, err := workflow.Use(hatchet.New(hatchet.Config{EnqueueOnly: true}), rec.EnqueuePlugin())
// Each worker process: steps, events and the run's end, into the same record.
worker, err := workflow.Use(hatchet.New(hatchet.Config{WorkerName: "thumbnails"}), rec.Plugin())
```

The web node records the run with its source, labels and input when the engine accepts it; the worker records the steps and the end into the same record, because every write is an upsert keyed by the run. Run the Reconciler on a worker, where `OnRunFinished` works. The same split runs without an engine on the SQL driver in [examples/recorder-split](../../examples/recorder-split/) ([Record run history › Web nodes that only enqueue](../observability/run-recorder.md#web-nodes-that-only-enqueue)); against a live engine the recorder's end-to-end suite runs it with `sqlrecorder` on SQLite.

## Reach the Hatchet SDK

[`Client.Native()`](ds:block?id=hatchet-client-native-zq53xzxt) returns the SDK client, the one escape hatch for what the workflow API does not cover (the run list, workers, the dashboard's data). It is nil before `Start` unless `Config.Client` was given. Do not start a second worker on it.

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
capabilities: Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|Queue|RunQuery|RetryRun|CancelAll|ScheduleList|Drain|ScheduleTrigger|ScheduleHistory|EnqueueOnly|Tenant|KeyFIFO|DueQuery|Result|CronSeconds|TickTime|DrainReport|WaitOwnRun
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

The schedule features, the defaults and the web-node client:

```sh
cd examples
go run ./hatchet-schedules
```

```text
StepTimeout: 24h0m0s
ScheduleTimeout: 24h0m0s
RegisterTimeout: 30s
RunFinishedPoll: 1s
ScheduleHistoryWindow: 168h0m0s
PayloadLimit: 1572864 bytes
GroupChunkRuns: 1000 GroupChunkBytes: 2097152
BatchCoordinatorRetry: 10 retries, 2s doubling, capped at 2m0s
trigger now: true last fire: true
OnRunFinished on a web node: ErrUnsupported = true
Start on a web node starts nothing: true
OverlapSkip: ErrUnsupported = true
export registered with a 2m0s step timeout: true
a name ending in __wf_delayed: ErrInvalid = true
a step named wf__start: ErrInvalid = true
HATCHET_CLIENT_TOKEN is not set: the live run is skipped
```

Against hatchet-lite v0.110.5 the last line becomes:

```text
schedule nightly: last run succeeded, next fire 02:00 UTC
enqueued from the web node: succeeded
keyed delayed start has a RunID: true same run for the key: true
Reschedule of a keyed start: ErrUnsupported = true
RunAt past IdempotencyMaxHold: ErrUnsupported = true
delayed start cancelled before it fired
```

## Pitfalls

Every refusal and difference is in [Limitations](#limitations); these are the ones that bite in production.

- `Config.Namespace` must match the SDK's namespace, or idempotency keys are scoped by a name the engine does not use ([`engineName`](ds:block?id=hatchet-enginename-q6x5kbtp)).
- [`OnRunFinished`](ds:block?id=hatchet-client-onrunfinished-ymfhyxkr) is best-effort. Use the memory or SQL driver when every run end must reach an observer.
- A run recorder without a Reconciler can leave records unfinished: `OnRunFinished` is the recorder's only end signal on a run whose end the poller missed ([Record run history](#record-run-history)).
- [`Cancel`](ds:block?id=hatchet-client-cancelref-55mg6gda) of a delayed start due within 15 seconds can block until the start's due time plus `VisibilityLag`, and can return [`ErrCancelTooLate`](ds:block?id=hatchet-errcanceltoolate-s9c8yhrj). The engine fires a start it has already loaded into its ticker even after its record is deleted: measured on hatchet-lite v0.110.5 with 15 samples per offset, a delete 10 or 5 seconds before due stopped every start, 3 seconds before let 6 of 15 start, 1 second before 12 of 15, and half a second before 13 of 15. `Cancel` therefore deletes the start, then watches for its run: none by the deadline, it returns nil and the start will not run; one listed, it cancels that run (unless it already ended) and returns `ErrCancelTooLate`, which matches `workflow.ErrWrongState`. A fast step can finish before that cancel lands, so cancel a delayed start well ahead of its due time when it must not run at all. `CancelAll` deletes every start first, watches the late ones together, and counts one that fired as cancelled with no error.

## Related

- [Choose a driver](../choosing/drivers.md) and [Capabilities](../concepts/capabilities.md)
- [Tuning › Hatchet tunings](../operations/tuning.md#hatchet-tunings)
- [Native batch groups](../batches/native-groups.md)
- Example: [hatchet-worker](../../examples/hatchet-worker/)
- Example: [hatchet-manage](../../examples/hatchet-manage/) (queues, run list, retry, schedules, drain)
- Example: [hatchet-pause](../../examples/hatchet-pause/) (workflow and schedule pause through `Config.API`)
- Example: [hatchet-schedules](../../examples/hatchet-schedules/) (defaults, trigger-now, last fire, web nodes, a keyed delayed start, a late cancel)
- [Run-finished hook](../observability/run-finished.md)
- [Record run history](../observability/run-recorder.md) and example [recorder-split](../../examples/recorder-split/) (a web node and a worker)
