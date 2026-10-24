# The client

`workflow.Client` is the one interface an app uses, and every driver implements it: the memory driver, the SQL driver and the Hatchet driver, plus every wrapper in the library (plugins, middleware, failover). App code depends on the interface alone, so moving from the memory driver in tests to SQLite on one server and Hatchet in a cluster changes one constructor call. This page covers its methods, its lifecycle and the validation every spec goes through.

## The Client interface

| Method | What it does | Errors to expect |
|---|---|---|
| `Register(wf Workflow) error` | Adds a workflow definition; validates it first. Registering a name again replaces it, and its declared `Cron` schedules are replaced, never duplicated | `ErrInvalid`, `ErrAlreadyStarted` after `Start`; the Hatchet driver also refuses here what its engine cannot express (`ErrUnsupported`) |
| `Start(ctx) error` | Checks every registered workflow against `Capabilities`, then starts the workers. Returns once they run; does not block | `ErrUnsupported` naming the workflow and the missing capabilities; `ErrAlreadyStarted` on a second call |
| `Enqueue(ctx, Job) (RunRef, error)` | Starts a run now, or at `Job.RunAt`. A delayed run's `RunRef` holds only a `ScheduledID` until it starts. With an idempotency key held by an unfinished run, starts nothing and returns that run's `RunRef` | `ErrNotStarted`, `ErrNotFound` (unknown workflow), `ErrUnsupported` for a `RunAt` or key the driver cannot honour |
| `Cancel(ctx, RunRef) error` | Stops a run or a delayed start. Idempotent: cancelling a finished run returns nil | `ErrNotFound`, `ErrUnsupported` without `CapCancel` |
| `Status(ctx, RunRef) (Status, error)` | Where a run is: scheduled, queued, running, waiting, succeeded, failed or cancelled | `ErrNotFound` |
| `Signal(ctx, Event) error` | Delivers an event to runs whose wait step matches its name and key. A signal nobody waits for is not an error | `ErrNotStarted` |
| `Schedule(ctx, Schedule) (ScheduleID, error)` | Registers a recurring trigger at run time and returns its ID | `ErrInvalid` (no `Location`, a bad expression), `ErrUnsupported` without `CapCron` or the zone or misfire capability it needs |
| `Unschedule(ctx, ScheduleID) error` | Removes a schedule. An empty ID is a no-op, so a stored ID can be passed unchecked | `ErrNotFound` |
| `Capabilities() Caps` | What this driver can really do. Never changes over the life of a client | — |

Optional interfaces sit beside it, discovered with a type assertion so a driver that cannot do them is never forced to pretend. Each has a capability, which is the reliable check, because wrappers implement every interface and refuse when the client inside lacks it:

| Interface | Methods | Capability | Guide |
|---|---|---|---|
| `workflow.RunNotifier` | `OnRunFinished` | `CapRunFinished` (guaranteed delivery; without it best effort) | [run-finished hook](../observability/run-finished.md) |
| `workflow.RunQuerier` | `Runs` | `CapRunQuery` | [manage runs](../jobs/manage-runs.md) |
| `workflow.RunRetrier` | `Retry` | `CapRetryRun` | [manage runs](../jobs/manage-runs.md#step-2-re-run-a-failed-run-from-the-step-that-failed) |
| `workflow.RunDeleter` | `Delete` | `CapDeleteRun` | [manage runs](../jobs/manage-runs.md#step-3-forget-a-finished-run) |
| `workflow.BulkCanceller` | `CancelAll` | `CapCancelAll` | [manage runs](../jobs/manage-runs.md#step-4-cancel-every-waiting-run) |
| `workflow.Pauser` | `Pause`, `Resume`, `Paused` | `CapPauseWorkflow`, `CapPauseQueue` | [manage runs](../jobs/manage-runs.md#pause-and-resume) |
| `workflow.TickNotifier` | `OnTick` | `CapTickEvents` | [schedule hooks and pings](../schedules/hooks.md) |
| `workflow.ScheduleLister` | `Schedules` | `CapScheduleList` | [manage schedules](../schedules/manage.md) |
| `workflow.SchedulePauser` | `PauseSchedule`, `ResumeSchedule` | `CapSchedulePause` | [manage schedules](../schedules/manage.md#pause-a-schedule) |
| `workflow.Drainer` | `Drain`, `Done` | `CapDrain` | [below](#stop-gracefully) |
| `workflow.ReportingDrainer` | `DrainWithReport` | `CapDrainReport` | [below](#stop-gracefully) |
| `batch.GroupRunner` | `RunGroup` | none | [native groups](../batches/native-groups.md) |

A wrapper you write gets all but the first and last by embedding `workflow.Forward{Inner: inner}` beside the `Client` it wraps; the client `workflow.Use` returns does this already.

<!-- snippet: examples/client-wrapper/main.go#wrapper -->
```go
// audited logs every Enqueue, then passes it on. Embedding Forward next to
// the Client makes it implement every optional interface of the run query,
// pause, schedule and drain families, each forwarded to the same inner
// client.
type audited struct {
	workflow.Client
	workflow.Forward
	out io.Writer
}

func newAudited(inner workflow.Client, out io.Writer) *audited {
	return &audited{Client: inner, Forward: workflow.Forward{Inner: inner}, out: out}
}

// Enqueue is the one method the wrapper changes.
func (a *audited) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	fmt.Fprintf(a.out, "audit: enqueue %s\n", job.Workflow)
	return a.Client.Enqueue(ctx, job)
}
```

A wrapper that embeds only the `Client` hides every optional interface, and `Forward` over a client that lacks one refuses with an `*workflow.UnsupportedError` naming the capability. The program [client-wrapper](../../examples/client-wrapper/) shows all three on the memory driver.

A `workflow.Handler` is `func(ctx context.Context, in []byte) ([]byte, error)`. `workflow.Middleware` wraps every step's handler and `workflow.EnqueueMiddleware` wraps every `Enqueue`; both are the same on every driver.

## Start and stop

Nothing runs until `Start`. A constructor (`memory.New`, `sqlstore.New`, `hatchet.New`) opens no connection and starts no goroutine, so an app can build its client, register workflows and check capabilities in a test without a server. The lifecycle is: register every workflow, then start once.

<!-- snippet: examples/first-workflow/main.go#start -->
```go
c := memory.New()
ended := make(chan workflow.RunEvent, 2)
// Observers are registered before Start; the memory driver reports
// every run that reaches a terminal status.
if err := c.OnRunFinished(func(ctx context.Context, ev workflow.RunEvent) { ended <- ev }); err != nil {
	return err
}
if err := c.Register(wf); err != nil {
	return err
}
ctx, cancel := context.WithCancel(ctx)
defer cancel() // stops every worker when run returns
if err := c.Start(ctx); err != nil {
	return err
}
```

The workers live until the context passed to `Start` is cancelled. That is how an app stops a client: no new step starts after that, and running steps see their own context cancelled. The interface has no `Stop`; the memory and SQL drivers add one (`Client.Stop`) that also waits until every goroutine they started has exited, which a graceful shutdown wants. What happens to unfinished runs is the driver's: the memory driver marks them cancelled, the SQL driver leaves them in the database for the next process.

### Stop gracefully

Cancelling the `Start` context cuts running steps off. A deploy wants the opposite: stop taking work and let what is running finish. `Drain` does that on a client that declares `workflow.CapDrain`: it stops claiming step attempts at once, waits for the running ones until its context ends, then shuts down as cancelling the `Start` context would. It returns nil when everything finished and the context's error when it had to cut something off. Unfinished runs stay queued on the SQL driver for the next process and become cancelled on the memory driver.

<!-- snippet: examples/graceful-drain/main.go#drain -->
```go
// On SIGTERM: stop taking work, give running steps up to 30 seconds.
dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()
if err := c.Drain(dctx); err != nil {
	fmt.Fprintln(out, "some steps were cut off:", err)
}
<-c.Done()
```

To bound the wait and learn what was cut off, use `workflow.DrainTimeout(ctx, c, grace)` or, on a client that declares `workflow.CapDrainReport`, `c.(workflow.ReportingDrainer).DrainWithReport(dctx)`, which returns a `workflow.DrainReport` listing every attempt still running at the deadline and what became of it (`requeued`, `cancelled` or `engine`). `workflow.ShutdownHookReport` packages both as a plain `func(context.Context) error` for a phased shutdown. The deploy guide covers them in [Give running steps a grace period](../operations/deploy.md#give-running-steps-a-grace-period-and-see-what-was-cut-off).

`Done` closes once the client has fully stopped, whichever way it stopped. A batch-style process gives the memory driver `WithStopWhenIdle(grace)`: once its last unfinished run has ended and nothing new arrived for `grace` on the injected clock, the client drains itself and `Done` closes. Schedules do not keep it alive, and a client that never ran anything never stops this way, so `Start` followed by `Enqueue` is safe.

<!-- snippet: examples/graceful-drain/main.go#idle -->
```go
c := memory.New(memory.WithClock(clk), memory.WithStopWhenIdle(time.Minute))
```

The program is [`examples/graceful-drain`](../../examples/graceful-drain).

Calls in the wrong order fail loudly rather than queueing work nobody will run:

<!-- snippet: examples/capabilities/main.go#sentinels -->
```go
c := memory.New()
if err := c.Register(wf); err != nil {
	return err
}
_, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping"})
fmt.Fprintln(out, "before Start:", errors.Is(err, workflow.ErrNotStarted))

if err := c.Start(ctx); err != nil {
	return err
}
fmt.Fprintln(out, "second Start:", errors.Is(c.Start(ctx), workflow.ErrAlreadyStarted))
fmt.Fprintln(out, "Register after Start:", errors.Is(c.Register(wf), workflow.ErrAlreadyStarted))

_, err = c.Enqueue(ctx, workflow.Job{Workflow: "pnig"})
fmt.Fprintln(out, "unknown workflow:", errors.Is(err, workflow.ErrNotFound))
_, err = c.Status(ctx, workflow.RunRef{RunID: "no-such-run"})
fmt.Fprintln(out, "unknown run:", errors.Is(err, workflow.ErrNotFound))
```

```text
before Start: true
second Start: true
Register after Start: true
unknown workflow: true
unknown run: true
```

## Validation

Specs are checked before anything runs, by functions every driver calls, so a spec means the same on every driver:

- `workflow.Validate(wf)`, called by every driver's `Register`: a name, at least one step, unique non-empty step names, exactly one of `Run`, `Wait` and `Sleep` per step, `After` naming existing steps with no cycle, and every policy within its range (retry counts and jitter, concurrency strategy, rate limits, misfire policy, `CronLocation` not `time.Local`).
- `workflow.ValidateSchedule(s)`, called by every driver's `Schedule`: a workflow, an expression, an explicit `Location` (use `time.UTC` for UTC), a well-formed misfire policy and no label under a [reserved prefix](labels.md#reserved-prefixes).

Every error wraps `workflow.ErrInvalid` and names the step or field. Cron expressions themselves are parsed by the driver's cron parser at `Register` (memory, SQL, Redis) so a typo fails at start-up, not at the first fire.

<!-- snippet: examples/capabilities/main.go#validate -->
```go
bad := workflow.Workflow{Name: "import", Steps: []workflow.Step{
	{Name: "load", After: []string{"parse"}, Run: func(ctx context.Context, in []byte) ([]byte, error) { return in, nil }},
}}
err := workflow.Validate(bad)
fmt.Fprintln(out, "Validate:", err)
fmt.Fprintln(out, "is ErrInvalid:", errors.Is(err, workflow.ErrInvalid))

err = memory.New().Register(bad) // every driver's Register validates
fmt.Fprintln(out, "Register:", errors.Is(err, workflow.ErrInvalid))

err = workflow.ValidateSchedule(workflow.Schedule{Workflow: "digest", Cron: "0 9 * * *"})
fmt.Fprintln(out, "ValidateSchedule:", err)
```

```text
Validate: workflow: invalid spec: workflow "import": step "load" is After unknown step "parse"
is ErrInvalid: true
Register: true
ValidateSchedule: workflow: invalid spec: schedule for "digest" has no Location; use time.UTC for UTC
```

Validation is about shape. Whether a driver can honour a valid spec is a separate check, against its [capabilities](capabilities.md).

## Try it

```sh
cd examples
go run ./first-workflow
go run ./capabilities
```

The first prints a workflow's retries, join and failure handler; the second prints the lines above among its output.

## Pitfalls

- `Start` returns at once. A `main` that returns after `Enqueue` exits before anything runs; block on a signal or on your server.
- Register through the client you will start. A client returned by `workflow.Use` or `middleware.WrapClient` wraps the driver; registering on the bare driver skips the plugin's step middleware.
- `Register` after `Start` is refused on every driver. Declare all workflows up front; use `Schedule` for triggers made at run time.
- `CronLocation: time.Local` is invalid. Name the zone (`time.LoadLocation`), or leave it nil for UTC.
- `ErrNotFound` from `Enqueue` means the workflow is not registered on this client, often because it was registered on another process's client.

## Related

- [Capabilities](capabilities.md#what-a-driver-declares) and [errors](errors.md#sentinel-errors)
- [Enqueue a job](../jobs/enqueue.md#enqueue-a-job), [cancel a run](../jobs/cancel-and-status.md#cancel-a-run), [schedules at run time](../schedules/cron.md#schedules-at-run-time)
- [Shut down cleanly](../operations/deploy.md#shut-down-cleanly)
- Examples: [quickstart](../../examples/quickstart/), [first-workflow](../../examples/first-workflow/), [capabilities](../../examples/capabilities/)
