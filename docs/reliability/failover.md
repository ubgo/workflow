# Failover

`failover.New(primary, secondary)` joins two clients into one `workflow.Client`: jobs go to the primary, and when the primary is unavailable they go to the secondary, so the app keeps accepting work while, for example, the Hatchet engine is down and a local SQL store takes it. The app's code does not change: it talks to one client.

Only `Enqueue` fails over. Runs are not moved between backends, and each backend runs the jobs it accepted.

## Before you start

- Two fresh, unstarted clients that the failover client will own: it registers the workflows on both and starts both.
- An `IdempotencyKey` on every job, or the decision to run some jobs twice (see below).
- The runnable program for this page is [examples/failover](../../examples/failover/). Both backends there are the memory driver, the primary behind a switch that makes it refuse every enqueue with an error marked `workflow.Unavailable`, standing in for an outage.

## Fail over to a second backend

Build the client, then register and start through it, never through the backends:

<!-- snippet: examples/failover/main.go#new -->
```go
c := failover.New(primary, secondary,
	failover.WithCoolOff(2, time.Minute), // skip the primary for 1m after 2 failovers in a row
	failover.WithClock(clk),
	failover.WithObserver(func(ctx context.Context, ev failover.Event) {
		fmt.Fprintln(out, "  event:", ev.Kind, ev.Workflow)
	}))
if err := c.Register(email); err != nil { // registers on both backends
	return err
}
if err := c.Start(ctx); err != nil { // starts both
	return err
}
```

While the primary answers, every job goes there. When the primary's `Enqueue` fails with an error the classifier calls an outage, the same job is sent to the secondary and the caller gets the secondary's `RunRef`. When the secondary fails too, the error names both: `failover: primary: ...; secondary: ...`.

## Decide what counts as an outage

A `Classifier` is `func(err error) bool`: true means "the primary is unavailable, use the secondary". It is consulted only while the caller's context is live; a cancelled or expired caller context never fails over.

- `DefaultClassifier` fails over on an error a driver marked with `workflow.Unavailable`, and on every other error except the core sentinels that describe the request or the client rather than the backend's health (`ErrInvalid`, `ErrUnsupported`, `ErrNotFound`, `ErrNotStarted`, `ErrAlreadyStarted`, `ErrDuplicate`, `ErrCancelled`, a context error) and except a `Permanent` error. It errs towards failing over on unknown errors, because an unknown error is most often a transport one and not every driver marks its outages yet. The `Unavailable` mark wins over the sentinels: a driver's own timeout talking to its engine wraps `context.DeadlineExceeded`, and marked `Unavailable` it fails over.
- `StrictClassifier` fails over only on an error marked `Unavailable` (`errors.Is(err, workflow.ErrUnavailable)`), never on an unknown one, so a bug a driver surfaces cannot send jobs to the secondary. Use it when every backend marks its outages.
- `WithClassifier(c)` takes your own, for a deployment that can tell another way (a gRPC `Unavailable` code, a refused connection).

A driver marks an outage with `workflow.Unavailable(err)`; `workflow.IsUnavailable(err)` tests for it, and the original error stays reachable through `errors.Is`. The example sends one job whose primary fails with an unmarked error, once per classifier: the default fails over, the strict one returns the error.

## Why a key is required

A failure can be ambiguous: the primary may have stored the job and then failed to answer, a timeout after the write. Failing over then runs the job on both backends. So a job without an `IdempotencyKey` is refused with `failover.ErrKeyRequired`, which also matches `workflow.ErrInvalid`:

<!-- snippet: examples/failover/main.go#key-required -->
```go
_, err := c.Enqueue(ctx, workflow.Job{Workflow: "email"})
fmt.Fprintln(out, "no key:", errors.Is(err, failover.ErrKeyRequired), errors.Is(err, workflow.ErrInvalid))
```

`AllowWithoutKey()` lifts the requirement for apps that accept a job running twice. A key dedups only within one backend: the primary and the secondary do not see each other's keys, so the key protects a caller's retries against the same backend, and handlers must still be idempotent across backends. A workflow with `IdempotencyOff` ignores keys altogether.

## Run references and capabilities

Every `RunRef` the failover client returns names its backend: `RunID` and `ScheduledID` carry the prefix `primary:` or `secondary:` in front of the driver's own ID, which is kept unchanged after it. `Status`, `Cancel` and the run-finished hook use the prefix to reach the backend holding the run:

<!-- snippet: examples/failover/main.go#status -->
```go
st, err := waitTerminal(ctx, c, ref) // the ref names its backend, so Status asks the secondary
```

The same prefix is on every `RunEvent` and on the `StepInfo` a handler reads; a driver's own middleware sees the driver's unprefixed ID. A ref without a prefix (one a driver handed out directly) is tried on the primary, then on the secondary when the primary does not know it or is unavailable.

`Capabilities()` is the intersection of both backends', so `Start` refuses a workflow the secondary could not run rather than discovering it during an outage, and `Enqueue` refuses a job one backend could not honour. Two exceptions: the schedule capabilities (`CapCron`, `CapScheduleWhen`, `CapScheduleWindow`, `CapSchedulePause`, `CapScheduleList`, `CapTickEvents`) are the primary's alone unless schedules are on both (see [Schedules](#schedules)), and `CapRunFinished` holds only when both backends implement `workflow.RunNotifier` and both guarantee delivery. `OnRunFinished` registers the observer on both backends and needs both to implement the hook.

`Signal` goes to both backends, since a waiting run may sit on either. When one fails, the other has already taken the event and the error names the failing one, so a retried `Signal` may reach that one twice.

## Query, retry, cancel and pause runs

The failover client implements every optional run interface (`workflow.RunQuerier`, `RunRetrier`, `RunDeleter`, `BulkCanceller`, `Pauser`, `Drainer`), because runs of one workflow can sit on either backend. Each needs its capability on both backends, so it is in `Capabilities()` only then, and a call without it returns an `*workflow.UnsupportedError` naming the capability before touching either backend, never half done on one:

| Call | What the failover client does |
|---|---|
| `Runs(ctx, q)` | Asks both backends and merges their feeds into one, newest enqueue first (ties by `RunID` descending), with every `Ref` prefixed like the refs `Enqueue` returns. Pages hold up to the query's limit; the cursor records where each backend's feed stands, so paging returns every run once as long as each backend's own paging is stable. A cursor the client did not make is `workflow.ErrInvalid`. |
| `Retry(ctx, ref)` | Goes to the backend the ref names and returns the new ref with that backend's prefix. An unprefixed ref is tried on the primary, then the secondary, as for `Status`. |
| `Delete(ctx, ref)` | Goes to the backend the ref names. Two backends can hand out the same driver ID; the prefix keeps a delete on one from touching the other's run. |
| `CancelAll(ctx, q)` | Runs on both backends and returns the total. If one fails, the other's cancellations stand and the error names the failing backend. |
| `Pause`, `Resume` | Applied to both backends, primary first, since a paused workflow's runs may sit on either. When the secondary refuses, the primary's pause stands and the error says so. |
| `Paused(ctx)` | The targets paused on either backend, without repeats, so a pause applied to one backend only can be seen and resumed. |
| `Drain(ctx)`, `Done()` | Drains both backends at once; `Done` closes once both have stopped. |

<!-- snippet: examples/failover-manage/main.go#retry -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "invoice", Statuses: []workflow.Status{workflow.StatusFailed}})
if err != nil {
	return err
}
retried := make([]workflow.RunRef, 0, len(page.Runs))
for _, r := range page.Runs {
	// r.Ref is "primary:..." or "secondary:...", so the retry reaches the backend holding the run.
	again, err := c.Retry(ctx, r.Ref)
	if err != nil {
		return err
	}
	retried = append(retried, again)
}
```

A prefixed `Ref` in a `RunQuery` asks only the backend it names and returns zero or one run:

<!-- snippet: examples/failover-manage/main.go#one -->
```go
// A prefixed Ref asks only the backend it names, and returns zero or one run.
one, err := c.Runs(ctx, workflow.RunQuery{Ref: refs[2]})
if err != nil {
	return err
}
```

The program [failover-manage](../../examples/failover-manage/) puts runs on both backends, then lists, retries, looks up, deletes, cancels and pauses them through the failover client.

`failover/manage_test.go` runs each of these on two memory backends whose driver IDs collide, so a call that reached the wrong backend would act on the other run.

## Schedules

`Workflow.Cron` and `Client.Schedule` live on the primary only: a schedule on both backends would fire on both, and every tick would run twice. The cost is that no schedule fires while the primary is down. `Schedule` never fails over, because a schedule made on the secondary during an outage would fire beside the primary's copy once the app retries it there.

`WithSchedulesOnBoth()` registers crons and schedules on both backends, so they keep firing during an outage, at the price of every tick firing on both while both are up. Use it only for scheduled workflows that tolerate running twice per tick; the schedule capabilities then need both backends.

Everything about schedules follows where they live. Without `WithSchedulesOnBoth`, the secondary's copy of a workflow has no `Cron`, `CronWindows` or `ScheduleWhen`, so a secondary that cannot evaluate them (Hatchet) is never asked to; the [schedule conditions](../schedules/conditions.md), [tick observers and schedule hooks](../schedules/hooks.md), the schedule list and schedule pause ([manage schedules](../schedules/manage.md)) are the primary's. `OnTick` reports each tick with its `ScheduleID` and `Run` prefixed by the backend, and a fired run's `workflow.LabelSchedule` label carries the same prefixed ID in `Runs`, `StepInfo` and run-finished events, so the three match and the label can be passed back to `PauseSchedule`, `TriggerSchedule` or a run query; a `LabelSchedule` filter in `Runs` and `CancelAll` is translated to each backend's own ID. `Schedules` lists each schedule with a prefixed ID, which `PauseSchedule` accepts; under `WithSchedulesOnBoth` a schedule appears twice, once per copy, and pausing one listed ID pauses that copy only, while the ID `Schedule` returned pauses both. A schedule ID that names only a backend holding no schedules is `workflow.ErrNotFound`. `TriggerSchedule` goes to the backend the prefix names, the primary unless `WithSchedulesOnBoth`, and the overlap policy, trigger-now and last fire (`CapOverlapSkip`, `CapScheduleTrigger`, `CapScheduleHistory`) are the primary's. `ForceComplete` and `ForceFail` are routed by the run's prefix, and `CapForceRun`, `CapErrorHistory`, `CapEnqueueOnly` and `CapShutdownRequeue` are declared only when both backends have them.

<!-- snippet: examples/failover-schedules/main.go#schedule -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{Name: "hourly", Workflow: "report", Cron: "0 * * * *", Location: time.UTC})
if err != nil {
	return err
}
fmt.Fprintln(out, "Schedule returned an ID for:", side(string(id)))
infos, err := c.Schedules(ctx)
if err != nil {
	return err
}
for _, s := range infos { // one entry per copy, its ID prefixed by the backend
	fmt.Fprintf(out, "  listed on %s\n", side(string(s.ID)))
}
```

<!-- snippet: examples/failover-schedules/main.go#pause -->
```go
// The ID Schedule returned pauses every copy; a listed ID pauses one.
if err := c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
	return err
}
```

The program [failover-schedules](../../examples/failover-schedules/) runs one hourly schedule both ways: on the primary alone it is listed once and fires once per tick; with `WithSchedulesOnBoth` it is listed once per backend, fires on both, and the ID `Schedule` returned pauses both copies.

## Cool-off

During an outage every enqueue would first wait for the primary to fail. `WithCoolOff(n, d)` skips the primary for `d` after `n` consecutive failovers and sends jobs straight to the secondary. After `d`, the next enqueue tries the primary again: one more failure starts a new cool-off, and a success ends the streak. `n` below 1 or `d` of zero or less disables it, which is the default. The cool-off reads time from `WithClock` (the real clock by default), which is how the example ends it at once with a fake clock.

## Watch failovers

`WithObserver(fn)` reports what happened, for a log line, a metric or an alert; by default nothing is reported. Each `Event` has a `Kind`, the job's `Workflow`, the primary's `Err` for a failover, `Until` for a cool-off that started, and `At` from the client's clock. It never carries the job's payload or labels, which can hold personal data.

| `EventKind` | Value | When |
|---|---|---|
| `EventFailedOver` | `failed_over` | The primary failed with an outage and the job went to the secondary |
| `EventPrimarySkipped` | `primary_skipped` | The primary was cooling off, so the job went straight to the secondary |
| `EventCoolOffStarted` | `cool_off_started` | The failover streak reached `WithCoolOff`'s `n`; `Until` says when the primary is tried again |
| `EventCoolOffEnded` | `cool_off_ended` | The primary took a job again after a cool-off |

The observer runs synchronously on the `Enqueue` that caused the event: it must return quickly and must not call `Enqueue` on the same client. A secondary that fails too is reported to the caller by `Enqueue`'s error, not here.

## Try it

```sh
cd examples
go run ./failover
```

```text
-- Fail over to a second backend
no key: true true
order-1 -> primary
-- the primary goes down
  event: failed_over email
order-2 -> secondary
  event: failed_over email
  event: cool_off_started email
order-3 -> secondary
  event: primary_skipped email
order-4 -> secondary
order-2 on the secondary: succeeded
-- the primary is back and the cool-off ends
  event: cool_off_ended email
order-5 -> primary
capabilities include Cron: true
-- Decide what counts as an outage
default classifier: failed over to secondary
strict classifier: returned "unexpected reply"
```

## Pitfalls

- Register and start through the failover client only. `Register` after `Start` returns `workflow.ErrAlreadyStarted`; when the secondary fails to start, the primary is stopped again.
- Do not hand the backends to anything else: the failover client owns them, and refs from a backend used directly have no prefix.
- The default classifier fails over on unknown errors. If your primary can return errors that are app bugs, use `StrictClassifier` and make sure your drivers mark outages, or a bug will quietly route work to the secondary.
- A ref whose `RunID` and `ScheduledID` name different backends was not made by this client and is `workflow.ErrNotFound`.
- Schedules stop while the primary is down unless `WithSchedulesOnBoth`, and with it every tick fires twice while both are up.
- A run-management call needs its capability on both backends. With a secondary that cannot list or retry runs, `Runs` and `Retry` are refused, not answered from the primary alone, because the answer would silently miss the runs taken during an outage.
- Without `WithCoolOff`, each enqueue during an outage pays the primary's full failure time before it fails over.

## Related

- [Errors](../reference/errors.md): `ErrUnavailable`, `Unavailable` and every sentinel the default classifier checks.
- [Capabilities](../reference/capabilities.md): what each driver declares.
- [Idempotency keys](../jobs/idempotency.md): what a key does within one backend.
- [Outbox](outbox.md): keep accepting jobs while an engine is down by relaying them later instead.
- Example: [examples/failover](../../examples/failover/).
- Example: [examples/failover-schedules](../../examples/failover-schedules/) (`WithSchedulesOnBoth`).
- Example: [examples/failover-manage](../../examples/failover-manage/) (runs on both backends: list, retry, delete, cancel, pause).
