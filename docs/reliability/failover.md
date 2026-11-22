# Failover

[`failover.New(primary, secondary)`](ds:block?id=failover-new-r9uzm3cq) joins two clients into one `workflow.Client`: jobs go to the primary, and when the primary is unavailable they go to the secondary, so the app keeps accepting work while, for example, the Hatchet engine is down and a local SQL store takes it. The app's code does not change: it talks to one client.

Only [`Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4) fails over. Runs are not moved between backends, and each backend runs the jobs it accepted.

## Before you start

- Two fresh, unstarted clients that the failover client will own: it registers the workflows on both ([`Register`](ds:block?id=failover-client-register-jugw7yen)) and starts both ([`Start`](ds:block?id=failover-client-start-dd5nj7n8)).
- An `IdempotencyKey` on every job, or the decision to run some jobs twice (see below).
- The runnable program for this page is [examples/failover](../../examples/failover/). Both backends there are the memory driver, the primary behind a switch that makes it refuse every enqueue with an error marked [`workflow.Unavailable`](ds:block?id=unavailable-wpp9cehg), standing in for an outage.
- [examples/failover-observer](../../examples/failover-observer/) is the program for the tuning and watching sections: its own classifier, the observer and its events, the cool-off, reset, edit and transient signals on either backend, and schedules on both.

## Fail over to a second backend

Build the client, then register and start through it, never through the backends:

<!-- snippet: examples/failover/main.go#new -->
```go
c := failover.New(primary, secondary,
	failover.WithCoolOff(2, time.Minute), // skip the primary for 1m after 2 failovers in a row
	failover.WithClock(clk),
	failover.WithObserver(func(ctx context.Context, ev failover.Event) {
		fmt.Println("  event:", ev.Kind, ev.Workflow)
	}))
if err := c.Register(email); err != nil { // registers on both backends
	return err
}
if err := c.Start(ctx); err != nil { // starts both
	return err
}
```

While the primary answers, every job goes there. When the primary's `Enqueue` fails with an error the classifier calls an outage, [`Client.Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4) sends the same job to the secondary and the caller gets the secondary's `RunRef`. When the secondary fails too, [`Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4)'s error names both: `failover: primary: ...; secondary: ...`.

## Decide what counts as an outage

A [`Classifier`](ds:block?id=failover-classifier-sb6segry) is `func(err error) bool`: true means "the primary is unavailable, use the secondary". It is consulted only while the caller's context is live ([`Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4) checks the context first); a cancelled or expired caller context never fails over.

- [`DefaultClassifier`](ds:block?id=failover-defaultclassifier-t7rpm2z9) fails over on an error a driver marked with `workflow.Unavailable`, and on every other error except the core sentinels in [`callerFault`](ds:block?id=failover-callerfault-9aj98stm) that describe the request or the client rather than the backend's health (`ErrInvalid`, `ErrUnsupported`, `ErrNotFound`, `ErrNotStarted`, `ErrAlreadyStarted`, `ErrDuplicate`, `ErrCancelled`, a context error) and except a `Permanent` error. It errs towards failing over on unknown errors, because an unknown error is most often a transport one and not every driver marks its outages yet. The `Unavailable` mark wins over the sentinels: a driver's own timeout talking to its engine wraps `context.DeadlineExceeded`, and marked `Unavailable` it fails over.
- [`StrictClassifier`](ds:block?id=failover-strictclassifier-7nn6bzz9) fails over only on an error marked `Unavailable` (`errors.Is(err, workflow.ErrUnavailable)`), never on an unknown one, so a bug a driver surfaces cannot send jobs to the secondary. Use it when every backend marks its outages.
- [`WithClassifier(c)`](ds:block?id=failover-withclassifier-ghwaub6c) takes your own, for a deployment that can tell another way (a gRPC `Unavailable` code, a refused connection).

A driver marks an outage with [`workflow.Unavailable(err)`](ds:block?id=unavailable-wpp9cehg); [`workflow.IsUnavailable(err)`](ds:block?id=isunavailable-emny24e5) tests for it, and the original error stays reachable through `errors.Is`. The example sends one job whose primary fails with an unmarked error, once per classifier: the default fails over, the strict one returns the error.

### Step 1: write the rule for your deployment

A [`Classifier`](ds:block?id=failover-classifier-sb6segry) is a plain function, so the usual one starts from `StrictClassifier` and adds what your backends return unmarked. This one also fails over on a refused connection, which an older driver returns as a bare `syscall.ECONNREFUSED`, and treats every other unknown error as a bug:

<!-- snippet: examples/failover-observer/main.go#classifier -->
```go
// outage is this deployment's rule for "the primary is unavailable": what a
// driver marked with workflow.Unavailable (StrictClassifier), plus a
// refused connection, which an older driver returns unmarked. Any other
// unknown error is treated as a bug and returned to the caller.
func outage(err error) bool {
	return failover.StrictClassifier(err) || errors.Is(err, syscall.ECONNREFUSED)
}
```

### Step 2: check it against the errors you know

Classifiers take an error and return a bool, so a table test (or a few lines in a program) shows what each one decides before any traffic depends on it:

<!-- snippet: examples/failover-observer/main.go#classify -->
```go
errs := []struct {
	name string
	err  error
}{
	{"marked Unavailable", workflow.Unavailable(errors.New("engine timeout"))},
	{"connection refused", fmt.Errorf("dial primary: %w", syscall.ECONNREFUSED)},
	{"unknown error", errors.New("unexpected reply")},
	{"invalid job", fmt.Errorf("bad payload: %w", workflow.ErrInvalid)},
	{"permanent", workflow.Permanent(workflow.Unavailable(errors.New("engine timeout")))},
}
for _, e := range errs {
	fmt.Fprintf(w, "  %-18s default %-5t strict %-5t ours %t\n", e.name,
		failover.DefaultClassifier(e.err), failover.StrictClassifier(e.err), outage(e.err))
}
```

```text
  marked Unavailable default true  strict true  ours true
  connection refused default true  strict false ours true
  unknown error      default true  strict false ours false
  invalid job        default false strict false ours false
  permanent          default false strict false ours false
```

A `Permanent` error never fails over, even when it also carries the `Unavailable` mark: both built-in classifiers ([`DefaultClassifier`](ds:block?id=failover-defaultclassifier-t7rpm2z9), [`StrictClassifier`](ds:block?id=failover-strictclassifier-7nn6bzz9)) check [`workflow.IsPermanent`](ds:block?id=ispermanent-pswuapn7) first.

### Step 3: hand it to the client

[`WithClassifier`](ds:block?id=failover-withclassifier-ghwaub6c) replaces the default; [`Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4) calls it only for a primary error while the caller's context is still live:

<!-- snippet: examples/failover-observer/main.go#new -->
```go
c := failover.New(primary, secondary,
	failover.WithClassifier(outage),
	failover.WithCoolOff(coolOffAfter, coolOffFor), // skip the primary for 1m after 2 failovers in a row
	failover.WithClock(clk),                        // the cool-off's time source
	failover.WithObserver(log.observe),
)
```

## Why a key is required

A failure can be ambiguous: the primary may have stored the job and then failed to answer, a timeout after the write. Failing over then runs the job on both backends. So [`Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4) refuses a job without an `IdempotencyKey` with [`failover.ErrKeyRequired`](ds:block?id=failover-errkeyrequired-5ywe568r), which also matches `workflow.ErrInvalid`:

<!-- snippet: examples/failover/main.go#key-required -->
```go
_, err := c.Enqueue(ctx, workflow.Job{Workflow: "email"})
fmt.Println("no key:", errors.Is(err, failover.ErrKeyRequired), errors.Is(err, workflow.ErrInvalid))
```

[`AllowWithoutKey()`](ds:block?id=failover-allowwithoutkey-4fzbzhje) lifts the requirement for apps that accept a job running twice. A key dedups only within one backend: the primary and the secondary do not see each other's keys, so the key protects a caller's retries against the same backend, and handlers must still be idempotent across backends. A workflow with `IdempotencyOff` ignores keys altogether.

## Run references and capabilities

Every `RunRef` the failover client returns names its backend: `RunID` and `ScheduledID` carry the prefix [`primary:`](ds:block?id=failover-primaryprefix-9n5byqpk) or [`secondary:`](ds:block?id=failover-secondaryprefix-qdx3xcxt) in front of the driver's own ID, which is kept unchanged after it ([`encodeRef`](ds:block?id=failover-encoderef-25vkwvhw)). `Status`, `Cancel` and the run-finished hook use the prefix to reach the backend holding the run ([`onRef`](ds:block?id=failover-client-onref-83za8ayr); [`TestRoutingByRef`](ds:block?id=failover-testroutingbyref-n3hkq2r5) cancels two runs whose driver IDs collide):

<!-- snippet: examples/failover/main.go#status -->
```go
st, err := waitTerminal(ctx, c, ref) // the ref names its backend, so Status asks the secondary
```

The same prefix is on every `RunEvent` ([`OnRunFinished`](ds:block?id=failover-client-onrunfinished-dnwajra9)) and on the `StepInfo` a handler reads ([`Register`](ds:block?id=failover-client-register-jugw7yen) wraps each handler); a driver's own middleware sees the driver's unprefixed ID. A ref without a prefix (one a driver handed out directly) is tried on the primary, then on the secondary when the primary does not know it or is unavailable.

[`Capabilities()`](ds:block?id=failover-client-capabilities-cb8n9pqm) is the intersection of both backends', so [`Start`](ds:block?id=failover-client-start-dd5nj7n8) refuses a workflow the secondary could not run rather than discovering it during an outage, and [`Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4) refuses a job one backend could not honour. The exceptions: the schedule capabilities ([`scheduleCaps`](ds:block?id=failover-schedulecaps-kg7jvm5x): `CapCron`, `CapCronSeconds`, `CapScheduleWhen`, `CapScheduleWindow`, `CapScheduleActive`, `CapSchedulePause`, `CapScheduleList`, `CapScheduleTrigger`, `CapScheduleHistory`, `CapScheduleKeep`, `CapTickEvents`, `CapTickTime`, `CapOverlapSkip`) are the primary's alone unless schedules are on both (see [Schedules](#schedules)); `CapClosure` and `CapWorkerScale` are always the primary's alone ([`primaryOnlyCaps`](ds:block?id=failover-primaryonlycaps-wycqgwrv); a closure has no idempotency key to retry it safely on the other backend, and the worker counts scaled are the primary's); and `CapRunFinished` holds only when both backends implement `workflow.RunNotifier` and both guarantee delivery. [`OnRunFinished`](ds:block?id=failover-client-onrunfinished-dnwajra9) registers the observer on both backends and needs both to implement the hook.

[`Signal`](ds:block?id=failover-client-signal-wfxbuzsq) goes to both backends, since a waiting run may sit on either. When one fails, the other has already taken the event and the error names the failing one, so a retried `Signal` reaches the backend that already took it a second time.

## Query, retry, cancel and pause runs

The failover client implements every optional run interface (`workflow.RunQuerier`, `RunRetrier`, `RunDeleter`, `BulkCanceller`, `Pauser`, `Drainer`), because runs of one workflow can sit on either backend. Each needs its capability on both backends, so it is in `Capabilities()` only then, and a call without it returns an `*workflow.UnsupportedError` naming the capability before touching either backend ([`need`](ds:block?id=failover-client-need-aases4hn)), never half done on one:

| Call | What the failover client does |
|---|---|
| [`Runs(ctx, q)`](ds:block?id=failover-client-runs-95ncxabn) | Asks both backends and merges their feeds into one, newest enqueue first (ties by `RunID` descending), with every `Ref` prefixed like the refs `Enqueue` returns. Pages hold up to the query's limit; the cursor records where each backend's feed stands, so paging returns every run once as long as each backend's own paging is stable. A cursor the client did not make is `workflow.ErrInvalid` ([`decodeCursor`](ds:block?id=failover-decodecursor-bmhynd64)). |
| [`Retry(ctx, ref)`](ds:block?id=failover-client-retry-n9n5z7v9) | Goes to the backend the ref names and returns the new ref with that backend's prefix. An unprefixed ref is tried on the primary, then the secondary, as for `Status`. |
| [`Delete(ctx, ref)`](ds:block?id=failover-client-delete-e2efcvzq) | Goes to the backend the ref names. Two backends can hand out the same driver ID; the prefix keeps a delete on one from touching the other's run. |
| [`CancelAll(ctx, q)`](ds:block?id=failover-client-cancelall-6rcdusm3) | Runs on both backends and returns the total. If one fails, the other's cancellations stand and the error names the failing backend. |
| [`Pause`](ds:block?id=failover-client-pause-pu9jm64b), `Resume` | Applied to both backends, primary first, since a paused workflow's runs may sit on either. When the secondary refuses, the primary's pause stands and the error says so. |
| [`Paused(ctx)`](ds:block?id=failover-client-paused-sgx6yvuy) | The targets paused on either backend, without repeats, so a pause applied to one backend only can be seen and resumed. |
| [`Drain(ctx)`](ds:block?id=failover-client-drain-w6unh5pn), [`Done()`](ds:block?id=failover-client-done-ad5wg59n) | Drains both backends at once; `Done` closes once both have stopped. |
| `Reschedule`, [`EditRun`](ds:block?id=failover-client-editrun-ybkr9r89), [`ResetRun`](ds:block?id=failover-client-resetrun-wc6ug2ny), `CancelStep` | Go to the backend the ref names and return refs with that backend's prefix. These carry no capability: a backend that does not implement the call refuses it with an `*workflow.UnsupportedError` naming that backend, and `workflow.Supports(c, op)` is true only when both backends do it ([`Supports`](ds:block?id=failover-client-supports-ngjgf3fg)). |
| [`Result(ctx, ref)`](ds:block?id=failover-client-result-uhpvxmyj) | Goes to the backend the ref names. It needs `CapResult` on both backends, like the calls above. |
| [`SignalWaiting(ctx, ev)`](ds:block?id=failover-client-signalwaiting-vr4r8t7j) | Delivers to the runs waiting now on both backends and returns how many it resumed in all. Both backends must implement `workflow.TransientSignaller`, else nothing is delivered; when one fails, the other's deliveries stand and the error names the failing one. |

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

A prefixed `Ref` in a `RunQuery` asks only the backend it names and returns zero or one run ([`Runs`](ds:block?id=failover-client-runs-95ncxabn)):

<!-- snippet: examples/failover-manage/main.go#one -->
```go
// A prefixed Ref asks only the backend it names, and returns zero or one run.
one, err := c.Runs(ctx, workflow.RunQuery{Ref: refs[2]})
if err != nil {
	return err
}
```

The program [failover-manage](../../examples/failover-manage/) puts runs on both backends, then lists, retries, looks up, deletes, cancels and pauses them through the failover client.

`failover/manage_test.go` ([`TestRetryAndDelete_RouteByPrefix`](ds:block?id=failover-testretryanddelete-routebyprefix-e9yct8z5) and its neighbours) runs each of these on two memory backends whose driver IDs collide, so a call that reached the wrong backend would act on the other run.

## Schedules

`Workflow.Cron` and [`Client.Schedule`](ds:block?id=failover-client-schedule-unuxqxtf) live on the primary only: a schedule on both backends would fire on both, and every tick would run twice. The cost is that no schedule fires while the primary is down. [`Schedule`](ds:block?id=failover-client-schedule-unuxqxtf) never fails over, because a schedule made on the secondary during an outage would fire beside the primary's copy once the app retries it there.

[`WithSchedulesOnBoth()`](ds:block?id=failover-withschedulesonboth-vp7d75qm) registers crons and schedules on both backends, so they keep firing during an outage, at the price of every tick firing on both while both are up. Use it only for scheduled workflows that tolerate running twice per tick; the schedule capabilities then need both backends ([`Capabilities`](ds:block?id=failover-client-capabilities-cb8n9pqm)).

<!-- snippet: examples/failover-observer/main.go#schedules-on-both -->
```go
// Every tick fires on both backends while both are up: only for
// scheduled work that tolerates running twice per tick.
c := failover.New(memory.New(), memory.New(), failover.WithSchedulesOnBoth())
if err := c.Register(digest); err != nil {
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
id, err := c.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "digest", Cron: "0 2 * * *", Location: time.UTC})
if err != nil {
	return err
}
infos, err := c.Schedules(ctx)
if err != nil {
	return err
}
```

[`Schedules`](ds:block?id=failover-client-schedules-ewhhbm8y) lists the schedule twice, once per backend (`primary:...` and `secondary:...`), and the ID `Schedule` returned starts with [`both:`](ds:block?id=failover-bothprefix-xez3gtmc): it holds both backends' IDs ([`encodeBothScheduleID`](ds:block?id=failover-encodebothscheduleid-gvpwf4ak)), so passing it to [`PauseSchedule`](ds:block?id=failover-client-onschedules-bfa8h3s3) or `Unschedule` reaches both copies.

Everything about schedules follows where they live. Without `WithSchedulesOnBoth`, the secondary's copy of a workflow has no `Cron`, `CronWindows` or `ScheduleWhen` ([`Register`](ds:block?id=failover-client-register-jugw7yen)), so a secondary that cannot evaluate them (Hatchet) is never asked to; the [schedule conditions](../schedules/conditions.md), [tick observers and schedule hooks](../schedules/hooks.md), the schedule list and schedule pause ([manage schedules](../schedules/manage.md)) are the primary's. [`OnTick`](ds:block?id=failover-client-ontick-8g3bpzkz) reports each tick with its `ScheduleID` and `Run` prefixed by the backend, and a fired run's `workflow.LabelSchedule` label carries the same prefixed ID ([`sideLabels`](ds:block?id=failover-sidelabels-y4ng37gx)) in `Runs`, `StepInfo` and run-finished events, so the three match and the label can be passed back to `PauseSchedule`, `TriggerSchedule` or a run query; a `LabelSchedule` filter in `Runs` and `CancelAll` is translated to each backend's own ID ([`sideQuery`](ds:block?id=failover-sidequery-dxj4p6nq)). `Schedules` lists each schedule with a prefixed ID, which `PauseSchedule` accepts; under `WithSchedulesOnBoth` a schedule appears twice, once per copy, and pausing one listed ID pauses that copy only, while the ID `Schedule` returned pauses both. A schedule ID that names only a backend holding no schedules is `workflow.ErrNotFound` ([`PauseSchedule`](ds:block?id=failover-client-onschedules-bfa8h3s3)). [`TriggerSchedule`](ds:block?id=failover-client-triggerschedule-dafkgtc7) goes to the backend the prefix names, and an ID `Schedule` returned under `WithSchedulesOnBoth` triggers the primary's copy only, so one trigger makes one run; without `WithSchedulesOnBoth` the overlap policy, trigger-now and last fire (`CapOverlapSkip`, `CapScheduleTrigger`, `CapScheduleHistory`) are the primary's ([`scheduleCaps`](ds:block?id=failover-schedulecaps-kg7jvm5x)). [`ForceComplete`](ds:block?id=failover-client-force-b5nft8hn) and `ForceFail` are routed by the run's prefix, and `CapForceRun`, `CapErrorHistory`, `CapEnqueueOnly` and `CapShutdownRequeue` are declared only when both backends have them ([`Capabilities`](ds:block?id=failover-client-capabilities-cb8n9pqm)).

<!-- snippet: examples/failover-schedules/main.go#schedule -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{Name: "hourly", Workflow: "report", Cron: "0 * * * *", Location: time.UTC})
if err != nil {
	return err
}
fmt.Println("Schedule returned an ID for:", side(string(id)))
infos, err := c.Schedules(ctx)
if err != nil {
	return err
}
for _, s := range infos { // one entry per copy, its ID prefixed by the backend
	fmt.Printf("  listed on %s\n", side(string(s.ID)))
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

During an outage every enqueue would first wait for the primary to fail. [`WithCoolOff(n, d)`](ds:block?id=failover-withcooloff-fjcjyt6q) skips the primary for `d` after `n` consecutive failovers and sends jobs straight to the secondary. After `d`, the next enqueue tries the primary again ([`tryPrimary`](ds:block?id=failover-client-tryprimary-xsdx3fs7)): one more failure starts a new cool-off, and a success ends the streak. `n` below 1 or `d` of zero or less disables it, which is the default ([`WithCoolOff`](ds:block?id=failover-withcooloff-fjcjyt6q)). The cool-off reads time from [`WithClock`](ds:block?id=failover-withclock-4b2e3nph) (the real clock by default), which is how the example ends it at once with a fake clock.

The client built in [Step 3](#step-3-hand-it-to-the-client) above sets `WithCoolOff(coolOffAfter, coolOffFor)` with `coolOffAfter = 2` and `coolOffFor = time.Minute`, and a fake clock in `WithClock`. In [failover-observer](../../examples/failover-observer/) the primary refuses two jobs in a row, which starts the cool-off; the third job goes to the secondary without trying the primary; the program then moves the clock by a minute and the primary, back up, takes the next job, which ends the cool-off. The streak counts failovers in a row ([`primaryFailed`](ds:block?id=failover-client-primaryfailed-jcs6ycqd)), not failures in a window: one success on the primary resets it.

## Watch failovers

[`WithObserver(fn)`](ds:block?id=failover-withobserver-r6smmcyd) reports what happened, for a log line, a metric or an alert; by default nothing is reported. Each [`Event`](ds:block?id=failover-event-3c2gmzub) has a `Kind`, the job's `Workflow`, the primary's `Err` for a failover, `Until` for a cool-off that started, and `At` from the client's clock. It never carries the job's payload or labels, which can hold personal data.

| `EventKind` | Value | When |
|---|---|---|
| [`EventFailedOver`](ds:block?id=failover-eventfailedover-ptyhkt89) | `failed_over` | The primary failed with an outage and the job went to the secondary |
| [`EventPrimarySkipped`](ds:block?id=failover-eventprimaryskipped-euuyyrdg) | `primary_skipped` | The primary was cooling off, so the job went straight to the secondary |
| [`EventCoolOffStarted`](ds:block?id=failover-eventcooloffstarted-9rzmaeb2) | `cool_off_started` | The failover streak reached `WithCoolOff`'s `n`; `Until` says when the primary is tried again |
| [`EventCoolOffEnded`](ds:block?id=failover-eventcooloffended-x9sd452m) | `cool_off_ended` | The primary took a job again after a cool-off |

The [`Observer`](ds:block?id=failover-observer-73etyu8p) runs synchronously on the [`Enqueue`](ds:block?id=failover-client-enqueue-w2azafu4) that caused the event: it must return quickly and must not call `Enqueue` on the same client. A secondary that fails too is reported to the caller by `Enqueue`'s error, not here.

An observer is a `failover.Observer`, `func(ctx context.Context, ev failover.Event)`; a method value works. This one logs each event with the fields that kind carries and counts the events per kind, as a metric would. `Enqueue` may run on many goroutines, so the counts are guarded:

<!-- snippet: examples/failover-observer/main.go#observer -->
```go
// observe logs every failover event and counts it, as a metric would. It
// returns quickly and never calls Enqueue on the failover client.
func (l *eventLog) observe(_ context.Context, ev failover.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.counts[ev.Kind]++
	line := fmt.Sprintf("  event %s workflow=%s at=%s", ev.Kind, ev.Workflow, ev.At.Format(timeOfDay))
	switch ev.Kind {
	case failover.EventFailedOver:
		line += fmt.Sprintf(" err=%q", ev.Err) // the primary's error
	case failover.EventCoolOffStarted:
		line += " until=" + ev.Until.Format(timeOfDay) // when the primary is tried again
	case failover.EventPrimarySkipped, failover.EventCoolOffEnded:
		// Kind, Workflow and At say it all.
	}
	fmt.Fprintln(l.w, line)
}
```

It is passed as `failover.WithObserver(log.observe)` in [Step 3](#step-3-hand-it-to-the-client). An outage of two jobs, one skipped job and the primary's return give two `failed_over`, one `cool_off_started`, one `primary_skipped` and one `cool_off_ended` (see [Try it](#try-it)). Alert on `cool_off_started`: it means the primary failed `n` times in a row, not once.

## Reset, edit and signal runs on either backend

`ResetRun`, `EditRun` and `SignalWaiting` work through the failover client like the other run calls in the table above. They carry no capability bit, so ask [`workflow.Supports(c, op)`](ds:block?id=supports-4xyjjwvz) with `workflow.OpResetRun`, `OpEditRun` or `OpSignalWaiting` before offering them (an admin button, a CLI command); the failover client answers true only when both backends do the operation ([`Client.Supports`](ds:block?id=failover-client-supports-ngjgf3fg)):

<!-- snippet: examples/failover-observer/main.go#supports -->
```go
for _, op := range []workflow.Op{workflow.OpResetRun, workflow.OpEditRun, workflow.OpSignalWaiting} {
	// True only when both backends do it, since a run may sit on either.
	fmt.Fprintf(w, "  supports %s: %t\n", op, workflow.Supports(c, op))
}
```

A reset ([`ResetRun`](ds:block?id=failover-client-resetrun-wc6ug2ny)) and an edit ([`EditRun`](ds:block?id=failover-client-editrun-ybkr9r89)) go to the backend the ref names, so a run that landed on the secondary during an outage is reset or edited there, and the ref that comes back keeps the `secondary:` prefix. A reset keeps the run's ID:

<!-- snippet: examples/failover-observer/main.go#reset -->
```go
fixed.Store(true)
again, err := c.ResetRun(ctx, failing, "render") // routed by the ref's "secondary:" prefix
if err != nil {
	return err
}
```

<!-- snippet: examples/failover-observer/main.go#edit -->
```go
payload, now := []byte(`"new@example.com"`), time.Now()
edited, err := c.EditRun(ctx, delayed, workflow.JobEdit{Payload: &payload, RunAt: &now})
if err != nil {
	return err // workflow.ErrWrongState once the run has started
}
```

[`SignalWaiting`](ds:block?id=failover-client-signalwaiting-vr4r8t7j) has no ref: it delivers to the runs waiting for the event now on both backends and returns how many it resumed in all:

<!-- snippet: examples/failover-observer/main.go#signal-waiting -->
```go
n, err := c.SignalWaiting(ctx, workflow.Event{Name: eventGo}) // both backends' waiting runs
if err != nil {
	return err
}
```

When one backend lacks the interface, `Supports` says false and the call is an `*workflow.UnsupportedError` (`errors.Is(err, workflow.ErrUnsupported)`). For [`SignalWaiting`](ds:block?id=failover-client-signalwaiting-vr4r8t7j) nothing is delivered at all, so the runs on the backend that could have taken the event are not resumed alone:

<!-- snippet: examples/failover-observer/main.go#supports-missing -->
```go
// A backend that hides the optional interfaces (here a bare
// workflow.Client) makes the failover client refuse them.
partial := failover.New(memory.New(), struct{ workflow.Client }{memory.New()})
fmt.Fprintf(w, "  with a bare secondary, supports %s: %t\n", workflow.OpSignalWaiting, workflow.Supports(partial, workflow.OpSignalWaiting))
_, err = partial.SignalWaiting(ctx, workflow.Event{Name: eventGo})
fmt.Fprintln(w, "  SignalWaiting refused:", errors.Is(err, workflow.ErrUnsupported))
```

The other clients that join backends route these calls the same way, by where the run lives:

| Client | `ResetRun`, `EditRun` | `SignalWaiting` | `Supports` |
|---|---|---|---|
| `failover` | The backend the ref's prefix names ([`ResetRun`](ds:block?id=failover-client-resetrun-wc6ug2ny)) | Both backends ([`SignalWaiting`](ds:block?id=failover-client-signalwaiting-vr4r8t7j)); when one fails, the other's deliveries stand and the error names the failing one | True when both backends do it |
| `regional` | The region the ref's prefix names ([`ResetRun`](ds:block?id=regional-client-resetrun-28bwdejv)) | The regions `Signal` would send the event to ([`SignalWaiting`](ds:block?id=regional-client-signalwaiting-uhckk3dw)) (every region, or the pick of `WithSignalRouter`); every one of them must implement `TransientSignaller`, else nothing is delivered | True when every region does it ([`Supports`](ds:block?id=regional-client-supports-uuvevscq)) |
| `cutover` | The backend the ref names; a run still on the old backend is reset there ([`ResetRun`](ds:block?id=cutover-client-resetrun-hj3x2j64)) | Both backends, as `failover` ([`SignalWaiting`](ds:block?id=cutover-client-signalwaiting-79gpg9qc)) | True when both backends do it ([`Supports`](ds:block?id=cutover-client-supports-k9xeak5n)) |
| `hybrid` | The jobs client, where every run lives (through `workflow.Forward`, embedded in [`hybrid.Client`](ds:block?id=hybrid-client-spwwj95u)) | The jobs client | The jobs client's answer |

The `newops_test.go` of `failover`, `regional`, `cutover` and `hybrid` ([`failover`](ds:block?id=failover-testresetandsignalflows-nts7g6ss), [`regional`](ds:block?id=regional-testresetandsignalflows-4xgew6t4), [`cutover`](ds:block?id=cutover-testresetandsignalflows-f438f95p), [`hybrid`](ds:block?id=hybrid-testresetandsignalflows-dks2u4vp)) each runs the shared `workflowtest.RunResetFlows` and `RunSignalFlows` suites through the wrapper, so a reset or a transient signal behaves as it does on the driver underneath.

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

The tuning and watching sections:

```sh
cd examples
go run ./failover-observer
```

```text
-- Decide what counts as an outage
  marked Unavailable default true  strict true  ours true
  connection refused default true  strict false ours true
  unknown error      default true  strict false ours false
  invalid job        default false strict false ours false
  permanent          default false strict false ours false
-- Watch failovers
approve a-1 -> primary
-- the primary goes down
  event failed_over workflow=report at=09:00:00 err="dial primary: connection refused"
report r-1 -> secondary
  event failed_over workflow=remind at=09:00:00 err="dial primary: connection refused"
  event cool_off_started workflow=remind at=09:00:00 until=09:01:00
remind m-1 -> secondary
  event primary_skipped workflow=approve at=09:00:00
approve a-2 -> secondary
-- the primary is back and the cool-off runs out
  event cool_off_ended workflow=approve at=09:01:00
approve a-3 -> primary
  count failed_over: 2
  count cool_off_started: 1
  count primary_skipped: 1
  count cool_off_ended: 1
-- Reset, edit and signal runs on either backend
report on the secondary is failed
  supports reset-run: true
  supports edit-run: true
  supports signal-waiting: true
reset in place: true succeeded
edited remind on the secondary sent to "new@example.com"
signal-waiting resumed 3 run(s)
  approve on the primary is succeeded
  approve on the secondary is succeeded
  approve on the primary is succeeded
  with a bare secondary, supports signal-waiting: false
  SignalWaiting refused: true
-- Schedules on both backends
schedule copies: primary, secondary
Schedule returned an ID for: both
```

## Pitfalls

- Register and start through the failover client only. `Register` after `Start` returns `workflow.ErrAlreadyStarted` ([`TestLifecycle`](ds:block?id=failover-testlifecycle-94pbdjes)); when the secondary fails to start, the primary is stopped again ([`Start`](ds:block?id=failover-client-start-dd5nj7n8)).
- Do not hand the backends to anything else: the failover client owns them ([`New`](ds:block?id=failover-new-r9uzm3cq)), and refs from a backend used directly have no prefix.
- The default classifier ([`DefaultClassifier`](ds:block?id=failover-defaultclassifier-t7rpm2z9)) fails over on unknown errors. If your primary can return errors that are app bugs, use `StrictClassifier` and make sure your drivers mark outages, or a bug will quietly route work to the secondary.
- A ref whose `RunID` and `ScheduledID` name different backends was not made by this client and is `workflow.ErrNotFound` ([`onRef`](ds:block?id=failover-client-onref-83za8ayr), [`TestUnprefixedRefFallsBackToSecondary`](ds:block?id=failover-testunprefixedreffallsbacktosecondary-ahw7vjee)).
- Schedules stop while the primary is down unless [`WithSchedulesOnBoth`](ds:block?id=failover-withschedulesonboth-vp7d75qm), and with it every tick fires twice while both are up.
- A run-management call needs its capability on both backends. With a secondary that cannot list or retry runs, `Runs` and `Retry` are refused ([`TestRefusesWhatOneBackendLacks`](ds:block?id=failover-testrefuseswhatonebackendlacks-2552bz8p)), not answered from the primary alone, because the answer would silently miss the runs taken during an outage.
- Without [`WithCoolOff`](ds:block?id=failover-withcooloff-fjcjyt6q), each enqueue during an outage pays the primary's full failure time before it fails over.
- The observer runs inside `Enqueue` ([`Observer`](ds:block?id=failover-observer-73etyu8p)). A slow observer (a network call to a metrics service) slows every enqueue that fails over; hand the event to a channel or a counter instead, and never call `Enqueue` on the same client from it.
- A [`Classifier`](ds:block?id=failover-classifier-sb6segry) that returns true for `workflow.ErrInvalid` or `ErrDuplicate` sends a job the primary rejected on its merits to the secondary, which may accept it. Start your own from `StrictClassifier` or `DefaultClassifier`, never from `func(error) bool { return true }`.
- [`SignalWaiting`](ds:block?id=failover-client-signalwaiting-vr4r8t7j) through a client whose backends do not both implement `TransientSignaller` delivers to neither; check `workflow.Supports(c, workflow.OpSignalWaiting)` first.

## Related

- [Errors](../reference/errors.md): `ErrUnavailable`, `Unavailable` and every sentinel the default classifier checks.
- [Capabilities](../reference/capabilities.md): what each driver declares.
- [Idempotency keys](../jobs/idempotency.md): what a key does within one backend.
- [Outbox](outbox.md): keep accepting jobs while an engine is down by relaying them later instead.
- [Regional routing](../operations/regional-routing.md): one backend per region, each job sent to its customer's region.
- Example: [examples/failover](../../examples/failover/).
- Example: [examples/failover-schedules](../../examples/failover-schedules/) (`WithSchedulesOnBoth`).
- Example: [examples/failover-manage](../../examples/failover-manage/) (runs on both backends: list, retry, delete, cancel, pause).
- Example: [examples/failover-observer](../../examples/failover-observer/) (a classifier of your own, the observer and its events, the cool-off, reset, edit and transient signals on either backend, schedules on both).
