# Manage runs

When a run fails at 3am, an operator needs to find it, see why, fix the cause and run it again; when a bad deploy floods a queue, they need to stop it and clear it. This page covers the operations the library offers for that: listing runs with filters and pages, re-running a failed run from the step that failed, forgetting a finished run, cancelling every waiting run at once, reading each attempt's error, forcing a run to an end, pausing a workflow, finding and moving the delayed runs due soon, and withdrawing one step of a running run.

## Before you start

- Each operation is an optional interface beside `workflow.Client`, declared by a capability: [`RunQuerier`](ds:block?id=runquerier-xkdf425r) (`CapRunQuery`), [`RunRetrier`](ds:block?id=runretrier-esdpff54) (`CapRetryRun`), [`RunDeleter`](ds:block?id=rundeleter-n6nnj22w) (`CapDeleteRun`), [`BulkCanceller`](ds:block?id=bulkcanceller-rfecxx2j) (`CapCancelAll`), [`RunForcer`](ds:block?id=runforcer-8jsm3gc9) (`CapForceRun`) and [`Pauser`](ds:block?id=pauser-5p4t8yg6) (`CapPauseWorkflow`, `CapPauseQueue`); error history is `CapErrorHistory`. The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-paritycaps-e5y5vqhk) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers have them all (the SQL driver's error history and force are in [`p4Caps`](ds:block?id=sql-p4caps-ns9ehh5e)); [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) has `RunQuerier`, `BulkCanceller` and `RunRetrier`, and `Pauser` for one workflow when `hatchet.Config.API` is set ([`capabilities`](ds:block?id=hatchet-client-capabilities-3ms5edtr)); check `Capabilities()` on others, and see [Pitfalls](#pitfalls).
- Every wrapper in the library (the client `workflow.Use` returns, middleware, the test doubles) forwards these interfaces through [`workflow.Forward`](ds:block?id=forward-e32a7p36), so a type assertion on a wrapped client works. A wrapper over a client that lacks one returns an `UnsupportedError` naming the capability.
- The example is [`examples/manage-runs`](../../examples/manage-runs). Run it with `go run ./manage-runs` from `examples/`.
- Filtering by due time needs `workflow.CapDueQuery`. Moving a delayed run ([`workflow.RunRescheduler`](ds:block?id=runrescheduler-rmgramx6)) and withdrawing one step ([`workflow.StepCanceller`](ds:block?id=stepcanceller-gp5urxfn)) are optional interfaces with no capability bit: check with [`workflow.Supports`](ds:block?id=supports-4xyjjwvz) (a bare type assertion succeeds on every wrapper), and a wrapper over a client without them returns an `UnsupportedError` from the call. The [memory](ds:block?id=memory-caps-yxkuq583), [Redis](ds:block?id=redis-caps-bfgdst9t) and [SQL](ds:block?id=sql-p5caps-wggh6esn) drivers have all three; [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) has `CapDueQuery` and [`Reschedule`](ds:block?id=hatchet-client-reschedule-3nfqbter) (for a delayed start due more than [`15 seconds`](ds:block?id=hatchet-dueguard-kj9nm3dg) away) but no `CancelStep`. Their example is [`examples/run-reschedule`](../../examples/run-reschedule), on the memory driver with a fake clock; run it with `go run ./run-reschedule`.

## Step 1: list runs

`Runs` takes a [`workflow.RunQuery`](ds:block?id=runquery-wv73z4g9) and returns one [`page`](ds:block?id=runpage-e988tsva), newest enqueue first. Every field you set must match ([`RunQuery.Match`](ds:block?id=runquery-match-rbndsdhq)): workflow, queue, statuses, labels (every pair exactly) and an enqueue time range `[From, To)`.

<!-- snippet: examples/manage-runs/main.go#list -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{
	Workflow: "fulfil-order",
	Statuses: []workflow.Status{workflow.StatusFailed},
	Labels:   map[string]string{"tenant": "acme"},
	Limit:    50, // one page; pass page.Next as Cursor for the next
})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Printf("failed: %s at step %q: %s\n", r.Ref.RunID, r.FailedStep, r.Error)
}
// page.Next is empty on the last page; pass it as Cursor for the next one.
```

Set `Ref` to the `RunRef` you hold to fetch one run: every field of it that is set must match, so the ref `Enqueue` returned for a delayed run still finds it after it started. Each [`workflow.RunInfo`](ds:block?id=runinfo-f6rpt8qe) carries the run's ref, workflow, queue, status, `Error` (the message of the failure or cancel) and `FailedStep`, its labels and payload, the enqueue, first-start and finish times, and the number of step attempts. `Limit` is the page size: zero means [`workflow.DefaultRunsLimit`](ds:block?id=defaultrunslimit-cemvqzky) (100) and more than [`workflow.MaxRunsLimit`](ds:block?id=maxrunslimit-ycr4zjnb) (1000) is refused ([`ValidateRunQuery`](ds:block?id=validaterunquery-bxy4ha24)). Pass `page.Next` back as `Cursor` for the next page; it is empty on the last. The cursor is a position, not an offset, so runs enqueued between two calls do not shift the next page.

## Search by label

Labels are how you find runs again: a tenant, an order number, a source. `RunQuery.Labels` matches values exactly; `RunQuery.LabelPrefixes` matches values that start with a prefix, and an empty prefix matches any run that has the key at all ([`RunQuery.Match`](ds:block?id=runquery-match-rbndsdhq)). Both may be combined with each other and with every other filter, and every pair must match. They work in `Runs` and `CancelAll`. The program is [`examples/label-search`](../../examples/label-search):

<!-- snippet: examples/label-search/main.go#search -->
```go
acmeA, err := c.Runs(ctx, workflow.RunQuery{
	Labels:        map[string]string{"tenant": "acme"}, // exact value
	LabelPrefixes: map[string]string{"order": "A-"},    // value starts with A-
})
if err != nil {
	return err
}
withCoupon, err := c.Runs(ctx, workflow.RunQuery{
	LabelPrefixes: map[string]string{"coupon": ""}, // the key is present, any value
})
if err != nil {
	return err
}
```

```text
acme, order A-*: A-1001 A-1002
has a coupon: A-1002 A-1003
```

A prefix filter needs `workflow.CapLabelPrefix`, which the [in-memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers declare. A driver without it refuses the query with an `UnsupportedError` for the field `RunQuery.LabelPrefixes` ([`workflow.CheckRunQuery`](ds:block?id=checkrunquery-ah3qd2ar) makes that decision) rather than returning a wider page. [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) refuses it, because the engine matches metadata by exact value only:

<!-- snippet: examples/label-search/main.go#refused -->
```go
q := workflow.RunQuery{LabelPrefixes: map[string]string{"order": "A-"}}
err = workflow.CheckRunQuery(hatchet.New(hatchet.Config{}).Capabilities(), q)
var unsupported *workflow.UnsupportedError
if errors.As(err, &unsupported) {
	fmt.Printf("hatchet refuses: field %s, missing %s\n", unsupported.Field, unsupported.Missing)
}
```

```text
hatchet refuses: field RunQuery.LabelPrefixes, missing LabelPrefix
```

### The label index on the SQL driver

Migration `005_run_labels` adds the table `<prefix>run_labels`, one row per label of a run (each key and value cut to [`191`](ds:block?id=sql-labelindexwidth-xdtnr3dj) characters), indexed on key, value and run. Enqueue writes the rows in the run's own transaction, and delete and retention remove them, so a label search reads the index instead of decoding every run's labels. Exact values are still checked in Go ([`labelFilter`](ds:block?id=sql-labelfilter-p5k8cs74) only narrows to candidates), so a long value that was cut never matches wrongly. A prefix is matched with `LIKE` on the indexed value, with `%`, `_` and the escape character escaped ([`likePrefix`](ds:block?id=sql-likeprefix-h6zscdhy)), so a prefix containing them is taken literally.

Runs written before 005, or by an older client during a rolling deploy, have no index rows until the backfill runs. [`Migrate`](ds:block?id=sql-client-migrate-vdt8uy3d) runs it after the scripts. If you apply the migrations with your own tool (`MigrationScripts`), call [`IndexLabels`](ds:block?id=sql-client-indexlabels-ccg4tdkz) once after 005 and once more after a rolling deploy in which an older client kept enqueuing; until then a label filter does not see those runs:

<!-- snippet: examples/label-search/main.go#backfill -->
```go
n, err := c.IndexLabels(ctx) // after MigrationScripts applied by your own tool
if err != nil {
	return err
}
again, err := c.IndexLabels(ctx) // safe to repeat: nothing past the watermark
if err != nil {
	return err
}
```

```text
IndexLabels: 4 runs, then 0
```

It works in chunks under a watermark kept in the locks row [`labels:indexed`](ds:block?id=sql-labelslockname-8rh9sh5m), rewriting the rows of each run past it, so it is safe to run twice and from several processes at once ([`IndexLabels`](ds:block?id=sql-client-indexlabels-ccg4tdkz)). The first call above rewrote the rows of the four runs enqueued after `Migrate`'s own pass; the second found nothing new.

## Search inside payloads

A customer writes in about order `o-1`; the runs that handled it carry the order in their payload, not in a label. [`RunQuery.Payload`](ds:block?id=runquery-wv73z4g9) matches runs whose payload is a JSON object holding every field with the given value:

<!-- snippet: examples/run-control/main.go#search -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{
	Workflow: "fulfil",
	Payload:  map[string]string{"order.id": "o-1", "rush": "true"}, // needs CapPayloadSearch
})
if err != nil {
	return err
}
```

A key is a field name or a dot path into nested objects, each segment a plain name. A value matches the field's text: a string unquoted, a number as written in the payload (`12.50` does not match `12.5`), `true`, `false` or `null`. A payload that is not a JSON object, or lacks the field, does not match. It needs `workflow.CapPayloadSearch` ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-paritycaps-e5y5vqhk), [Redis](ds:block?id=redis-caps-bfgdst9t)); [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z), which filters metadata only, refuses the query with an `UnsupportedError` ([`CheckRunQuery`](ds:block?id=checkrunquery-ah3qd2ar)). `CancelAll` refuses a payload filter ([`ValidateCancelQuery`](ds:block?id=validatecancelquery-ewgpn2hh)); list the runs and cancel each.

The drivers match payloads after their own filters, reading on until the page is full, so narrow the query by workflow, status or time when the table is large: a payload search scans what the other filters leave. A payload sealed by a codec or `Encrypt` cannot be searched; put what you search on into a label instead.

## Step 2: re-run a failed run from the step that failed

[`Retry(ref)`](ds:block?id=runretrier-esdpff54) re-opens a failed run. The steps that succeeded keep their outputs and do not run again; the step that failed, and any branch its failure cancelled, start again with fresh attempt counts. A charge that already went through is not charged twice because shipping failed.

An app holds a `workflow.Client`, not a driver type, so it reaches `Retry` through `workflow.RunRetrier`. Check the capability as well as the assertion: every wrapper in the library has the method and forwards it ([`workflow.Forward`](ds:block?id=forward-e32a7p36)), so the assertion alone succeeds on a wrapper whose inner client cannot retry, and the call then fails with an `UnsupportedError`. `Capabilities()` tells you up front.

<!-- snippet: examples/manage-runs/main.go#retry -->
```go
retrier, ok := cl.(workflow.RunRetrier)
if !ok || !cl.Capabilities().Has(workflow.CapRetryRun) {
	return errors.New("this client cannot retry runs")
}
warehouseDown.Store(false) // the outage is over
ref, err := retrier.Retry(ctx, failed)
if err != nil {
	return err
}
```

```text
retried run-2: succeeded; charge ran 1 time(s), ship 2
```

The run keeps its `RunRef` and goes back to `queued`. When it ends again, run-finished observers see a second event for it, which they already tolerate (delivery is at least once). Its idempotency key is taken again; if another unfinished run took it meanwhile, [`Retry`](ds:block?id=runretrier-esdpff54) returns `workflow.ErrDuplicate`. Only a `failed` run can be retried; any other status returns `workflow.ErrWrongState`:

<!-- snippet: examples/manage-runs/main.go#retry-again -->
```go
_, err = retrier.Retry(ctx, ref) // it succeeded: only a failed run re-opens
fmt.Println("retry a succeeded run: ErrWrongState", errors.Is(err, workflow.ErrWrongState))
```

```text
retry a succeeded run: ErrWrongState true
```

## Re-run a failed run from an earlier step

`Retry` always resumes at the step that failed. When the cause was upstream (a transform step that succeeded with bad output, fixed and deployed since; a source corrected by hand), re-run from that step instead with [`workflow.RunResetter`](ds:block?id=runresetter-pe26r9jr):

<!-- snippet: examples/manage-runs/main.go#reset -->
```go
if !workflow.Supports(cl, workflow.OpResetRun) { // no capability bit: ask Supports
	return errors.New("this client cannot reset runs")
}
resetter := cl.(workflow.RunResetter)
fmt.Println("steps that run again:", workflow.StepsFrom(order, "charge"))
warehouseDown.Store(false)
again, err := resetter.ResetRun(ctx, broken, "charge") // not only "ship", the step that failed
if err != nil {
	return err // ErrWrongState unless the run failed; ErrInvalid for an unknown step
}
```

```text
steps that run again: [charge ship]
reset run-16 from charge: succeeded, same ref true; charge ran 1 more time(s)
```

[`ResetRun(ref, step)`](ds:block?id=runresetter-pe26r9jr) drops the outputs of `step` and of every step downstream of it ([`workflow.StepsFrom(wf, step)`](ds:block?id=stepsfrom-4jngfaw7) lists them), keeps the outputs of the other steps that succeeded, and re-opens the run exactly as `Retry` does: the same `RunRef`, back to `queued`, fresh attempt counts for every step that runs again, its idempotency key taken again. A wait step that runs again waits for a new event. Only a `failed` run can be reset (`workflow.ErrWrongState` otherwise), and a step the workflow does not have returns `workflow.ErrInvalid`. The memory, SQL ([`ResetRun`](ds:block?id=sql-client-resetrun-prngumah)) and Redis ([`ResetRun`](ds:block?id=redis-client-resetrun-ax2tev3t)) drivers implement it; the SQL and Redis drivers need the workflow registered on the client that calls it, to know which steps are downstream ([`ResetRunWith`](ds:block?id=sql-client-resetrunwith-hrw54cxc) on SQL, [`retryFrom`](ds:block?id=redis-client-retryfrom-8hgvpxc8) on Redis, an error matching `ErrInvalid` otherwise). `workflow.Use` forwards it, and a wrapper whose inner client lacks it refuses with an `UnsupportedError`.

## Reset a run that succeeded

A run can succeed with a wrong result: a transform with a bug that did not fail. Once the fix is deployed, `ResetRun` refuses the run (only a failed run can be reset), so ask for it explicitly with [`workflow.RunResetterWith`](ds:block?id=runresetterwith-792722gn) and [`ResetOptions.Succeeded`](ds:block?id=resetoptions-fgtq654p). The program is [`examples/reset-fork`](../../examples/reset-fork):

<!-- snippet: examples/reset-fork/main.go#reset-succeeded -->
```go
fixed.Store(true)
rs := c.(workflow.RunResetterWith)
again, err := rs.ResetRunWith(ctx, ref, "transform", workflow.ResetOptions{Succeeded: true})
if err != nil {
	return err
}
```

The run is reset in place, exactly as `ResetRun` resets a failed one: the same `RunRef`, `step` and every step downstream of it run again, the steps before keep their outputs, the stored result is cleared and replaced by the next end. Without `Succeeded`, `ResetRunWith` is `ResetRun`. What happens around the run, so nothing runs twice by accident:

- The run gets the label [`workflow.LabelRedo`](ds:block?id=labelredo-7f5kcp9k) (`ubgo.workflow/redo`), so observers and operators can tell a redo from a first run, and a label query finds it.
- Its chain (`Job.Chain`) was handed on when it succeeded and is not handed on again; the jobs it started are runs of their own, reset those if they must run again. On the SQL driver, a run whose chain still waits for a client that registers the next workflow is refused with `workflow.ErrWrongState` ([`markRedo`](ds:block?id=sql-client-markredo-yt3wcuhc)); the Redis driver refuses a run that still holds its chain because the handoff failed the same way ([`ResetRunWith`](ds:block?id=redis-client-resetrunwith-wmvvxtaj)).
- A step that runs again does all its work again: a child it spawns is a new run (`Spawn` gets back a child that is still unfinished, since that child holds its key). Children of the steps that are kept are untouched.
- Run-finished observers see the new end, as after `Retry`. The batch package ignores a repeated end of a job; the `Then` plugin skips runs carrying `LabelRedo` unless [`workflow.ThenAfterRedo()`](ds:block?id=thenafterredo-zh59sk4a) is passed ([then](../workflows/then.md)).

## Fork a run into a new run

[`workflow.RunForker`](ds:block?id=runforker-9cpe2f7f) leaves the original as it is and starts a new run from a step, with the original's journal up to there: the outputs of every step the original succeeded in except `step` and the steps downstream of it.

<!-- snippet: examples/reset-fork/main.go#fork -->
```go
fk := c.(workflow.RunForker)
fork, err := fk.ForkRun(ctx, ref, "publish", workflow.ForkOptions{Labels: map[string]string{"ticket": "OPS-7"}})
if err != nil {
	return err
}
```

The fork has a new RunID and the original's workflow, payload, tenant, queue, priority, job retry policy and workflow version ([`RunForker`](ds:block?id=runforker-9cpe2f7f)). Only a finished run (succeeded, failed, cancelled or discarded) can be forked: a step still running in the original could otherwise run in both (`workflow.ErrWrongState`). The fork:

- never carries the original's chain (it would run once for the fork and again when the original is retried) or its idempotency key; [`ForkOptions.IdempotencyKey`](ds:block?id=forkoptions-6g2ac7wq) gives it one of its own, and a key held by an unfinished run returns that run, as `Enqueue` does;
- keeps the original's labels except the membership labels that would make it count as something it is not (batch membership, parent run and step, schedule and tick, trigger, partition, object key, `Then` and continue-as-new lineage, backfill key; [`workflow.ForkLabels`](ds:block?id=forklabels-479w9npr) is the list), adds `ForkOptions.Labels` (app labels only), `workflow.LabelForkOf` (the original's RunID), `workflow.LabelForkStep` and, when the original had succeeded or was itself a redo ([`ForkRedo`](ds:block?id=forkredo-k36kx3xp)), `LabelRedo`;
- spawns new children from the steps it runs (its RunID is new), and its wait steps wait for events sent after the fork.

There is no payload override: the kept outputs were made from the original payload, and a payload set by an operator would bypass the enqueue middleware (codec, encryption). A new input is a new `Enqueue`.

The memory ([`ForkRun`](ds:block?id=memory-client-forkrun-4g3qxuas)), SQL and Redis drivers implement both; the SQL and Redis drivers need the workflow registered on the calling client (the SQL driver at the original's workflow version: [`ForkRun`](ds:block?id=sql-client-forkrun-dax2n4ug) on SQL, [`ForkRun`](ds:block?id=redis-client-forkrun-bvcdjaks) on Redis). `workflow.Use`, `failover`, `regional`, `cutover` and `hybrid` forward them (a fork lives beside its original, on the backend or region its ref names), and [`workflow.Supports(c, workflow.OpResetSucceeded)`](ds:block?id=supports-4xyjjwvz) and `workflow.OpForkRun` say whether a client really does them. The admin JSON API ([HTTP API](../admin/http-api.md)) and the `workflow` command ([CLI](../admin/cli.md)) expose both.

## Step 3: forget a finished run

`Delete(ref)` forgets a terminal run: afterwards `Status` returns `workflow.ErrNotFound` and `Runs` no longer lists it. An unfinished run returns `ErrWrongState`; cancel it first. It lives on [`workflow.RunDeleter`](ds:block?id=rundeleter-n6nnj22w), gated by `workflow.CapDeleteRun` ([Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) has no run delete, so it lacks both):

<!-- snippet: examples/manage-runs/main.go#delete -->
```go
deleter, ok := cl.(workflow.RunDeleter)
if !ok || !cl.Capabilities().Has(workflow.CapDeleteRun) {
	return errors.New("this client cannot delete runs")
}
err = deleter.Delete(ctx, ref)
_, statusErr := cl.Status(ctx, ref)
fmt.Println("deleted:", err == nil, "then Status is ErrNotFound:", errors.Is(statusErr, workflow.ErrNotFound))
```

```text
deleted: true then Status is ErrNotFound: true
```

To forget many, page through `Runs` and delete each, or use the SQL driver's retention, which prunes finished runs by age.

## Step 4: cancel every waiting run

`CancelAll(query)` cancels every run that matches and returns how many. With no `Statuses` it takes only runs not yet started, `scheduled` and `queued` ([`CancelStatuses`](ds:block?id=cancelstatuses-8fnq9hyv)); running and waiting runs are cancelled only when you list their status. A terminal status, a `Limit` or a `Cursor` is refused, since a bulk cancel covers every match: [`workflow.ValidateCancelQuery`](ds:block?id=validatecancelquery-ewgpn2hh) is that check, which every driver runs, and you can run it first to reject a bad filter from an operator form before calling the driver. Each run is cancelled exactly as `Cancel` would: its key released, its run-finished event delivered. The interface is [`workflow.BulkCanceller`](ds:block?id=bulkcanceller-rfecxx2j), gated by `workflow.CapCancelAll`:

<!-- snippet: examples/manage-runs/main.go#cancel-all -->
```go
bulk, ok := cl.(workflow.BulkCanceller)
if !ok || !cl.Capabilities().Has(workflow.CapCancelAll) {
	return errors.New("this client cannot cancel in bulk")
}
q := workflow.RunQuery{Workflow: "fulfil-order"} // no Statuses: runs not yet started
if err := workflow.ValidateCancelQuery(q); err != nil {
	return err // a terminal status, a Limit or a Cursor
}
n, err := bulk.CancelAll(ctx, q)
if err != nil {
	return err
}
fmt.Println("cancelled waiting runs:", n)
```

```text
cancelled waiting runs: 2
```

## Error history

On a driver with `CapErrorHistory` ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-p4caps-ns9ehh5e), [Redis](ds:block?id=redis-caps-bfgdst9t)), `RunInfo.Errors` keeps every failed attempt of a run, oldest first: the step, the attempt number, the error message, when it failed and, when the error carries one, a stack trace ([`workflow.AttemptError`](ds:block?id=attempterror-udfhvqdf)). It records retried and final failures alike, so "it failed three times with 451 and then with something else" is visible without logs. The program is [`examples/run-repair`](../../examples/run-repair):

<!-- snippet: examples/run-repair/main.go#client -->
```go
c := memory.New(
	memory.WithErrorHistory(10), // keep the newest 10 failed attempts per run; 0 turns it off
	memory.WithMiddleware(middleware.Recover(slog.New(slog.DiscardHandler))),
)
```

<!-- snippet: examples/run-repair/main.go#history -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
if err != nil {
	return err
}
for _, e := range page.Runs[0].Errors { // oldest first
	fmt.Printf("  attempt %d of %q: %s (stack: %t)\n", e.Attempt, e.Step, e.Error, e.Stack != "")
}
```

```text
  attempt 1 of "send": smtp 451: try again later (stack: false)
  attempt 2 of "send": smtp 451: try again later (stack: false)
  attempt 3 of "send": workflow/middleware: step panicked: nil template (stack: true)
```

The history is bounded per run: the driver option ([`memory.WithErrorHistory(n)`](ds:block?id=memory-witherrorhistory-rte3qjyb), and the same name on the [`SQL`](ds:block?id=sql-witherrorhistory-fzd4btky) and [`Redis`](ds:block?id=redis-witherrorhistory-h3km7hu2) drivers) keeps the newest `n` entries, [`workflow.DefaultErrorHistory`](ds:block?id=defaulterrorhistory-cexsqn9u) (20) when unset; `n <= 0` turns it off. A stack is taken from any error that implements [`workflow.StackTracer`](ds:block?id=stacktracer-jaqqtjr2) (`StackTrace() string`), cut to [`workflow.MaxErrorStack`](ds:block?id=maxerrorstack-99dw4vyj) (8 KiB) by [`StackOf`](ds:block?id=stackof-hqj4syzc); the error of a panic caught by `middleware.Recover` implements it, and your own error types can too. An attempt put back by a clean shutdown is not a failure and is not recorded ([Deploy workers](../operations/deploy.md#shut-down-cleanly)). A driver that keeps history elsewhere builds entries with [`workflow.NewAttemptError`](ds:block?id=newattempterror-pbsc4hxw) and bounds them with [`workflow.AppendAttemptError`](ds:block?id=appendattempterror-zp4rg4cs).

## Force a run to an end

Sometimes a step cannot finish on its own: the email was sent by hand, or a feed is down for days and the run should stop now. A client that implements [`workflow.RunForcer`](ds:block?id=runforcer-8jsm3gc9) (`CapForceRun`) ends a run from outside.

`ForceComplete(ref)` records a run as succeeded. It accepts an unfinished run, or a failed one (accepting the failure as done; [`CanForceComplete`](ds:block?id=canforcecomplete-ttxafdxp)). An attempt in progress has its context cancelled with the cause [`workflow.ErrForced`](ds:block?id=errforced-2cdhp8xs) and its result is ignored; remaining steps never run. The run is then treated as a success: its idempotency key is freed, its chain continues, and run-finished observers see `succeeded` (a second event for a run that had failed, which they already tolerate).

<!-- snippet: examples/run-repair/main.go#complete -->
```go
// The email went out by hand: record the run as done.
if err := c.ForceComplete(ctx, ref); err != nil {
	return err
}
```

`ForceFail(ref, reason)` records an unfinished run as failed, with an error that matches `workflow.ErrForced` and ends with your reason ([`ForcedFailure`](ds:block?id=forcedfailure-c4nm5c52)). No retry is made and `OnFailure` does not run, because the operator decided, not the workflow; `Retry` can still re-run it later.

<!-- snippet: examples/run-repair/main.go#fail -->
```go
err = c.ForceFail(ctx, stuck, "feed provider is down until Monday")
if err != nil {
	return err
}
```

```text
  import-feed: failed, "workflow: run ended by an operator: feed provider is down until Monday", OnFailure ran 0 times
```

Any other state returns `workflow.ErrWrongState` (a succeeded or cancelled run, or a failed run for `ForceFail`), and an unknown ref `workflow.ErrNotFound`. [`workflow.CanForceComplete`](ds:block?id=canforcecomplete-ttxafdxp) and [`workflow.CanForceFail`](ds:block?id=canforcefail-4hmwjp3r) are the rules every driver applies. To move a failed run to another worker, retry it: any worker may pick the retried run up.

## Pause and resume

[`Pause`](ds:block?id=pauser-5p4t8yg6) holds runs without cancelling them. The target is one workflow (`workflow.PauseWorkflow`), one queue (`workflow.PauseQueue`) or everything (`workflow.PauseAll`); [`CheckPause`](ds:block?id=checkpause-9fyvd7wv) refuses a target the driver lacks the capability for. While a target is paused no step attempt of a run it holds is claimed: new runs and the next step of a running run alike stay `queued`. Attempts already running finish, enqueues are accepted, and timers (delays, sleeps, wait deadlines, backoffs) keep running.

<!-- snippet: examples/manage-runs/main.go#pause -->
```go
if err := c.Pause(ctx, workflow.PauseWorkflow("fulfil-order")); err != nil {
	return err
}
held, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil-order", Payload: []byte(`"o-4"`)})
if err != nil {
	return err
}
st, err := c.Status(ctx, held)
if err != nil {
	return err
}
fmt.Println("while paused:", st)
if err := c.Resume(ctx, workflow.PauseWorkflow("fulfil-order")); err != nil {
	return err
}
```

[`Resume`](ds:block?id=pauser-5p4t8yg6) lifts exactly the target given, except [`Resume(workflow.PauseAll())`](ds:block?id=pauseall-ntt6srtz), which lifts every pause. `Paused` lists the pauses in force. On the SQL driver a pause is stored, so every node obeys it and it survives a restart.

## Suspend one run

A pause holds a whole workflow or queue. To hold one run (an import that looks wrong while you check its source), add the [`workflow.Suspension`](ds:block?id=suspension-tvpj5mcy) plugin and mark the run:

<!-- snippet: examples/run-control/main.go#suspend -->
```go
journal := workflow.NewMemoryJournal() // a durable JournalStore makes the flag fleet-wide
// The second argument of Suspension is how often a parked step looks
// at its flag again.
c, err := workflow.Use(memory.New(), workflow.Journal(journal), workflow.Suspension(journal, time.Millisecond))
if err != nil {
	return err
}
```

<!-- snippet: examples/run-control/main.go#suspend-call -->
```go
if err := workflow.SuspendRun(ctx, journal, ref); err != nil { // holds it at its next step
	return err
}
```

A suspended run parks at the start of its next step: the step snoozes for the plugin's poll ([`DefaultSuspendPoll`](ds:block?id=defaultsuspendpoll-z8mrjrqz), 5 seconds, when zero) without running its handler, holding no worker and spending no attempt, and looks again after each snooze; steps already running finish. [`ResumeRun`](ds:block?id=resumerun-td25p96u) clears the flag and the parked step runs at its next look. [`RunSuspended`](ds:block?id=runsuspended-wjxk9sqy) reads the flag. It needs `CapSnooze` ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-p5caps-wggh6esn), [Redis](ds:block?id=redis-caps-bfgdst9t)). A run that has not started yet has no RunID to key the flag by ([`SuspendRun`](ds:block?id=suspendrun-6dhpewn2) refuses it): move it with `Reschedule` instead.

## Detours: hold, skip, redo

An operator can take one run off its planned path without changing the workflow, by combining the operations on this page:

| Detour | How |
|---|---|
| Hold the run between steps, then go on | `SuspendRun`, later `ResumeRun` ([Suspend one run](#suspend-one-run)) |
| Redo from an earlier step after fixing the cause | `ResetRun(ref, step)` on the failed run ([Re-run a failed run from an earlier step](#re-run-a-failed-run-from-an-earlier-step)) |
| Redo only the step that failed | `Retry(ref)` |
| Redo from a step a run that succeeded with a wrong result | `ResetRunWith(ref, step, ResetOptions{Succeeded: true})` ([Reset a run that succeeded](#reset-a-run-that-succeeded)) |
| Try again from a step without touching the original | `ForkRun(ref, step, opts)` ([Fork a run into a new run](#fork-a-run-into-a-new-run)) |
| Skip the rest and call the run done, or failed | `ForceComplete`, `ForceFail` ([Force a run to an end](#force-a-run-to-an-end)) |
| Skip one step that is waiting or running | [Cancel one step](#cancel-one-step) |
| Change what a scheduled run will do before it starts | `RunEditor` ([Edit a scheduled run](edit-runs.md)) |

For example, hold an import that looks wrong, let it go on, and once its transform fails because of a mapping bug, deploy the fix and redo from the transform; the fetch that succeeded is not repeated:

<!-- snippet: examples/run-control/main.go#detour -->
```go
if err := workflow.SuspendRun(ctx, journal, ref); err != nil { // hold
	return err
}
close(gate) // fetch ends; transform parks instead of running
// The source checks out: let the run go on.
if err := workflow.ResumeRun(ctx, journal, ref); err != nil {
	return err
}
if err := waitStatus(ctx, c, ref, workflow.StatusFailed); err != nil { // the mapping bug
	return err
}
mappingFixed.Store(true) // the fix is deployed
if _, err := c.(workflow.RunResetter).ResetRun(ctx, ref, "transform"); err != nil {
	return err
}
```

```text
detour:
  held, resumed, failed in transform, reset from transform: succeeded; fetch ran 1 time(s)
```

A detour that jumps ahead to a later step is not built, and [`ResetRun`](ds:block?id=runresetter-pe26r9jr) re-opens failed runs only: going back from a succeeded run takes `ResetRunWith` with [`ResetOptions.Succeeded`](ds:block?id=resetoptions-fgtq654p) or `ForkRun`, as the table shows.

## List the runs due soon

A run enqueued with `Job.RunAt` waits as `scheduled` until it is due. [`RunInfo.RunAt`](ds:block?id=runinfo-f6rpt8qe) is the time it is (or was) due, kept after it starts, and [`RunQuery.DueFrom`](ds:block?id=runquery-wv73z4g9) and `RunQuery.DueTo` bound it to `[DueFrom, DueTo)`, which answers "what fires in the next hour?":

<!-- snippet: examples/run-reschedule/main.go#due -->
```go
now := clk.Now()
page, err := c.Runs(ctx, workflow.RunQuery{
	DueFrom: now,
	DueTo:   now.Add(time.Hour), // exclusive
})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Printf("  %s due at +%s, %s\n", r.Labels["name"], r.RunAt.Sub(now), r.Status)
}
```

With either bound set, only runs enqueued with a `RunAt` match; a zero bound is open ([`RunQuery.Match`](ds:block?id=runquery-match-rbndsdhq)). `DueFrom` not before `DueTo` fails validation with `ErrInvalid` ([`ValidateRunQuery`](ds:block?id=validaterunquery-bxy4ha24)), and a driver without `CapDueQuery` refuses the query with an `UnsupportedError` rather than ignoring the bounds ([`CheckRunQuery`](ds:block?id=checkrunquery-ah3qd2ar)). The filter combines with the others, so `Statuses: []workflow.Status{workflow.StatusScheduled}` narrows it to runs that have not started yet.

## Reschedule a delayed run

[`RunRescheduler.Reschedule(ctx, ref, at)`](ds:block?id=runrescheduler-rmgramx6) moves a run that has not started to a new time. It keeps the run's `RunRef`, payload, labels and idempotency key, and `RunInfo.RunAt` reads the new time, so nothing that holds the ref has to change. It has no capability bit, so [`workflow.Supports(c, workflow.OpReschedule)`](ds:block?id=supports-4xyjjwvz) is the check (a bare type assertion succeeds on every wrapper, which answers for what it wraps only through `Supports`); code that must run on any client falls back to cancelling the run and enqueuing it again:

<!-- snippet: examples/run-reschedule/main.go#move-run -->
```go
// moveRun moves a delayed run to at. A client that really reschedules
// (workflow.Supports answers for what a wrapper wraps, where a bare type
// assertion would succeed on any wrapper) keeps the run's ref, labels and
// idempotency key; on any other client the
// fallback cancels the run and enqueues job again with the new start,
// which gives the run a new ref. The fallback's status check and cancel
// are two calls, so a run that starts between them is cancelled midway:
// that race is why the driver's own Reschedule is preferred.
func moveRun(ctx context.Context, c workflow.Client, ref workflow.RunRef, job workflow.Job, at time.Time) (workflow.RunRef, error) {
	if workflow.Supports(c, workflow.OpReschedule) {
		return c.(workflow.RunRescheduler).Reschedule(ctx, ref, at) // ErrWrongState once the run has started
	}
	st, err := c.Status(ctx, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if st != workflow.StatusScheduled {
		return workflow.RunRef{}, fmt.Errorf("move run %s: it is %s: %w", ref.RunID, st, workflow.ErrWrongState)
	}
	if err := c.Cancel(ctx, ref); err != nil {
		return workflow.RunRef{}, err
	}
	job.RunAt = at
	return c.Enqueue(ctx, job)
}
```

<!-- snippet: examples/run-reschedule/main.go#reschedule -->
```go
reminder := workflow.Job{Workflow: "send-reminder", Labels: map[string]string{"name": "r-2h"}}
moved, err := moveRun(ctx, c, refs["r-2h"], reminder, clk.Now().Add(45*time.Minute)) // inside the next hour, so the listing shows it
if err != nil {
	return err
}
```

```text
due in the next hour:
  r-30m due at +30m0s, scheduled
rescheduled r-2h to +45m, same ref true
due in the next hour:
  r-2h due at +45m0s, scheduled
  r-30m due at +30m0s, scheduled
```

Only a run in `StatusScheduled` can move ([`RunRescheduler`](ds:block?id=runrescheduler-rmgramx6)). Any other status returns `ErrWrongState`, since a run that has started or ended has no start time left to change, and an unknown ref returns `ErrNotFound`. An `at` in the past, or zero, means now: the run is admitted before `Reschedule` returns, so the ref it returns carries the run's `RunID` as well as its `ScheduledID` on every driver ([memory](ds:block?id=memory-client-reschedule-ennyvcq7), [SQL](ds:block?id=sql-client-reschedule-xpqtkgcm), [Redis](ds:block?id=redis-client-reschedule-j5u3fw5f), [Hatchet](ds:block?id=hatchet-client-startnow-upqvz45e)); a later `at` returns a ref that holds only the `ScheduledID` until the run starts. On a client without the interface, the fallback in `moveRun` is `Cancel` plus a new `Enqueue` with the new `RunAt`, which gives the run a new ref; the example's `TestMoveRunFallback` drives that path through a client that hides the interface.

## Cancel one step

`Cancel` stops a whole run. Sometimes only one step is the problem: a price feed hangs while the rest of the run could carry on with yesterday's prices. [`StepCanceller.CancelStep(ctx, ref, step)`](ds:block?id=stepcanceller-gp5urxfn) withdraws that step and lets the run decide what happens next:

<!-- snippet: examples/run-reschedule/main.go#cancel-step -->
```go
if err := c.CancelStep(ctx, ref, "fetch"); err != nil {
	return err
}
```

For a step with an attempt running, the attempt's `ctx` is cancelled with the cause [`workflow.ErrStepCancelled`](ds:block?id=errstepcancelled-bxbppyku) (read it with `context.Cause`), and the handler's return decides:

- **It returns an output:** the step succeeded, and the run continues with that output. This is how a step offers a fallback when it is interrupted:

<!-- snippet: examples/run-reschedule/main.go#fallback -->
```go
// fetchPrices hangs on the price feed and falls back to cached prices
// when an operator withdraws it.
fetchPrices := func(ctx context.Context, _ []byte) ([]byte, error) {
	started <- "fetch"
	<-ctx.Done() // the price feed is hanging
	if errors.Is(context.Cause(ctx), workflow.ErrStepCancelled) {
		return []byte(`"yesterday's prices"`), nil // an operator withdrew the step: fall back
	}
	return nil, context.Cause(ctx)
}
```

- **It returns an error:** the step fails for good, without retries, with an error matching [`ErrStepCancelled`](ds:block?id=errstepcancelled-bxbppyku); the run fails as any failed run does, and `OnFailure` runs.

A step with no attempt running but not done (waiting out a retry backoff, snoozed, or a wait or sleep step that is suspended) fails for good at once, with an error matching `ErrStepCancelled`. A step that has not started, or has ended, returns `ErrWrongState`; an unknown run or step name `ErrNotFound` ([`StepCanceller`](ds:block?id=stepcanceller-gp5urxfn)).

```text
cancel one step:
  store got "yesterday's prices"
  update-prices succeeded
  sync-feed failed, ErrStepCancelled true
  cancel it again: ErrWrongState true
```

## Try it

```sh
cd examples
go run ./manage-runs
```

```text
failed: run-2 at step "ship": memory: step "ship" failed after 1 attempt(s): warehouse API: 503
retried run-2: succeeded; charge ran 1 time(s), ship 2
retry a succeeded run: ErrWrongState true
deleted: true then Status is ErrNotFound: true
cancelled waiting runs: 2
while paused: queued
after Resume: succeeded
steps that run again: [charge ship]
reset run-16 from charge: succeeded, same ref true; charge ran 1 more time(s)
```

Every line comes from one run of the program: a failed run listed with its step and error, retried from that step (`charge` ran once in all), refused a second retry once it succeeded, deleted, two delayed runs cancelled in bulk, a paused workflow holding a new run in `queued` until `Resume`, and a failed run reset from `charge`, which ran again along with every step after it. `go run ./run-reschedule` prints the due-time, reschedule and cancel-step lines shown in those sections.

## Pitfalls

- **A type assertion is not a capability check.** Wrappers have every optional method and forward it, so `cl.(workflow.RunRetrier)` succeeds on a wrapper whose inner client cannot retry; the call then returns an `UnsupportedError`. Check `Capabilities().Has` for the interfaces that have a bit (`CapRetryRun`, `CapDeleteRun`, `CapCancelAll`, `CapForceRun`, `CapPauseWorkflow`), and [`workflow.Supports(c, op)`](ds:block?id=supports-4xyjjwvz) for those that have none (`OpReschedule`, `OpCancelStep`, `OpResetRun`, `OpForkRun`, `OpEditRun`): it asks the wrapper ([`Forward`](ds:block?id=forward-e32a7p36)) what the client underneath really does.
- A step that `ResetRun` runs again makes its side effects again, and a chain edit it made (`PrependToChain`, `AppendToChain`) is made again. With [`workflow.JournalKeepFailed`](ds:block?id=journalkeepfailed-zngp4v2c), its `Memo` results are kept and returned again; leave that option off when the point of the reset is to recompute.

- **A label search on the SQL driver misses runs that were never indexed.** Runs written before migration 005, or by a client older than it, are found only after [`IndexLabels`](ds:block?id=sql-client-indexlabels-ccg4tdkz) has run. [`Migrate`](ds:block?id=sql-client-migrate-vdt8uy3d) runs it; a team applying `MigrationScripts` with another tool must call it.
- **Hatchet supports most of it.** It declares `CapRunQuery`, `CapCancelAll` and `CapRetryRun` ([caps](ds:block?id=hatchet-caps-d6x9fv8z)). [`Retry`](ds:block?id=hatchet-client-retry-jgtn26c3) is Hatchet's replay of the failed and cancelled step tasks, which keeps the steps that succeeded (checked live). It does not declare `CapDeleteRun`, because the engine has no run delete. Retry does not take the idempotency key again on the engine: it refuses with `ErrDuplicate` when another unfinished run holds the key, but an `Enqueue` with that key while the retried run is unfinished starts a second run. Pause needs `hatchet.Config.API` (see [the Hatchet driver](../drivers/hatchet.md#step-6-pause-a-workflow-or-a-schedule)): with it the driver declares `CapPauseWorkflow` ([`capabilities`](ds:block?id=hatchet-client-capabilities-3ms5edtr)). [`PauseAll()`](ds:block?id=hatchet-client-pause-g7q4ssw8) and `PauseQueue` are refused, and [`Resume(PauseAll())`](ds:block?id=hatchet-client-resume-3tkbjrce) clears every paused workflow of the namespace. It has no `EditRun`, `ResetRun` or `ForkRun`, and a delayed start with an idempotency key cannot be rescheduled there ([`errRescheduleGated`](ds:block?id=hatchet-errreschedulegated-9akd6fmr)); see [Hatchet limitations](../drivers/hatchet.md#limitations).
- **Retry runs your code again.** The failed step runs a second time with the same input. Make it idempotent, as every retried step must be.
- **`Error` is a string.** An engine stores only the message, so [`RunInfo`](ds:block?id=runinfo-f6rpt8qe) cannot give you the original error value to match with `errors.Is`; use the run-finished hook on an in-process driver when you need that.
- **The memory driver forgets old finished runs.** By default it keeps finished runs for a day and at most 10,000 of them ([`memory.DefaultRunRetention`](ds:block?id=memory-defaultrunretention-72hekrav)), so a long-lived process stays bounded; after that `Status`, `Runs` and `Retry` no longer find them. [`memory.WithRunRetention`](ds:block?id=memory-withrunretention-a7b2awty) changes the limits, and `RunRetention{}` keeps everything until `Delete`. Unfinished runs are never forgotten.
- **Force complete records an end, it does not do the work.** Steps that did not run never run ([`RunForcer`](ds:block?id=runforcer-8jsm3gc9)), and an attempt cancelled midway may have done part of its side effect. Check what happened before you force a run.
- **A step must return when its context is cancelled for a force to stop it.** Go cannot kill a goroutine: a step that ignores `ctx` keeps running after the run is already recorded as ended, and its result is thrown away ([`ErrForced`](ds:block?id=errforced-2cdhp8xs) is only the context cause).
- **Error history grows each run's record.** Twenty entries ([`DefaultErrorHistory`](ds:block?id=defaulterrorhistory-cexsqn9u)) with 8 KiB stacks ([`MaxErrorStack`](ds:block?id=maxerrorstack-99dw4vyj)) are up to about 160 KiB per run on a durable driver; lower `WithErrorHistory` for workflows that retry many times.
- **`CancelStep` only asks.** Like a run cancel, it cancels the attempt's `ctx`; a handler that ignores it runs on, and its output, if it returns one, still counts as the step's ([`StepCanceller`](ds:block?id=stepcanceller-gp5urxfn)).
- **A fallback output looks like success to everything after it.** The run ends `succeeded` ([`StepCanceller`](ds:block?id=stepcanceller-gp5urxfn)); if downstream code must know the data is stale, put that in the output.
- **Keep the ref `Reschedule` returns.** Moved to now or earlier, the run has started, and the returned ref carries its `RunID` ([`RunRescheduler`](ds:block?id=runrescheduler-rmgramx6)); the ref you passed holds only the `ScheduledID`. Both still find the run, but code that logs, signals or stores the `RunID` must take it from the returned ref.
- **`DueFrom` and `DueTo` skip runs with no `RunAt`.** A run enqueued to start now is never "due" in this sense, even while it waits for a worker ([`RunQuery.Match`](ds:block?id=runquery-match-rbndsdhq)).
- **A paused workflow still accepts enqueues.** A deploy that pauses before a migration and forgets to resume leaves runs piling up silently; alert on [`Paused`](ds:block?id=pauser-5p4t8yg6) being non-empty.

## Related

- [Named queues](queues.md): routing runs and pausing one queue.
- [Cancel and status](cancel-and-status.md): one run at a time.
- [React when a run finishes](../observability/run-finished.md): the run-finished hook.
- [Errors](../concepts/errors.md): `ErrWrongState`, `ErrDuplicate`, `ErrNotFound`.
- [Run a job later](delays.md): `Job.RunAt`.
- Example: [examples/run-reschedule](../../examples/run-reschedule/) (due-time filters, reschedule, cancel one step).
