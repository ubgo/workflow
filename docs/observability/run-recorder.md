# Record run history

A run recorder keeps a history of runs that your app owns, separate from the driver's own store: one record per run, whatever started it, with its trigger source, its input, every step attempt with its input, output, error and timeline events, the result, the outcome and the timings. A runs page, a support tool or an audit reads that history instead of the driver, so it still answers after an engine pruned its runs, and it can say who started a run and how well its work went.

## Before you start

- You know [plugins](../concepts/plugins.md): the recorder is applied with `workflow.Use` before `Register` and `Start`.
- Any driver works: the recorder is built only from enqueue middleware, step middleware and the [run-finished hook](run-finished.md), which every driver runs. On the Hatchet driver, whose run-finished delivery is best effort, run the [Reconciler](#close-the-gaps-with-the-reconciler) next to it.
- Add the modules you use: `go get github.com/ubgo/workflow/recorder` (the contract, an in-memory store, redaction, the Reconciler; standard library and the core only), and `go get github.com/ubgo/workflow/recorder/sqlrecorder` for the SQL store. Neither pulls a database driver: you open the `*sql.DB` with the driver you already use.
- The runnable program for this page is [examples/run-recorder](../../examples/run-recorder/).

## Why a recorder

A driver's run history is the driver's store. On an engine it ends where the engine's retention ends, it lists runs the way the engine sees them, and no driver keeps a step's timeline or says who started a run. The recorder adds what an operator asks first: "who started this, what did each step do, and did the work succeed?" It changes nothing for an app that does not import it.

| Question | Where the recorder answers it |
|---|---|
| Who started this run? | `RunRecord.Source` and `SourceRef` ([Trigger source](#trigger-source)) |
| What did each step do? | `RunRecord.Steps`: one record per attempt, with input, output, error, node, timings and events |
| Did the work succeed, not only the worker? | `RunRecord.Outcome`: success, partial, error or skipped ([Step events and outcome](#step-events-and-outcome)) |
| What happened to a run the engine already forgot? | The record stays until your own [retention](#retention) removes it |
| Which runs did an operator start from an engine UI? | The Reconciler records them with source `external` |

## Wire it in

Build a store, a recorder over it, and apply the recorder's plugin. Here the store is SQLite through `sqlrecorder`, with card numbers redacted from everything it stores; `requireSource` is a one-line plugin adding `middleware.RequireSource()`, so no run is recorded without its source:

<!-- snippet: examples/run-recorder/main.go#wire -->
```go
store, err := sqlrecorder.New(db, sqlrecorder.SQLite) // the app's own *sql.DB
if err != nil {
	return err
}
if err := store.Migrate(ctx); err != nil { // idempotent, on every start
	return err
}
redact, err := recorder.JSONPaths("card.number")
if err != nil {
	return err
}
rec, err := recorder.New(store, recorder.WithRedactor(redact))
if err != nil {
	return err
}
c, err := workflow.Use(memory.New(), rec.Plugin(), requireSource{})
if err != nil {
	return err
}
```

Register and start through `c`, the client `Use` returned. For tests and single-process apps, `recorder.NewMemoryStore()` is the in-process store; it keeps nothing across a restart.

What each seam records:

| Seam | Write | When |
|---|---|---|
| Enqueue middleware | `UpsertRun`: workflow, queue, tenant, labels, source, input, priority, due time | `Enqueue` succeeded (a refused enqueue records nothing) |
| Step middleware | `StartStep`, `EndStep`, `AddEvent`, `SetOutcome` | Every attempt of every step, `OnFailure` and `OnTimeout` included |
| Run-finished observer | `FinishRun`: status, error, failed step, end time, result | The run reached a terminal status |

On a web or API node built in a driver's enqueue-only mode (`sqlstore.WithEnqueueOnly()`, the Redis driver's, `hatchet.Config{EnqueueOnly: true}`), which has no run-finished hook, apply `rec.EnqueuePlugin()` instead: it records each accepted run with its source and input, and the worker processes, wrapped with `rec.Plugin()` over the same store, record the steps and the end into the same record.

A run started past the enqueue seam (a schedule fire, a chained job, a child) is recorded from its first step. A write that fails never fails the run: it goes to `recorder.WithOnError` (logged with `slog.Default` by default). Every write is an upsert keyed by the run, so the at-least-once delivery of each seam is harmless.

## Trigger source

A run's source is kept in two reserved labels, `workflow.LabelSource` and `workflow.LabelSourceRef`, so every driver stores it with no change. `workflow.WithSource(job, source, ref)` sets them on a copy of the job; they are the only reserved labels an app may set itself. `workflow.SourceOf(labels)` reads them, and derives a source from the labels the library already stamps when none was set:

| Source | Derived from | Reference |
|---|---|---|
| `schedule-manual` | `LabelTrigger` = `manual` (`TriggerSchedule`) | the schedule ID |
| `schedule` | `LabelSchedule` (a schedule or declared cron fired the run) | the schedule ID |
| `child` | `LabelParentRun` (`Spawn`) | the parent run |
| `batch` | the batch ID label | the batch ID |
| `event` | the event label (`events.Bus`) | the event name |
| `external` | set by the Reconciler for a run it found but never saw enqueued | |

Your own sources are any name that passes `workflow.ValidateSource`: lower-case letters, digits and `. _ -`, at most 64 bytes (`api`, `webhook`, `slack`, `admin`). `middleware.RequireSource(allowed...)` refuses a job whose source cannot be told, and, with an allow-list, one whose source is not listed, so a typo never becomes a filter value nobody looks for:

<!-- snippet: examples/run-recorder/main.go#enqueue -->
```go
payload := []byte(`{"shop":"s-1","card":{"number":"4111111111111111"}}`)
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "import-orders", Payload: payload})
fmt.Fprintln(out, "no source refused:", errors.Is(err, workflow.ErrInvalid))

job := workflow.WithSource(workflow.Job{Workflow: "import-orders", Payload: payload}, "webhook", "delivery-42")
ref, err := c.Enqueue(ctx, job)
if err != nil {
	return err
}
```

A source is provenance for people and filters, never an authorization input: anything that can enqueue can claim any source.

## Step events and outcome

`workflow.RecordEvent(ctx, msg, attrs...)` adds a line to the current step's timeline, and `workflow.SetOutcome(ctx, outcome, note)` says how well the run's work went, separately from its status: a sync that skipped because nothing changed, or that finished with 3 bad rows, is `succeeded` either way.

<!-- snippet: examples/run-recorder/main.go#steps -->
```go
fetch := func(ctx context.Context, in []byte) ([]byte, error) {
	workflow.RecordEvent(ctx, "fetched orders", slog.Int("rows", 120), slog.String("shop", "s-1"))
	return []byte(`{"rows":120}`), nil
}
save := func(ctx context.Context, in []byte) ([]byte, error) {
	// The run succeeds either way; the outcome says how well.
	_ = workflow.SetOutcome(ctx, workflow.OutcomePartial, "3 of 120 rows rejected")
	return []byte(`{"saved":117}`), nil
}
```

- `RecordEvent` hands the event to every `workflow.StepEventSink` bound on the step's context: the recorder binds one, `middleware.CaptureOutput` binds one (the event joins the step's captured log records), and your own middleware can bind more with `workflow.WithStepEventSink`. With none bound it does nothing, so a step records events the same way with or without a recorder. A message is cut to 1 KiB, at most 32 attributes are kept, each value cut to 1 KiB.
- `SetOutcome` takes `OutcomeSuccess`, `OutcomePartial`, `OutcomeError` or `OutcomeSkipped`, and the last call in a run wins. Without a recorder it returns an `UnsupportedError`, as `ReportProgress` does, because the caller asked for data to be kept; a step that does not care ignores it. A run that set none gets `workflow.DefaultOutcome(status)`: success for succeeded, error for failed or discarded, none for cancelled.

## Read the history

`Store.Run(ctx, ref)` returns one run with its steps and their events; `Store.Records(ctx, query)` lists runs with the filters only a recorder can answer (`Sources`, `Outcomes`, `Search` over the run ID, scheduled ID, workflow and source reference, `OnlyStale`) on top of every `workflow.RunQuery` filter; and every store is a `workflow.RunQuerier`, so anything that lists runs can read it:

<!-- snippet: examples/run-recorder/main.go#read -->
```go
r, err := store.Run(ctx, ref)
if err != nil {
	return err
}
fmt.Fprintf(out, "run %s source=%s/%s outcome=%s (%s)\n", r.Status, r.Source, r.SourceRef, r.Outcome, r.OutcomeNote)
fmt.Fprintf(out, "input as stored: %s\n", r.Payload)
for _, s := range r.Steps {
	fmt.Fprintf(out, "  step %s attempt %d %s output=%s\n", s.Step, s.Attempt, s.Status, s.Output)
	for _, e := range s.Events {
		fmt.Fprintf(out, "    event %q %v\n", e.Message, e.Attrs)
	}
}
page, err := store.Records(ctx, recorder.Query{Sources: []workflow.Source{"webhook"}, Outcomes: []workflow.Outcome{workflow.OutcomePartial}})
if err != nil {
	return err
}
fmt.Fprintln(out, "webhook runs with a partial outcome:", len(page.Runs))
```

The admin API reads the recorder when you give it one: listings, run detail, failures and queue stats come from the store, while actions (retry, cancel, delete, force, the bulk ones) still go to the client, which owns the runs. A run the store does not hold is looked up on the client:

<!-- snippet: examples/run-recorder/main.go#admin -->
```go
a := admin.New(c, admin.WithRunQuerier(store, recorder.QueryCaps))
info, err := a.Run(ctx, ref)
if err != nil {
	return err
}
fmt.Fprintln(out, "admin reads the recorder:", info.Status)
```

## Try it

```sh
cd examples
go run ./run-recorder
```

```text
no source refused: true
run succeeded source=webhook/delivery-42 outcome=partial (3 of 120 rows rejected)
input as stored: {"card":{"number":"[REDACTED]"},"shop":"s-1"}
  step fetch attempt 1 succeeded output={"rows":120}
    event "fetched orders" [{rows 120} {shop s-1}]
  step save attempt 1 succeeded output={"saved":117}
webhook runs with a partial outcome: 1
admin reads the recorder: succeeded
reconcile: checked=0 finished=0 stale=0
```

## Redaction and payload bounds

A runs table is read by more people than the queue, so it is where a token in a payload leaks. A `recorder.Redactor` scrubs every body (the run's input and result, each step's input and output) and every event attribute before it is stored:

- `recorder.JSONPaths(paths...)` replaces the values at dot paths of a JSON body with `[REDACTED]`; `*` matches any key or array element, a number an array index (`"password"`, `"card.number"`, `"users.*.token"`). An event attribute is redacted when its key matches a path. A body that is not JSON is stored unchanged, so an app with non-JSON payloads writes its own `Redactor`.
- `recorder.PerWorkflow(map, fallback)` picks a redactor per workflow; `recorder.Chain(...)` applies several in order.
- `recorder.WithMaxPayload(n)` keeps a body only up to n bytes (64 KiB by default) and records the size of a larger one. `WithMaxPayload(0)` keeps no bodies at all, only sizes; `WithStepBodies(false)` drops step inputs and outputs while keeping the run's own.

Event messages are not redacted: put values in attributes.

## Retention

Both bundled stores prune in two stages, measured from a run's end, so the heavy part goes first and the summary row a runs page lists stays longer:

```go
store, err := sqlrecorder.New(db, sqlrecorder.Postgres, sqlrecorder.WithRetention(recorder.Retention{
	RetentionRule: recorder.RetentionRule{Steps: 7 * 24 * time.Hour, Runs: 30 * 24 * time.Hour},
	PerWorkflow:   map[string]recorder.RetentionRule{"billing": {Runs: 365 * 24 * time.Hour}},
	Archive:       func(ctx context.Context, runs []recorder.RunRecord) error { return coldStorage.Put(ctx, runs) },
}))
// later, next to the client:
go recorder.PruneEvery(ctx, store, time.Hour, func(err error) { log.Warn("prune", "err", err) })
```

- After `Steps`, a finished run's step and event rows are dropped; its record stays.
- After `Runs`, the record is handed to `Archive` (with whatever steps are left) and deleted. An `Archive` error stops the prune and deletes nothing of that batch, so nothing is lost while an archive is down.
- A zero age keeps that stage; `PerWorkflow` overrides both ages for a workflow. Unfinished runs are never pruned, and each prune works in batches (`Retention.Batch`, 500 by default).

## Close the gaps with the Reconciler

The seams cannot see everything: a worker that died before its step ended, a run cancelled from an engine's UI, a run started from an engine's CLI, a run-finished event a best-effort driver lost. `recorder.NewReconciler(client, store, opts...)` closes those gaps:

<!-- snippet: examples/run-recorder/main.go#reconcile -->
```go
rc, err := recorder.NewReconciler(c, store, recorder.WithStaleAfter(10*time.Minute))
if err != nil {
	return err
}
rep, err := rc.Pass(ctx) // or go rc.Run(ctx) next to the client
if err != nil {
	return err
}
fmt.Fprintf(out, "reconcile: checked=%d finished=%d stale=%d\n", rep.Checked, rep.Finished, rep.Stale)
```

- **Check.** Each pass compares a batch of unfinished records (least recently checked first) with the client. A run the client reports terminal is finished on the record; a run the client still reports unfinished takes the client's status, and is flagged `Stale` when nothing was recorded for it for `StaleAfter`; a run the client does not know is flagged `Stale`. Nothing is ever cancelled: `Stale` is for an operator, and any later write for the run clears it.
- **Backfill.** When the client is a `workflow.RunQuerier`, each pass lists its runs enqueued within the backfill window (`WithBackfillWindow`, an hour by default; `WithBackfillQuery` scopes it to a workflow, tenant or labels on an engine other apps share) and upserts them. A run the recorder never saw gets a record with source `external`; one it holds is left as recorded. A delayed run recorded under its `ScheduledID` is joined with its `RunID`.

Run it with `go rc.Run(ctx)` (every 30 seconds by default) on one node or several: every write is an upsert, so two reconcilers never conflict.

## The SQL store

`sqlrecorder.New(db, dialect, opts...)` works on Postgres 13+, MySQL 8 and SQLite 3.35+ through `database/sql` alone. Its tables, under the prefix `wf_rec_` (`WithTablePrefix`):

| Table | One row per | Indexed for |
|---|---|---|
| `runs` | run | listing order, workflow, status, tenant, source, `Pending`, retention |
| `steps` | step attempt | the run's steps |
| `events` | step event | the attempt's timeline |
| `labels` | run label | label and label-prefix filters |
| `schema` | applied schema version | `Migrate` |

Steps and events are rows of their own, so a busy run never rewrites one large row per event. Times are Unix nanoseconds in UTC (0 for unset), identifiers compare byte by byte on every engine, and every write is one transaction that locks the run's row first and is retried on contention. `Migrate` is idempotent and safe to run on every start; `MigrationScripts` returns its statements for your own migration tool.

## Bring your own store

`recorder.Store` is an interface: any database can hold the history. The `Apply*` functions of the `recorder` package hold the merge rules, so your store merges a write exactly as the bundled ones do, and `recordertest.RunStore(t, newStore)` holds it to the same contract they pass (fill-only upserts, attempts, events, outcome, finish and re-open, the join of a delayed run's two IDs, every filter, paging, `Pending`, copies on read, concurrent writes, retention). `recordertest.RunDriver(t, harness, recordertest.WithStore(newStore))` then proves it under a real driver.

## Pitfalls

- Registering on the bare driver instead of the client `Use` returned skips the step middleware: those runs get a record only from the run-finished observer and the Reconciler.
- A delayed run is recorded under its `ScheduledID` at enqueue and under its `RunID` once it starts; the two records are joined by the first write that carries both IDs (its run-finished event, or a Reconciler pass). Until then a listing can show both.
- On a driver without `workflow.CapRunFinished` (Hatchet), a run end can be missed: run the Reconciler.
- The recorder's history is not the driver's: `admin.WithRunQuerier` shows the recorder, while a retry or a cancel acts on the client. A run the engine already pruned can be shown but no longer retried.
- `RecordEvent` and the recorder's writes run on the step's goroutine: a slow store slows steps, up to `WithWriteTimeout` (5 seconds by default) per write.

## Related

- [React when a run finishes](run-finished.md): the hook the recorder observes run ends with.
- [Labels](../concepts/labels.md): the reserved prefixes, and the two source labels an app may set.
- [Admin operations](../admin/operations.md): the API `admin.WithRunQuerier` points at the recorder.
- [Retention](../operations/retention.md): the SQL driver's own retention, separate from the recorder's.
- [Write a plugin](../extending/writing-a-plugin.md): the seams the recorder is built from.
