# Record run history

A run recorder keeps a history of runs that your app owns, separate from the driver's own store: one record per run, whatever started it, with its trigger source, its input, every step attempt with its input, output, error and timeline events, the result, the outcome and the timings. A runs page, a support tool or an audit reads that history instead of the driver, so it still answers after an engine pruned its runs, and it can say who started a run and how well its work went.

## Before you start

- You know [plugins](../concepts/plugins.md): the recorder is applied with `workflow.Use` before `Register` and `Start`.
- Any driver works: [`rec.Plugin()`](ds:block?id=rec-recorder-plugin-u7hfc2yc) is built only from enqueue middleware, step middleware and the [run-finished hook](run-finished.md), which every driver runs. On the Hatchet driver, whose run-finished delivery is best effort, run the [Reconciler](#close-the-gaps-with-the-reconciler) next to it.
- Add the modules you use: `go get github.com/ubgo/workflow/recorder` (the contract, an in-memory store, redaction, the Reconciler; standard library and the core only), and `go get github.com/ubgo/workflow/recorder/sqlrecorder` for the SQL store. Neither pulls a database driver: you open the `*sql.DB` with the driver you already use.
- The runnable program for this page is [examples/run-recorder](../../examples/run-recorder/). Two more cover the rest: [examples/recorder-custom-store](../../examples/recorder-custom-store/) (redaction of your own, the error hook, the Reconciler's options, a store of your own proven with `recordertest`) and [examples/recorder-sql](../../examples/recorder-sql/) (the SQL store's dialects, retries, two-stage retention and archive table).

## Why a recorder

A driver's run history is the driver's store. On an engine it ends where the engine's retention ends, it lists runs the way the engine sees them, and no driver keeps a step's timeline or says who started a run. The recorder adds what an operator asks first: "who started this, what did each step do, and did the work succeed?" It changes nothing for an app that does not import it.

| Question | Where the recorder answers it |
|---|---|
| Who started this run? | `RunRecord.Source` and `SourceRef` ([Trigger source](#trigger-source)) |
| What did each step do? | [`RunRecord.Steps`](ds:block?id=rec-runrecord-xjfqvqc3): one [`StepRecord`](ds:block?id=rec-steprecord-tsk32v93) per attempt, with input, output, error, node, timings and events |
| Did the work succeed, not only the worker? | `RunRecord.Outcome`: success, partial, error or skipped ([Step events and outcome](#step-events-and-outcome)) |
| What happened to a run the engine already forgot? | The record stays until your own [retention](#retention) removes it |
| Which runs did an operator start from an engine UI? | The Reconciler records them with source [`external`](ds:block?id=sourceexternal-pq4saxv5) |

## Wire it in

Build a store, a recorder over it, and apply the recorder's plugin. Here the store is SQLite through `sqlrecorder`, with card numbers redacted from everything it stores; `requireSource` is a one-line plugin adding [`middleware.RequireSource()`](ds:block?id=mw-requiresource-3tsntxp2), so no run is recorded without its source:

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

Register and start through `c`, the client `Use` returned. The plugin reports the name [`recorder`](ds:block?id=rec-defaultpluginname-wtsqd67u) to `workflow.Use`; [`WithName`](ds:block?id=rec-withname-cuvqnswq) changes it, so two recorders, one per store, can wrap one client. [`rec.Store()`](ds:block?id=rec-recorder-store-2gdjppvt) returns the store the recorder writes to. For tests and single-process apps, [`recorder.NewMemoryStore()`](ds:block?id=rec-newmemorystore-5yrbgb94) is the in-process store; it keeps nothing across a restart.

What each seam records:

| Seam | Write | When |
|---|---|---|
| Enqueue middleware ([`rec.EnqueueMiddleware()`](ds:block?id=rec-recorder-enqueuemiddleware-tkqchzg3)) | `UpsertRun`: workflow, queue, tenant, labels, source, input, priority, due time | `Enqueue` succeeded (a refused enqueue records nothing) |
| Step middleware ([`rec.StepMiddleware()`](ds:block?id=rec-recorder-stepmiddleware-tbta9pzy)) | `StartStep`, `EndStep`, `AddEvent`, `SetOutcome` | Every attempt of every step, `OnFailure` and `OnTimeout` included |
| Run-finished observer ([`rec.OnRunFinished`](ds:block?id=rec-recorder-onrunfinished-9a8agy3p)) | `FinishRun`: status, error, failed step, end time, result | The run reached a terminal status |

The plugin is these three; they are exported for an app that wires the seams by hand. On a web or API node that only enqueues, apply [`rec.EnqueuePlugin()`](ds:block?id=rec-recorder-enqueueplugin-rn5k8npv) instead ([Web nodes that only enqueue](#web-nodes-that-only-enqueue)).

A run started past the enqueue seam (a schedule fire, a chained job, a child) is recorded from its first step. A write that fails never fails the run: it goes to [`recorder.WithOnError`](ds:block?id=rec-withonerror-r4hkn36k) (logged with `slog.Default` by default). Every write is an upsert keyed by the run, so the at-least-once delivery of each seam is harmless.

## Web nodes that only enqueue

A web or API node built in a driver's enqueue-only mode (`workflow.CapEnqueueOnly`: `sqlstore.WithEnqueueOnly()`, `redis.WithEnqueueOnly()`, `hatchet.Config{EnqueueOnly: true}`) runs no steps and delivers no run ends: the SQL and Redis drivers start no run-end delivery on it, and Hatchet's enqueue-only client refuses `OnRunFinished`, so `workflow.Use(web, rec.Plugin())` fails there with `workflow.ErrUnsupported`. `rec.EnqueuePlugin()` is the recorder's enqueue half alone: it records each accepted run with its source, input and labels. The workers, wrapped with [`rec.Plugin()`](ds:block?id=rec-recorder-plugin-u7hfc2yc) over the same store, record the steps, their events and the end into the same record, because every write is an upsert keyed by the run. The program is [examples/recorder-split](../../examples/recorder-split/), on the SQL driver over one SQLite file:

<!-- snippet: examples/recorder-split/main.go#web -->
```go
// The web node enqueues and never runs a step, so it has no run-end
// hook: it takes the recorder's enqueue half only.
webDriver, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithEnqueueOnly())
if err != nil {
	return err
}
web, err := workflow.Use(webDriver, rec.EnqueuePlugin())
if err != nil {
	return err
}
```

<!-- snippet: examples/recorder-split/main.go#worker -->
```go
// A worker node takes the full plugin over the same store, and
// registers and starts through the client Use returned.
workerDriver, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
if err != nil {
	return err
}
worker, err := workflow.Use(workerDriver, rec.Plugin())
if err != nil {
	return err
}
if err := worker.Register(thumbnail); err != nil {
	return err
}
if err := worker.Start(ctx); err != nil {
	return err
}
```

```text
web node recorded: source=api/req-7 input={"image":"img-1"}
worker finished it: succeeded source=api/req-7 outcome=success
  step resize attempt 1 succeeded output="thumb-1.png" events=1
records for the run: 1
```

The store must be one both nodes reach: a SQL store on a shared database (here the jobs database itself), or a store of your own, never `recorder.NewMemoryStore()`, which lives in one process.

## Trigger source

A run's source is kept in two reserved labels, [`workflow.LabelSource`](ds:block?id=labelsource-m6wjahhr) and [`workflow.LabelSourceRef`](ds:block?id=labelsourceref-qfv7bbhp), so every driver stores it with no change. [`workflow.WithSource(job, source, ref)`](ds:block?id=withsource-g5xwt2m5) sets them on a copy of the job; they are the only reserved labels an app may set itself. [`workflow.SourceOf(labels)`](ds:block?id=sourceof-7mrryr2k) reads them, and derives a source from the labels the library already stamps when none was set:

| Source | Derived from | Reference |
|---|---|---|
| `schedule-manual` | `LabelTrigger` = `manual` (`TriggerSchedule`) | the schedule ID |
| `schedule` | `LabelSchedule` (a schedule or declared cron fired the run) | the schedule ID |
| `child` | `LabelParentRun` (`Spawn`) | the parent run |
| `batch` | the batch ID label | the batch ID |
| `event` | the event label (`events.Bus`) | the event name |
| `external` | set by the Reconciler for a run it found but never saw enqueued | |

Your own sources are any name that passes [`workflow.ValidateSource`](ds:block?id=validatesource-73nzwq5k): lower-case letters, digits and `. _ -`, at most [64 bytes](ds:block?id=maxsourcelen-sx2rxxmg) (`api`, `webhook`, `slack`, `admin`). `middleware.RequireSource(allowed...)` refuses a job whose source cannot be told, and, with an allow-list, one whose source is not listed, so a typo never becomes a filter value nobody looks for:

<!-- snippet: examples/run-recorder/main.go#enqueue -->
```go
payload := []byte(`{"shop":"s-1","card":{"number":"4111111111111111"}}`)
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "import-orders", Payload: payload})
fmt.Println("no source refused:", errors.Is(err, workflow.ErrInvalid))

job := workflow.WithSource(workflow.Job{Workflow: "import-orders", Payload: payload}, "webhook", "delivery-42")
ref, err := c.Enqueue(ctx, job)
if err != nil {
	return err
}
```

A source is provenance for people and filters, never an authorization input: anything that can enqueue can claim any source. The whole model, with a runnable program, is on [Trigger source](../concepts/trigger-source.md).

## Step events and outcome

[`workflow.RecordEvent(ctx, msg, attrs...)`](ds:block?id=recordevent-k3aqhnwy) adds a line to the current step's timeline, and [`workflow.SetOutcome(ctx, outcome, note)`](ds:block?id=setoutcome-p5kkstu4) says how well the run's work went, separately from its status: a sync that skipped because nothing changed, or that finished with 3 bad rows, is `succeeded` either way.

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

- `RecordEvent` hands the event to every [`workflow.StepEventSink`](ds:block?id=stepeventsink-f4enzkvf) bound on the step's context: the recorder binds one, [`middleware.CaptureOutput`](ds:block?id=mw-captureoutput-fk79ecqu) binds one (the event joins the step's captured log records), and your own middleware can bind more with [`workflow.WithStepEventSink`](ds:block?id=withstepeventsink-ehcpt8xf). With none bound it does nothing, so a step records events the same way with or without a recorder. A message is cut to [1 KiB](ds:block?id=maxstepeventmessage-yfsr3k3h), at most [32](ds:block?id=maxstepeventattrs-das9dx82) attributes are kept, each value cut to [1 KiB](ds:block?id=maxstepeventvalue-ukkmnhkc).
- Both have pages of their own: [Step events](../concepts/step-events.md) (sinks, bounds, `CaptureOutput`) and [Run outcome](../concepts/run-outcome.md) (the four outcomes, errors, the default).
- `SetOutcome` takes [`OutcomeSuccess`](ds:block?id=outcomesuccess-x2k6rmg2), [`OutcomePartial`](ds:block?id=outcomepartial-6umpq82v), [`OutcomeError`](ds:block?id=outcomeerror-tbsjn5wb) or [`OutcomeSkipped`](ds:block?id=outcomeskipped-c5b9by4k), and the last call in a run wins. Without a recorder it returns an `UnsupportedError`, as `ReportProgress` does, because the caller asked for data to be kept; a step that does not care ignores it. A run that set none gets [`workflow.DefaultOutcome(status)`](ds:block?id=defaultoutcome-qffu2upr): success for succeeded, error for failed or discarded, none for cancelled.

## Read the history

[`Store.Run(ctx, ref)`](ds:block?id=rec-store-pgu7xhxs) returns one run with its steps and their events; `Store.Records(ctx, query)` lists runs, without their steps, with the filters only a recorder can answer ([`recorder.Query`](ds:block?id=rec-query-7ufc7wgu): `Sources`, `Outcomes`, `Search` over the run ID, scheduled ID, workflow and source reference, `OnlyStale`) on top of the `workflow.RunQuery` filters of [`recorder.QueryCaps`](ds:block?id=rec-querycaps-x2zk9xju); and every store is a `workflow.RunQuerier`, so anything that lists runs can read it:

<!-- snippet: examples/run-recorder/main.go#read -->
```go
r, err := store.Run(ctx, ref)
if err != nil {
	return err
}
fmt.Printf("run %s source=%s/%s outcome=%s (%s)\n", r.Status, r.Source, r.SourceRef, r.Outcome, r.OutcomeNote)
fmt.Printf("input as stored: %s\n", r.Payload)
for _, s := range r.Steps {
	fmt.Printf("  step %s attempt %d %s output=%s\n", s.Step, s.Attempt, s.Status, s.Output)
	for _, e := range s.Events {
		fmt.Printf("    event %q %v\n", e.Message, e.Attrs)
	}
}
page, err := store.Records(ctx, recorder.Query{Sources: []workflow.Source{"webhook"}, Outcomes: []workflow.Outcome{workflow.OutcomePartial}})
if err != nil {
	return err
}
fmt.Println("webhook runs with a partial outcome:", len(page.Runs))
```

- `Records` returns a [`recorder.RecordPage`](ds:block?id=rec-recordpage-8ehzyqwp): its `Runs`, and `Next`, the cursor of the following page (empty on the last), which goes back as the query's `Cursor`.
- [`recorder.ValidateQuery`](ds:block?id=rec-validatequery-nz2h9e24) checks a query before a store answers it: a `Search` longer than [256 bytes](ds:block?id=rec-maxsearchlen-ve8yffru), an unknown source or outcome, or a `RunQuery` filter outside `QueryCaps` is refused, with an error wrapping `workflow.ErrInvalid` or `workflow.ErrUnsupported`.
- A step record's `Status` is a [`recorder.StepStatus`](ds:block?id=rec-stepstatus-nxdj7ers), one of [`running`, `succeeded`, `failed` and `snoozed`](ds:block?id=rec-stepstatuses-8svs5ers); a switch on it keeps a default branch, because a later version may add statuses.

The admin API reads the recorder when you give it one: listings, run detail, failures and queue stats come from the store, while actions (retry, cancel, delete, force, the bulk ones) still go to the client, which owns the runs. A run the store does not hold is looked up on the client:

<!-- snippet: examples/run-recorder/main.go#admin -->
```go
a := admin.New(c, admin.WithRunQuerier(store, recorder.QueryCaps))
info, err := a.Run(ctx, ref)
if err != nil {
	return err
}
fmt.Println("admin reads the recorder:", info.Status)
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

The two other programs of this page print what their sections below explain:

```sh
go run ./recorder-custom-store
go run ./recorder-sql
```

```text
input as stored: {"card":{"number":"[REDACTED]"},"order":"order-1"}
  step charge output=none (51 bytes before redaction)
  step notify output={"sent":true} (13 bytes before redaction)
    event "receipt sent" [{email a***@example.com}]
-- the history store goes into maintenance
history writes that failed: add-event end-step finish-run start-step upsert-run
run status from the driver: succeeded; in the history: true
-- maintenance over: one Reconciler pass
reconcile: checked=0 finished=0 backfilled=2 errors=0
second run in the history: succeeded source=api/order-2 steps=0
```

```text
dialects: [postgres mysql sqlite]
refused: history database "oracle": want one of [postgres mysql sqlite]
retried: sqlite busy true | proxy busy true | syntax error false
migration statements: 15, tables: hist_events hist_labels hist_runs hist_schema hist_steps
day 0: report succeeded steps=1, billing succeeded steps=1
prune after a week: steps cleared=1 runs deleted=0 archived=0
day 7: report succeeded steps=0, billing succeeded steps=1
prune after a month, archive down: deleted=0 error=sqlrecorder: archive: cold storage unreachable
day 31: report succeeded steps=0, billing succeeded steps=1
archived: report (steps kept: 0)
after PruneEvery: report gone, billing succeeded steps=1
archive store: migration statements: 19, tables: arch_archive arch_events arch_labels arch_runs arch_schema arch_steps
archive prune: runs deleted=3 moved=3
archived runs, pages of 2: 2+1 runs: exp-1 exp-2 exp-3
archived run exp-1: succeeded steps=1 events=1
still in the runs table: false
```

## Redaction and payload bounds

A runs table is read by more people than the queue, so it is where a token in a payload leaks. A [`recorder.Redactor`](ds:block?id=rec-redactor-x3e2p5bt) scrubs every body (the run's input and result, each step's input and output) and every event attribute before it is stored:

- [`recorder.JSONPaths(paths...)`](ds:block?id=rec-jsonpaths-66ph5w5h) replaces the values at dot paths of a JSON body with `[REDACTED]`; `*` matches any key or array element, a number an array index (`"password"`, `"card.number"`, `"users.*.token"`). An event attribute is redacted when its key matches a path. A body that is not JSON is stored unchanged, so an app with non-JSON payloads writes its own `Redactor`.
- [`recorder.PerWorkflow(map, fallback)`](ds:block?id=rec-perworkflow-vx8puukg) picks a redactor per workflow; [`recorder.Chain(...)`](ds:block?id=rec-chain-ryg3zfvp) applies several in order.
- [`recorder.WithMaxPayload(n)`](ds:block?id=rec-withmaxpayload-3f6m2zdh) keeps a body only up to n bytes ([64 KiB](ds:block?id=rec-defaultmaxpayload-43z9t84h) by default) and records the size of a larger one. `WithMaxPayload(0)` keeps no bodies at all, only sizes; [`WithStepBodies(false)`](ds:block?id=rec-withstepbodies-7dpfqzyb) drops step inputs and outputs while keeping the run's own.
- [`WithErrorHistory(n)`](ds:block?id=rec-witherrorhistory-3fap7agv) bounds the attempt errors a record keeps (`workflow.DefaultErrorHistory` by default; zero keeps none). The value `JSONPaths` writes is the constant [`recorder.Redacted`](ds:block?id=rec-redacted-uvn3fy6r).

Event messages are not redacted: put values in attributes.

### Step 1: write a Redactor of your own

A `Redactor` has two methods, one for bodies and one for event attributes, and each is told what it is looking at through a [`recorder.PayloadInfo`](ds:block?id=rec-payloadinfo-9axyr3hj): the workflow, the step (empty for the run's input and result) and the [`PayloadKind`](ds:block?id=rec-payloadkind-2vp35d73), one of `PayloadInput`, `PayloadStepInput`, `PayloadStepOutput`, `PayloadResult` and `PayloadEvent`. This one keeps none of the `charge` step's output (the payment provider's raw response) and masks an email in any event attribute:

<!-- snippet: examples/recorder-custom-store/main.go#redactor -->
```go
// receipts is a Redactor of the app's own. It reads PayloadInfo to treat
// one step differently: the charge step's output is the payment provider's
// raw response, so none of it is stored (nil keeps the size only), and an
// email in any event attribute is masked down to its domain. It must not
// change the slice it is given, and it runs on the step's goroutine, so it
// stays quick.
type receipts struct{}

// RedactPayload implements recorder.Redactor: it drops the charge step's
// output and keeps every other body as it is.
func (receipts) RedactPayload(info recorder.PayloadInfo, body []byte) []byte {
	if info.Kind == recorder.PayloadStepOutput && info.Step == "charge" {
		return nil
	}
	return body
}

// RedactAttr implements recorder.Redactor: it masks an email attribute
// down to its first letter and domain.
func (receipts) RedactAttr(_ recorder.PayloadInfo, a workflow.EventAttr) workflow.EventAttr {
	if at := strings.IndexByte(a.Value, '@'); a.Key == "email" && at > 0 {
		a.Value = a.Value[:1] + "***" + a.Value[at:]
	}
	return a
}
```

Returning `nil` stores no body; the record still holds the size the body had. Returning a body larger than the bound stores none either, so a redactor cannot push a body past `WithMaxPayload`.

### Step 2: combine redactors and set the bounds

[`PerWorkflow`](ds:block?id=rec-perworkflow-vx8puukg) gives `checkout` both redactors, which [`Chain`](ds:block?id=rec-chain-ryg3zfvp) applies in order, card paths first, and every other workflow the card paths only. The error hook is `onError`, typed as a [`recorder.ErrorFunc`](ds:block?id=rec-errorfunc-nrd8pzfd) ([Be told about a failed write](#be-told-about-a-failed-write)):

<!-- snippet: examples/recorder-custom-store/main.go#recorder -->
```go
cards, err := recorder.JSONPaths("card.number")
if err != nil {
	return err
}
// checkout gets both redactors, in order; every other workflow the
// card paths only.
redact := recorder.PerWorkflow(map[string]recorder.Redactor{
	"checkout": recorder.Chain(cards, receipts{}),
}, cards)
// onError is the error hook, a recorder.ErrorFunc: told about each
// failed history write while the run goes on, on the goroutine that
// made the write, so it only counts.
var onError recorder.ErrorFunc = func(_ context.Context, op recorder.WriteOp, ref workflow.RunRef, err error) {
	failed.add(op) // in an app: a counter by op, and a log line with ref and err
}
rec, err := recorder.New(db,
	recorder.WithRedactor(redact),
	recorder.WithMaxPayload(maxBody),        // larger bodies are kept by size only
	recorder.WithStepBodies(true),           // the default; false keeps only the run's input and result
	recorder.WithErrorHistory(errorHistory), // attempt errors kept per run
	recorder.WithClock(clk),                 // the driver's clock, so records and driver agree
	recorder.WithOnError(onError),           // every failed write, by its WriteOp
)
if err != nil {
	return err
}
```

The first run of `checkout` is stored like this (the full output is under [Try it](#try-it)):

```text
input as stored: {"card":{"number":"[REDACTED]"},"order":"order-1"}
  step charge output=none (51 bytes before redaction)
  step notify output={"sent":true} (13 bytes before redaction)
    event "receipt sent" [{email a***@example.com}]
```

## Be told about a failed write

A recorder never fails a run: when a history write fails (the store is down, a write times out after [`WithWriteTimeout`](ds:block?id=rec-withwritetimeout-rn35n8vw), the store panics), the run goes on and the failure goes to the hook set with [`recorder.WithOnError`](ds:block?id=rec-withonerror-r4hkn36k). The hook is a [`recorder.ErrorFunc`](ds:block?id=rec-errorfunc-nrd8pzfd), `func(ctx, op recorder.WriteOp, ref workflow.RunRef, err error)`; without one, each failure is [logged](ds:block?id=rec-logerror-ne6hyt2t) with `slog.Default` at warn level. `op` says which write failed:

| `WriteOp` | Value | The write |
|---|---|---|
| `OpUpsertRun` | [`upsert-run`](ds:block?id=rec-opupsertrun-zvthqrsn) | The run was accepted (enqueue seam) |
| `OpStartStep` | [`start-step`](ds:block?id=rec-opstartstep-yh65yvgr) | A step attempt started |
| `OpEndStep` | [`end-step`](ds:block?id=rec-opendstep-9gt5czpt) | A step attempt ended |
| `OpAddEvent` | [`add-event`](ds:block?id=rec-opaddevent-kpqzyqhe) | A step event (`workflow.RecordEvent`) |
| `OpFinishRun` | [`finish-run`](ds:block?id=rec-opfinishrun-c5chxmfv) | The run ended (run-finished observer) |
| `OpReadResult` | [`read-result`](ds:block?id=rec-opreadresult-rkjnuah5) | Reading a succeeded run's result with [`WithResultReader`](ds:block?id=rec-withresultreader-wrjsqy26) failed; the end is still written, without the result |
| `OpSetOutcome` | [`set-outcome`](ds:block?id=rec-opsetoutcome-c9naecw4) | A step's `workflow.SetOutcome`; the error (a store panic turned into one) also goes back to the step |

The example's hook, `onError` in the `recorder` snippet above, is a named `recorder.ErrorFunc` that collects the ops. It then takes the store down for maintenance and runs the workflow again: every write fails, the hook hears of each, and the run still succeeds on the driver:

```text
-- the history store goes into maintenance
history writes that failed: add-event end-step finish-run start-step upsert-run
run status from the driver: succeeded; in the history: true
```

The hook runs on the goroutine that made the write (a step's, an enqueuer's, the driver's run-finished goroutine), so it must return quickly: count by `op` and log with `ref`, never write to the same store again from it. A run whose writes failed is recorded later by the [Reconciler](#close-the-gaps-with-the-reconciler)'s backfill.

## Retention

Both bundled stores prune in [two stages](ds:block?id=rec-retention-gz6xhhm8), measured from a run's end, so the heavy part goes first and the summary row a runs page lists stays longer:

- After `Steps`, a finished run's step and event rows are dropped; its record stays.
- After `Runs`, the record is handed to `Archive` (with whatever steps are left) and deleted. An `Archive` error stops the prune and deletes nothing of that batch, so nothing is lost while an archive is down. `Archive` sees a run at least once on `sqlrecorder` (see [Pitfalls](#pitfalls)); for an exactly-once archive in the same database, use [`sqlrecorder.WithArchiveTable`](ds:block?id=sqlrec-witharchivetable-cduufku5) (below).
- A zero age keeps that stage; `PerWorkflow` overrides both ages for a workflow. Unfinished runs are never pruned, and each prune works in batches ([`Retention.Batch`](ds:block?id=rec-retention-gz6xhhm8), [500](ds:block?id=rec-defaultprunebatch-4n8gxw2n) by default).
- A batch is at most [10,000](ds:block?id=rec-maxprunebatch-f8u6nphh) runs: [`recorder.ValidateRetention`](ds:block?id=rec-validateretention-pwmvywwn) refuses a larger one or a negative age with `workflow.ErrInvalid`, so `sqlrecorder.WithRetention` and `recorder.NewMemoryStore` fail on one.
- Ages are measured on the store's clock, the real one by default: [`sqlrecorder.WithClock`](ds:block?id=sqlrec-withclock-dbbcdpey), or [`recorder.WithMemoryClock`](ds:block?id=rec-withmemoryclock-mejrhz64) on the in-memory store. The in-memory store takes its retention from [`recorder.WithMemoryRetention`](ds:block?id=rec-withmemoryretention-2a2a7wde); without one its `Prune` does nothing.

The program is [examples/recorder-sql](../../examples/recorder-sql/). Its store keeps step rows a week and run rows a month, a [`recorder.Retention`](ds:block?id=rec-retention-gz6xhhm8) with a [`RetentionRule`](ds:block?id=rec-retentionrule-33x6hwfr) for every workflow and a `PerWorkflow` rule that keeps `billing` runs a year, and an `Archive` hook that takes each run before it is deleted:

<!-- snippet: examples/recorder-sql/main.go#store -->
```go
dialect, err := dialectFor(historyDialect)
if err != nil {
	return err
}
store, err := sqlrecorder.New(db, dialect,
	sqlrecorder.WithTablePrefix(tablePrefix),
	sqlrecorder.WithTxAttempts(txAttempts),
	sqlrecorder.WithRetryable(retryable),
	sqlrecorder.WithClock(clk), // retention ages are measured on this clock
	sqlrecorder.WithRetention(recorder.Retention{
		RetentionRule: recorder.RetentionRule{Steps: stepsAge, Runs: runsAge},
		// A PerWorkflow rule replaces both ages: billing keeps its
		// steps (zero) and its runs for a year.
		PerWorkflow: map[string]recorder.RetentionRule{"billing": {Runs: billingRunsAge}},
		// Every run is handed here before stage two deletes it.
		Archive: cold.Put,
	}),
)
if err != nil {
	return err
}
if err := store.Migrate(ctx); err != nil { // idempotent, on every start
	return err
}
```

A store with retention is a [`recorder.Pruner`](ds:block?id=rec-pruner-tz43nn4x): `Prune` runs both stages once and returns a [`PruneResult`](ds:block?id=rec-pruneresult-2nc4x76t). Call it from a scheduled job of your own:

<!-- snippet: examples/recorder-sql/main.go#prune -->
```go
clk.Advance(stepsAge + time.Hour) // an hour past the step age
res, err := store.Prune(ctx)      // or a scheduled job of your own
if err != nil {
	return err
}
fmt.Fprintf(w, "prune after a week: steps cleared=%d runs deleted=%d archived=%d\n", res.StepsCleared, res.RunsDeleted, res.Archived)
```

or run [`recorder.PruneEvery`](ds:block?id=rec-pruneevery-7jzsmuxr) next to the client, which prunes at once, then every interval until its context ends, and hands each error to its callback:

<!-- snippet: examples/recorder-sql/main.go#prune-every -->
```go
go func() {
	// Prunes at once, then every pruneInterval, until pctx ends.
	done <- recorder.PruneEvery(pctx, store, pruneInterval, func(err error) {
		fmt.Fprintln(w, "prune failed:", err) // a log line in an app
	})
}()
```

The program moves a fake clock a week, then a month, with the archive down at the first try:

```text
day 0: report succeeded steps=1, billing succeeded steps=1
prune after a week: steps cleared=1 runs deleted=0 archived=0
day 7: report succeeded steps=0, billing succeeded steps=1
prune after a month, archive down: deleted=0 error=sqlrecorder: archive: cold storage unreachable
day 31: report succeeded steps=0, billing succeeded steps=1
archived: report (steps kept: 0)
after PruneEvery: report gone, billing succeeded steps=1
```

### Archive into a table, exactly once

[`sqlrecorder.WithArchiveTable()`](ds:block?id=sqlrec-witharchivetable-cduufku5) moves each run that stage two deletes into `<prefix>archive` ([`wf_rec_archive`](ds:block?id=sqlrec-defaulttableprefix-af54bca4) by default) in the same transaction as the delete: one row per run, keyed like the runs table, with `run_id`, `workflow`, `tenant`, `status`, `finished_at`, `archived_at` (the store's clock at the move) and `record`, the whole `recorder.RunRecord` (steps and events included) as JSON. The archive copy and the delete commit together, so each run is in exactly one of the two places: a cancelled context, a crash or a failed commit rolls the batch back whole, and the next prune moves it. Two processes pruning the same tables move each run once ([`TestSQLRecorderArchiveTableExactlyOnce`](ds:block?id=e2e-testsqlrecorderarchivetableexactlyonce-c7vjhu87) holds both on every engine).

The second store of [examples/recorder-sql](../../examples/recorder-sql/) lives in the same SQLite database under its own prefix, `arch_`, keeps runs a month and moves them to the table:

<!-- snippet: examples/recorder-sql/main.go#archive-store -->
```go
store, err := sqlrecorder.New(db, sqlrecorder.SQLite,
	sqlrecorder.WithTablePrefix(archivePrefix),
	sqlrecorder.WithClock(clk),
	// Retention decides what moves and when; the table is where it goes.
	sqlrecorder.WithRetention(recorder.Retention{RetentionRule: recorder.RetentionRule{Runs: runsAge}}),
	sqlrecorder.WithArchiveTable(), // Migrate also creates arch_archive
)
if err != nil {
	return err
}
if err := store.Migrate(ctx); err != nil {
	return err
}
```

A month on, one prune moves every expired run. [`ArchivedRuns`](ds:block?id=sqlrec-store-archivedruns-39uycka3) reads the table a page at a time, oldest finish first, with an [`ArchiveQuery`](ds:block?id=sqlrec-archivequery-56dg4s52) (`Workflow`, `Tenant`, `Limit`, `Cursor`); each [`ArchivePage`](ds:block?id=sqlrec-archivepage-fgu6n8d6) carries its runs and `Next`, the cursor of the following page, empty on the last. [`ArchivedRun`](ds:block?id=sqlrec-store-archivedrun-x7b4wy6a) returns one run with its steps and events, as `Run` returned it before the move, or `workflow.ErrNotFound` when the table does not hold it:

<!-- snippet: examples/recorder-sql/main.go#archive-read -->
```go
res, err := store.Prune(ctx) // stage two moves each expired run, exactly once
if err != nil {
	return err
}
fmt.Fprintf(w, "archive prune: runs deleted=%d moved=%d\n", res.RunsDeleted, res.Archived)

// Read the archive a page at a time, oldest finish first.
var sizes, sources []string
q := sqlrecorder.ArchiveQuery{Workflow: exportWorkflow, Limit: archivePageSize}
for {
	page, err := store.ArchivedRuns(ctx, q)
	if err != nil {
		return err
	}
	sizes = append(sizes, fmt.Sprint(len(page.Runs)))
	for _, r := range page.Runs {
		sources = append(sources, r.SourceRef)
	}
	if page.Next == "" { // the last page
		break
	}
	q.Cursor = page.Next
}
slices.Sort(sources)
fmt.Fprintf(w, "archived runs, pages of %d: %s runs: %s\n", archivePageSize, strings.Join(sizes, "+"), strings.Join(sources, " "))

// One run, as Run returned it before the move.
r, err := store.ArchivedRun(ctx, first)
if err != nil {
	return err
}
events := 0
for _, st := range r.Steps {
	events += len(st.Events)
}
fmt.Fprintf(w, "archived run %s: %s steps=%d events=%d\n", r.SourceRef, r.Status, len(r.Steps), events)
_, err = store.Run(ctx, first) // the runs table no longer holds it
fmt.Fprintln(w, "still in the runs table:", !errors.Is(err, workflow.ErrNotFound))
```

```text
archive store: migration statements: 19, tables: arch_archive arch_events arch_labels arch_runs arch_schema arch_steps
archive prune: runs deleted=3 moved=3
archived runs, pages of 2: 2+1 runs: exp-1 exp-2 exp-3
archived run exp-1: succeeded steps=1 events=1
still in the runs table: false
```

The three runs ended at the same instant on the program's fake clock; the two pages still hold each of them once.

- The table is opt-in: [`Migrate`](ds:block?id=sqlrec-store-migrate-63gjrdbn) creates it (and its indexes on `run_id`, `finished_at` and `workflow`) only for a store with the option, idempotently on every call, and [`MigrationScripts`](ds:block?id=sqlrec-store-migrationscripts-xbepgg67) then lists its statements last: 19 here, the 15 of the default schema and then the table and its three indexes. The default schema and [`SchemaVersion`](ds:block?id=sqlrec-schemaversion-scf4u7mm) do not change, so older binaries keep working against the same database.
- It needs [`WithRetention`](ds:block?id=sqlrec-withretention-jvnmngh9), which decides what moves and when; the archive table has no retention of its own, so trim it with your own `DELETE` on `finished_at` or `archived_at`.
- `Retention.Archive` can be set beside it: the callback still sees each batch first and keeps its at-least-once delivery, while the table holds each run once. [`PruneResult.Archived`](ds:block?id=sqlrec-store-prune-843j2gjz) counts the runs handed to `Archive` when it is set, otherwise the runs moved to the table (`moved=3` above).
- `ArchivedRuns` pages by finish time, then run key, so runs that finished at the same instant are never skipped across a page boundary. A zero `Limit` is `workflow.DefaultRunsLimit`; a negative one, one over `workflow.MaxRunsLimit` or a cursor it did not issue is refused with `workflow.ErrInvalid`.

## Close the gaps with the Reconciler

The seams cannot see everything: a worker that died before its step ended, a run cancelled from an engine's UI, a run started from an engine's CLI, a run-finished event a best-effort driver lost. [`recorder.NewReconciler(client, store, opts...)`](ds:block?id=rec-newreconciler-su363hcv) closes those gaps:

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
fmt.Printf("reconcile: checked=%d finished=%d stale=%d\n", rep.Checked, rep.Finished, rep.Stale)
```

- **Check.** Each pass compares [a batch](ds:block?id=rec-defaultreconcilebatch-6zk3gwwh) of unfinished records (least recently checked first) with the client. A run the client reports terminal is finished on the record; a run the client still reports unfinished takes the client's status, and is flagged `Stale` when nothing was recorded for it for [`StaleAfter`](ds:block?id=rec-withstaleafter-94d6u87k); a run the client does not know is flagged `Stale`. Nothing is ever cancelled: `Stale` is for an operator, and any later write for the run clears it.
- **Backfill.** When the client is a `workflow.RunQuerier`, each pass lists its runs enqueued within the backfill window ([`WithBackfillWindow`](ds:block?id=rec-withbackfillwindow-tb9q9u8e), [an hour](ds:block?id=rec-defaultbackfillwindow-7nsg79u2) by default; [`WithBackfillQuery`](ds:block?id=rec-withbackfillquery-qc3kqct7) scopes it to a workflow, tenant or labels on an engine other apps share) and upserts them. A run the recorder never saw gets a record with source `external`; one it holds is left as recorded. A delayed run recorded under its `ScheduledID` is joined with its `RunID`.

Run it with [`go rc.Run(ctx)`](ds:block?id=rec-reconciler-run-hkpv3q3x) (every [30 seconds](ds:block?id=rec-defaultreconcileinterval-6wpsuxsy) by default) on one node or several: every write is an upsert, so two reconcilers never conflict.

### Tune the Reconciler

| Option | Default | What it changes |
|---|---|---|
| [`WithInterval(d)`](ds:block?id=rec-withinterval-xygvejf4) | [30 seconds](ds:block?id=rec-defaultreconcileinterval-6wpsuxsy) | The pause between passes of `Run` |
| [`WithStaleAfter(d)`](ds:block?id=rec-withstaleafter-94d6u87k) | [10 minutes](ds:block?id=rec-defaultstaleafter-ecr2t7eq) | How long an unfinished run may go with nothing recorded before it is flagged `Stale` |
| [`WithBatch(n)`](ds:block?id=rec-withbatch-7wqg86rr) | [100](ds:block?id=rec-defaultreconcilebatch-6zk3gwwh) | Unfinished records checked per pass, and the page size of the backfill listing (1 to `workflow.MaxRunsLimit`) |
| [`WithBackfill(on)`](ds:block?id=rec-withbackfill-euvuqmgs) | on | Whether a pass lists the client's runs at all |
| `WithBackfillWindow(d)` | [an hour](ds:block?id=rec-defaultbackfillwindow-7nsg79u2) | How far back the listing reaches: longer than the longest store outage you expect |
| [`WithBackfillPages(n)`](ds:block?id=rec-withbackfillpages-eyrfe4qc) | [10](ds:block?id=rec-defaultbackfillpages-mejfjkfb) | The pages one backfill pass reads at most, so a large window cannot make a pass unbounded |
| `WithBackfillQuery(q)` | every run | The workflows, tenant or labels the listing is scoped to |
| [`WithReconcileClock(c)`](ds:block?id=rec-withreconcileclock-9n7x2xtd) | the real clock | The clock ages and the window are measured on: the recorder's |
| [`WithReconcileOnError(fn)`](ds:block?id=rec-withreconcileonerror-muvwebjc) | dropped by `Pass`, logged by `Run` | Told about each run whose check or backfill failed; the pass goes on and counts it in `Errors` |

After the maintenance in [Be told about a failed write](#be-told-about-a-failed-write), the example runs one pass with every option set:

<!-- snippet: examples/recorder-custom-store/main.go#reconciler -->
```go
rc, err := recorder.NewReconciler(c, store,
	recorder.WithReconcileClock(clk),            // the recorder's clock
	recorder.WithBatch(reconcileBatch),          // records checked per pass, and the backfill page size
	recorder.WithBackfill(true),                 // the default: list the client's recent runs too
	recorder.WithBackfillWindow(backfillWindow), // how far back the listing reaches
	recorder.WithBackfillPages(backfillPages),   // and how many pages one pass reads at most
	recorder.WithStaleAfter(staleAfter),
	recorder.WithInterval(reconcileEvery), // between passes of rc.Run
	recorder.WithReconcileOnError(func(_ context.Context, err error) {
		fmt.Fprintln(w, "reconcile error:", err) // one run's failure; the pass goes on
	}),
)
if err != nil {
	return err
}
rep, err := rc.Pass(ctx) // go rc.Run(ctx) next to the client in an app
if err != nil {
	return err
}
fmt.Fprintf(w, "reconcile: checked=%d finished=%d backfilled=%d errors=%d\n", rep.Checked, rep.Finished, rep.Backfilled, rep.Errors)
```

```text
-- maintenance over: one Reconciler pass
reconcile: checked=0 finished=0 backfilled=2 errors=0
second run in the history: succeeded source=api/order-2 steps=0
```

`backfilled` counts every run the listing returned (here both), not only new ones: the first run was already recorded and is left as it was, because upserts only fill. The second run gets its record back from the listing, with its source from its labels and its end, but no steps: step detail is only written by the step seam.

## Per driver

The recorder is the same on every driver; what differs is which run ends the driver delivers and what its store or engine forgets. The recorder's end-to-end module, `recorder/e2e`, runs the shared driver cases of [`recordertest.RunDriver`](ds:block?id=rectest-rundriver-y5s57z34) (a run with its source, steps, events and outcome; retries and failure; redaction; a delayed run cancelled before it starts; the Reconciler's backfill and check) on each driver below but hybrid, and the web-node split on the SQL driver and on Hatchet. Each service's cases skip without its connection variable (`WFTEST_PG_DSN`, `WFTEST_MYSQL_DSN`, `WFTEST_REDIS_URL`, `HATCHET_CLIENT_TOKEN`); SQLite always runs.

| Driver | Run ends reach the recorder | Store in the e2e suite | Notes |
|---|---|---|---|
| [memory](../drivers/memory.md#record-run-history) | Every end, at least once | `sqlrecorder` on SQLite, Postgres and MySQL | Runs vanish on exit; records in a SQL store do not |
| [sqlstore](../drivers/sqlstore.md#record-run-history) | Every end, at least once | `sqlrecorder` in the driver's own database | `EnqueueTx` passes no enqueue middleware; enqueue-only nodes take `EnqueuePlugin` |
| [redis](../drivers/redis.md#record-run-history) | Every end, at least once | [`recorder.MemoryStore`](ds:block?id=rec-memorystore-smz35v44) | Finished runs expire after the driver's retention; the record stays |
| [hatchet](../drivers/hatchet.md#record-run-history) | Best effort: run the Reconciler | `recorder.MemoryStore`; `sqlrecorder` on SQLite for the split | The record keeps every attempt's error, which the driver does not report |
| [hybrid](../drivers/hybrid.md#record-run-history) | The jobs client's | not run on its own | Cron runs are recorded from their first step, as `schedule` |

The same cases run on your driver, or with your store, from a test of your own. `RunDriver` takes a [`workflowtest.Harness`](../extending/writing-a-driver.md#run-the-conformance-suite) that builds a fresh client per case, and [`recordertest.WithStore`](ds:block?id=rectest-withstore-uh7nyewq), one of its [`recordertest.DriverOption`](ds:block?id=rectest-driveroption-rbmxvkrq)s, records into stores your function builds instead of the in-memory default. The function is a [`recordertest.NewStore`](ds:block?id=rectest-newstore-ppek35b3), `func(t, cfg recordertest.StoreConfig) recorder.Store`; [`StoreConfig`](ds:block?id=rectest-storeconfig-ehg5t6ys) carries the `Retention` and `Clock` a case wants, which a store without retention ignores:

<!-- snippet: examples/recorder-custom-store/main_test.go#contract-driver -->
```go
// TestRecorderOnMemoryDriver runs the recorder's driver cases (source,
// steps, events and outcome; retries and failure; redaction; a delayed run
// cancelled before it starts; the Reconciler's backfill and check) on the
// memory driver, recording into docStore instead of the default store.
func TestRecorderOnMemoryDriver(t *testing.T) {
	recordertest.RunDriver(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC))
		return workflowtest.Instance{Client: memory.New(memory.WithClock(clk)), Clock: clk}
	}}, recordertest.WithStore(func(t *testing.T, _ recordertest.StoreConfig) recorder.Store {
		return newDocStore()
	}))
}
```

## The SQL store

[`sqlrecorder.New(db, dialect, opts...)`](ds:block?id=sqlrec-new-yw65j5ct) works on [Postgres 13+, MySQL 8 and SQLite 3.35+](ds:block?id=sqlrec-dialect-kbxe8aw4) through `database/sql` alone. Its tables, under the prefix [`wf_rec_`](ds:block?id=sqlrec-defaulttableprefix-af54bca4) ([`WithTablePrefix`](ds:block?id=sqlrec-withtableprefix-avt2n8sd)):

| Table | One row per | Indexed for |
|---|---|---|
| `runs` | run | listing order, workflow, status, tenant, source, `Pending`, retention |
| `steps` | step attempt | the run's steps |
| `events` | step event | the attempt's timeline |
| `labels` | run label | label and label-prefix filters |
| `schema` | applied schema version | `Migrate` |

Pick the dialect from the same setting that picked your database driver, and refuse an unknown one before anything is opened. [`sqlrecorder.Dialects()`](ds:block?id=sqlrec-dialects-f8qtn3z5) lists the three ([`Postgres`](ds:block?id=sqlrec-postgres-h97qy2kb), [`MySQL`](ds:block?id=sqlrec-mysql-pq4wcncq), [`SQLite`](ds:block?id=sqlrec-sqlite-pa63vhwe)); `New` itself refuses any other value with `workflow.ErrInvalid`:

<!-- snippet: examples/recorder-sql/main.go#dialect -->
```go
// dialectFor turns a configuration value into a sqlrecorder.Dialect,
// refusing an engine the store does not support before anything is opened.
func dialectFor(name string) (sqlrecorder.Dialect, error) {
	for _, d := range sqlrecorder.Dialects() {
		if string(d) == name {
			return d, nil
		}
	}
	return "", fmt.Errorf("history database %q: want one of %v", name, sqlrecorder.Dialects())
}
```

Steps and events are rows of their own, so a busy run never rewrites one large row per event. Times are Unix nanoseconds in UTC (0 for unset), identifiers compare byte by byte on every engine, and every write is one transaction that locks the run's row first and is [retried on contention](ds:block?id=sqlrec-defaultretryable-h3ustvp4). [`Migrate`](ds:block?id=sqlrec-store-migrate-63gjrdbn) is idempotent and safe to run on every start: it records [`SchemaVersion`](ds:block?id=sqlrec-schemaversion-scf4u7mm) in the `schema` table and refuses a database whose recorded version is newer than the release running it; [`MigrationScripts`](ds:block?id=sqlrec-store-migrationscripts-xbepgg67) returns its statements for your own migration tool.

A write transaction that hits contention (two first writes of one run racing, a deadlock, a serialization failure, a busy SQLite file) runs again, up to [`WithTxAttempts(n)`](ds:block?id=sqlrec-withtxattempts-dqc6w5mx) times ([8](ds:block?id=sqlrec-defaulttxattempts-a3fk8dhr) by default). Which errors count as contention is [`DefaultRetryable`](ds:block?id=sqlrec-defaultretryable-h3ustvp4): it matches the error text of each engine (`40001`, `40P01`, `deadlock`, `Error 1213`, `database is locked`, the unique violations and more), so the module needs no driver's error types. A driver or a proxy whose errors read differently adds its own with [`WithRetryable`](ds:block?id=sqlrec-withretryable-cx2mmux2); keep `DefaultRetryable` in it, as here:

<!-- snippet: examples/recorder-sql/main.go#retryable -->
```go
// retryable is DefaultRetryable plus the proxy's own contention error, for
// sqlrecorder.WithRetryable.
func retryable(err error) bool {
	return sqlrecorder.DefaultRetryable(err) || (err != nil && strings.Contains(err.Error(), proxyBusy))
}
```

```text
dialects: [postgres mysql sqlite]
refused: history database "oracle": want one of [postgres mysql sqlite]
retried: sqlite busy true | proxy busy true | syntax error false
migration statements: 15, tables: hist_events hist_labels hist_runs hist_schema hist_steps
```

The SQL store's whole set-up, with the table prefix, the retries, the clock and the retention, is the `store` snippet under [Retention](#retention).

## Bring your own store

[`recorder.Store`](ds:block?id=rec-store-pgu7xhxs) is an interface: any database can hold the history. The program is [examples/recorder-custom-store](../../examples/recorder-custom-store/); its `docStore` keeps one document per run, the shape of a store over a document database.

### Step 1: load, apply the shared rule, save

The `Apply*` functions of the `recorder` package ([`ApplyRunWrite`](ds:block?id=rec-applyrunwrite-f6w7phc8) and the rest) hold the merge rules (fill and never overwrite, the earliest enqueue and start, a terminal status changed only by a finish or a new attempt, the join of a delayed run's `ScheduledID` and `RunID` with [`MergeRecords`](ds:block?id=rec-mergerecords-d5syda8z)), so your store merges a write exactly as the bundled ones do. Each write loads the run's document, applies the rule and saves it, in one transaction:

<!-- snippet: examples/recorder-custom-store/store.go#store -->
```go
// docStore is a recorder.Store of the app's own, shaped like a store over a
// document database: one document per run (the run with its steps and their
// events) under the run's key, and a secondary index from a delayed run's
// ScheduledID to that key. Every write is the same three moves in one
// transaction: load the document, apply the recorder's merge rule for the
// write (the recorder.Apply* functions), save it. The rules are shared, so
// this store merges a write exactly as the bundled stores do, and
// recordertest.RunStore holds it to their contract (see main_test.go).
//
// Here the "database" is a map behind a mutex; over a real one, update is
// a transaction that reads the document, runs fn and writes it back.
type docStore struct {
	mu      sync.Mutex
	docs    map[string]recorder.RunRecord
	bySched map[workflow.ScheduledID]string
}

var _ recorder.Store = (*docStore)(nil)

// newDocStore returns an empty store.
func newDocStore() *docStore {
	return &docStore{docs: map[string]recorder.RunRecord{}, bySched: map[workflow.ScheduledID]string{}}
}

// update is one write: it loads the run's document (joining a record kept
// under the ScheduledID into the one under the RunID when ref carries
// both, as recorder.MergeRecords says), hands it to fn, and saves it under
// its key, moving it off a ScheduledID-only key once it has a RunID.
func (s *docStore) update(ref workflow.RunRef, fn func(r *recorder.RunRecord, isNew bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var byRun, bySched *recorder.RunRecord
	if !ref.RunID.IsZero() {
		if d, ok := s.docs[recorder.KeyOf(workflow.RunRef{RunID: ref.RunID})]; ok {
			byRun = &d
		}
	}
	if k, ok := s.bySched[ref.ScheduledID]; ok && !ref.ScheduledID.IsZero() {
		if d, ok := s.docs[k]; ok {
			bySched = &d
		}
	}
	var r recorder.RunRecord
	isNew := false
	switch {
	case byRun != nil && bySched != nil && byRun.Key() != bySched.Key():
		delete(s.docs, bySched.Key())
		r = recorder.MergeRecords(*byRun, *bySched)
	case byRun != nil:
		r = *byRun
	case bySched != nil:
		r = *bySched
	default:
		r, isNew = recorder.NewRecord(ref), true
	}
	fn(&r, isNew)
	key := r.Key()
	if !r.Ref.ScheduledID.IsZero() {
		if old := recorder.KeyOf(workflow.RunRef{ScheduledID: r.Ref.ScheduledID}); old != key {
			delete(s.docs, old)
		}
		s.bySched[r.Ref.ScheduledID] = key
	}
	s.docs[key] = r.Clone() // the document owns its data, never the caller's slices
}
```

### Step 2: one rule per write

Every write method is a check ([`recorder.CheckRef`](ds:block?id=rec-checkref-mdjaph5q) refuses a write that names no run, [`recorder.CheckStep`](ds:block?id=rec-checkstep-9z4s2dzb) a step write without a `RunID`, a step name or an attempt from 1, both with `workflow.ErrInvalid`) and one call to `update` with its rule. The other four writes follow the same pattern; the file has all of them:

<!-- snippet: examples/recorder-custom-store/store.go#writes -->
```go
// UpsertRun implements recorder.Store.
func (s *docStore) UpsertRun(_ context.Context, w recorder.RunWrite) error {
	if err := recorder.CheckRef("UpsertRun", w.Ref); err != nil {
		return err
	}
	s.update(w.Ref, func(r *recorder.RunRecord, isNew bool) { recorder.ApplyRunWrite(r, isNew, w) })
	return nil
}

// StartStep implements recorder.Store.
func (s *docStore) StartStep(_ context.Context, st recorder.StepStart) error {
	if err := recorder.CheckStep("StartStep", st.Ref, st.Step, st.Attempt); err != nil {
		return err
	}
	s.update(st.Ref, func(r *recorder.RunRecord, _ bool) {
		i := attempt(r.Steps, st.Step, st.Attempt)
		var existing *recorder.StepRecord
		if i >= 0 {
			existing = &r.Steps[i]
		}
		recorder.ApplyStepStart(r, recorder.StartsNewAttempt(existing), st)
		if i < 0 {
			r.Steps = append(r.Steps, recorder.NewStepRecord(st))
		} else {
			recorder.ResetStepRecord(&r.Steps[i], st) // a repeated start keeps the attempt's events
		}
		recorder.SortSteps(r.Steps)
	})
	return nil
}
```

### Step 3: answer listings the shared way

`Records` checks the query with [`recorder.ValidateQuery`](ds:block?id=rec-validatequery-nz2h9e24), filters with [`recorder.Query.Match`](ds:block?id=rec-query-match-hevjbnr4), orders with [`recorder.SortRecords`](ds:block?id=rec-sortrecords-7cv25nfq) (newest enqueue first) and pages with `recorder.EncodeCursor`, so a page reads the same as on the bundled stores; a database store turns the same filters into its own query. `Runs` is `Records` through [`recorder.InfoPage`](ds:block?id=rec-infopage-zbeybkpx), which makes the store a `workflow.RunQuerier` the admin API can read; `Pending` sorts with [`recorder.SortPending`](ds:block?id=rec-sortpending-6uhd5bsp) (never checked first, then least recently checked) for the Reconciler:

<!-- snippet: examples/recorder-custom-store/store.go#records -->
```go
// Records implements recorder.Store: the filters are recorder.Query.Match,
// the order recorder.SortRecords, the cursor recorder.EncodeCursor, so a
// page reads the same as on the bundled stores. A database store turns the
// same filters into its own query.
func (s *docStore) Records(_ context.Context, q recorder.Query) (recorder.RecordPage, error) {
	if err := recorder.ValidateQuery(q); err != nil {
		return recorder.RecordPage{}, err
	}
	var after *recorder.Cursor
	if q.Cursor != "" {
		c, err := recorder.DecodeCursor(q.Cursor)
		if err != nil {
			return recorder.RecordPage{}, err
		}
		after = &c
	}
	s.mu.Lock()
	var page []recorder.RunRecord
	for _, r := range s.docs {
		if q.Match(r) && (after == nil || after.After(r)) {
			c := r.Clone()
			c.Steps = nil // listings carry no steps
			page = append(page, c)
		}
	}
	s.mu.Unlock()
	recorder.SortRecords(page)
	out := recorder.RecordPage{Runs: page}
	if limit := q.PageLimit(); len(page) > limit {
		out.Runs = page[:limit]
		last := out.Runs[limit-1]
		out.Next = recorder.EncodeCursor(recorder.Cursor{EnqueuedAt: last.EnqueuedAt, Key: last.Key()})
	}
	return out, nil
}
```

### Step 4: prove it

The `Apply*` functions of the `recorder` package hold the merge rules, so your store merges a write exactly as the bundled ones do, and [`recordertest.RunStore(t, newStore)`](ds:block?id=rectest-runstore-w3fd43ef) holds it to the same contract they pass (fill-only upserts, attempts, events, outcome, finish and re-open, the join of a delayed run's two IDs, every filter, paging, `Pending`, copies on read, concurrent writes, retention). It also checks the refusals, and runs the retention cases only when the store is a `recorder.Pruner`. `recordertest.RunDriver` with `WithStore` then runs the recorder over a real driver into it ([Per driver](#per-driver)):

<!-- snippet: examples/recorder-custom-store/main_test.go#contract-store -->
```go
// TestStoreContract holds docStore to the recorder.Store contract the
// bundled stores pass: fill-only upserts, attempts, events, outcome,
// finish and re-open, the join of a delayed run's two IDs, every filter,
// paging, Pending, copies on read, concurrent writes and the refusals.
// docStore is no recorder.Pruner, so the retention cases skip.
func TestStoreContract(t *testing.T) {
	recordertest.RunStore(t, func(t *testing.T, _ recordertest.StoreConfig) recorder.Store {
		return newDocStore()
	})
}
```

```sh
cd examples
go test -v -run 'Contract|Driver' ./recorder-custom-store
```

Every case passes; `prune_two_stages`, `prune_archive_failure` and `prune_cancelled_archives_once` skip, because `docStore` has no retention.

## Pitfalls

- Registering on the bare driver instead of the client `Use` returned skips the [step middleware](ds:block?id=rec-recorder-stepmiddleware-tbta9pzy): those runs get a record only from the run-finished observer and the Reconciler.
- A delayed run is recorded under its `ScheduledID` at enqueue and under its `RunID` once it starts; the two records are [joined](ds:block?id=rec-mergerecords-d5syda8z) by the first write that carries both IDs (its run-finished event, or a Reconciler pass). Until then a listing can show both.
- On a driver without `workflow.CapRunFinished` ([Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)), a run end can be missed: run the Reconciler.
- The recorder's history is not the driver's: [`admin.WithRunQuerier`](ds:block?id=admin-withrunquerier-g356xuy3) shows the recorder, while a retry or a cancel acts on the client. A run the engine already pruned can be shown but no longer retried.
- **`sqlrecorder`'s `Archive` callback sees a run at least once; `MemoryStore`'s exactly once; `WithArchiveTable` moves it exactly once.** Cancelling a prune no longer repeats a batch: once [`Archive`](ds:block?id=sqlrec-store-prune-843j2gjz) accepted it, its delete runs even after the context is cancelled (bounded at [30 seconds](ds:block?id=sqlrec-archiveddeletetimeout-66323gs2)). A batch still comes again when the process dies or the delete fails between `Archive` returning and the delete committing, or when two processes prune the same tables at once, because an outside archive and the database cannot commit together. A repeat carries the same records (same `RunRef` and `Key`), so key what the archive writes by the run and a second copy replaces the first. `MemoryStore` runs one prune at a time and deletes what `Archive` accepted at once, so it hands each run over once. Where the archive can live in the same database, [`WithArchiveTable`](#archive-into-a-table-exactly-once) commits the archive copy with the delete, so nothing repeats.
- **A failed `SetOutcome` is reported twice, on purpose.** The hook hears it as [`OpSetOutcome`](ds:block?id=rec-opsetoutcome-c9naecw4), and `workflow.SetOutcome` also returns the error to the step that called it, which asked for the outcome to be kept; a store panic there becomes that error, as on every other write.
- **A [`Redactor`](ds:block?id=rec-redactor-x3e2p5bt) must not change the slice it gets.** Return a new one; the body it was handed is the step's own input or output.
- **The backfill window must outlast an outage.** A run that started and ended while the store was down for longer than [`WithBackfillWindow`](ds:block?id=rec-withbackfillwindow-tb9q9u8e) is never listed again, so it never gets a record.
- `RecordEvent` and the recorder's writes run on the step's goroutine: a slow store slows steps, up to [`WithWriteTimeout`](ds:block?id=rec-withwritetimeout-rn35n8vw) ([5 seconds](ds:block?id=rec-defaultwritetimeout-wbngg5vx) by default) per write.

## Related

- [React when a run finishes](run-finished.md): the hook the recorder observes run ends with.
- [Labels](../concepts/labels.md): the reserved prefixes, and the two source labels an app may set.
- [Admin operations](../admin/operations.md): the API `admin.WithRunQuerier` points at the recorder.
- [Retention](../operations/retention.md): the SQL driver's own retention, separate from the recorder's.
- [Write a plugin](../extending/writing-a-plugin.md): the seams the recorder is built from.
- [Trigger source](../concepts/trigger-source.md), [Step events](../concepts/step-events.md), [Run outcome](../concepts/run-outcome.md): the run metadata the recorder keeps.
- Examples: [run-recorder](../../examples/run-recorder/), [recorder-split](../../examples/recorder-split/) (a web node and a worker), [recorder-custom-store](../../examples/recorder-custom-store/) (redaction, the error hook, the Reconciler, a store of your own), [recorder-sql](../../examples/recorder-sql/) (dialects, retries, retention, the archive table), [step-events](../../examples/step-events/).
