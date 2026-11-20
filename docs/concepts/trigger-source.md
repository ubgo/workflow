# Trigger source

A run's trigger source says what started it: a schedule, an operator pressing "run now", a parent run, a batch, an event, or a name your app chooses (`api`, `webhook`, `slack`, `admin`). It is plain run metadata kept in two reserved labels, so every driver stores and returns it with no change, and a runs page, a support tool or a [run recorder](../observability/run-recorder.md) can tell "fired at 02:00" apart from "a webhook asked for it". This page shows how to set it, how the library derives it when you did not, how to read it, and how to refuse a job that has none.

## Before you start

- You know [labels](labels.md): `Job.Labels` travels with a run and reaches every step and run-finished observer.
- The runnable program for this page is [examples/trigger-source](../../examples/trigger-source/): the in-memory driver with a fake clock, so a 02:00 cron fires at once.

## Step 1: name the source of a job

[`workflow.WithSource(job, source, ref)`](ds:block?id=withsource-g5xwt2m5) returns a copy of the job whose labels carry the source and an optional reference within it (a webhook delivery ID, a chat thread, the user who pressed a button). The caller's `Labels` map is not changed. An empty `ref` removes a reference the job carried.

<!-- snippet: examples/trigger-source/main.go#enqueue -->
```go
_, err := c.Enqueue(ctx, workflow.Job{Workflow: "nightly-report"})
fmt.Println("no source:", err)

_, err = c.Enqueue(ctx, workflow.WithSource(workflow.Job{Workflow: "nightly-report"}, "webook", ""))
fmt.Println("typo:", err)

job := workflow.WithSource(workflow.Job{Workflow: "nightly-report"}, "webhook", "delivery-42")
if _, err := c.Enqueue(ctx, job); err != nil {
	return err
}
```

The first two enqueues are refused by [`middleware.RequireSource`](ds:block?id=mw-requiresource-3tsntxp2) ([Step 4](#step-4-refuse-a-job-without-a-source)); the third is accepted.

The two labels are [`workflow.LabelSource`](ds:block?id=labelsource-m6wjahhr) and [`workflow.LabelSourceRef`](ds:block?id=labelsourceref-qfv7bbhp), both under the reserved prefix `ubgo.workflow/`. They are the only reserved labels an app may set itself: every driver's `Enqueue` lets them through [`workflow.CheckJobLabels`](ds:block?id=checkjoblabels-p6m28b9h) when their values are valid, and refuses them with `workflow.ErrInvalid` otherwise.

## Step 2: keep source names valid

A source must pass [`workflow.ValidateSource`](ds:block?id=validatesource-73nzwq5k): non-empty, at most [`workflow.MaxSourceLen`](ds:block?id=maxsourcelen-sx2rxxmg) (64) bytes, only lower-case letters, digits and `. _ -`, so it is safe as a filter value, a metric label and a URL segment. A reference is any text up to [`workflow.MaxSourceRefLen`](ds:block?id=maxsourcereflen-gygg9xx4) (256) bytes. `WithSource` itself never fails: an invalid source is refused when the job is enqueued, and `ValidateSource` lets a caller check earlier. Its error wraps `workflow.ErrInvalid`.

<!-- snippet: examples/trigger-source/main.go#validate -->
```go
fmt.Println("validate:", workflow.ValidateSource("Slack"))
fmt.Println("library sources:", workflow.LibrarySources())
```

## Step 3: read the source back

[`workflow.SourceOf(labels)`](ds:block?id=sourceof-7mrryr2k) returns the source and its reference. An explicit `LabelSource` wins. Otherwise it derives one from the labels the library already stamps, most specific first, and returns `""` when nothing says where the run came from:

| Source | Constant | Derived from | Reference |
|---|---|---|---|
| `schedule-manual` | [`workflow.SourceScheduleManual`](ds:block?id=sourceschedulemanual-rjzgkf9k) | `LabelTrigger` = `manual` (`TriggerSchedule`) | the schedule ID |
| `schedule` | [`workflow.SourceSchedule`](ds:block?id=sourceschedule-q6meeq7a) | `LabelSchedule` (a schedule or a declared cron fired the run) | the schedule ID |
| `child` | [`workflow.SourceChild`](ds:block?id=sourcechild-yqm4kgby) | `LabelParentRun` (a step spawned the run) | the parent run ID |
| `batch` | [`workflow.SourceBatch`](ds:block?id=sourcebatch-3pgw8v9a) | the batch ID label (`batch`) | the batch ID |
| `event` | [`workflow.SourceEvent`](ds:block?id=sourceevent-4czra9fx) | the event label (`events.Bus`) | the event name |
| `external` | [`workflow.SourceExternal`](ds:block?id=sourceexternal-pq4saxv5) | never derived: a run recorder's Reconciler gives it to a run it found on the engine but never saw enqueued | |

[`workflow.LibrarySources()`](ds:block?id=librarysources-crdb5g3y) returns these six as a fresh slice. A step reads the source from its `StepInfo.Labels`, and a run-finished observer from `RunEvent.Labels`:

<!-- snippet: examples/trigger-source/main.go#step -->
```go
report := func(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	src, ref := workflow.SourceOf(info.Labels) // set by the app, or derived from the library's labels
	seen <- fmt.Sprintf("source=%s ref=%s", src, ref)
	return nil, nil
}
```

In the example the same step runs three times: from the webhook job (`webhook`, `delivery-42`), from the 02:00 cron fire (`schedule`), and from `TriggerSchedule` on the same cron (`schedule-manual`). The cron and the manual trigger never pass through enqueue middleware; their source comes from the labels the driver stamped.

## Step 4: refuse a job without a source

`middleware.RequireSource(allowed...)` is enqueue middleware that refuses, with an error wrapping `workflow.ErrInvalid`, a job whose source cannot be told (`SourceOf` returns `""`). With allowed sources given, the job's source must also be one of them, so a typo is refused at the enqueue site instead of creating a filter value nobody looks for:

<!-- snippet: examples/trigger-source/main.go#require -->
```go
// Every enqueue through c must name a source, and one of these three.
c := middleware.WrapClient(mem, middleware.RequireSource("api", "webhook", "admin"))
```

A derived source counts: a job carrying a library label (a schedule fire, a spawned child, a batch member, an event) passes the bare `RequireSource()`. With an allow-list, list the derived sources you expect to reach this client too. More on where the middleware sits in [Enqueue middleware › Require a trigger source](../middleware/enqueue-middleware.md#require-a-trigger-source).

## Try it

```sh
cd examples
go run ./trigger-source
```

```text
no source: workflow: invalid spec: job for "nightly-report" has no source; set one with workflow.WithSource
typo: workflow: invalid spec: job for "nightly-report" has source "webook", not one of [api webhook admin]
webhook: source=webhook ref=delivery-42
cron fire: source=schedule ref=cron:nightly-report:0
run now: source=schedule-manual ref=cron:nightly-report:0
validate: workflow: invalid spec: source "Slack" has the character 'S'; use lower-case letters, digits and . _ -
library sources: [schedule schedule-manual child batch event external]
```

`go test ./trigger-source` checks every line.

## Pitfalls

- **A source is provenance, never an authorization input.** Anything that can enqueue can claim any source; never grant or skip a check because a run says it came from `admin`.
- **`RequireSource` sees only the jobs it wraps.** A cron fire, a chained job and a child are enqueued by the driver, past enqueue middleware, so they are never refused by it; a chained job's source is its own labels'.
- **A bad allow-list refuses everything.** Each allowed source must pass `ValidateSource`; if one does not, every enqueue returns that error, so the mistake shows on the first call.
- **An explicit source hides the derived one.** A job a step spawns with `WithSource` reads as that source, not `child`; the parent link is still in `LabelParentRun`.
- **Upper-case and spaces are refused.** `Slack` or `web hook` fails at `Enqueue`; pick `slack` or `web-hook`.

## Related

- [Labels › Source labels](labels.md#source-labels): the reserved prefix the two keys live under.
- [Record run history › Trigger source](../observability/run-recorder.md#trigger-source): filtering a run history by source.
- [Step events](step-events.md) and [Run outcome](run-outcome.md): the other run metadata a recorder keeps.
- [Trigger a schedule now](../schedules/overlap.md#trigger-a-schedule-now): where `schedule-manual` comes from.
- Example: [trigger-source](../../examples/trigger-source/).
