# run-recorder

A run history the app owns, on the in-memory driver: [`recorder.New`](ds:block?id=rec-new-wrqg5nug) over a SQLite store from `recorder/sqlrecorder`, applied with `workflow.Use` next to a one-line plugin adding [`middleware.RequireSource()`](ds:block?id=mw-requiresource-3tsntxp2). A job without a source is refused; a job from a webhook is recorded with its source and delivery ID, its two steps with their outputs, a timeline event, the partial outcome the second step set, and its input with the card number redacted. The program then reads the record back, lists runs by source and outcome, reads the same run through [`admin.WithRunQuerier`](ds:block?id=admin-withrunquerier-g356xuy3), and runs one Reconciler pass.

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

The redacted input is re-encoded, so its keys come out sorted. The Reconciler checks nothing here because the run already finished on the record; next to a driver that can lose a run end (Hatchet), it is what finishes such records.

The guide that copies this program is [record run history](../../docs/observability/run-recorder.md). `go test ./run-recorder` runs it on a temporary SQLite file and checks every line.
