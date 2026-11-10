# trigger-source

Where a run's trigger source comes from, on the in-memory driver with a fake clock. The client is wrapped with `middleware.RequireSource("api", "webhook", "admin")`, so a job with no source and a job whose source is a typo (`webook`) are refused before they reach the driver. A job made with `workflow.WithSource(job, "webhook", "delivery-42")` passes, and its step reads the source back with `workflow.SourceOf`. The 02:00 cron fires past the enqueue middleware and its run is derived as `schedule`; `TriggerSchedule` runs the same schedule now and its run is derived as `schedule-manual`. The last two lines show `workflow.ValidateSource` refusing an upper-case name and the sources the library itself names.

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

`go test ./trigger-source` runs the program and checks every line.

## Guides

- [Trigger source](../../docs/concepts/trigger-source.md)
- [Enqueue middleware › Require a trigger source](../../docs/middleware/enqueue-middleware.md#require-a-trigger-source)
