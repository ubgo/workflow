# Queue alerts

`admin.Monitor` watches each queue's depth and the age of its oldest waiting run, and calls your `Alerter` when one goes over a threshold and again when it is back under. It polls the [admin queue report](operations.md#queue-depth-and-oldest-wait) on an injected clock, so it works on any driver with `CapRunQuery` and needs no metrics backend. For dashboards, the same numbers are an [OpenTelemetry gauge and histogram](../observability/opentelemetry.md#queue-wait-and-queue-depth).

## Before you start

- An `*admin.Admin` over a client with `CapRunQuery` ([Admin operations](operations.md)). `NewMonitor` refuses a client without it.
- Somewhere to send alerts: a chat webhook, a pager, a log.
- The runnable program for this page is [examples/admin](../../examples/admin/).

## Step 1: set thresholds and an alerter

An `admin.Threshold` names a `Queue` (empty: the default for every queue without its own), a `MaxDepth` and a `MaxWait`; a zero field is not checked. `admin.AlerterFunc` adapts a function to the `admin.Alerter` interface.

<!-- snippet: examples/admin/main.go#monitor -->
```go
m, err := admin.NewMonitor(adm, admin.AlerterFunc(func(_ context.Context, a admin.Alert) {
	state := "ALERT"
	if a.Resolved {
		state = "resolved"
	}
	fmt.Fprintf(out, "%s %s queue=%s depth=%d (max %d)\n", state, a.Kind, a.Queue, a.Depth, a.Threshold.MaxDepth)
}), []admin.Threshold{{Queue: "emails", MaxDepth: 2}, {MaxDepth: 1000, MaxWait: 10 * time.Minute}},
	admin.WithInterval(30*time.Second))
if err != nil {
	return err
}
// go m.Run(ctx) polls every 30s until ctx ends; Check polls once.
if err := m.Check(ctx); err != nil {
	return err
}
```

An `admin.Alert` carries the `Kind` (`admin.AlertQueueDepth` or `admin.AlertQueueWait`), the `Queue`, its `Depth` and `Wait` at that poll, the `Threshold` crossed, `Resolved` (true on the alert saying it is back under), `Truncated` (the depth is a lower bound) and `At`.

`NewMonitor` refuses, with `workflow.ErrInvalid`, a nil alerter, no thresholds, a negative field, a threshold that checks nothing, an invalid queue name and two thresholds for one queue.

## Step 2: run it

`Monitor.Run(ctx)` polls every `admin.WithInterval` (30 seconds by default, `admin.DefaultMonitorInterval`) on the Admin's clock until `ctx` ends; nothing runs before it is called. `Monitor.Check(ctx)` polls once, for tests or a cron job that alerts on its own schedule.

- An alert is sent once when a queue crosses a threshold and once when it resolves, including when the queue empties and disappears from the report. `admin.WithRepeat(d)` re-sends it every `d` while it stays over.
- A poll error (the database is down) goes to `admin.WithErrorHandler` and the next poll still runs. A panic in the alerter is recovered and reported there too.
- The alerter runs on the Monitor's goroutine, one call at a time; a slow one delays the next poll.

## Try it

```sh
cd examples
go run ./admin
```

```text
anonymous GET /failures: 401
viewer GET /failures: 200, 1 failed run, step "send": memory: step "send" failed after 1 attempt(s): smtp: connection refused
viewer POST retry: 403 {"error":{"code":"forbidden","message":"not allowed: runs.retry"}}
admin POST retry: 200, run now succeeded
admin POST /pause emails: 200
ALERT queue_depth queue=emails depth=3 (max 2)
workflow.queue.depth{workflow.queue=emails} = 3
admin POST /resume emails: 200
resolved queue_depth queue=emails depth=0 (max 2)
```

The `ALERT` line comes from the paused `emails` queue holding three runs against a `MaxDepth` of 2; after the resume drains it, the next `Check` reports it resolved.

## Pitfalls

- **One Monitor per deployment.** Which alerts are open lives in the Monitor's memory, so a Monitor on every node sends every alert once per node.
- **A truncated depth is a lower bound.** With more queued runs than the max scan, `Depth` stops at the scan limit and the oldest run may be missed; raise `admin.WithMaxScan` or alert on the gauge instead.
- **The wait is the oldest queued run's age.** A paused queue's wait grows on purpose; give a queue you pause routinely its own, looser threshold.

## Related

- [Admin operations](operations.md)
- [OpenTelemetry](../observability/opentelemetry.md#queue-wait-and-queue-depth)
- Example: [examples/admin](../../examples/admin/)
