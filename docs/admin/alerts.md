# Queue alerts

`admin.Monitor` watches each queue's depth and the age of its oldest waiting run, and calls your `Alerter` when one goes over a threshold and again when it is back under. It polls the [admin queue report](operations.md#queue-depth-and-oldest-wait) on an injected clock, so it works on any driver with `CapRunQuery` and needs no metrics backend. For dashboards, the same numbers are an [OpenTelemetry gauge and histogram](../observability/opentelemetry.md#queue-wait-and-queue-depth).

## Before you start

- An `*admin.Admin` over a client with `CapRunQuery` ([Admin operations](operations.md)). `NewMonitor` refuses a client without it.
- Somewhere to send alerts: a chat webhook, a pager, a log.
- The runnable program for this page is [examples/admin-alerts](../../examples/admin-alerts/), with fake clocks so every time it prints is fixed. [examples/admin](../../examples/admin/) runs a Monitor beside the JSON API.

## Step 1: set thresholds and an alerter

An `admin.Threshold` names a `Queue` (empty: the default for every queue without its own), a `MaxDepth` and a `MaxWait`; a zero field is not checked. `admin.AlerterFunc` adapts a function to the `admin.Alerter` interface.

<!-- snippet: examples/admin-alerts/main.go#monitor -->
```go
// alerter turns each alert, raised or resolved, into one line.
alerter := admin.AlerterFunc(func(_ context.Context, a admin.Alert) {
	state, over := "ALERT", ">"
	if a.Resolved {
		state, over = "resolved", "<="
	}
	switch a.Kind {
	case admin.AlertQueueDepth:
		sent.add(fmt.Sprintf("%s %s %s %s: depth %d %s %d", a.At.Format(time.TimeOnly), state, a.Kind, a.Queue, a.Depth, over, a.Threshold.MaxDepth))
	case admin.AlertQueueWait:
		sent.add(fmt.Sprintf("%s %s %s %s: oldest wait %s %s %s", a.At.Format(time.TimeOnly), state, a.Kind, a.Queue, a.Wait, over, a.Threshold.MaxWait))
	}
})
m, err := admin.NewMonitor(adm, alerter,
	[]admin.Threshold{
		{Queue: queueEmails, MaxDepth: maxDepth, MaxWait: maxWait}, // this queue's own threshold
		{MaxDepth: anyDepth}, // the default for every queue without its own
	},
	admin.WithInterval(interval),
	admin.WithRepeat(repeat),
	admin.WithErrorHandler(func(err error) { // failed polls and alerter panics
		sent.add(fmt.Sprintf("%s monitor error: %v", monClk.Now().Format(time.TimeOnly), err))
	}),
)
if err != nil {
	return err
}
```

A queue's own threshold replaces the default entirely: the emails queue above checks depth and wait, every other queue only depth. An `admin.Alert` carries the `Kind` (`admin.AlertQueueDepth` or `admin.AlertQueueWait`, both of type `admin.AlertKind`), the `Queue`, its `Depth` and `Wait` at that poll, the `Threshold` crossed, `Resolved` (true on the alert saying it is back under), `Truncated` (the depth is a lower bound) and `At`.

`NewMonitor` refuses, with `workflow.ErrInvalid`, a nil alerter, no thresholds, a negative field, a threshold that checks nothing, an invalid queue name and two thresholds for one queue.

## Step 2: run it

`Monitor.Run(ctx)` polls every `admin.WithInterval` (30 seconds by default, `admin.DefaultMonitorInterval`) on the Admin's clock until `ctx` ends; nothing runs before it is called. `Monitor.Check(ctx)` polls once, for tests or a cron job that alerts on its own schedule, and returns a poll error to its caller.

<!-- snippet: examples/admin-alerts/main.go#run -->
```go
runCtx, stop := context.WithCancel(ctx)
done := make(chan error, 1)
go func() { done <- m.Run(runCtx) }() // returns when runCtx ends
```

- An alert is sent once when a queue crosses a threshold and once when it resolves, including when the queue empties and disappears from the report. `admin.WithRepeat(d)` re-sends it every `d` while it stays over.
- Under `Run`, a poll error (the database is down) goes to `admin.WithErrorHandler(func(error))` and the next poll still runs. A panic in the alerter is recovered and reported there too, from `Run` and from `Check`. Without a handler both are dropped.
- The alerter runs on the Monitor's goroutine, one call at a time; a slow one delays the next poll.

## Try it

```sh
cd examples
go run ./admin-alerts
```

```text
-- Check polls once
09:00:00 ALERT queue_depth emails: depth 3 > 2
09:11:00 ALERT queue_wait emails: oldest wait 11m0s > 10m0s
a threshold that checks nothing: true
two thresholds for one queue: true
-- Run polls every interval
09:11:30 monitor error: run store: connection refused
09:15:00 ALERT queue_depth emails: depth 3 > 2
09:16:00 resolved queue_depth emails: depth 0 <= 2
09:16:00 resolved queue_wait emails: oldest wait 0s <= 10m0s
Run returned: context canceled
```

The paused emails queue holds three runs: `Check` raises the depth alert at once and the wait alert after eleven minutes. Then `Run` takes over: its first poll meets the run store down and reaches the error handler, the poll at 09:15:00 re-sends the depth alert because `WithRepeat` is fifteen minutes and the wait alert is not due yet, and after the queue is resumed and drains both alerts resolve on one poll. The program prints each poll's alerts sorted, because alerts that resolve on one poll come in no fixed order.

## Pitfalls

- **One Monitor per deployment.** Which alerts are open lives in the Monitor's memory, so a Monitor on every node sends every alert once per node.
- **A truncated depth is a lower bound.** With more queued runs than the max scan, `Depth` stops at the scan limit and the oldest run may be missed; raise `admin.WithMaxScan` or alert on the gauge instead.
- **A queue's own threshold replaces the default.** It is not merged with it: give the queue every field you want checked, as the example's emails threshold does with `MaxDepth` and `MaxWait`.
- **Alerts resolving on one poll come in no fixed order.** Sort or key them in the alerter when the order matters.
- **The wait is the oldest queued run's age.** A paused queue's wait grows on purpose; give a queue you pause routinely its own, looser threshold.

## Related

- [Admin operations](operations.md)
- [OpenTelemetry](../observability/opentelemetry.md#queue-wait-and-queue-depth)
- Examples: [examples/admin-alerts](../../examples/admin-alerts/), [examples/admin](../../examples/admin/)
