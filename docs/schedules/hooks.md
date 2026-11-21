# Schedule hooks and pings

A scheduled task often needs code around it that is about the schedule, not about the work: tell a heartbeat service the nightly backup started and finished, page someone when the 09:00 report fails, count ticks a window skipped. Here every tick outcome is reported to tick observers, `workflow.ScheduleHooks` turns those reports and run ends into per-schedule callbacks, and `middleware.Pinger` makes heartbeat pings out of URLs.

## Before you start

- Tick observers need `workflow.CapTickEvents` (the client implements `workflow.TickNotifier`); run hooks need the run-finished hook (`workflow.RunNotifier`, guaranteed with `CapRunFinished`). The memory and SQL drivers have both. Hatchet's engine fires crons out of our sight, so it does not declare `CapTickEvents` and reports no ticks.
- Observers and hooks are registered before `Start`; afterwards registration returns `workflow.ErrAlreadyStarted`.
- The example is [`examples/schedule-hooks`](../../examples/schedule-hooks), on the memory driver with a fake clock and a local HTTP server standing in for the heartbeat service. Run it with `go run ./schedule-hooks` from `examples/`.

## Watch every tick

`TickNotifier.OnTick(fn)` registers a `workflow.TickObserver`, called for every tick of every schedule with a `workflow.Tick`:

<!-- snippet: examples/schedule-hooks/main.go#observer -->
```go
err = c.(workflow.TickNotifier).OnTick(func(_ context.Context, t workflow.Tick) {
	ticks <- t // every fired, skipped and misfired tick, from every schedule
})
```

`Tick.Outcome` is one of three:

| Outcome | Meaning | Fields |
|---|---|---|
| `TickFired` | the tick enqueued a run, or tried to and the enqueue failed | `Run`, or `Err` |
| `TickSkipped` | the tick was due and on time but did not fire | `Reason`: `TickReasonWhen` (`ScheduleWhen` said no), `TickReasonWindow` (outside the [windows](conditions.md)), `TickReasonPaused` ([paused](manage.md#pause-a-schedule)) |
| `TickMisfired` | the scheduler woke late and the [misfire policy](misfire.md) dropped ticks | `Missed` (how many), `At` (the newest dropped) |

An observer usually switches on the outcome and, for a skipped tick, on the reason. The example prints each tick with this function:

<!-- snippet: examples/schedule-hooks/main.go#outcome -->
```go
func describe(t workflow.Tick) string {
	switch t.Outcome {
	case workflow.TickFired: // a run was enqueued (t.Run), or the enqueue failed (t.Err)
		if t.Err != nil {
			return "fired, enqueue failed: " + t.Err.Error()
		}
		if t.Manual {
			return "fired by hand"
		}
		return "fired"
	case workflow.TickSkipped: // due and on time, but not fired
		switch t.Reason {
		case workflow.TickReasonWindow:
			return "skipped (window)" // outside the schedule's Windows
		case workflow.TickReasonWhen:
			return "skipped (when)" // ScheduleWhen returned false
		case workflow.TickReasonPaused:
			return "skipped (paused)" // SchedulePauser holds the schedule
		}
		return "skipped (" + t.Reason + ")"
	case workflow.TickMisfired: // the misfire policy dropped late ticks
		return fmt.Sprintf("misfired, %d dropped, newest %s", t.Missed, t.At.Format("15:04"))
	}
	return string(t.Outcome)
}
```

`Tick.Manual` is set on a `TickFired` that [trigger now](overlap.md#trigger-a-schedule-now) (`ScheduleTriggerer`) started by hand; `At` is then the driver's clock at the trigger. A misfired report stands for a batch of ticks, not one: `Missed` says how many were dropped and `At` is the newest.

Every tick also carries `ScheduleID`, `Name` (for a declared cron, its expression), `Workflow`, `Declared`, `At`, `Location` and the schedule's `Labels`. Misfired ticks are reported too: a scheduler that only looks at the current minute cannot see a tick missed during a deploy, so it would simply never exist.

Every run a schedule fires carries two reserved labels, `workflow.LabelSchedule` (the `ScheduleID`) and `workflow.LabelScheduleName` (the name), so a run-finished observer on any node can tell which schedule a run came from.

## Hooks for one schedule

`workflow.AttachScheduleHooks(c, hooks...)` wires per-schedule callbacks from those two streams. A `ScheduleHooks` names its `Workflow` (required) and a schedule `Name` (empty: every schedule of the workflow), and any of five callbacks:

| Field | Called when |
|---|---|
| `Fired` | a tick enqueued a run, or failed to (`t.Err`) |
| `Skipped` | a tick was skipped or misfired |
| `OnSuccess` | a run the schedule fired succeeded |
| `OnFailure` | a run the schedule fired failed |
| `After` | a run the schedule fired ended, in any status |

<!-- snippet: examples/schedule-hooks/main.go#hooks -->
```go
err = workflow.AttachScheduleHooks(c, workflow.ScheduleHooks{
	Workflow:  "sync-orders",
	Name:      "office-hours",               // one schedule; empty means all of the workflow's
	Fired:     ping.Tick(hb.URL + "/start"), // a run started
	OnSuccess: ping.Run(hb.URL + "/ok"),     // it succeeded
	OnFailure: ping.Run(hb.URL + "/fail"),   // it failed
	After: func(_ context.Context, ev workflow.RunEvent) { // after
		ends <- ev
	},
})
```

`ping` is the heartbeat pinger built in [the next section](#ping-a-heartbeat-service).

`AttachScheduleHooks` refuses, before registering anything, a hook without a `Workflow` (`workflow.ErrInvalid`) and a client that lacks the notifier a hook needs (an `*workflow.UnsupportedError` naming `CapTickEvents` or `CapRunFinished`). A cancelled run calls only `After`.

`Fired` runs in the scheduler, not on the worker: the run is enqueued and runs on whichever worker takes it. To run code on the worker just before the step, use [step middleware](../middleware/step-middleware.md); `Fired` is for the schedule's side: a ping, a metric, a log line.

## Ping a heartbeat service

`middleware.NewPinger(send, opts...)` returns a `*middleware.Pinger`. The example builds one with the HTTP sender, a timeout and a logger:

<!-- snippet: examples/schedule-hooks/main.go#pinger -->
```go
ping := middleware.NewPinger(
	middleware.HTTPPing(hb.Client()),                                  // a GET; a status outside 2xx is a failure
	middleware.WithPingTimeout(pingTimeout),                           // a dead service cannot hold up the hooks
	middleware.WithPingLogger(slog.Default().With("check", "orders")), // failures at Warn, never with the URL
)
```

Its methods return functions with the shapes `ScheduleHooks` takes, so each ping is one assignment in the hooks above:

| Pinger | Hook field |
|---|---|
| `p.Tick(url)` | `Fired` |
| `p.Run(url)` | `After` |
| `p.Run(url)` | `OnSuccess` |
| `p.Run(url)` | `OnFailure` |

`p.Tick` pings only for a tick that enqueued a run: a skipped tick, or one whose enqueue failed, started nothing to report. `p.Tick(url)` is also a `workflow.TickObserver` on its own.

A ping is best effort. A failure is logged at Warn on the logger from `middleware.WithPingLogger` (`slog.Default()` when not given; nil drops it) and never fails the run or the tick. Each ping is bounded by `middleware.WithPingTimeout` (`middleware.DefaultPingTimeout`, ten seconds, when not given), so a dead heartbeat service cannot hold up the hook goroutine. The log line never contains the URL, because a heartbeat URL usually carries the check's secret token.

Sending is pluggable. `send` is a `middleware.PingFunc`, `func(ctx, url string) error`; nil means `middleware.HTTPPing(nil)`, a GET that fails on a status outside 2xx. Pass `HTTPPing(client)` for your own `*http.Client` (a proxy, mTLS), or write a `PingFunc` to POST a body, add an auth header or call a service's client library.

## Try it

```sh
cd examples
go run ./schedule-hooks
```

The ticks and the pings the heartbeat server received:

```text
-- schedules
  sync-orders  office-hours "0 * * * *" next Mon 08:00 UTC
-- ticks
  08:00 skipped (window)
  09:00 fired, run succeeded
  10:00 skipped (when)
  11:00 fired, run failed
  12:00 skipped (window)
  13:00 fired, run succeeded
-- pause
  sync-orders  office-hours "0 * * * *" next never (paused)
  14:00 skipped (paused)
  15:00 fired, run succeeded
-- heartbeat pings
  /fail x1, /ok x3, /start x4
```

Four ticks fired (09:00, 11:00, 13:00, 15:00), so `/start` was pinged four times; the 11:00 run failed, so `/fail` once and `/ok` three times. `go test ./schedule-hooks` checks every line.

## Pitfalls

- **Hooks run at least once.** Tick and run-finished delivery are at least once, as for every observer, so a hook can see the same event twice: make it idempotent (a heartbeat ping is). On a driver without `CapRunFinished` and `CapTickEvents`, delivery is also best effort: a crash can lose an event.
- **Hooks run on the driver's observer goroutines.** A slow hook delays the hooks after it; the Pinger's timeout bounds pings, and anything slower should hand its work to a goroutine or enqueue a job.
- **`Name` matches the schedule's name.** For a declared cron that is its expression, such as `"0 9 * * *"`; for `Client.Schedule`, `Schedule.Name`. A schedule made without a name is matched only by a hook with an empty `Name`.
- **A tick observer is not a run observer.** A `TickFired` means the run was enqueued, not that it succeeded; use `OnSuccess`, `OnFailure` or `After` for the outcome.

## Related

- [Schedule conditions](conditions.md): the windows and `ScheduleWhen` that skip ticks.
- [Manage schedules](manage.md): list and pause.
- [Run-finished hook](../observability/run-finished.md): the run stream the run hooks are built on.
- [Misfire policies](misfire.md): what a `TickMisfired` reports.
- Example: [examples/schedule-hooks](../../examples/schedule-hooks/).
