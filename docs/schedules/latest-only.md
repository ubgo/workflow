# Latest-only runs

Some scheduled work only matters for the newest tick: refreshing a cache, rebuilding a dashboard, pushing the current stock level. When a misfire catch-up fires three missed hours at once, or a backlog delays the 09:00 run until after 10:00, running the older ticks is wasted work, and on a slow source it makes the backlog worse. [`middleware.LatestOnly`](ds:block?id=mw-latestonly-g8qwm9je) skips the work of a scheduled run whose next tick is already due, so only the newest run does it.

Use it for idempotent "bring it up to date" work. Do not use it for work where every tick counts, such as a daily report or an hourly billing run: there each tick is its own period, and [misfire policies](misfire.md) with `StepInfo.TickAt` are the tool.

## Before you start

- The run must carry its tick time: drivers with [`workflow.CapTickTime`](ds:block?id=capticktime-6gn5wpmj) ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-p5caps-wggh6esn), [Redis](ds:block?id=redis-caps-bfgdst9t), [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)) set [`workflow.LabelTickAt`](ds:block?id=labeltickat-6ry4j4ub) on every run a schedule fires. See [which tick a run is for](cron.md#which-tick-a-run-is-for).
- The middleware [reads the schedule's expression](ds:block?id=mw-latestnewer-myhc2v57) through a [`workflow.ScheduleLister`](ds:block?id=schedulelister-2vgjk9bh) ([`workflow.CapScheduleList`](ds:block?id=capschedulelist-3hbmmd6n)), usually the client itself, or from a `Next` function you give it.
- Misfire catch-up is on [misfire policies](misfire.md); step middleware in general on [step middleware](../middleware/step-middleware.md).
- The runnable program for this page is [examples/latest-only](../../examples/latest-only/): an hourly cache refresh on the memory driver with a fake clock, a three-hour outage and a catch-up of the missed ticks.

## Step 1: write the step as usual

The handler does the work for the newest state and needs no knowledge of ticks. The example reads the tick only to print which run did the work; [`StepInfo.TickAt`](ds:block?id=stepinfo-tickat-rt2h9tgz) reports false for a run no schedule fired:

<!-- snippet: examples/latest-only/main.go#handler -->
```go
// refresh is the expensive work: rebuild the cache from the source.
refresh := func(ctx context.Context, in []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	label := "none"
	tick, ok := info.TickAt() // false for a run no schedule fired
	if ok {
		label = tick.Format(hourFormat)
	}
	j.add(tick, fmt.Sprintf("tick %s, run at %s: refreshed the cache", label, clk.Now().Format(hourFormat)))
	return in, nil
}
```

## Step 2: wrap the step in LatestOnly

[`middleware.LatestOnly(cfg)`](ds:block?id=mw-latestonly-g8qwm9je) returns a `workflow.Middleware`. Wrap the steps whose work should be skipped:

<!-- snippet: examples/latest-only/main.go#latest -->
```go
latest := middleware.LatestOnly(middleware.LatestOnlyConfig{
	Schedules: c,   // the client lists its schedules (CapScheduleList)
	Clock:     clk, // the driver's clock, so a fake clock decides in tests
	OnSkip: func(ctx context.Context, info workflow.StepInfo, tick, newer time.Time) {
		j.add(tick, fmt.Sprintf("tick %s, run at %s: skipped, the %s tick is due", tick.Format(hourFormat), clk.Now().Format(hourFormat), newer.Format(hourFormat)))
	},
})
err := c.Register(workflow.Workflow{Name: "refresh-cache", Steps: []workflow.Step{
	{Name: "refresh", Run: latest(refresh)}, // only this step's work is skipped
}})
```

For each attempt it reads the run's tick (`StepInfo.TickAt`), looks up the schedule that fired the run (the `workflow.LabelSchedule` label) in `Schedules`, finds the first tick after the run's own that the schedule really fires, and compares it with `Clock`. That tick [follows the schedule's own rules](ds:block?id=mw-nextfire-725thfze): the cron in its zone, its `Active` period and `Windows`, `When` for the workflow's `ScheduleWhen`, and its [misfire policy](misfire.md). A due tick counts only when the policy fires it on a catch-up from the run's tick to now ([`Misfire.Due`](ds:block?id=misfire-due-5butt4h7)): whether the scheduler fired every tick on time or was down and catches up now, that tick or a newer one fires. A tick the policy drops (`MisfireSkip` more than `workflow.MisfireGrace` late, a tick older than `Within`, a `MisfireFireOnce` newest tick outside the windows) may never have fired, so it does not count. A paused schedule fires no tick, and neither does one with `OverlapSkip` while this run is unfinished. When that tick is already due, a newer run exists or is about to, so the step returns its input unchanged without calling the handler, and `OnSkip` is told the run's tick and the newer one. A fire the driver recorded after the run's tick (`ScheduleInfo.Last`, on drivers with [`workflow.CapScheduleHistory`](ds:block?id=capschedulehistory-5y6e8cr6)) [makes the run stale too](ds:block?id=mw-latestnewer-myhc2v57), whatever the policy: on such a driver a run delayed past ticks that fired on time skips under `MisfireSkip` as well. Otherwise the handler runs.

The [`LatestOnlyConfig`](ds:block?id=mw-latestonlyconfig-vv2859xc) fields:

| Field | What it does |
|---|---|
| `Schedules` | looks up the schedule that fired the run; usually the client. Required unless `Next` is set |
| `Next` | `func(ctx, info, tick) (next time.Time, ok bool, err error)`: the tick after `tick`, in place of the lookup, for a schedule kept outside the client or a custom calendar; `ok` false means unknown, and the step runs |
| `When` | the workflow's `ScheduleWhen`, when it has one: a due tick the condition rejects fired no run, so it does not make a run stale. Asked on the step's context about each due tick after the run's own; not used with `Next` |
| `DefaultMisfire` | the policy the driver applies to a schedule without one, so `LatestOnly` knows which missed ticks fire; zero is [`MisfireFireOnce`](ds:block?id=mw-nextfire-725thfze) (memory, SQL, Redis). Set `workflow.Misfire{Policy: workflow.MisfireSkip}` on Hatchet, which never catches up |
| `Parser` | the cron dialect of the schedules when the drivers use a custom one (their `WithCronParser` option); nil is the default parser |
| `OnSkip` | told about every attempt it skips, for a log line or a metric |
| `Clock` | the now the next tick is compared with; [nil is the real clock](ds:block?id=mw-latestonly-g8qwm9je). Pass the driver's clock |

## Step 3: schedule the workflow

Any schedule works. The example fires every missed tick after an outage, so there are stale runs to skip:

<!-- snippet: examples/latest-only/main.go#schedule -->
```go
_, err = c.Schedule(ctx, workflow.Schedule{
	Name:     "hourly",
	Workflow: "refresh-cache",
	Cron:     "0 * * * *",
	Location: time.UTC,
	// After an outage, fire every missed tick (at most three), oldest first.
	Misfire: workflow.Misfire{Policy: workflow.MisfireFireAll, Max: catchUp},
})
```

With `MisfireFireOnce` (the default of the [memory](ds:block?id=memory-effectivemisfire-cz2fnaea), [SQL](ds:block?id=sql-client-fireschedules-9tkj9xrk) and [Redis](ds:block?id=redis-effectivemisfire-ued3ypmw) drivers) a catch-up fires only the newest missed tick, so `LatestOnly` then matters for the other case: a run that waited in a backlog until after its successor's tick.

## Step 4: runs nobody scheduled always work

A run without a tick label (enqueued by hand, or by a driver without `CapTickTime`) and a run whose schedule is gone (unscheduled since) [always run the handler](ds:block?id=mw-latestonly-g8qwm9je):

<!-- snippet: examples/latest-only/main.go#manual -->
```go
// A run enqueued by hand has no tick, so LatestOnly always lets it work.
if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "refresh-cache"}); err != nil {
	return err
}
```

A run someone asked for always works too: a manual `ScheduleTriggerer.TriggerSchedule`, a [`workflow.Backfill`](backfill.md) run and a `workflow.FireAtStartup` run carry [`workflow.LabelTrigger`](ds:block?id=labeltrigger-5wqkhz8p), and `LatestOnly` [never skips a run with that label](ds:block?id=mw-testlatestonlyrunsrequestedruns-chgpzhry), whatever its tick.

## Try it

```sh
cd examples
go run ./latest-only
```

```text
-- on time
  tick 09:00, run at 09:00: refreshed the cache
-- after a three-hour outage
  tick 10:00, run at 12:30: skipped, the 11:00 tick is due
  tick 11:00, run at 12:30: skipped, the 12:00 tick is due
  tick 12:00, run at 12:30: refreshed the cache
-- not fired by a schedule
  tick none, run at 12:30: refreshed the cache
```

The 09:00 run is on time: its next tick, 10:00, is in the future. The process is then down from 09:30 to 12:30; on return the misfire policy fires the 10:00, 11:00 and 12:00 ticks at once. At 12:30 the 10:00 and 11:00 runs are stale (their next ticks are due) and skip; the 12:00 run is the newest and refreshes. `go test ./latest-only` checks every line.

## Pitfalls

- **A skipped step still succeeds.** It [returns its input as its output](ds:block?id=mw-latestonly-g8qwm9je), and the steps after it run with that input. Put `LatestOnly` on the step that does the expensive work, or on every step of the run; a later step that writes somewhere must not assume the refresh happened.
- **Without `CapScheduleHistory`, a late run cannot tell an on-time fire from a dropped tick.** Under `MisfireSkip` or a `Within` deadline, a run delayed past its next tick by more than the policy allows does its work, because that tick may have been dropped while the scheduler was down; the newer runs then do it too. That is wasted work, never lost work. On a driver with `CapScheduleHistory` ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-p4caps-ns9ehh5e), [Redis](ds:block?id=redis-caps-bfgdst9t), [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)) the recorded fire [settles it](ds:block?id=mw-testlatestonlymisfirerule-nytp7njq). Before v0.2.2 such a tick counted, and a run delayed past a dropped tick skipped its work with no newer run to do it.
- **A tick whose enqueue failed still counts while the policy would fire it.** Neither the policy nor the history shows a failed enqueue (the driver records only fires), so a run delayed past such a tick skips; the next tick's run does the work. For a schedule with a rule `LatestOnly` cannot read (a custom calendar), give `Next`.
- **Set `When` when the workflow has a `ScheduleWhen`.** Without it [every due tick counts](ds:block?id=mw-nextfire-725thfze), so a run whose next tick the condition skipped finds that tick due and skips its work, with no newer run to do it.
- **A missing `Schedules` and `Next` fails at run time, not at build time.** `LatestOnly(LatestOnlyConfig{})` builds, and every attempt then [fails permanently](ds:block?id=mw-latestonly-g8qwm9je) with an error matching `workflow.ErrInvalid`.
- **A lookup error fails the attempt.** An error from `Schedules.Schedules` or from parsing the expression (a custom dialect without `Parser`) is [returned as `middleware: latest only: ...`](ds:block?id=mw-latestonly-g8qwm9je), and the step's retry policy decides what happens next.
- **The comparison uses the clock you pass.** Give it the driver's clock; with the default real clock a test on a fake clock decides on the wall time.

## Related

- [Misfire policies](misfire.md): what fires after an outage.
- [Cron schedules › Which tick a run is for](cron.md#which-tick-a-run-is-for): the tick label `LatestOnly` reads.
- [Overlap](overlap.md): skip or replace a tick while the previous run is still running, a different problem.
- [Step middleware](../middleware/step-middleware.md): how middleware wraps a step.
- Example: [examples/latest-only](../../examples/latest-only/).
