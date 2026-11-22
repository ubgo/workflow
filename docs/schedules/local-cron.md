# Local cron

Some periodic work belongs to each process, not to the fleet: warming an in-memory cache, cleaning a node's temp directory, flushing per-node metrics. A schedule on a shared driver is the wrong tool for it, because it fires once across every node and may run on any worker. [`workflow.LocalCron`](ds:block?id=localcron-vcntvcyq) is the opposite: an in-process cron that runs its functions on every node that starts one, here, with no queue, no retries and no persistence.

## Before you start

- No driver is needed: a [`LocalCron`](ds:block?id=localcron-start-u6drtdsb) lives in the core package and runs on its own goroutines.
- The example is [`examples/schedule-specs`](../../examples/schedule-specs). Its local cron runs on a fake clock, so three minutes pass at once. Run it with `go run ./schedule-specs` from `examples/`.
- For work that must run once per tick across the fleet, use [cron schedules](cron.md) on a shared driver.

## Run a function on every node

[`workflow.NewLocalCron(opts...)`](ds:block?id=newlocalcron-yf6kspx6) returns an empty cron; [`Add(name, expr, loc, fn)`](ds:block?id=localcron-add-nxm3w52c) adds an entry and [`Start(ctx)`](ds:block?id=localcron-start-u6drtdsb) runs every entry until `ctx` ends, without blocking. `Done()` closes once every entry, including a run in progress, has returned.

<!-- snippet: examples/schedule-specs/main.go#localcron -->
```go
lc := workflow.NewLocalCron(
	workflow.WithLocalCronClock(clk),
	workflow.WithLocalCronErrors(func(name string, at time.Time, err error) {
		fmt.Fprintf(os.Stderr, "local cron %s at %s: %v\n", name, at, err)
	}),
)
err := lc.Add("warm-cache", "* * * * *", time.UTC, func(ctx context.Context, at time.Time) error {
	ran <- at // at is the tick, not the wall time the run started
	return nil
})
if err != nil {
	return err
}
```

The [function](ds:block?id=localcronfunc-pt52qxp8) gets the tick it runs for, not the wall time it started. [Names are unique](ds:block?id=localcron-add-nxm3w52c), the zone is required (`time.UTC` for UTC; `time.Local` is refused, as on a `Schedule`), and every validation error wraps `workflow.ErrInvalid`. Entries are fixed once the cron runs: `Add` after `Start`, and a second `Start`, return [`workflow.ErrLocalCronStarted`](ds:block?id=errlocalcronstarted-6yxzcj5p).

Options, each a [`LocalCronOption`](ds:block?id=localcronoption-y46rxcak): [`WithLocalCronClock`](ds:block?id=withlocalcronclock-utszah2h) injects the clock ([`clock.Real` by default](ds:block?id=newlocalcron-yf6kspx6), `clock.Fake` in tests), [`WithLocalCronParser`](ds:block?id=withlocalcronparser-p2p7xdgr) takes another cron dialect (`cron.ParseSeconds` for a seconds field), and [`WithLocalCronErrors`](ds:block?id=withlocalcronerrors-xvucry86) receives every error and recovered panic with the entry's name and tick.

## How ticks run

Each entry [runs on its own goroutine](ds:block?id=localcron-start-u6drtdsb), one tick at a time. [A tick that falls due](ds:block?id=localcron-loop-w2mts36z) while the previous run is still going is skipped, not queued, so a slow run never piles up behind itself. Ticks missed while the process was down are never caught up. An error or a [panic](ds:block?id=localcron-run-rpwg75c5) goes to the error handler and the entry keeps its schedule.

## Try it

```sh
cd examples
go run ./schedule-specs
```

The local cron's part, a minute entry on a fake clock moved three minutes:

```text
local cron:
  ran for 00:01
  ran for 00:02
  ran for 00:03
```

## Pitfalls

- **It is not durable.** [Nothing is stored](ds:block?id=localcron-vcntvcyq): a restart forgets every entry and every missed tick.
- **It is not once per fleet.** [Ten replicas run each tick ten times](ds:block?id=localcron-loop-w2mts36z). That is the point for per-node work and a bug for anything else; use `Workflow.Cron` or `Client.Schedule` for work that must run once.
- **No retries.** [A failed run is reported to the error handler](ds:block?id=localcron-loop-w2mts36z) and waits for the next tick; wrap the function yourself if it must retry.
- **Overlapping ticks are dropped.** [A run longer than the interval](ds:block?id=localcron-loop-w2mts36z) skips the ticks that fall due during it.
- **Stop it with the context.** Cancel the [`Start`](ds:block?id=localcron-start-u6drtdsb) context and wait on `Done()` before the process exits, so a run in progress finishes.

## Related

- [Cron schedules](cron.md): schedules that fire once across the fleet.
- [Time zones](time-zones.md), [Time and clocks](../concepts/time.md).
- Example: [examples/schedule-specs](../../examples/schedule-specs/).
