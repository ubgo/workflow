# Start when ready

An edge device often boots with its real-time clock at 1970 and only learns the time from the network or GPS a little later. A scheduler started in that window computes every tick from a wrong now: it can fire decades of misfires, or skip the next real tick. `workflow.StartWhen` holds a client's `Start` until a readiness check passes, so nothing is scheduled or run before the clock can be trusted.

## Before you start

- It works with every driver: it only decides when `Start` is called.
- Until the gate opens, `Enqueue` and `Signal` return `workflow.ErrNotStarted`, as they do before any `Start`.

## Hold Start until the clock is synchronised

```go
// buildTime is set at link time; a clock that reads before it has not synced.
ready := workflow.ClockAfter(clock.Real{}, buildTime)
if err := workflow.StartWhen(ctx, c, ready, workflow.StartWhenOptions{Poll: 5 * time.Second}); err != nil {
	return err
}
```

`StartWhen(ctx, client, ready, opts)` asks `ready` every `Poll` (one second by default) on `opts.Clock`, and calls `client.Start(ctx)` once it reports true. It returns `Start`'s error, the error `ready` returned, or `ctx.Err()` when the ctx ends first. `ctx` is the ctx passed to `Start`, so it is the client's lifetime, as with `Start`.

`workflow.ClockAfter(clk, floor)` is ready once the clock reads `floor` or later. Use a floor the device cannot see before a sync, such as the binary's build time. A `workflow.ReadyFunc` can ask anything else: an NTP client's status, a database being reachable, a migration having run.

## Pitfalls

- `StartWhen` blocks. Run it where `Start` would run, or in its own goroutine when the app has other work to do meanwhile.
- A readiness check that never passes keeps the client stopped for ever. Give the ctx a deadline when that should be an error.
