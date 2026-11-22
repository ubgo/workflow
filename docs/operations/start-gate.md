# Start when ready

An edge device often boots with its real-time clock at 1970 and only learns the time from the network or GPS a little later. A scheduler started in that window computes every tick from a wrong now: it can fire decades of misfires, or skip the next real tick. [`workflow.StartWhen`](ds:block?id=startwhen-rgx6keu2) holds a client's `Start` until a readiness check passes, so nothing is scheduled or run before the clock can be trusted. The same gate waits for anything else the app must have first: a database that answers, a migration that ran, a VPN that is up.

Use it in place of `Start` whenever starting too early does harm. A server whose clock is synchronised before the process starts, and whose dependencies are checked by the orchestrator, does not need it: a readiness probe ([Nodes and health](nodes.md#health-and-readiness-probes)) holds traffic back instead.

## Before you start

- It works with every driver: it only decides when `Start` is called. It takes any `workflow.Client` ([`StartWhen`](ds:block?id=startwhen-rgx6keu2)).
- Until the gate opens, `Enqueue` and `Signal` return [`workflow.ErrNotStarted`](ds:block?id=errnotstarted-x6pc2d38), as they do before any `Start`.
- The example is [`examples/start-gate`](../../examples/start-gate). A fake clock stands in for the device's real-time clock, so the boot at 1970 and the network sync happen on cue.

## Step 1 — Hold Start until the clock is synchronised

[`workflow.ClockAfter(clk, floor)`](ds:block?id=clockafter-3nzw3qf4) is a [`workflow.ReadyFunc`](ds:block?id=readyfunc-qcg9688f) that is ready once the clock reads `floor` or later. Use a floor the device cannot see before a sync, such as the binary's build time set at link time: a clock reset to its epoch reads far before it, and the first sync moves it past.

<!-- snippet: examples/start-gate/main.go#gate -->
```go
ready := workflow.ClockAfter(device, buildTime) // ready once the clock reads the build time or later
started := make(chan error, 1)
go func() {
	// StartWhen blocks until ready reports true, then calls c.Start(ctx).
	started <- workflow.StartWhen(ctx, c, ready, workflow.StartWhenOptions{
		Poll:  poll,   // ask every 5s; zero means DefaultStartWhenPoll
		Clock: device, // times the pauses; nil means clock.Real
	})
}()
```

[`StartWhen(ctx, client, ready, opts)`](ds:block?id=startwhen-rgx6keu2) asks `ready` at once, then again every `opts.Poll` ([`workflow.DefaultStartWhenPoll`](ds:block?id=defaultstartwhenpoll-h7z7x3ch), one second, when zero) timed on `opts.Clock` (`clock.Real` when nil; [`StartWhenOptions`](ds:block?id=startwhenoptions-ryd4cehw)). Once `ready` reports true it calls `client.Start(ctx)` and returns `Start`'s error ([`TestStartWhenHoldsUntilReady`](ds:block?id=teststartwhenholdsuntilready-ea7yfcvn)). `ctx` is the ctx handed to `Start`, so it is the client's lifetime, exactly as with `Start`. `StartWhen` blocks, so the example runs it in a goroutine and reads the result from a channel; an app with nothing else to do meanwhile calls it where it would call `Start`.

## Step 2 — Expect ErrNotStarted until the gate opens

The rest of the app may already be serving requests while the gate waits. Every call that needs workers fails fast with [`workflow.ErrNotStarted`](ds:block?id=errnotstarted-x6pc2d38) instead of queueing work against a wrong clock:

<!-- snippet: examples/start-gate/main.go#before -->
```go
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "report"})
fmt.Fprintf(w, "clock %s, enqueue: %v\n", device.Now().Format(time.DateTime), err)
fmt.Fprintf(w, "not started yet: %t\n", errors.Is(err, workflow.ErrNotStarted))
```

Return a "try again later" to the caller on `workflow.ErrNotStarted`, or keep the job in the app's own outbox until the client has started.

## Step 3 — Ask anything else with a ReadyFunc

A [`workflow.ReadyFunc`](ds:block?id=readyfunc-qcg9688f) is `func(ctx) (bool, error)`. `false` means "not yet, ask again after `Poll`"; an error ends the wait, and [`StartWhen`](ds:block?id=startwhen-rgx6keu2) returns it wrapped as `workflow: start when: <err>`. Combine checks in one function:

<!-- snippet: examples/start-gate/main.go#ready-func -->
```go
clockOK := workflow.ClockAfter(device, buildTime)
pingDB := func(ctx context.Context) error { return errNoDatabase } // stands in for db.PingContext
// readyAll passes once the clock has synced and the database answers.
var readyAll workflow.ReadyFunc = func(ctx context.Context) (bool, error) {
	if ok, err := clockOK(ctx); !ok || err != nil {
		return ok, err // not yet: ask again after Poll
	}
	if err := pingDB(ctx); err != nil {
		return false, err // an error ends the wait, and StartWhen returns it
	}
	return true, nil
}
err := workflow.StartWhen(ctx, c, readyAll, workflow.StartWhenOptions{Clock: device})
fmt.Fprintf(w, "check failed: %v\n", err)
```

Return an error only for a failure that waiting will not fix. A database that is still booting should return `false, nil`, so the gate asks again; a wrong password should return the error, so the process exits and the operator sees it. For a sync status the OS reports (`timedatectl`, an NTP client's state), write a `ReadyFunc` that asks it.

## Step 4 — Give the wait a deadline

A check that never passes keeps the client stopped for ever, with no error ([`StartWhen`](ds:block?id=startwhen-rgx6keu2) loops until ready, an error or the ctx ends). When that should page someone, give the ctx a deadline:

<!-- snippet: examples/start-gate/main.go#deadline -->
```go
ctx, cancel := context.WithTimeout(ctx, failTimeout) // in production: minutes, not milliseconds
defer cancel()
never := func(context.Context) (bool, error) { return false, nil } // a check that never passes
err := workflow.StartWhen(ctx, c, never, workflow.StartWhenOptions{Poll: time.Millisecond})
fmt.Fprintf(w, "gave up: %v\n", err)
```

When the ctx ends first, [`StartWhen`](ds:block?id=startwhen-rgx6keu2) returns `ctx.Err()`. Note that the same ctx is the client's lifetime once it starts: a `WithTimeout` ctx would stop the workers at the deadline too. In a long-running app, keep the long-lived ctx for `StartWhen` and put the deadline inside the `ReadyFunc` instead: note when the wait began and return an error once it has gone on too long.

## Try it

```sh
cd examples
go run ./start-gate
```

```text
default poll: 1s
clock 1970-01-01 00:00:00, enqueue: workflow: client not started
not started yet: true
clock synced, started; the job ran at 2026-11-21 09:30:00
check failed: workflow: start when: database unreachable
gave up: context deadline exceeded
```

The first enqueue is refused while the clock reads 1970. The sync sets the clock to 2026, the gate's pending pause ends, the check passes, `Start` runs and the job runs with the right time. The second gate stops on the database error; the third gives up at its deadline.

## Pitfalls

- [`StartWhen`](ds:block?id=startwhen-rgx6keu2) blocks. Run it where `Start` would run, or in its own goroutine when the app has other work to do meanwhile.
- A readiness check that never passes keeps the client stopped for ever. Give the ctx a deadline when that should be an error, and remember that ctx becomes the client's lifetime once `Start` runs (it is the ctx [`StartWhen`](ds:block?id=startwhen-rgx6keu2) passes to `Start`).
- `StartWhen` refuses a nil client or a nil `ReadyFunc` with `workflow.ErrInvalid` ([`TestStartWhenErrors`](ds:block?id=teststartwhenerrors-d9cyqwrc)).
- [`ClockAfter`](ds:block?id=clockafter-3nzw3qf4) with a floor in the future (a build time read from the wrong place, a time zone mix-up) never opens. Use a floor that is certainly in the past on a synced clock, such as the build time in UTC.
- An error from the check ends the wait ([`StartWhen`](ds:block?id=startwhen-rgx6keu2)). A check that fails on a short outage (a DNS blip) should report `false, nil` instead, so the next poll asks again.
- The gate only delays `Start`. Schedules created while it waits are stored but not fired; once started, the client's misfire policy decides what a tick that fell due before the start means ([Misfires](../schedules/misfire.md)).

## Related

- [Laptops that sleep](sleep-and-wake.md): the other clock problem of a desktop or edge app.
- [Time and clocks](../concepts/time.md): the clocks a driver can take.
- [Nodes and health](nodes.md#health-and-readiness-probes): readiness probes on servers.
- [The client](../concepts/client.md): `Start`, `Stop` and `ErrNotStarted`.
