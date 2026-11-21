# Laptops that sleep

A desktop or CLI app schedules a reminder for 23:00 and the laptop sleeps from 22:30 to 07:00. On some platforms a timer measures a monotonic clock that stops while the machine sleeps, so the reminder would fire at 07:30, half an hour of running time after the resume, instead of at once. `clock.WakeChecked` gives a driver timers that fire by the wall clock, so everything that fell due during the sleep runs within one check of the resume.

You want it in an app that runs on a machine that suspends: a desktop app, a menu-bar tool, a CLI daemon on a laptop, an edge device that powers down between jobs. A server that never sleeps does not need it.

## Before you start

- Every driver takes its clock as an option (`memory.WithClock`, `sqlstore.WithClock`, `redis.WithClock`), so no driver change is needed. The SQL and Redis drivers already re-read the clock on every poll; the memory driver relies on timers, so it is the one that needs this.
- The example is [`examples/sleep-wake`](../../examples/sleep-wake). A simulated laptop clock stands in for the hardware: its wall clock jumps over the sleep while its timers count running time only, which is how a real sleep looks to a program on an affected platform.

## Wrap the clock

`clock.WakeChecked(inner, every)` wraps any `clock.Clock`. Its `Now` is the inner clock's; its timers never wait longer than `every` at a time:

<!-- snippet: examples/sleep-wake/main.go#wrap -->
```go
clk := clock.WakeChecked(laptop, check) // zero or less means clock.DefaultWakeCheck
c := memory.New(memory.WithClock(clk))
```

In an app, `laptop` is `clock.Real{}` and `check` is `clock.DefaultWakeCheck` (30 seconds):

```go
c := memory.New(memory.WithClock(clock.WakeChecked(clock.Real{}, clock.DefaultWakeCheck)))
```

Each time a wake-checked timer wakes, it compares the wall clock with its deadline (its creation time plus its delay, as a wall-clock instant, with the monotonic reading dropped) and fires once the deadline has passed. Before the deadline it re-arms for the time left or `every`, whichever is shorter.

## Schedule as usual

Nothing else changes. Cron ticks, delayed runs, retry backoffs, snoozes and durable sleeps all take their timers from the client's clock, so they all fire by the wall clock now:

<!-- snippet: examples/sleep-wake/main.go#enqueue -->
```go
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "remind", RunAt: dueAt}) // due at 23:00
```

After a resume, every timer that fell due during the sleep fires within `every`. For a cron tick, the schedule's misfire policy then decides what a missed tick means: run it once now, skip it, or run every missed tick ([Misfires](../schedules/misfire.md)).

## Try it

```sh
cd examples
go run ./sleep-wake
```

```text
reminder due 23:00, laptop sleeps 22:30 to 07:00
default check: 30s
plain clock:        ran at 07:30, 30m0s of running time after the resume
wake-checked clock: ran at 07:01, within one check (1m0s) of the resume
```

The plain clock's timer was armed at 22:30 for 30 minutes of running time; the sleep did not count, so it fired 30 minutes after the resume. The wake-checked timer re-checked one minute after the resume, saw that the wall clock was past 23:00 and fired.

## Pitfalls

- Each pending timer wakes once per `every`. `clock.DefaultWakeCheck` (30 seconds) suits a desktop app; a shorter interval costs more wake-ups on a battery, a longer one makes a fire after a resume later.
- `every` of zero or less means `clock.DefaultWakeCheck`, not "check constantly".
- It fixes timers, not the clock itself. A device whose clock is wrong after a power cycle needs [Start when ready](start-gate.md) as well.
- A run that was executing when the machine slept carries on after the resume; its step timeout and lease are measured as the driver measures them, so a long sleep can expire a lease on a shared database driver and hand the step to another worker.
- A server that never sleeps does not need it: it only adds wake-ups.

## Related

- [Start when ready](start-gate.md): hold `Start` until the clock has synchronised.
- [Time and clocks](../concepts/time.md): the clocks a driver can take, and the fake clock tests use.
- [Misfires](../schedules/misfire.md): what a tick missed during the sleep does.
- [Memory driver](../drivers/memory.md): the driver that relies on timers.
