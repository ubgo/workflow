# Laptops that sleep

A desktop or CLI app schedules a reminder for 23:00 and the laptop sleeps from 22:30 to 07:00. On some platforms a timer measures a monotonic clock that stops while the machine sleeps, so the reminder would fire at 07:30, half an hour of running time after the resume, instead of at once. `clock.WakeChecked` gives a driver timers that fire by the wall clock.

## Before you start

- Every driver takes its clock as an option (`memory.WithClock`, `sqlstore.WithClock`, `redis.WithClock`), so no driver change is needed. The SQL and Redis drivers already re-read the clock on every poll; the memory driver relies on timers, so it is the one that needs this.

## Wrap the clock

```go
c := memory.New(memory.WithClock(clock.WakeChecked(clock.Real{}, clock.DefaultWakeCheck)))
```

A timer from `clock.WakeChecked(inner, every)` never waits longer than `every` at a time; each time it wakes it compares the wall clock with its deadline (its creation time plus its delay, as a wall-clock instant) and fires once the deadline has passed. After a resume, every timer that fell due during the sleep fires within `every`: cron ticks (with their misfire policy deciding what a missed tick means), delayed runs, retry backoffs, snoozes and durable sleeps.

## Pitfalls

- Each pending timer wakes once per `every`. `DefaultWakeCheck` (30 seconds) suits a desktop app; a shorter interval costs more wake-ups on a battery.
- A server that never sleeps does not need it.
