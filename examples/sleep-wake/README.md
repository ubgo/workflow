# sleep-wake

A reminder due at 23:00 on a laptop that sleeps from 22:30 to 07:00. A simulated laptop clock stands in for the hardware: its wall clock jumps over the sleep while its timers count running time only, as a monotonic timer does on platforms whose monotonic clock stops during a suspend. On a plain clock the memory driver runs the reminder at 07:30, half an hour of running time after the resume. Wrapped in `clock.WakeChecked`, the same driver runs it at 07:01, within one check of the resume.

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

## The code

- `wrap`: `clock.WakeChecked` around the laptop clock, handed to `memory.WithClock`.
- `enqueue`: a delayed run (`Job.RunAt`), the timer the sleep would delay.
- `laptopClock` (not a region): the simulated clock, a `clock.Clock` whose `Now` includes the time asleep and whose timers do not.

`main_test.go` runs the program and checks every line. The guide [docs/operations/sleep-and-wake.md](../../docs/operations/sleep-and-wake.md) copies its code from the regions marked `snippet:`.
