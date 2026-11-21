# memory-options

Every option of the in-memory driver at work, on a fake clock so nothing waits in real time:

- A worker pool, a fake clock, a logger, a step middleware that counts calls and a pinned retry jitter source: a step fails once and is retried after exactly 7.5 seconds of fake time (a 10 second backoff with `Jitter: 0.5` and the draw pinned to 0.5), and an `OnFailure` error that no caller sees goes to the logger.
- Named queues: a job for a queue the client does not serve is refused at `Enqueue`, and a workflow on one at `Start`.
- Run retention: with `MaxRuns: 2`, the oldest of three finished runs is forgotten and its `Status` is `ErrNotFound`.
- Stop when idle: a batch-style client stops by itself 30 seconds after its last run ended.
- The schedule heap: three schedules hold three timers by default and one with `WithScheduleHeap`, and fire the same ticks.

```sh
cd examples
go run ./memory-options
```

```text
sync after 7s: retrying
sync after 7.5s: succeeded, attempts 2
level=ERROR msg="memory: OnFailure handler failed" workflow=import run=run-6 error="pager is down"
middleware calls: 4
job on queue video: ErrInvalid = true
welcome on queue emails: succeeded
workflow on unserved queue at Start: ErrInvalid = true
default retention: 24h0m0s, at most 10000 runs
run-2: forgotten
run-5: succeeded
run-8: succeeded
stopped by itself after 30s idle
enqueue after the stop: ErrNotStarted = true
schedule heap false: 3 schedules, 3 pending timer(s), 3 fired at 09:05
schedule heap true: 3 schedules, 1 pending timer(s), 3 fired at 09:05
```

## The code

- `options`: `WithWorkers`, `WithClock`, `WithLogger`, `WithMiddleware`, `WithJitterSource`.
- `queues`: `WithQueues` and the refusal of a queue the client does not serve.
- `retention`: `WithRunRetention` with `RunRetention{MaxAge, MaxRuns}`; the program also prints `DefaultRunRetention`.
- `stop-when-idle`: `WithStopWhenIdle` and `Done`.
- `schedule-heap`: `WithScheduleHeap`.

`main_test.go` runs the program and checks every line. The guide [docs/drivers/memory.md](../../docs/drivers/memory.md) copies its code from the regions marked `snippet:`.
