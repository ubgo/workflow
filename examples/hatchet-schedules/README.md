# hatchet-schedules

The Hatchet driver's defaults, its schedule features and its client for web nodes. Without an engine it prints the default tunings, builds a client from a `Config`, shows that an enqueue-only client refuses `OnRunFinished` and starts nothing, that `OverlapSkip` is refused, and registers a step with a timeout near its real duration. With `HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT` and `HATCHET_CLIENT_TLS_STRATEGY` set for a running engine it also runs a named schedule now with `TriggerSchedule`, reads its last fire from the engine's run list, enqueues a job from an enqueue-only client, and cancels a delayed start with `ErrCancelTooLate` handled.

```sh
cd examples
go run ./hatchet-schedules
```

```text
StepTimeout: 24h0m0s
ScheduleTimeout: 24h0m0s
RegisterTimeout: 30s
RunFinishedPoll: 1s
ScheduleHistoryWindow: 168h0m0s
PayloadLimit: 1572864 bytes
GroupChunkRuns: 1000 GroupChunkBytes: 2097152
BatchCoordinatorRetry: 10 retries, 2s doubling, capped at 2m0s
trigger now: true last fire: true
OnRunFinished on a web node: ErrUnsupported = true
Start on a web node starts nothing: true
OverlapSkip: ErrUnsupported = true
export registered with a 2m0s step timeout: true
HATCHET_CLIENT_TOKEN is not set: the live run is skipped
```

Against hatchet-lite v0.110.5 the last line becomes:

```text
schedule nightly: last run succeeded, next fire 02:00 UTC
enqueued from the web node: succeeded
delayed start cancelled before it fired
```

To start a local engine and mint a token, see [hatchet-worker](../hatchet-worker/README.md).

## The code

- `defaults`: the `Default*` constants behind every zero `Config` field the guides name.
- `new`: `hatchet.New` with a `Config`, including `ScheduleHistoryWindow`.
- `enqueue-only`: `Config.EnqueueOnly`.
- `overlap`: `CronOverlap: OverlapSkip` refused at `Register`.
- `quirks`: `VisibilityLag`, `FailedSettle` and a `Step.Timeout` for the engine's reclaim of a dead worker's step.
- `trigger` and `last`: `TriggerSchedule` and `ScheduleInfo.Last`.
- `cancel-too-late`: `Cancel` of a delayed start and `hatchet.ErrCancelTooLate`.

`main_test.go` checks every line offline; `TestLive` checks the live lines and skips without `HATCHET_CLIENT_TOKEN`. The guides [docs/drivers/hatchet.md](../../docs/drivers/hatchet.md) and [docs/operations/tuning.md](../../docs/operations/tuning.md) copy their code from the regions marked `snippet:`.
