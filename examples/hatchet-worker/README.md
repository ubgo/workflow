# hatchet-worker

The Hatchet driver: what it refuses before it connects, and one job run on a real engine.

The refusals always run: a cron in Europe/Berlin (Hatchet evaluates crons in UTC only), a concurrency key written as a Go function (Hatchet's keys are CEL expressions), a retry backoff with jitter (Hatchet waits exactly Factor^n seconds), each refused at `Register` with an error wrapping `workflow.ErrUnsupported`, and a `GroupChunkRuns` over the SDK's limit, refused at `Start` with `workflow.ErrInvalid`.

```sh
cd examples
go run ./hatchet-worker
```

```text
capabilities: Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|Queue|RunQuery|RetryRun|CancelAll|ScheduleList|Drain|ScheduleTrigger|ScheduleHistory|EnqueueOnly|Tenant|KeyFIFO|DueQuery|Result|CronSeconds|TickTime|DrainReport
berlin-report: ErrUnsupported=true
  workflow: Workflow.CronLocation unsupported by this driver (missing CronZone): workflow berlin-report: Hatchet evaluates crons in UTC only, not Europe/Berlin; shifting the expression would break across DST
per-tenant: ErrUnsupported=true
  workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow per-tenant: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression
jittered: ErrUnsupported=true
  workflow "jittered" step "only": workflow: Retry.Backoff.Jitter unsupported by this driver (missing RetryJitter): Hatchet waits exactly Factor^n seconds and has no jitter
GroupChunkRuns 5000: ErrInvalid=true
  workflow: invalid spec: hatchet driver: GroupChunkRuns 5000 is over the SDK's bulk limit of 1000
HATCHET_CLIENT_TOKEN is not set: the live run is skipped
```

## Run it against an engine

The live part starts a worker with a full `hatchet.Config` (including the batch tunings), runs one job, waits for it to succeed and lists the workers through `Client.Native()`. It needs the variables the Hatchet SDK reads. On the local stack's `hatchet-lite` (gRPC on port 7004, no TLS), with the token from that stack's `.env`:

```sh
export HATCHET_CLIENT_TOKEN=...            # an API token for the engine
export HATCHET_CLIENT_HOST_PORT=localhost:7004
export HATCHET_CLIENT_TLS_STRATEGY=none
go run ./hatchet-worker
```

The last two lines then read:

```text
live run: succeeded
worker listed through Native: true
```

The driver adds a per-process suffix to `WorkerName`, so the worker shows in the dashboard as `hatchet-worker-example-<8 characters>`.

## The code

- `refusals` and `refuse-tuning`: what fails before any connection.
- `new`: every `hatchet.Config` field, with its default in a comment.
- `tunings`: the batch limits ([docs/operations/tuning.md](../../docs/operations/tuning.md)).
- `start` and `native`: `Register`, `Start`, `Enqueue`, then the SDK client.

`main_test.go` checks the refusals with no engine, and runs the live part only when `HATCHET_CLIENT_TOKEN` is set (skipped otherwise). The guide [docs/drivers/hatchet.md](../../docs/drivers/hatchet.md) copies its code from the regions marked `snippet:`.
