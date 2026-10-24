# hatchet-manage

Queue and run management on the Hatchet driver: named queues served by their own workers, a filtered run list, retrying a failed run from the step that failed, named schedules and their listing, and a graceful drain. It also shows what the driver refuses, before it connects.

```sh
cd examples
go run ./hatchet-manage
```

Without an engine only the refusals run:

```text
Retry.Until: ErrUnsupported=true
Job.Retry: ErrUnsupported=true
QueueConfig.Priority: ErrUnsupported=true
PauseWorkflow: false
DeleteRun: false
Discard: false
HATCHET_CLIENT_TOKEN is not set: the live run is skipped
```

## Run it against an engine

The live part needs the variables the Hatchet SDK reads. On the local stack's `hatchet-lite` (gRPC on port 7004, no TLS), with the token from that stack's `.env`:

```sh
export HATCHET_CLIENT_TOKEN=...            # an API token for the engine
export HATCHET_CLIENT_HOST_PORT=localhost:7004
export HATCHET_CLIENT_TLS_STRATEGY=none
go run ./hatchet-manage
```

It registers a two-step workflow on the `reports` queue whose second step fails once, lists the failed run, retries it (the first step does not run again), schedules a named cron twice (same ID back), lists it, pauses the workflow (a run enqueued meanwhile waits, then runs after the resume), and drains the worker. Pause needs `Config.API`, which the example builds with `hatchet.NewAPI` from the same token. The live lines read:

```text
failed run: queue=reports step=render error="renderer offline"
after retry: succeeded, render attempts=2
same name, same definition, same ID: true
schedule nightly: 0 2 * * *, next fire in UTC at 02:00
workflow listed as paused: true
after resume: succeeded
drained
```

## The code

- `refusals`: what fails before any connection.
- `queues`: `Config.Queues` and `Workflow.Queue`.
- `api`: `hatchet.NewAPI`, the REST API pause needs.
- `runs`, `retry`, `schedules`, `pause`, `drain`: the management calls.

`main_test.go` checks the refusals with no engine, and runs the live part only when `HATCHET_CLIENT_TOKEN` is set. The guide [docs/drivers/hatchet.md](../../docs/drivers/hatchet.md#queues-run-management-and-drain) copies its code from the regions marked `snippet:`.
