# hatchet-pause

Workflow pause and schedule pause on the Hatchet driver. Both go through the engine's REST API, which the driver reaches only when `Config.API` is set (`hatchet.NewAPI(token, serverURL)` reads the tenant from the client token). Without it the driver declares neither `CapPauseWorkflow` nor `CapSchedulePause` and refuses `Pause` and `PauseSchedule` before it connects.

```sh
cd examples
go run ./hatchet-pause
```

Without an engine only the refusals run:

```text
without API: PauseWorkflow=false SchedulePause=false
Pause: ErrUnsupported=true
PauseSchedule: ErrUnsupported=true
NewAPI with a bad token: ErrInvalid=true
HATCHET_CLIENT_TOKEN is not set: the live run is skipped
```

## Run it against an engine

The live part needs the variables the Hatchet SDK reads. On the local stack's `hatchet-lite` (gRPC on port 7004, no TLS), with the token from that stack's `.env`:

```sh
export HATCHET_CLIENT_TOKEN=...            # an API token for the engine
export HATCHET_CLIENT_HOST_PORT=localhost:7004
export HATCHET_CLIENT_TLS_STRATEGY=none
go run ./hatchet-pause
```

It builds `Config.API` from the token, pauses a workflow (a run enqueued meanwhile is accepted but does not start, then succeeds after the resume), shows that `PauseAll` is refused because the engine keeps one pause per workflow, and pauses and resumes a named schedule. The live lines read:

```text
with API: PauseWorkflow=true SchedulePause=true
workflow listed as paused: true
held run finished while paused: false
after resume: succeeded
PauseAll: ErrUnsupported=true
schedule nightly: paused=true
schedule nightly: paused=false
```

## The code

- `refusals`: what fails without `Config.API`, before any connection.
- `api`: `hatchet.NewAPI` and the client that uses it.
- `pause-workflow`, `resume-workflow`: holding and releasing one workflow.
- `pause-schedule`: disabling a cron trigger.

`main_test.go` checks the refusals with no engine, and runs the live part only when `HATCHET_CLIENT_TOKEN` is set. The guide [docs/drivers/hatchet.md](../../docs/drivers/hatchet.md#step-6-pause-a-workflow-or-a-schedule) copies its code from the regions marked `snippet:`.
