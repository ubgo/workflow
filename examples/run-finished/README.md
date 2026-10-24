# run-finished

The run-finished hook on the in-memory driver: find it with a type assertion for `workflow.RunNotifier`, check `CapRunFinished`, register an observer before `Start` (one registered after is refused with `workflow.ErrAlreadyStarted`), and receive the `RunEvent` of a run that succeeded, one that failed, and a delayed run cancelled before it ever started.

```sh
cd examples
go run ./run-finished
```

```text
every run end is delivered: true
observer after Start refused: true
charge succeeded order=o-1 err=<nil> cancelled=false
charge failed order=o-2 err=memory: step "run" failed after 1 attempt(s): card declined cancelled=false
charge cancelled order=o-3 err=workflow: run cancelled cancelled=true
```

## On Hatchet

`hatchet.go` registers the same kind of observer on the Hatchet driver, with `RunFinishedPoll`, `RunFinishedWindow` and `RunFinishedCatchUp` set, and runs one job. It runs only when `HATCHET_CLIENT_TOKEN` is set. Against the shared local stack's Hatchet engine (gRPC on port 7004, no TLS), with the token from the stack's `.env`:

```sh
HATCHET_CLIENT_TOKEN=... HATCHET_CLIENT_HOST_PORT=localhost:7004 HATCHET_CLIENT_TLS_STRATEGY=none go run ./run-finished
```

After the lines above it prints:

```text
hatchet: every run end is delivered: false
hatchet: run succeeded
```

The Hatchet driver's delivery is best effort, so it does not declare `CapRunFinished`. The workflow name carries a timestamp, so runs of this example never take each other's jobs on a shared engine.

The guide that copies this program is [react when a run finishes](../../docs/observability/run-finished.md). `go test ./run-finished` runs the memory part and checks every line; `TestHatchet` skips, with the reason, without the token.
