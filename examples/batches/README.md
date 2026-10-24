# batches

Batches of jobs on the in-memory driver with Then, Catch and Finally callbacks: a batch whose jobs all succeed, one whose failing job cancels the other, one the app cancels after reading its progress, labels copied onto the callbacks, batch IDs from `WithIDFunc`, a `Finally` whose enqueue failed and is retried with `RetryCallbacks`, and the refusal of a reserved batch label.

```sh
cd examples
go run ./batches
```

```text
-- every job succeeds
dispatched thumbs-1
finally thumbs-1 tenant=acme succeeded=3 failed=0 cancelledJobs=0 cancelled=false
then thumbs-1 tenant=acme succeeded=3 failed=0 cancelledJobs=0 cancelled=false
-- a job fails
catch thumbs-2 tenant=acme succeeded=0 failed=1 cancelledJobs=0 cancelled=true
finally thumbs-2 tenant=acme succeeded=0 failed=1 cancelledJobs=1 cancelled=true
-- the app cancels
before cancel: 0 of 2 processed (0%)
finally thumbs-3 tenant=acme succeeded=0 failed=0 cancelledJobs=2 cancelled=true
-- a Finally that failed to enqueue
pending finally: attempts=1 last error: batch thumbs-4: enqueue finally callback "finally": engine unavailable
finally thumbs-4 tenant=globex succeeded=1 failed=0 cancelledJobs=0 cancelled=false
-- a reserved label
refused: true
```

Callbacks are separate runs that may finish in any order, so the program sorts each group of lines.

## On Hatchet

`hatchet.go` dispatches 50 jobs on the Hatchet driver through the batch coordinator (`hatchet.Config.BatchCoordinator`), with the driver's `BatchSource` as the completion source a coordinated batch needs, and shows that a Manager without such a source is refused. It runs only when `HATCHET_CLIENT_TOKEN` is set. Against the shared local stack's Hatchet engine (gRPC on port 7004, no TLS), with the token from the stack's `.env`:

```sh
HATCHET_CLIENT_TOKEN=... HATCHET_CLIENT_HOST_PORT=localhost:7004 HATCHET_CLIENT_TLS_STRATEGY=none go run ./batches
```

After the lines above it prints:

```text
hatchet: coordinated=true, one parent run=true
hatchet: finally succeeded=50 of 50
hatchet: coordinated batch without a LabelSource refused: true
```

The guides that copy this program: [batches](../../docs/batches/batches.md), [batch completion](../../docs/batches/completion.md) and [native group runs](../../docs/batches/native-groups.md). `go test ./batches` runs the memory part and checks every line; `TestHatchet` skips, with the reason, without the token.
