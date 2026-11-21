# batch-completion

Where a batch hears that its jobs ended, on the in-memory driver. A Manager on a client without the run-finished hook gets the status poller by default; `batch.WithStatusPoll` sets its interval and a fake clock drives its tick, so the output shows the batch lagging behind the engine until the poll. A second Manager replaces the default sources with `batch.WithCompletionSource` and a `batch.Source` of the app's own, which hands every run end from a feed (a channel standing for a message bus) to `Manager.RunFinished`. Last, `RunFinished` ignores a run that is not a batch job and a job of a batch the store does not hold.

```sh
cd examples
go run ./batch-completion
```

```text
-- a poller, on a client without the run-finished hook
the job ran; before the poll: 0 of 1 processed
finally p1 succeeded=1 of 1
after one poll: 1 of 1 processed, finished=true
-- an app's own feed of run ends
finally f1 succeeded=2 of 2
then f1 succeeded=2 of 2
f1: 2 of 2 processed, finished=true
not a batch job, error: <nil>
unknown batch, error: <nil>
```

Callbacks are separate runs that may finish in any order, so the program sorts each group of lines. The guide that copies this program is [batch completion](../../docs/batches/completion.md). `go test ./batch-completion` runs it and checks every line.
