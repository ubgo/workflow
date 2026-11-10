# recorder-split

A run history across a web node that only enqueues and a worker node that runs the work, on the SQL driver over one SQLite file. The web node is built with `sqlstore.WithEnqueueOnly()`, which delivers no run ends, so it is wrapped with the recorder's `EnqueuePlugin()`: it records each accepted run with its source and input. The worker is wrapped with the full `Plugin()` over the same `sqlrecorder` store and records the step, its event and the end into the same record. One record per run, whichever node wrote each part.

```sh
cd examples
go run ./recorder-split
```

```text
web node recorded: source=api/req-7 input={"image":"img-1"}
worker finished it: succeeded source=api/req-7 outcome=success
  step resize attempt 1 succeeded output="thumb-1.png" events=1
records for the run: 1
```

On Hatchet only the two clients change: `hatchet.New(hatchet.Config{EnqueueOnly: true})` for the web node, a worker `hatchet.New(hatchet.Config{...})` for the worker, and a recorder store both reach. `go test ./recorder-split` runs the program on a temporary SQLite file and checks every line.

## Guides

- [Record run history › Web nodes that only enqueue](../../docs/observability/run-recorder.md#web-nodes-that-only-enqueue)
- [The Hatchet driver › Record runs from web nodes](../../docs/drivers/hatchet.md#record-runs-from-web-nodes)
- [Deploy workers › Web nodes that only enqueue](../../docs/operations/deploy.md#web-nodes-that-only-enqueue)
