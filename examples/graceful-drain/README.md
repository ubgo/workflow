# graceful-drain

Two graceful ways a client stops on the in-memory driver. `Drain` stops claiming work, lets the running step finish (up to the deadline you give it) and shuts the client down. `memory.WithStopWhenIdle` stops a batch-style process once its last run has ended and nothing new arrived for a grace period. Both close `Done`.

```sh
cd examples
go run ./graceful-drain
```

```text
resize finished
drained and stopped
enqueue after Drain: workflow: client not started: the client was stopped
backfill 2026-01-01
backfill 2026-01-02
backfill 2026-01-03
idle for a minute: stopped
```

The resize step was running when `Drain` was called, so it finished before the client stopped; a new enqueue is refused because nothing would run it. The backfill client stopped by itself one fake-clock minute after its third run ended.

The guide that copies this program is [the client](../../docs/concepts/client.md#stop-gracefully). `go test ./graceful-drain` checks every line.
