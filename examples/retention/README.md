# retention

The SQL driver deleting finished runs on purpose. A fake clock stands in for the days passing. The rules: successes kept a day, failures a week, cancelled runs forever, and the "report" workflow capped at its newest two runs. An archive hook copies each chunk (two runs per chunk here) to a slice standing in for cold storage before it is deleted, and the app calls `Prune` itself.

A second database shows the guard that surprises people: with no `OnRunFinished` observer in the cluster and `Undelivered` left false, nothing is deleted and `Prune` says why in `HeldUndelivered`.

```sh
cd examples
go run ./retention
```

```text
now: deleted 3, held undelivered 0, idempotency 0, batch 0
a day later: deleted 2
a week later: deleted 1
archived: email failed "fail", email succeeded "a", email succeeded "b", report succeeded "r1", report succeeded "r2", report succeeded "r3"
no observer, Undelivered false: deleted 0, held undelivered 1
Undelivered true: deleted 1
```

## The code

- `retention`: `WithRetention` (ages, `PerWorkflow`, `KeepLast`, `Undelivered`), `WithArchive`, `WithPruneChunk`, `WithoutAutoPrune`.
- `prune`: `Client.Prune` and its `PruneResult`.
- `housekeeping`: `WithTickRetention` and `WithHousekeepingEvery`, the dispatcher's clean-up cadence.

`main_test.go` runs the program and checks every line. The guides [docs/operations/retention.md](../../docs/operations/retention.md) and [docs/operations/tuning.md](../../docs/operations/tuning.md) copy their code from the regions marked `snippet:`.
