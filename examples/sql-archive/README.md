# sql-archive

The SQL driver keeping its queue tables small without losing history. Succeeded runs are kept an hour; with `WithArchiveTable`, `Prune` moves each finished run, with the outputs of its succeeded steps, into the cold tables `wf_runs_archive` and `wf_steps_archive` in the same transaction that deletes it from the hot ones. `ArchivedRuns` reads them back newest first (ties by run ID), two per page, paging with the last run's `FinishedAt` as `FinishedBefore` and its run ID as `AfterRunID`; `INV-1` and `INV-2` finish at the same instant and the page boundary falls between them, so `INV-2` on page 2 is the tie-breaker at work (`FinishedBefore` alone would skip it). The client also sets `WithFetchBatch` and makes its own run and step IDs with `WithIDGenerator`, so the printed IDs are the same on every run. A fake clock stands in for the hours passing.

```sh
cd examples
go run ./sql-archive
```

```text
pruned 3 runs from the hot tables
status of run_000001: not found (it left the queue)
page 1: run_000003 succeeded "INV-3" outputs render="pdf:INV-3" send="sent"
page 1: run_000001 succeeded "INV-1" outputs render="pdf:INV-1" send="sent"
page 2: run_000002 succeeded "INV-2" outputs render="pdf:INV-2" send="sent"
```

## The code

- `archive`: `WithRetention`, `WithArchiveTable`, `WithFetchBatch`, `WithIDGenerator`, `WithoutAutoPrune`.
- `prune`: `Client.Prune` moving the runs out of the hot tables; `Status` no longer finds them.
- `read`: `Client.ArchivedRuns` with `ArchiveQuery` (`Workflow`, `FinishedBefore`, `AfterRunID`, `Limit`).
- `ids`: an `IDGenerator` that shapes each `IDKind` (`IDRun`, `IDStep`, `IDScheduled`, `IDSchedule`).

`main_test.go` runs the program and checks every line. The guide [docs/drivers/sqlstore.md](../../docs/drivers/sqlstore.md#keep-the-hot-tables-small) copies its code from the regions marked `snippet:`.
