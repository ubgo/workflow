# datasets-shared

Where `events.Datasets` keeps what its triggers already used. First `events.MemoryLineage` on its own, as a unit test would use it: a record saved twice is kept once, `Find` returns the newest first and refuses a negative `Limit`. Then two processes, each with its own connection pool and `sqlstore` client over one SQLite file, sharing a `sqlstore.LineageStore` (the first through `Client.LineageStore()`, the second through `sqlstore.NewLineageStore` on its own `*sql.DB`): a version the first process announced and consumed starts nothing when the second announces it again. Last, the same two processes with a `MemoryLineage` each, where the second starts the trigger a second time for the same version. SQLite runs in a temporary file, so nothing needs to be installed.

```sh
cd examples
go run ./datasets-shared
```

```text
memory lineage:
  records: 2, newest: 2026-11-20T10, negative Limit is ErrInvalid: true
shared SQL lineage store:
  process A announces 2026-11-20T09: started audit
  process B announces 2026-11-20T09: started nothing
  process B announces 2026-11-20T10: started audit
  audit runs recorded in the shared store: 2
a MemoryLineage per process:
  process A announces 2026-11-20T11: started audit
  process B announces 2026-11-20T11: started audit
```

## The code

`MemoryLineage` used directly:

<!-- snippet: examples/datasets-shared/main.go#memory-lineage -->
```go
store := events.NewMemoryLineage() // in-process; what NewDatasets uses without WithLineage
v09 := events.Lineage{Kind: events.LineageProduced, Dataset: orders, Version: "2026-11-20T09",
	Workflow: "load-orders", Step: "write", Meta: map[string]string{"rows": "1200"}, At: t0}
v10 := v09
v10.Version, v10.At = "2026-11-20T10", t0.Add(time.Hour)
for _, l := range []events.Lineage{v09, v09, v10} { // the second v09 is the same fact: ignored
	if err := store.Record(ctx, l); err != nil {
		return err
	}
}
all, err := events.Producers(ctx, store, orders) // newest first
if err != nil {
	return err
}
newest, err := store.Find(ctx, events.LineageQuery{Kind: events.LineageProduced, Dataset: orders, Limit: 1})
if err != nil {
	return err
}
_, refused := store.Find(ctx, events.LineageQuery{Limit: -1})
```

One SQL lineage store, reached by two processes:

<!-- snippet: examples/datasets-shared/main.go#shared-store -->
```go
la := a.c.LineageStore() // the lineage table in the client's database and prefix
if err := la.Migrate(ctx); err != nil {
	return err // Client.Migrate does not create this table: the store's own Migrate does
}
lb, err := sqlstore.NewLineageStore(b.db, sqlb.SQLiteDialect{}) // any *sql.DB, no Client needed
if err != nil {
	return err
}
dsA, err := a.datasets(la) // events.NewDatasets(bus, events.WithLineage(la)) plus the trigger
if err != nil {
	return err
}
dsB, err := b.datasets(lb)
if err != nil {
	return err
}
```

The guide that copies this program is [Datasets and lineage](../../docs/events/datasets.md#share-the-store-between-processes). `go test ./datasets-shared` checks every line.
