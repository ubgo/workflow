# Debounce

Debouncing folds a burst of jobs with the same key into one run: a product edited ten times in a minute is reindexed once, a user who changes five settings gets one sync. Each enqueue delays its run by a quiet window and cancels the pending run of the job before it, so only the last job of the burst runs, once the key has been quiet for the window. A `maxWait` cap makes a key that never goes quiet still run at least that often.

[`middleware.Debounce`](ds:block?id=mw-debounce-5g2ska6s) is enqueue middleware built on two things the driver already does, a delayed start (`Job.RunAt`) and `Client.Cancel`, so it runs on any driver that declares both ([`debounceNeeds`](ds:block?id=mw-debounceneeds-8ngd42s6)). Where the burst state lives decides its reach: in one process by default, or in every process that shares a `DebounceStore`.

## Before you start

- A client with `CapDelayedStart` and `CapCancel`: the memory, SQL, Redis and Hatchet drivers all have both ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t), [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)).
- A key function that returns the debounce key for a job, or `""` for a job that should not be debounced.
- The runnable program for this page is [examples/debounce](../../examples/debounce/).

## Debounce a burst

[`middleware.Debounce(c, clk, key, window, maxWait, opts...)`](ds:block?id=mw-debounce-5g2ska6s) takes the client the jobs finally reach (it calls `c.Cancel`), the clock the windows are measured on (`nil` means the real clock), the key function, the quiet window and the cap. A `maxWait` of zero means no cap. It returns the middleware and an error; wrap the client with it.

The example runs the memory driver on a fake clock, so the ten-second window passes when the program says so:

<!-- snippet: examples/debounce/main.go#debounce -->
```go
clk := clock.NewFake(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))
mc := memory.New(memory.WithClock(clk))
if err := mc.Register(reindex(ran)); err != nil {
	return err
}
// Each job waits 10s of quiet; a key that never goes quiet still runs
// every minute. Debounce needs CapDelayedStart and CapCancel.
debounce, err := middleware.Debounce(mc, clk, byProduct, 10*time.Second, time.Minute)
if err != nil {
	return err
}
c := middleware.WrapClient(mc, debounce)
```

Each job of a burst is enqueued with `RunAt` set to now plus the window (capped at the burst's first job plus `maxWait`), and the run it replaces is cancelled ([`debouncer.wrap`](ds:block?id=mw-debouncer-wrap-pesvu4h9)). Three changes two seconds apart leave the first two runs cancelled; ten quiet seconds later the third one runs:

<!-- snippet: examples/debounce/main.go#burst -->
```go
var refs []workflow.RunRef
for n := 1; n <= 3; n++ {
	ref, err := c.Enqueue(ctx, workflow.Job{
		Workflow: "reindex",
		Payload:  fmt.Appendf(nil, `{"change":%d}`, n),
		Labels:   map[string]string{"product": "p-42"},
	})
	if err != nil {
		return err
	}
	refs = append(refs, ref)
	clk.Advance(2 * time.Second) // the next change comes 2s later
}
```

The burst state is a [`DebounceBurst`](ds:block?id=mw-debounceburst-x7dgffne) per key (`First`, `RunAt` and `Ref`, the pending run), kept in a `DebounceStore`. The default is a [`MemoryDebounce`](ds:block?id=mw-memorydebounce-abd56ahw) of the middleware's own, so debouncing holds inside one process, and calls in that process are serialised by one lock held across the driver calls ([`Debounce`](ds:block?id=mw-debounce-5g2ska6s)). [`DebounceStore`](ds:block?id=mw-debouncestore-gp9rakek) is the seam for a store of your own: `Get` returns a key's burst and is only a hint, and `Swap` must replace the burst and return the one it replaced in one atomic step, because `Swap` is what decides which run is cancelled ([`TestDebounce_SwapDecidesWhatIsCancelled`](ds:block?id=mw-testdebounce-swapdecideswhatiscancelled-pfkp9ket)).

## A store shared by every process

With two web servers, a key enqueued from both is debounced in each one separately: each keeps its own burst and both runs fire. [`middleware.WithDebounceStore(store)`](ds:block?id=mw-withdebouncestore-5r4vkz95) puts the bursts in a store the processes share ([`TestDebounce_SharedStoreAcrossProcesses`](ds:block?id=mw-testdebounce-sharedstoreacrossprocesses-7wkwjqux)). [`sqlstore.DebounceStore`](ds:block?id=sql-debouncestore-jsm48sbx) keeps them as rows of `<prefix>debounce`, and its [`Swap`](ds:block?id=sql-debouncestore-swap-xd9g7ped) is atomic across processes: it locks the key's row before reading the burst it replaces, and for a key with no row yet the primary key lets one insert win while the loser retries and finds the winner's row. Every replaced run is returned to exactly one caller and cancelled once ([`TestDebounceSwapRace`](ds:block?id=enginetest-testdebounceswaprace-xyrjyux2)).

Like the unique store it needs no running SQL driver: [`sqlstore.NewDebounceStore(db, dialect, opts...)`](ds:block?id=sql-newdebouncestore-4ymc7ecv) builds it over any `*sql.DB` and its [`Migrate`](ds:block?id=sql-debouncestore-migrate-b4je72m3) creates only the debounce table; an app on the SQL driver takes [`Client.DebounceStore()`](ds:block?id=sql-client-debouncestore-hxs5szus), whose table `Client.Migrate` creates.

The example runs two sqlstore clients on one SQLite file, standing in for two servers on one Postgres or MySQL database. Each has its own `Debounce` middleware over its own client, and both use the debounce table of the same database. Give `Debounce` the clock the client schedules on (`sqlstore.WithClock`; both default to the real clock): the middleware sets `RunAt` on its clock and the driver starts the run on its own. The example shares one fake clock between both clients and both middlewares and advances it by the window after the burst:

<!-- snippet: examples/debounce/main.go#sql-store -->
```go
// The window is measured on the clock the client schedules on.
debounce, err := middleware.Debounce(sc, clk, byProduct, time.Second, 0,
	middleware.WithDebounceStore(sc.DebounceStore()))
if err != nil {
	return process{}, nil, err
}
c := middleware.WrapClient(sc, debounce)
```

A burst that alternates between the two processes (A, B, A) still runs once, with the last job, and the two runs it replaced are cancelled no matter which process enqueued them ([`TestDebounceAcrossProcesses`](ds:block?id=enginetest-testdebounceacrossprocesses-9j3drg7t)).

Rows whose run has started are over: [`Debounce`](ds:block?id=mw-debouncer-wrap-pesvu4h9) treats a burst with a past `RunAt` as no burst. A running `sqlstore.Client` deletes them on its [housekeeping](ds:block?id=sql-client-prune-5kgznfp8); an app with no SQL client calls [`DebounceStore.PruneExpired`](ds:block?id=sql-debouncestore-pruneexpired-hbamxymb).

## Try it

```sh
cd examples
go run ./debounce
```

```text
-- Debounce a burst
job 1: cancelled
job 2: cancelled
ran with {"change":3}
-- A store shared by every process
ran with {"change":3}
job 1 (from process A): cancelled
job 2 (from process B): cancelled
job 3 (from process A): succeeded
```

## Pitfalls

- [`Debounce`](ds:block?id=mw-debounce-5g2ska6s) returns an `UnsupportedError` (matching `workflow.ErrUnsupported`) when the client lacks `CapDelayedStart` or `CapCancel`, rather than letting every job through undebounced ([`TestDebounce_RefusesWithoutCapabilities`](ds:block?id=mw-testdebounce-refuseswithoutcapabilities-m46rn3wv)).
- A nil key function, a window of zero or less, or a `maxWait` that is neither zero nor at least the window is `workflow.ErrInvalid` ([`Debounce`](ds:block?id=mw-debounce-5g2ska6s)).
- The job's own `RunAt` is replaced by the debounce time ([`debouncer.wrap`](ds:block?id=mw-debouncer-wrap-pesvu4h9)).
- Do not set `Job.IdempotencyKey` on debounced jobs: the pending run holds the key, so the replacement would be folded into it instead of replacing it.
- The new run is enqueued before the old one is cancelled, so a failed enqueue never loses the burst. If the cancel then fails, `Debounce` returns the new `RunRef` together with the error, and both runs may fire ([`debouncer.wrap`](ds:block?id=mw-debouncer-wrap-pesvu4h9)).
- A previous run whose start time has passed is left alone and the job opens a new burst ([`debouncer.wrap`](ds:block?id=mw-debouncer-wrap-pesvu4h9)); cancelling it could stop a run that is already working.
- A store error fails closed: on `Get` the job is not enqueued, and on `Swap` the job's new run is cancelled and the error returned, so the store and the engine never disagree about which run is pending ([`TestDebounce_StoreErrorFailsClosed`](ds:block?id=mw-testdebounce-storeerrorfailsclosed-9zxznnyv)).
- A shared store needs a shared backend. [`Debounce`](ds:block?id=mw-debouncer-wrap-pesvu4h9) cancels the replaced run through the client it was given and ignores `workflow.ErrNotFound`, so processes that each run their own in-memory driver would not find each other's runs and both would fire. Share the store between processes that enqueue onto the same database or the same engine.
- **On Hatchet the burst state stays in the process.** The Hatchet driver offers no `DebounceStore`, so with several processes pass the [SQL](ds:block?id=sql-debouncestore-jsm48sbx) or [Redis](ds:block?id=redis-debouncestore-mhpmmnn9) driver's store. See [Hatchet limitations](../drivers/hatchet.md#limitations).

## Related

- [Unique jobs](unique-jobs.md): keep the first job instead of the last.
- [Delays](../jobs/delays.md) and [cancel and status](../jobs/cancel-and-status.md): the two driver features debouncing is built on.
- [Enqueue middleware](../middleware/enqueue-middleware.md): how `WrapClient` and middleware order work.
- [The SQL driver](../drivers/sqlstore.md): migrations, housekeeping and the tables.
- Example: [examples/debounce](../../examples/debounce/).
