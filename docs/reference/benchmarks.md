# Benchmarks

How fast each driver enqueues, runs and picks up jobs, measured with the same workload for every driver. The harness is the `bench/` module (`github.com/ubgo/workflow/bench`); `task bench` runs it.

These numbers come from one developer laptop that was running other work at the same time. Read them as orders of magnitude and as the shape of each system (what limits it, how it reacts to more workers), not as figures to quote. Run the harness on your own hardware before you size anything.

## What is measured

Every system runs the same one-step job: decode a JSON payload, record when the step started, return. The same job count, worker concurrency and payload size go to every system.

| Measure | How |
|---|---|
| Enqueue jobs/s | 5,000 jobs enqueued from as many goroutines as there are workers, while the workers run; jobs divided by the time until the last enqueue call returned |
| End-to-end jobs/s | The same 5,000 jobs divided by the time from the first enqueue call to the last step starting |
| Pickup latency p50, p95, p99, max | 200 probes, one at a time, on an otherwise idle system: the time from just before the enqueue call to the step starting. Each probe first waits a pause between 10 ms and 1.01 s, spread evenly, so probes land at every point of a 1 s poll interval instead of right after the last poll |
| Duplicates | Throughput jobs whose step ran more than once (every run below had 0) |

Before timing, 100 warm-up jobs run so pools, prepared statements and worker goroutines exist. The payload is 256 bytes. "End-to-end" stops at the step starting, which is the same point for every system; a driver's own bookkeeping after the step returns is not in it.

Two ways to enqueue:

- **Same client**: the app enqueues through the client that also runs the workers. A driver may then wake its own workers in-process, which says nothing about another machine.
- **Separate producer**: the app enqueues through a second client that runs no workers, the way a web node enqueues for worker nodes. For sqlstore, the Redis driver and Hatchet this is a client built with the enqueue-only option. The memory driver has no second client, so it has no row there.

Every system runs with its defaults except the worker count: sqlstore polls every second and uses the sqlb pgx driver's pool of 8 connections; the Redis driver polls every second and also wakes workers through pub/sub.

## Setup

| | |
|---|---|
| Date | 2026-10-13 (sqlstore rows re-measured the same evening after its fixes) |
| Machine | Apple M1 Max, 10 CPUs, 64 GiB, macOS 26.4.1 |
| Go | go1.27.1 |
| ubgo/workflow | with the chain-edit pool fix; sqlstore rows include the throughput fix described below |
| Postgres | 17.10 (Postgres.app on the host); each system gets its own database, created for the run and dropped after |
| SQLite | modernc.org/sqlite v1.45.0, a file in a fresh temporary directory |
| Redis | a local `redis-server` 8.4.0 on 127.0.0.1 with no persistence (`--save '' --appendonly no`); the Redis driver used its own key prefix, deleted after the run |
| Hatchet | the hatchet-lite engine of a local Docker stack, SDK v0.107.1 |

The machine's 1-minute load average is printed before each system; the harness waits while it is above 15 (up to 10 minutes). Rows below give the load each system started at. Other test suites and builds were running on the same machine, the same Postgres server and the same Docker engine throughout.

## Results: 10 workers, same client

| System | Load | Enqueue jobs/s | End-to-end jobs/s | Pickup p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|---:|
| memory | 13.8 | 51,714 | 40,698 | 0.05 ms | 0.09 ms | 0.15 ms | 0.21 ms |
| sqlstore, SQLite | 11.7 | 981 | 983 | 1.19 ms | 4.26 ms | 9.04 ms | 16.3 ms |
| sqlstore, Postgres | 5.0 | 2,384 | 1,154 | 5.74 ms | 24.4 ms | 54.4 ms | 71.9 ms |
| sqlstore, Postgres + LISTEN/NOTIFY | 10.4 | 1,394 | 752 | 10.3 ms | 22.7 ms | 31.8 ms | 38.9 ms |
| Redis driver | 6.2 | 22,750 | 11,504 | 0.68 ms | 3.76 ms | 7.39 ms | 24.1 ms |
| Hatchet | 10.0 | 268 | 120 | 12.7 ms | 36.9 ms | 253 ms | 323 ms |

## Results: 10 workers, separate producer

This is the table for a deployment where web nodes enqueue and worker nodes run the jobs.

| System | Load | Enqueue jobs/s | End-to-end jobs/s | Pickup p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|---:|
| sqlstore, SQLite | 5.8 | 396 | 400 | 493 ms | 936 ms | 980 ms | 998 ms |
| sqlstore, Postgres | 5.0 | 2,441 | 579 | 415 ms | 858 ms | 918 ms | 1,009 ms |
| sqlstore, Postgres + LISTEN/NOTIFY | 5.5 | 1,546 | 1,179 | 7.20 ms | 18.7 ms | 24.2 ms | 43.5 ms |
| Redis driver | 6.1 | 21,393 | 5,509 | 0.66 ms | 3.72 ms | 9.61 ms | 25.7 ms |
| Hatchet | 6.2 | 354 | 174 | 12.9 ms | 24.0 ms | 38.1 ms | 402 ms |

## Results: 100 workers, same client

| System | Load | Enqueue jobs/s | End-to-end jobs/s | Pickup p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|---:|
| memory | 11.6 | 41,717 | 18,286 | 0.07 ms | 0.13 ms | 0.31 ms | 0.51 ms |
| sqlstore, SQLite | 5.8 | 705 | 705 | 1.06 ms | 2.69 ms | 4.91 ms | 10.0 ms |
| sqlstore, Postgres | 6.2 | 2,581 | 1,097 | 6.68 ms | 23.2 ms | 33.1 ms | 45.8 ms |
| sqlstore, Postgres + LISTEN/NOTIFY | 7.2 | 1,994 | 783 | 6.60 ms | 28.9 ms | 70.4 ms | 125 ms |
| Redis driver | 14.0 | 18,066 | 9,505 | 3.36 ms | 16.1 ms | 24.3 ms | 257 ms |
| Hatchet | 8.5 | 269 | 269 | 12.2 ms | 21.8 ms | 46.5 ms | 543 ms |

## What the numbers say

- **Polling sets the pickup latency of an idle queue.** A worker on another machine notices a new job on its next poll, so a 1 s poll interval gives a median near 500 ms and a tail near 1 s: sqlstore without a notifier shows it. LISTEN/NOTIFY on Postgres brings sqlstore's separate-producer median from 415 ms to 7 ms and its p99 from 918 ms to 24 ms; the Redis driver's pub/sub wake-up does the same on Redis (under 1 ms median).
- **Same-client numbers flatter polling drivers.** When the enqueuing client also runs the workers, sqlstore wakes its own dispatcher in-process, so its pickup latency is milliseconds even without a notifier. Use the separate-producer table to size a web-node and worker-node deployment.
- **The SQL driver is bound by its transactions.** sqlstore runs roughly 600 to 1,200 jobs a second on Postgres and 400 to 1,000 on SQLite here, one order of magnitude below the Redis driver. On Postgres it now scales with workers (1,154 jobs/s end to end at 10, 1,182 at 50, 1,097 at 100, against 515 and 96 at 10 and 100 before the fix) on the default 8-connection pool; SQLite does not, since it has one writer. The sqlstore rows on this page were measured after the throughput fix described below; the table there has the numbers from before it.
- **Hatchet is bound by the engine round trip.** Every enqueue is a gRPC call to the engine and every step a dispatch from it, so it ran 250 to 350 enqueues a second and 120 to 270 jobs a second end to end here, with a pickup median near 12 ms whichever client enqueued, because the engine pushes work to the worker rather than the worker polling. More workers raised end-to-end throughput (120 to 269 jobs/s) while the enqueue rate stayed flat. This engine is the single-container hatchet-lite on the same laptop; a production Hatchet deployment is sized differently.
- **The memory driver is the ceiling of the code itself**: about 40,000 jobs/s end-to-end at 10 workers, with no storage behind it. It is for tests and single-process tools, not durability.
- **No system ran a throughput job twice** in any run.

## Found by the benchmark

The first run at 100 workers on Postgres hung for ten minutes: every worker held a transaction while a lazy read of the run's chain asked the pool for another connection, and the sqlb pgx pool has 8. The read now happens only when a step edits its chain, while the step runs, before any transaction is open. Any app with more than 8 sqlstore workers on Postgres and the default pool would have hit it.

## sqlstore before and after the throughput fix

The first runs showed sqlstore on Postgres getting slower as workers were added, and one pickup taking 30.1 s. Three causes were found and fixed: every enqueue ran an admission sweep that competed for the same rows (on Postgres and MySQL a run is now admitted inside its own insert transaction); every idle worker woke on every enqueue and step end (now one wakes at a time); and a claimed step whose follow-up read could not get a pool connection stayed leased until its 30 s lease ran out (the claim and the read are now one transaction). Same client, end-to-end jobs/s and pickup p50; "before" is from the first runs, so the load differed:

| sqlstore on | Workers | Before | After |
|---|---:|---:|---:|
| Postgres | 10 | 515 jobs/s, 16.3 ms | 1,154 jobs/s, 5.74 ms |
| Postgres | 50 | not measured | 1,182 jobs/s, 5.17 ms |
| Postgres | 100 | 96 jobs/s, 43.2 ms | 1,097 jobs/s, 6.68 ms |
| Postgres + LISTEN/NOTIFY | 10 | 378 jobs/s, 24.1 ms | 752 jobs/s, 10.3 ms |
| Postgres + LISTEN/NOTIFY | 50 | not measured | 1,162 jobs/s, 5.73 ms |
| Postgres + LISTEN/NOTIFY | 100 | 87 jobs/s, 46.9 ms (max 30.1 s) | 783 jobs/s, 6.60 ms (max 125 ms) |
| SQLite | 10 | 500 jobs/s, 1.94 ms | 983 jobs/s, 1.19 ms |
| SQLite | 50 | not measured | 729 jobs/s, 1.11 ms |
| SQLite | 100 | 424 jobs/s, 10.8 ms | 705 jobs/s, 1.06 ms |

The 783 jobs/s for Postgres with LISTEN/NOTIFY at 100 workers is run-to-run noise, not a cost of NOTIFY: three alternating polling and NOTIFY pairs at 100 workers (load 6 to 11) gave polling 805, 1,221 and 1,264 jobs/s against NOTIFY 1,199, 1,200 and 1,131.

The 10-worker Postgres rows were first measured at load 14.5 and came out at 423 and 557 jobs/s; re-run on a quieter machine (load 5 and 10) they gave the figures above, which shows how much the load moves these numbers. The after-fix runs at 50 and 100 workers overlapped a Hatchet test suite running on the same machine and Postgres server.

## Caveats

- One laptop, one run per configuration, with other work running: expect several-fold run-to-run variation in throughput and in the tails. The load column says how busy the machine was when each system started.
- Everything, including Postgres, Redis and the Hatchet engine, ran on the same machine as the benchmark, so there is no network latency. A real deployment adds a round trip per database or engine call.
- Redis ran without persistence, which makes its writes cheaper than a production Redis with AOF.
- The job does almost nothing. With real work, the step's own time dominates, and these figures matter only for very short jobs or for latency.

## Running it

```sh
task bench                                   # every system, the defaults above
task bench -- -systems memory,sqlite         # a subset
task bench -- -separate-producer             # enqueue through a worker-less client
task bench -- -concurrency 100               # more workers (and enqueuers)
```

`bench/run.sh` points the harness at the host Postgres (`postgres://$USER@localhost:5432/postgres`; it creates and drops one database per system beside it), at Redis on `localhost:7012` (`REDIS_ADDR` overrides it) and at the Hatchet engine of the local stack, reading the engine token from the stack's `.env` without printing it. A system whose service is not configured is skipped. Run `go run . -h` in `bench/` for every flag: job count, payload size, warm-up, probe count and pauses, timeouts, the load limit and sqlstore's Postgres pool size.

The harness prints the machine, the Go and library versions, the Postgres and Redis server versions, the load at start and each system's row as soon as it finishes, then the whole table.
