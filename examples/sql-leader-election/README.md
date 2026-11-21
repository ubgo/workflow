# sql-leader-election

How the SQL driver keeps the nodes of one database in agreement when their clocks and their health differ. Each part runs on its own SQLite file; the clients live in one process so the program can print what each did.

- **Leader election.** Two clients with `WithLeaderElection`: the first leads the role `scheduler` (cron fires and housekeeping), the second follows, and when the first stops it resigns and the second takes over. A standalone `NewLeaderElector` campaigns for a role of the app's own, and its `Term` grows on the handover. `WithElector` takes an elector of the app's own instead; `Client.IsLeader` reports the answer.
- **The database clock.** `NewDatabaseClock` reads the server's clock once (`Offset`, `Uncertainty`); `WithDatabaseClock` makes it a client's one time source, with `WithLeaseSkew` as the margin other nodes wait past a lease.
- **Leases.** node-a claims a step and stalls: its fake lease clock jumps a whole lease with no renewal, so the attempt is cancelled with `ErrLeaseLost` and writes nothing. node-b, whose lease clock is past the lease end, takes the step over and runs it. With `WithMaxReclaims(0)` the same takeover fails the attempt with `ErrAbandoned` instead.

```sh
cd examples
go run ./sql-leader-election
```

```text
node-a leads: true, node-b leads: false
after node-a stopped: node-a leads: false, node-b leads: true
node-1 won: true (term 1), node-2 won: false
after node-1 resigned: node-2 won: true (term 2)
client with a follower elector may fire cron: false
and once it leads: true
database clock: offset under 1s: true, uncertainty under 1s: true
client runs on the database clock: true
node-a stopped its attempt with ErrLeaseLost: true
node-b took the step over and ran it: run succeeded
WithMaxReclaims(0): run failed, abandoned: true
error: sqlstore: step "charge" failed after 1 attempt(s): sqlstore: step "charge" abandoned by 1 worker (its lease expired 1 time, over WithMaxReclaims 0): sqlstore: step abandoned by its workers
```

## The code

- `election`, `handover`: `WithLeaderElection` with `LeaderLease`, `Client.IsLeader`, the handover on `Stop`.
- `elector`: `NewLeaderElector` with `LeaderLease` and `LeaderTablePrefix`; `Campaign`, `Term`, `Resign`.
- `custom-elector`, `with-elector`: an `Elector` of the app's own and `WithElector`.
- `dbclock`: `NewDatabaseClock`, `Sync`, `Offset`, `Uncertainty`, `WithDatabaseClock`, `WithLeaseSkew`.
- `lease-holder`: `WithLease`, `WithLeaseClock`, `WithLeaseSkew` and a handler that tells `ErrLeaseLost` apart with `context.Cause`.
- `lease-taker`: `WithMaxReclaims` on the node that takes the step over.

`main_test.go` runs the program and checks every line. The guide [docs/drivers/sqlstore.md](../../docs/drivers/sqlstore.md#clocks-skew-and-one-fire-node) copies its code from the regions marked `snippet:`.
