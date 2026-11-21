# redis-parity

What the Redis driver does at the level of the SQL driver. Without a server it prints what the driver declares and shows its two refusals at `Start` (an expression key and a queue limit across clients). With `REDIS_URL` set it runs, under a fresh key prefix deleted at the end:

- every worker option (`WithClock`, `WithLeaseClock`, `WithLease`, `WithMaxReclaims`, `WithPollInterval`, `WithWorkers`, `WithIDFunc`, `WithLogger`, `WithMiddleware`, `WithCronParser`, `WithJitterSource`);
- a durable wait released by `Signal`, seen by a run-finished observer;
- a concurrency key holding three runs of one customer to one at a time across two clients;
- a rate limit of two per hour, the third run held in Redis;
- a named queue served by its own worker, and a workflow pause stored in Redis;
- a chain, a `ScheduleWhen` condition that skips a tick, and a clean stop that hands a run to the next worker with the same attempt number;
- the shared stores built on their own with `NewDataStore`, `NewPromiseStore`, `NewJournalStore`, `NewInbox`, `NewBucketStore` and `NewAggregateStore`.

```sh
cd examples
go run ./redis-parity                                   # the offline part
REDIS_URL=redis://127.0.0.1:36379/0 go run ./redis-parity # everything
```

```text
WaitForEvent: true
ConcurrencyKey: true
RateLimit: true
Queue: true
PauseWorkflow: true
PauseQueue: true
RunFinished: true
Chain: true
ScheduleWhen: true
ShutdownRequeue: true
Closure: false
QueueLimitGlobal: false
KeyExpr at Start: ErrUnsupported = true
QueueConfig.Limit at Start: ErrUnsupported = true
approval: the step after the wait got "by ops"
run-finished observer saw approval: succeeded
invoice: 3 runs of customer acme on two clients, at most 1 at a time
sms: 2 succeeded, the next one is queued (2 per hour)
render while paused: queued, paused targets 1
render after resume: succeeded on queue reports by [reports-node]
chain: extract then load
heartbeat: a tick skipped (reason "when"), a later one fired
long: cut short on deploy-old, finished on deploy-new, attempts [1 1]
data: plan="pro" version 1
promise: first settle true, second settle false, value "paid"
journal: fetch=42 found true
```

Without `REDIS_URL` the output stops after the refusals with `REDIS_URL is not set: the live run is skipped`. The live part takes about ten seconds, most of it the schedule waiting for its second tick.

## The code

- `declares` and `refuses`: what the driver declares, and the refusals at `Start`.
- `options`: every worker option.
- `workflows`: the wait step, the concurrency key, the rate limit, the chain's two workflows and the schedule condition.
- `observers`: `OnRunFinished` and `OnTick`, registered before `Start`.
- `queues`, `signal`, `pause`, `chain`, `condition`: one feature each.
- `clean-stop`: a clean stop hands the run back without spending the attempt.
- `stores`: the six standalone store constructors.

`main_test.go` runs the program and checks every line, offline or live. The guide [docs/drivers/redis.md](../../docs/drivers/redis.md) copies its code from the regions marked `snippet:`.
