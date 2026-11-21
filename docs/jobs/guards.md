# Job guards

Some jobs should not run at all, or not now: a one-time login code mailed twenty minutes late, an export that sat behind a dead worker for an hour, the ten-thousandth job of a runaway import, a push notification at three in the morning in the user's zone, an order whose payload is missing its amount. The guards on this page stop each of those at the edge, before a worker spends time on it or a customer sees the result, and the retry ladder and invariants keep failing work honest once it does run.

Every guard is middleware or a plain function over the `workflow.Client` interface, so it works on any driver unless its section says otherwise. Enqueue middleware goes in a `middleware.WrapClient` chain on the side that enqueues; step middleware goes in the worker's step middleware (`memory.WithMiddleware`, `sqlstore.WithMiddleware`, `hatchet.Config.Middleware`) or around one step's handler.

## Before you start

- Expiry (`Expire`, `ExpireAfter`, `ExpireAt`) runs on every driver. `ExpireMaxWait` needs a driver that fills `StepInfo.ReadyAt` (memory, sqlstore, Redis). The sweep `workflow.ExpireWaiting` needs `CapRunQuery` and `CapCancel` (memory, sqlstore, Redis, Hatchet).
- Bounds and quotas run on every driver with your own count; the stock counts `DepthFromRuns` and `TenantCount` need `CapRunQuery`, and `TenantCount` also `CapTenant`. `BoundedDelay` and `QuietHours` move a job's start, which needs `CapDelayedStart`.
- `LadderPolicy` and `RetryLadder` set `RetryPolicy.DelayFor`, which needs `CapRetryDelay` (memory, sqlstore, Redis; Hatchet refuses it).
- Validation, invariants and `Check` run on every driver.
- The runnable program for this page is [examples/job-guards](../../examples/job-guards/). The counts of your own for bounds and quotas are in [examples/queue-guards](../../examples/queue-guards/).

## Expire a job that did not start in time

`middleware.ExpireAt(job, t)` stamps a job with the instant after which it must not start; `middleware.ExpireAfter(d, clk)` stamps every job with an expiry `d` after its start (after `RunAt` for a delayed job) and lets the reserved label through to the driver. On the worker, `middleware.Expire(clk)` checks the label when a step's first attempt begins and, when the expiry has passed, ends the run with `workflow.Discard` of an error matching `middleware.ErrExpired` instead of running it:

<!-- snippet: examples/job-guards/main.go#expire -->
```go
clk := clock.NewFake(start)
// The worker ends a run that starts after its expiry, instead of running it.
mc := memory.New(memory.WithClock(clk), memory.WithMiddleware(middleware.Expire(clk)))
// The enqueuing side stamps every job with an expiry 15 minutes after its start.
c := middleware.WrapClient(mc, middleware.ExpireAfter(15*time.Minute, clk))
```

<!-- snippet: examples/job-guards/main.go#expire-at -->
```go
// A one-time code mailed later than 12:05 is useless: expire it then.
job := middleware.ExpireAt(workflow.Job{Workflow: "send-code", RunAt: start.Add(10 * time.Minute)}, start.Add(5*time.Minute))
ref, err := c.Enqueue(ctx, job)
```

The job's own expiry, 12:05, wins over the 15-minute default; its start at 12:10 is past it. The run ends `discarded` (with `CapRunStates`; `cancelled` with only `CapDiscard`, failed on a driver with neither) and the code is never sent. `ExpireFail()` ends it failed instead, for work whose lapse an operator must see. A retry of a step whose first attempt began in time is left alone: the run had started. For a workflow of several steps, `ExpireSteps("first")` limits the check to the steps that start a run, so a run that began in time is never cut off between two steps.

## Time out the wait for a worker

`ExpireMaxWait(d)` adds a schedule-to-start timeout to `Expire`: any step attempt that waited longer than `d` since it became ready (`StepInfo.ReadyAt`, which counts from the run's admission, the parents' end or the retry backoff's end) is expired the same way. That catches work a busy cluster picks up too late.

A run that no worker ever takes (every worker of its queue is down, or nobody serves the queue) never reaches a step, so step middleware cannot see it. `workflow.ExpireWaiting` is the sweep for that case: call it from a schedule or a ticker and it cancels every not-yet-started run that has waited longer than the bound:

<!-- snippet: examples/job-guards/main.go#sweep -->
```go
// Cancel every export that has waited more than 15 minutes for a worker.
res, err := workflow.ExpireWaiting(ctx, mc, workflow.RunQuery{Workflow: "export"}, 15*time.Minute, clk.Now())
if err != nil {
	return err
}
fmt.Printf("  checked %d waiting run(s), cancelled %d\n", res.Checked, len(res.Cancelled))
```

The wait counts from the later of the run's enqueue and its due time, so a delayed run starts waiting when it becomes due. On a client with `CapCancelAll` each cancel is guarded by the run's listed status, so a run a worker took between the listing and the cancel is left running and counted in `Raced`. Pair the sweep with an `admin.Monitor` queue-depth alert when the operator should hear about it, not just the caller.

## Bound a queue and cap a tenant

`middleware.Bounded(depth, limit)` refuses an enqueue with `middleware.ErrQueueFull` when the job's queue already holds its limit of waiting runs; `middleware.Quota(count, max)` does the same per tenant with `middleware.ErrQuotaExceeded`. `DepthFromRuns(c)` counts the waiting runs of a queue and `TenantCount(c)` the unfinished runs of a tenant, both through `RunQuerier`; any function with the `DepthFunc` signature can replace them, such as a counter your own store keeps:

<!-- snippet: examples/job-guards/main.go#bounded -->
```go
depth, err := middleware.DepthFromRuns(mc) // waiting runs per queue, through RunQuerier
if err != nil {
	return err
}
perTenant, err := middleware.TenantCount(mc) // unfinished runs per tenant
if err != nil {
	return err
}
c := middleware.WrapClient(mc,
	middleware.Quota(perTenant, 2), // at most two unfinished runs per tenant
	middleware.Bounded(depth, middleware.QueueLimits(map[string]int{workflow.DefaultQueue: 3})),
)
```

Two options change what a full queue does. `BoundedWait(poll, clk)` blocks the enqueue, re-reading the count every `poll`, until there is room or the caller's ctx ends, for producers that should slow down rather than drop work. `BoundedDelay(d, clk)` makes the limit soft: the job is enqueued to start `d` later instead of being refused, which spreads a burst out.

On the memory driver a bound is also a memory bound: every waiting run lives in the process, so `Bounded` with `DepthFromRuns` keeps a burst from growing it without end (an offline device queueing uploads, for instance). The run a worker holds is not waiting and does not count.

### Count with a function of your own

`Bounded` takes a `middleware.DepthFunc`, the count of what a job would join, and `Quota` a `middleware.CountFunc`, the count of what a job counts against; `CountFunc` is the same type, `func(ctx, job) (int, error)`. The stock ones count a queue's waiting runs and a tenant's unfinished runs; write your own when the bound is about something else. This one bounds the waiting runs of each workflow, whatever queue they are on, and the limit function bounds only `export`:

<!-- snippet: examples/queue-guards/main.go#depth -->
```go
// Waiting runs of the job's workflow, whatever queue they are on: the
// stock DepthFromRuns counts per queue instead.
perWorkflow := middleware.DepthFunc(func(ctx context.Context, job workflow.Job) (int, error) {
	page, err := mc.Runs(ctx, workflow.RunQuery{
		Workflow: job.Workflow,
		Statuses: []workflow.Status{workflow.StatusQueued, workflow.StatusScheduled},
		Limit:    workflow.MaxRunsLimit,
	})
	if err != nil {
		return 0, err // the enqueue fails: a bound that cannot be checked is not let through
	}
	return len(page.Runs), nil
})
// limit is the bound per job: only export is bounded.
limit := func(job workflow.Job) int {
	if job.Workflow == exportWorkflow {
		return maxWaitingExports
	}
	return 0 // no bound: the job passes without a count
}
```

The quota here reads a tenant's open uploads from a table the app keeps, so it never asks the driver:

<!-- snippet: examples/queue-guards/main.go#count -->
```go
// Open uploads per tenant, from a table the app keeps (a SQL count in
// production); Quota never asks the driver.
openUploads := map[string]int{"acme": maxOpenUploads, "globex": 1}
var tableUp atomic.Bool
tableUp.Store(true)
fromTable := middleware.CountFunc(func(_ context.Context, job workflow.Job) (int, error) {
	if !tableUp.Load() {
		return 0, errTableDown
	}
	return openUploads[job.Tenant], nil
})

c := middleware.WrapClient(mc,
	middleware.Quota(fromTable, maxOpenUploads),
	middleware.Bounded(perWorkflow, limit),
)
```

```text
globex export 1: accepted
globex export 2: accepted
globex export 3: refused, queue full (workflow/middleware: queue is full: 2 of 2 for workflow "export")
globex report: accepted
acme report: refused, quota (workflow/middleware: tenant quota exceeded: 5 of 5 for workflow "report")
report without a tenant: accepted
globex report, table down: failed (workflow/middleware: Quota count: uploads table unreachable)
```

The count runs on every enqueue that has a limit: a limit of zero or less (the `report` workflow under `Bounded`, a job without a tenant under `Quota`) passes without a call. A count that returns an error fails the enqueue with that error, which matches neither `ErrQueueFull` nor `ErrQuotaExceeded`: a bound that cannot be checked is not let through. A nil count or limit function fails every enqueue with `workflow.ErrInvalid`.

`ErrQueueFull` and `ErrQuotaExceeded` are backpressure, not outages: neither matches `workflow.ErrUnavailable`, so a failover client never sends a job to its secondary because the primary is busy.

## Keep jobs out of quiet hours

`middleware.QuietHours(windows, zoneOf, clk)` moves a job whose start falls in quiet hours to the first minute it may start, on the wall clock of the zone `zoneOf` returns for that job. The windows are the same `workflow.Windows` a schedule uses: quiet hours are `Unless` windows, sending hours `Between` windows:

<!-- snippet: examples/job-guards/main.go#quiet -->
```go
night := workflow.Windows{Unless: []workflow.Window{{From: "21:00", To: "08:00"}}}
zoneOf := func(job workflow.Job) *time.Location {
	loc, err := time.LoadLocation(job.Labels["tz"]) // the recipient's zone
	if err != nil {
		return nil // UTC
	}
	return loc
}
c := middleware.WrapClient(mc, middleware.QuietHours(night, zoneOf, clk))
```

At 12:00 UTC it is 21:00 in Tokyo and 13:00 in Berlin, so the Tokyo notification is moved to 08:00 the next morning and the Berlin one starts now. A job that is not moved needs nothing from the driver; a moved one is enqueued with `RunAt` and needs `CapDelayedStart`. The move follows the zone across daylight-saving changes, landing on the first minute that exists.

## Validate payloads at enqueue

`middleware.ValidateJSON[T](workflow)` decodes each payload of that workflow as a `T` and calls its `Validate() error` method when it has one; `middleware.ValidatePayload(rules)` takes any check per workflow, and `JSONRule[T]()` is the JSON check for a rules table. A failing check refuses the enqueue with an error wrapping `workflow.ErrInvalid`, so the caller sees the mistake while it can still fix it, instead of a worker failing later:

<!-- snippet: examples/job-guards/main.go#validate-type -->
```go
// Order is the payload of the charge workflow. Its Validate method runs at
// enqueue, through ValidateJSON.
type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func (o *Order) Validate() error {
	if o.Amount <= 0 {
		return fmt.Errorf("order %s: amount %d is not positive", o.ID, o.Amount)
	}
	return nil
}
```

<!-- snippet: examples/job-guards/main.go#validate -->
```go
c := middleware.WrapClient(mc, middleware.ValidateJSON[Order]("charge"))
_, err := c.Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`{"id":"o-7","amount":0}`)})
fmt.Println("  invalid:", errors.Is(err, workflow.ErrInvalid), err)
```

The jobs of `Job.Chain` are checked too, since the driver enqueues them later without middleware.

## Retry on an explicit ladder

`workflow.LadderPolicy(delays...)` retries once per delay, in order: the shape webhook senders publish ("after 5 seconds, 5 minutes, 30 minutes, 2 hours"). `workflow.RetryLadder(delays...)` is the `DelayFor` function alone, for a policy whose `Max` is larger than the ladder: retries past the end wait the last delay again.

<!-- snippet: examples/job-guards/main.go#ladder -->
```go
policy := workflow.LadderPolicy(5*time.Second, 5*time.Minute, 30*time.Minute, 2*time.Hour)
wf := workflow.Workflow{Name: "deliver-webhook", Retry: policy, Steps: []workflow.Step{{Name: "post", Run: noop}}}
```

## State invariants and step conditions

`workflow.Invariant(cond, format, args...)` returns nil when the condition holds and otherwise a permanent error matching `workflow.ErrConstraint`: retrying the same data cannot make it hold, so the run fails at once and the message names the rule. `middleware.Check(pre, post)` states a step's contract from outside the step: `pre` checks the input before the step runs, `post` the output after it succeeds, and a breach of either becomes the same permanent `ErrConstraint` error (a `post` breach also drops the output, so no child step consumes it):

<!-- snippet: examples/job-guards/main.go#check -->
```go
reserve := func(ctx context.Context, in []byte) ([]byte, error) {
	var qty int
	if err := json.Unmarshal(in, &qty); err != nil {
		return nil, workflow.Permanent(err)
	}
	stock := 10 - qty
	if err := workflow.Invariant(stock >= 0, "stock would drop to %d", stock); err != nil {
		return nil, err // permanent: retrying the same order cannot fix it
	}
	return json.Marshal(stock)
}
positive := func(_ context.Context, in []byte) error {
	if string(in) == "0" {
		return errors.New("quantity is zero")
	}
	return nil
}
step := workflow.Step{Name: "reserve", Run: middleware.Check(positive, nil)(reserve)}
```

Both the zero quantity (the precondition) and the order larger than the stock (the invariant) fail after one attempt, although the workflow allows three. For routing on data rather than refusing it, use `workflow.Choice`.

## Try it

```sh
cd examples
go run ./job-guards
```

The guards part of the output (the program goes on with the pools of [Pools, critical sections and auto-disable](../reliability/pools.md)):

```text
expiry:
  late run: discarded, code sent 0 time(s)
sweep:
  checked 2 waiting run(s), cancelled 2
bounds:
  acme: enqueued
  acme: enqueued
  acme: refused, quota exceeded
  globex: enqueued
  globex: refused, queue full
quiet hours:
  Asia/Tokyo: starts Tue 08:00 JST
  Europe/Berlin: starts now
validation:
  invalid: true workflow: invalid spec: payload of workflow "charge": order o-7: amount 0 is not positive
  valid order enqueued
ladder:
  4 retries after 5s, 5m0s, 30m0s, 2h0m0s
conditions:
  qty 4: stock left 6
  qty 0: failed after 1 attempt(s), constraint violated: true
  qty 12: failed after 1 attempt(s), constraint violated: true
```

The late login code is discarded before its step runs; the sweep cancels the two runs that waited too long; the tenant `acme` hits its quota of two and `globex` the queue bound of three; the Tokyo push waits for 08:00 while the Berlin one starts now; the invalid order is refused at enqueue; the ladder lists its four delays; and both orders that break an invariant fail on their first attempt. `go test ./job-guards` checks every line.

The counts of your own:

```sh
cd examples
go run ./queue-guards
```

The output is the one under [Count with a function of your own](#count-with-a-function-of-your-own); `go test ./queue-guards` checks it.

## Pitfalls

- `ExpireAt` sets a reserved label, so a job carrying it must reach the driver through a client wrapped with `ExpireAfter` (with a `d` of zero when no default expiry is wanted). `Unique` and `ClaimCheck` refuse reserved labels they do not own, so put `ExpireAfter` after them in the chain and let it stamp the expiry; a job built with `ExpireAt` cannot pass through them.
- The expiry is only enforced where `Expire` is in the worker's step middleware. Enqueuing with `ExpireAfter` alone stamps a label nobody reads.
- Without `CapDueQuery`, `ExpireWaiting` measures a delayed run from its enqueue, which overstates its wait, and never cancels a `scheduled` run. Without `CapCancelAll` it cancels with `Client.Cancel`, which also stops a run a worker took in the instant between the listing and the cancel.
- `Bounded` and `Quota` count, then insert: enqueues racing each other, in one process or several, can all see room and overshoot the limit by the number of racers. They are backpressure, not a hard cap. A cap that must hold exactly belongs to the driver (`Concurrency` per key, the queue's global limit).
- `DepthFromRuns` and `TenantCount` read at most `middleware.MaxDepthScan` (10,000) runs per count, one `Runs` call per thousand, on every enqueue that has a limit. A deeper bound needs a `DepthFunc` that asks your store directly.
- The middleware cannot see `Workflow.Queue`, so `QueueLimits` and `DepthFromRuns` take a job's queue from `Job.Queue` (or `workflow.DefaultQueue`). Set `Job.Queue` on jobs of a workflow declared on its own queue.
- Put `ValidatePayload` before `WithCodec` and `ClaimCheck` (the `WrapClient` outside `WithCodec`), so it sees the payload the app wrote rather than encrypted or offloaded bytes.
- A ladder needs `CapRetryDelay`; on Hatchet the policy is refused at registration, never silently replaced by the backoff.

## Related

- [Enqueue middleware](../middleware/enqueue-middleware.md): how `WrapClient` chains enqueue middleware such as `Bounded`, `Quota` and `QuietHours`.
- [Step middleware](../middleware/step-middleware.md): where `Expire` and other step guards go.
- [Retries](retries.md): the policy `LadderPolicy` and `RetryLadder` set.
- [Queues](queues.md): queues, per-queue workers and the cluster-wide limit, the exact caps `Bounded` is not.
- [Pools, critical sections and auto-disable](../reliability/pools.md): the rest of the job-guards program.
- Example: [examples/job-guards](../../examples/job-guards/).
- Example: [examples/queue-guards](../../examples/queue-guards/) (`DepthFunc`, `CountFunc` of your own).
