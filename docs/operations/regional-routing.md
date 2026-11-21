# Regional routing

`regional.New(regions, router)` joins one client per region into one `workflow.Client` that sends each job to the region a `Router` picks. An app keeps its EU customers' runs, payloads and results in an EU database or engine and its US customers' in a US one, while its code talks to one client. Each region runs only the jobs routed to it: a job never moves to another region, not even when its region is down.

## Before you start

- One fresh, unstarted client per region, owned by the regional client: it registers the workflows in every region and starts every region.
- A rule that says which region a job belongs to: a label the app already sets, the job's tenant, a lookup in the app's own customer table.
- The runnable program for this page is [examples/regional-routing](../../examples/regional-routing/). Every region there is the memory driver.

## Route each job to its region

Build the client with a map of region names to clients and a router, then register and start through it, never through the regions:

<!-- snippet: examples/regional-routing/main.go#new -->
```go
c, err := regional.New(map[string]workflow.Client{
	"eu": memory.New(), // in production: a client on the EU database or engine
	"us": memory.New(),
},
	regional.ByLabel("region", "eu"),  // the job's "region" label; eu when it has none
	regional.WithScheduleRegion("eu"), // where declared Cron fires
)
if err != nil {
	return err
}
for _, wf := range []workflow.Workflow{invoice, approval} {
	if err := c.Register(wf); err != nil { // registers in every region
		return err
	}
}
if err := c.Start(ctx); err != nil { // starts every region
	return err
}
```

`regional.ByLabel(key, fallback)` reads the job's label `key`; a job without the label goes to `fallback`, and with an empty fallback such a job is refused with an error matching `workflow.ErrInvalid`, so a job that forgot its region is never placed by accident. Region names are 1 to 64 letters, digits, `.`, `_` or `-`; `New` refuses an empty map, a nil client or router, a bad name and a `WithScheduleRegion` naming no region, each with `workflow.ErrInvalid`.

Every `RunRef` the client returns names its region: `RunID` and `ScheduledID` carry the region and a colon (`us:`) in front of the driver's own ID, so `Status`, `Cancel` and every run operation reach the region holding the run:

<!-- snippet: examples/regional-routing/main.go#enqueue -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"region": "us"}})
if err != nil {
	return err
}
fmt.Println("us customer's invoice ->", region(string(ref.RunID)))
st, err := waitStatus(ctx, c, ref, workflow.StatusSucceeded) // the ref names its region, so Status asks us
```

The same prefix is on every `RunEvent`, `Tick`, `LifecycleEvent` and on the `StepInfo` a handler reads. A ref without a region prefix, or naming a region the client lacks, is `workflow.ErrNotFound`: there is no default region to try, and asking every region would let a driver ID that exists in two regions act on the wrong run.

## Refuse an unknown region

A region the router names that the client was not built with is refused, never sent to a default, because a typo in a region label must not move a customer's data to another region:

<!-- snippet: examples/regional-routing/main.go#unknown -->
```go
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"region": "ap"}})
fmt.Println("unknown region refused:", errors.Is(err, regional.ErrUnknownRegion), errors.Is(err, workflow.ErrInvalid))
```

`regional.ErrUnknownRegion` is returned wrapped together with `workflow.ErrInvalid`. An error from the router itself is returned to the caller as it is, so a router that cannot decide refuses the job.

## Write your own router

A `Router` has one method, `Region(ctx, job) (string, error)`; `regional.RouterFunc` turns a function into one. This one routes by the job's tenant through the app's own table:

<!-- snippet: examples/regional-routing/main.go#router -->
```go
customers := map[string]string{"acme": "eu", "globex": "us"} // the app's own table
// router picks the region of the job's tenant from that table.
router := regional.RouterFunc(func(ctx context.Context, job workflow.Job) (string, error) {
	r, ok := customers[job.Tenant]
	if !ok {
		return "", fmt.Errorf("%w: no region for tenant %q", workflow.ErrInvalid, job.Tenant)
	}
	return r, nil
})
```

## Signals

`Signal` goes to every region by default, since a run waiting for the event may sit in any of them:

<!-- snippet: examples/regional-routing/main.go#signal -->
```go
// Signal goes to every region, since a waiting run may sit in any.
if err := c.Signal(ctx, workflow.Event{Name: "approved", Key: "42"}); err != nil {
	return err
}
```

Every region is tried; when one fails, the others have already taken the event and the error names the failing region, so a retried `Signal` may reach a region twice.

An app whose event payloads must stay in one region too passes `regional.WithSignalRouter(fn)`. `fn` is a `regional.SignalRouter`: it returns the regions an event goes to, at least one, each a region of the client (`ErrUnknownRegion` otherwise; an empty list is `workflow.ErrInvalid`), or an error that `Signal` returns wrapped as `regional: signal router: <err>`, before any region is tried. This one reads the app's own order table:

<!-- snippet: examples/regional-routing/main.go#signal-router -->
```go
orders := map[string]string{"42": "us"} // the app's own table: which region holds an order
c, err := regional.New(map[string]workflow.Client{"eu": memory.New(), "us": memory.New()},
	regional.ByLabel("region", "eu"),
	regional.WithSignalRouter(func(ctx context.Context, ev workflow.Event) ([]string, error) {
		r, ok := orders[ev.Key]
		if !ok {
			return nil, fmt.Errorf("%w: no region for order %q", workflow.ErrInvalid, ev.Key)
		}
		return []string{r}, nil // at least one region of the client
	}),
)
```

A run waiting in a region the router leaves out never sees the event. In the example the same order key waits in both regions; after `Signal` the US run has succeeded and the EU run is still waiting, and an event for an unknown order is refused with the router's error (see Try it below). [`SignalWaiting`](../workflows/signals.md#deliver-only-to-runs-waiting-now) follows the same router. A nil `fn` keeps the default, every region.

## Schedules

`Schedule` asks the same router, with a job made of the schedule's `Workflow`, `Input`, `Labels` and `Tenant`, so a schedule lives in the region its runs would be sent to, and returns a `ScheduleID` with that region's prefix:

<!-- snippet: examples/regional-routing/main.go#schedule -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{
	Name: "us-nightly", Workflow: "invoice", Cron: "0 2 * * *", Location: time.UTC,
	Labels: map[string]string{"region": "us"}, // read by the router, like a job's
})
```

A workflow's declared `Cron` has no job to route. It fires in the region `regional.WithScheduleRegion(name)` names, and the other regions get each workflow without its `Cron` (and the fields that apply to it alone: `CronWindows`, `Misfire`, `CronOverlap`, `CronSeconds`, `CronActive`, `CronKeepRuns`), so a tick never fires twice. `ScheduleWhen` stays on every copy, because it also conditions the ticks of `Schedule`, which may live in any region. With one region it defaults to that region; with more, `Register` refuses a workflow with `Cron` until it is set:

<!-- snippet: examples/regional-routing/main.go#schedule-region -->
```go
// regions builds a fresh pair of regions, one per client below.
regions := func() map[string]workflow.Client {
	return map[string]workflow.Client{"eu": memory.New(), "us": memory.New()}
}
without, err := regional.New(regions(), regional.ByLabel("region", "eu"))
if err != nil {
	return err
}
err = without.Register(nightly) // which region fires 02:00? Register refuses to guess
fmt.Println("Cron without a schedule region refused:", errors.Is(err, workflow.ErrInvalid))

with, err := regional.New(regions(), regional.ByLabel("region", "eu"),
	regional.WithScheduleRegion("eu"), // eu fires the tick; us gets the workflow without its Cron
)
if err != nil {
	return err
}
fmt.Println("with WithScheduleRegion(\"eu\"):", with.Register(nightly))
```

A `WithScheduleRegion` naming a region the client was not built with is refused by `regional.New` with `ErrUnknownRegion`.

`Schedules` lists every region's schedules with prefixed IDs; `PauseSchedule`, `ResumeSchedule`, `TriggerSchedule` and `Unschedule` go to the region the ID names (`PauseSchedule` with `All` to every region). A fired run's `workflow.LabelSchedule` label carries the prefixed ID, and a `LabelSchedule` filter in `Runs` and `CancelAll` asks only the region holding that schedule.

## Manage runs across regions

The client implements the optional run interfaces. Each needs its capability, and its interface, in every region; otherwise the call is refused with an `*workflow.UnsupportedError` before any region is touched, never answered from some regions alone:

| Call | What the regional client does |
|---|---|
| `Runs(ctx, q)` | Asks every region and merges their feeds, newest enqueue first (ties by `RunID` descending), with prefixed refs. The cursor records each region's position; a cursor the client did not make is `workflow.ErrInvalid`. A prefixed `Ref` in `q` asks only its region. |
| `Retry`, `Delete`, `ForceComplete`, `ForceFail`, `Reschedule`, `EditRun`, `ResetRun`, `CancelStep`, `Result` | Go to the region the ref names; a returned ref keeps that region's prefix. |
| `SignalWaiting(ctx, ev)` | Goes to the regions `Signal` would send `ev` to (every region, or the `WithSignalRouter` pick) and returns how many waiting runs it resumed in all. Every one of those regions must implement `workflow.TransientSignaller`, else nothing is delivered. |
| `CancelAll(ctx, q)` | Runs in every region and returns the total. When a region fails, the others' cancellations stand and the error names the failing region. |
| `Pause`, `Resume`, `Paused` | Applied to every region; `Paused` lists the union, so a pause applied to some regions only can be seen and resumed. |
| `Drain`, `DrainWithReport`, `Done` | Drain every region at once; `Done` closes once all have stopped. |
| `Nodes`, `Health` | Every region's nodes; healthy and ready only while every region is, each check named after its region. |

<!-- snippet: examples/regional-routing/main.go#runs -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "invoice"})
if err != nil {
	return err
}
perRegion := map[string]int{}
for _, r := range page.Runs { // every region's runs, newest first, refs prefixed
	perRegion[region(string(r.Ref.RunID))]++
}
```

## Capabilities

`Capabilities()` is the intersection of every region's, so `Start` refuses a workflow some region could not run, and the answer does not depend on where the router sends a job. `CapRunFinished` also needs every region to implement `workflow.RunNotifier`. `CapClosure` and `CapWorkerScale` are never declared: an in-process closure carries no job for the router to read, and a worker count belongs to one region's workers. `Region(name)` returns a region's own client for those; calls made on it hand out the driver's own, unprefixed IDs.

## Try it

```sh
cd examples
go run ./regional-routing
```

```text
-- Route each job to its region
us customer's invoice -> us
status read from us: succeeded
invoice without a label -> eu
unknown region refused: true true
-- Signal a run in another region
approval in us: succeeded
-- Place a schedule with the same router
schedule -> us
-- List runs across regions
invoices: eu 1, us 1
-- Route by tenant
acme -> eu
globex -> us
initech: refused (true)
-- Keep an event in its region
order 42: us succeeded, eu waiting
unknown order refused: true
-- Name the region that fires declared Cron
Cron without a schedule region refused: true
with WithScheduleRegion("eu"): <nil>
```

## Pitfalls

- Register and start through the regional client only. Refs from a region used directly have no prefix, and the regional client answers `workflow.ErrNotFound` for them.
- A region that is down takes no jobs: `Enqueue` returns its error instead of moving the job. For an outage inside one region, make that region's client a [failover](../reliability/failover.md) client of two backends in the same region.
- `Signal` sends the event's payload to every region by default. When the payload is personal data that must stay in one region, use `WithSignalRouter`.
- A chained job (`Job.Chain`) is enqueued by the region whose run finished it, so a chain stays in the region of its first job, whatever the router would say about the later jobs.
- With more than one region, a workflow with a declared `Cron` needs `WithScheduleRegion`, and its ticks run in that region only.
- Node IDs are each region's own; give each region's nodes distinct IDs (`memory.WithNodeID`, `sqlstore.WithNodeID`) to tell them apart in `Nodes` and `RunInfo.AttemptedBy`.

## Related

- [Failover](../reliability/failover.md): two backends for one region, with jobs moving to the second during an outage.
- [Tenants](../jobs/tenants.md): `Job.Tenant`, which a router can read.
- [Capabilities](../reference/capabilities.md): what each driver declares.
- Example: [examples/regional-routing](../../examples/regional-routing/).
