# Tenants

A multi-tenant app runs jobs for many customers on the same workers, and sooner or later needs to ask "what is running for acme?", to filter the admin page to one customer, or to tag every trace with the customer it was for. Labels can carry a tenant ID, but every app spells the label differently and nothing can rely on it. `Job.Tenant` is a first-class field: stored with the run, handed to every step, reported on run events, and filtered by `RunQuery.Tenant`.

## Before you start

- `Job.Tenant`, `Schedule.Tenant` and `RunQuery.Tenant` need `workflow.CapTenant`. The memory, Redis, SQL and Hatchet drivers have it. A driver without it refuses rather than dropping the field: a job that sets `Tenant` at `Enqueue` and a schedule that sets it at `Schedule`, each with an `*workflow.UnsupportedError` naming the field (`Job.Tenant`, `Schedule.Tenant`), and a query that filters by it.
- The example is [`examples/results-progress`](../../examples/results-progress). It runs on the memory driver. Run it with `go run ./results-progress` from `examples/`.

## Enqueue a job for a tenant

Set `Job.Tenant` when you enqueue. It is the app's own ID for the customer, workspace or account; the example enqueues a checkout for each of two tenants:

<!-- snippet: examples/results-progress/main.go#tenant -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "checkout", Tenant: tenant})
if err != nil {
	return err
}
```

A tenant ID is 1 to 128 characters of letters, digits and `.`, `_`, `:`, `-` and `@`, the queue-name characters plus `@`, so it fits into a key, a label or a log line without escaping. `workflow.ValidateTenant` is the check; `Enqueue` refuses anything else with `workflow.ErrInvalid`. Empty means no tenant. Each job in a `Chain` carries its own `Tenant`, so a chained job for the same customer sets it too.

## Read the tenant where the run is

The tenant travels with the run:

- A step reads it from `workflow.StepInfoFrom(ctx)` as `StepInfo.Tenant`, to pick the customer's database or API key.
- `RunInfo.Tenant` carries it in run listings, `RunEvent.Tenant` on the [run-finished hook](../observability/run-finished.md), and `LifecycleEvent.Tenant` on lifecycle events.
- The OpenTelemetry middleware records it as the `workflow.tenant` span attribute when built with `otel.WithTenant()`; it is off by default, since a tenant ID may be data you do not want in traces.

## Give a schedule a tenant

A schedule made with `Client.Schedule` can belong to a tenant too: a nightly report per customer is one schedule per customer. `Schedule.Tenant` becomes the `Job.Tenant` of every run the schedule fires, by a tick or by `TriggerSchedule`, so those runs are found by `RunQuery.Tenant` like any other run of that tenant:

<!-- snippet: examples/results-progress/main.go#tenant-schedule -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{
	Name:     "acme-nightly",
	Workflow: "checkout",
	Cron:     "0 2 * * *",
	Location: time.UTC,
	Tenant:   "acme", // every run it fires belongs to acme
	Labels:   map[string]string{"source": "schedule"},
})
if err != nil {
	return err
}
```

It must pass `ValidateTenant`, and it is stored with the schedule: `ScheduleInfo.Tenant` reports it in schedule listings, the admin API and the `workflow` command show it, and a schedule stored as JSON keeps it as `"tenant"`. A cron declared on the workflow (`Workflow.Cron`) belongs to no single customer, so its runs have no tenant; make one `Client.Schedule` per tenant instead.

## List one tenant's runs

`RunQuery.Tenant` matches the runs whose `Job.Tenant` equals it exactly; empty matches every run. It combines with the other filters (workflow, status, queue, labels):

<!-- snippet: examples/results-progress/main.go#tenant-query -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Tenant: "acme"})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(out, "  %s %s %s", r.Tenant, r.Workflow, r.Status)
	if r.Labels["source"] == "schedule" {
		fmt.Fprint(out, " (fired by acme-nightly)")
	}
	fmt.Fprintln(out)
}
```

```text
tenants:
  acme checkout succeeded (fired by acme-nightly)
  acme checkout succeeded
  acme export-orders succeeded
  tenant with a space: ErrInvalid true
```

The program enqueued checkouts for `globex` and `acme` and fired acme's nightly schedule once with `TriggerSchedule`; the query returns acme's three runs, the scheduled one first. The admin JSON API takes the same filter as `?tenant=` on its run listings.

## Try it

```sh
cd examples
go run ./results-progress
```

The `tenants:` lines above are the end of its output. `go test ./results-progress` checks every line.

## Pitfalls

- **A tenant is a field, not a boundary.** The library stores and filters it; it does not stop one tenant's code from reading another's data, and it does not limit a tenant's share of the workers. For a fair share, key a [concurrency limit](concurrency.md) on the tenant (`workflow.KeyFunc(func(j workflow.Job) string { return j.Tenant })`).
- **Filtering by tenant is exact.** There is no prefix match on `RunQuery.Tenant`; put a hierarchy you want to search by prefix in a label and use `RunQuery.LabelPrefixes`.
- **A run enqueued without a tenant stays without one.** Set it at every enqueue site, on every job of a chain and on every tenant's `Schedule`. Runs of a declared `Workflow.Cron` never have one.
- **Changing a schedule's tenant does not move old runs.** Runs already fired keep the tenant they were fired with.
- **Never take a tenant ID from untrusted input unchecked.** Validation only checks the characters; whether the caller may act for that tenant is the app's decision.

## Related

- [Manage runs](manage-runs.md): `Runs` and its other filters.
- [Concurrency and rate limits](concurrency.md): a cap per tenant.
- [Enqueue a job](enqueue.md): the other `Job` fields.
- Example: [examples/results-progress](../../examples/results-progress/).
