# regional-routing

Join one client per region with `regional.New` and route each job by its `region` label (`regional.ByLabel`, with `eu` as the fallback): the refs name their region, `Status` reads the run from the region holding it, a job naming a region the client lacks is refused with `ErrUnknownRegion` (which also matches `workflow.ErrInvalid`), a signal reaches a run waiting in `us`, a schedule is placed by the same router, `Runs` lists every region's runs, and a `RouterFunc` routes by tenant through the app's own table. Every region is the memory driver.

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
```

In production each region is a `sqlstore` or Hatchet client running in that region; the program's code does not change.

The guide [Regional routing](../../docs/operations/regional-routing.md) explains each step. `main_test.go` runs the same program and checks every line above.
