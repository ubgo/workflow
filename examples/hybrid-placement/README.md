# hybrid-placement

Package `hybrid` splits one `workflow.Client` across two backends: schedules fire on a scheduler client, runs live on a jobs client. Both are memory clients here so nothing needs to be installed; in an app the jobs client is a durable driver. The scheduler's clock is fake, so the 02:00 tick fires at once.

```sh
cd examples
go run ./hybrid-placement
```

```text
cron: true (scheduler)  run query: true (jobs)  trigger: false
tick 02:00 ran on node "jobs", schedule cron:nightly-report:0
runs on the jobs client: 1
CronOverlap refused: true
TriggerSchedule refused: true
```

## The code

Two clients behind one, and a declared cron:

<!-- snippet: examples/hybrid-placement/main.go#new -->
```go
scheduler := memory.New(memory.WithClock(clk), memory.WithNodeID("sched")) // in process: fires the crons
jobs := memory.New(memory.WithNodeID("jobs"))                              // in an app: sqlstore or redis
c := hybrid.New(scheduler, jobs)

err := c.Register(workflow.Workflow{
	Name:         "nightly-report",
	Cron:         []string{"0 2 * * *"},
	CronLocation: time.UTC,
	Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		ran <- info
		return nil, nil
	}}},
})
if err != nil {
	return err
}
if err := c.Start(ctx); err != nil { // starts jobs, then the scheduler
	return err
}
```

Run queries go to the jobs client:

<!-- snippet: examples/hybrid-placement/main.go#runs -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "nightly-report"}) // run queries go to the jobs client
if err != nil {
	return err
}
fmt.Fprintln(out, "runs on the jobs client:", len(page.Runs))
```

What the split refuses:

<!-- snippet: examples/hybrid-placement/main.go#refused -->
```go
err = c.Register(workflow.Workflow{
	Name: "sync", Cron: []string{"*/5 * * * *"}, CronLocation: time.UTC,
	CronOverlap: workflow.OverlapSkip, // the scheduler would see only its forwarder runs
	Steps:       []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
})
fmt.Fprintln(out, "CronOverlap refused:", errors.Is(err, workflow.ErrUnsupported))
_, err = c.TriggerSchedule(ctx, "cron:nightly-report:0")
fmt.Fprintln(out, "TriggerSchedule refused:", errors.Is(err, workflow.ErrUnsupported))
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Hybrid placement](../../docs/drivers/hybrid.md)
