# failure-alerts

Tell people when runs fail: an alert sink (a function standing in for Slack, email or a pager) called on final failures and deduplicated per workflow and step, with a count of the alerts held back, and an error reporter (standing in for an error tracker) that receives each final failure and, on opt-in, each failed attempt, without the payload. It runs on the in-memory driver with a fake clock, so nothing needs to be installed and the output never varies.

```sh
cd examples
go run ./failure-alerts
```

```text
-- first failure
  ALERT charge/capture: memory: step "capture" failed after 2 attempt(s): card declined (and 0 more)
  report charge step=capture final=false attempt=1 tenant=acme: card declined
  report charge step=capture final=false attempt=2 tenant=acme: card declined
  report charge step=capture final=true attempt=0 tenant=acme: memory: step "capture" failed after 2 attempt(s): card declined
-- two more within 10 minutes: reported, not alerted
  report charge step=capture final=false attempt=1 tenant=acme: card declined
  report charge step=capture final=false attempt=1 tenant=acme: card declined
  report charge step=capture final=false attempt=2 tenant=acme: card declined
  report charge step=capture final=false attempt=2 tenant=acme: card declined
  report charge step=capture final=true attempt=0 tenant=acme: memory: step "capture" failed after 2 attempt(s): card declined
  report charge step=capture final=true attempt=0 tenant=acme: memory: step "capture" failed after 2 attempt(s): card declined
-- 10 minutes later
  ALERT charge/capture: memory: step "capture" failed after 2 attempt(s): card declined (and 2 more)
  report charge step=capture final=false attempt=1 tenant=acme: card declined
  report charge step=capture final=false attempt=2 tenant=acme: card declined
  report charge step=capture final=true attempt=0 tenant=acme: memory: step "capture" failed after 2 attempt(s): card declined
```

## The code

<!-- snippet: examples/failure-alerts/main.go#sinks -->
```go
// The app's own senders: here they print, in production they post to
// Slack or call an error tracker's SDK.
slack := middleware.AlerterFunc(func(_ context.Context, a middleware.Alert) error {
	got.add("ALERT %s: %v (and %d more)", a.Key, a.Err, a.Suppressed)
	return nil
})
tracker := workflow.ReporterFunc(func(_ context.Context, f workflow.Failure) {
	got.add("report %s step=%s final=%t attempt=%d tenant=%s: %v", f.Workflow, f.Step, f.Final, f.Attempt, f.Labels["tenant"], f.Err)
})
```

<!-- snippet: examples/failure-alerts/main.go#plugins -->
```go
c, err := workflow.Use(mem,
	// One alert per workflow and step every 10 minutes (the default
	// dedup), on the fake clock so the demo can move time.
	middleware.AlertPlugin(slack, middleware.AlertConfig{Clock: clk}),
	// Every final failure, plus every failed attempt that will be
	// retried.
	middleware.Report(tracker, middleware.ReportConfig{Attempts: true, Clock: clk}),
)
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Failure alerts and error reporting](../../docs/observability/failure-alerts.md)
