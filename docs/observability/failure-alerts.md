# Failure alerts and error reporting

A job that fails at night should wake someone, once, not two hundred times; and its error should land in the error tracker the team already reads, without the customer data in its payload. This page covers the alert sink called when runs fail for good, its deduplication and rate limits, the error reporter interface, and the Sentry integration built on it.

## Before you start

- A client that delivers run ends to observers (`workflow.RunNotifier`). The memory, SQL and Redis drivers deliver every end (`CapRunFinished`); Hatchet delivers them best effort ([React when a run finishes](run-finished.md)).
- The examples are [`examples/failure-alerts`](../../examples/failure-alerts) and [`examples/sentry-reporting`](../../examples/sentry-reporting). Run them with `go run ./failure-alerts` and `go run ./sentry-reporting` from `examples/`.

## Step 1: write the senders

The library decides when to alert and what to report; where the message goes is yours. An alert sink is a `middleware.Alerter` (`AlerterFunc` adapts a function), an error tracker a `workflow.Reporter` (`ReporterFunc`):

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

Both receive a `workflow.Failure`: the run's ref, workflow, status, the failed step, the attempt (zero for a run's end), the error, whether the run has ended (`Final`), its labels and the time. It never carries the payload, so job data cannot reach a third-party service through it. An `Alert` adds the dedup `Key` and `Suppressed`, how many alerts for that key were held back since the last one sent, so a message can say "and 12 more".

## Step 2: add them as plugins

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

`middleware.AlertPlugin` (or `middleware.Alerts`, the bare run-finished observer) calls the sink for every run that ends `failed`, and for `discarded` runs too with `AlertConfig.Discarded` (a policy with `RetryPolicy.DiscardOnExhaust` ends its exhausted runs discarded, so they alert only with that option). `middleware.Report` (or `ReportFailures`) hands every failed run to the reporter; with `ReportConfig.Attempts` it also adds step middleware that reports each failed attempt with `Final` false and status `retrying`, from the worker that ran it.

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

## Deduplication and rate limits

By default an alert is sent once per workflow and step every ten minutes (`middleware.DefaultAlertDedup`); the next one after the window says how many were held back. `AlertConfig` changes it:

| Field | What it does |
|---|---|
| `Key` | The dedup key of a failure; nil means `workflow + "/" + step`. Add a label to it to alert once per tenant |
| `When` | A filter: only failures it returns true for alert |
| `Limiter` | The `middleware.AlertLimiter` that decides; nil means a `MemoryAlertLimiter` with a ten-minute dedup on `Clock` |
| `Discarded` | Alert on discarded runs too |
| `Clock`, `Logger` | The clock that stamps failures and drives the default limiter; where sink and limiter errors go |

`middleware.NewMemoryAlertLimiter(clk, middleware.AlertLimits{Dedup, Max, Per})` dedups per key for `Dedup` and caps alerts across all keys at `Max` per sliding `Per`; `AlertLimits{}` lets every alert through. Its state is in one process, so each replica of a fleet dedups on its own; a limiter over a shared store (a database row per key) makes the limit fleet-wide. A limiter that returns an error does not hide the failure: the alert is sent anyway and the error logged.

## Report to Sentry

The module `github.com/ubgo/workflow/sentry` is a `workflow.Reporter` on sentry-go. It is a separate module, so an app that does not use Sentry never downloads its SDK.

<!-- snippet: examples/sentry-reporting/main.go#init -->
```go
// In an app: sentrygo.Init(sentrygo.ClientOptions{Dsn: os.Getenv("SENTRY_DSN")})
// once at start-up, then sentry.New(nil) uses the current hub.
client, err := sentrygo.NewClient(sentrygo.ClientOptions{Dsn: "https://key@sentry.invalid/1", Transport: tr})
if err != nil {
	return err
}
hub := sentrygo.NewHub(client, sentrygo.NewScope())

// The tenant label becomes a tag; no other label and never the
// payload leaves the process.
reporter := sentry.New(hub, sentry.WithLabels("tenant"))
mem := memory.New()
c, err := workflow.Use(mem, middleware.Report(reporter, middleware.ReportConfig{}))
```

Each failure becomes one Sentry event: the error with its type chain, level `error` for a run's end and `warning` for an attempt (`WithLevels`), the tags `workflow.name`, `workflow.step`, `workflow.status` and `workflow.final`, and a `workflow` context with the run ID, attempt and time. Labels are sent only on opt-in, as `workflow.label.<key>` tags: `WithLabels(keys...)` for chosen keys or `WithAllLabels()`. The default fingerprint groups events by workflow, step and the type of the innermost error, so a failure that recurs is one issue, not one per run; `WithFingerprint` replaces it, and `WithEvent` sees each event and its failure before sending and may change or drop it.

```text
events sent: 1
level: error
error: memory: step "capture" failed after 1 attempt(s): card declined
tag workflow.final=true
tag workflow.label.tenant=acme
tag workflow.name=charge
tag workflow.status=failed
tag workflow.step=capture
fingerprint: ubgo-workflow charge capture *errors.errorString
payload sent: false user label sent: false
```

`sentry.New(nil)` sends through the hub in the report's context, or the current hub. Each report works on a clone of that hub, so the app's scope is never changed. sentry-go sends in the background; flush before the process exits:

<!-- snippet: examples/sentry-reporting/main.go#flush -->
```go
hub.Flush(2 * time.Second) // before the process exits, as with any sentry-go event
```

## Try it

```sh
cd examples
go run ./failure-alerts
go run ./sentry-reporting
```

The output of each is shown above.

## Pitfalls

- **The default limiter dedups per process.** Three replicas send up to three alerts per window; give `AlertConfig.Limiter` a shared store when that matters.
- **Attempt reports include the last attempt.** With `ReportConfig.Attempts` the final attempt of a failing run is reported twice: once as an attempt, once as the run's end. Filter on `Final` in the reporter if that is noise.
- **A run that ends while no observer runs is alerted when one is.** Run ends are delivered at least once, so on the SQL driver an end that happened during a deploy reaches the sink after it; on Hatchet an end can be missed (best effort).
- **Labels are data.** A label holding a user's email reaches the tracker as soon as you opt it in; choose keys with `WithLabels` rather than `WithAllLabels` unless every label is safe to share.
- **The sink runs on a driver goroutine.** A slow Slack call delays the next observer; hand the message to a queue or use a short timeout.
- **`OnFailure` is not an alert.** It runs inside the workflow as its failure path; alerts are an observer that sees every failed run of every workflow.

## Related

- [React when a run finishes](run-finished.md): the run-finished hook the alerts sit on.
- [Queue alerts](../admin/alerts.md): alerts on queue depth and wait, not on failures.
- [Plugins](../concepts/plugins.md): how `workflow.Use` bundles observers and middleware.
- [Manage runs › Error history](../jobs/manage-runs.md#error-history): every attempt's error on the run itself.
