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

The second part of the example keys alerts per tenant instead of per step, never alerts for a load-test tenant that fails on purpose, and caps alerts at two an hour across every key. It registers the bare observer `middleware.Alerts` with `OnRunFinished`; `AlertPlugin` is the same observer bundled for `workflow.Use`:

<!-- snippet: examples/failure-alerts/main.go#limiter -->
```go
limiter := middleware.NewMemoryAlertLimiter(clk, middleware.AlertLimits{
	Dedup: tenantDedup, // one alert per key in this window
	Max:   alertCap,    // and at most this many alerts, all keys together,
	Per:   capWindow,   // in any sliding window this long
})
alerts := middleware.Alerts(slack, middleware.AlertConfig{
	// One alert per workflow and tenant, not per workflow and step.
	Key: func(f workflow.Failure) string { return f.Workflow + "/" + f.Labels["tenant"] },
	// The load-test tenant fails on purpose: never page for it.
	When:    func(f workflow.Failure) bool { return f.Labels["tenant"] != "loadtest" },
	Limiter: limiter,
	Clock:   clk,
})
if err := mem.OnRunFinished(alerts); err != nil { // the bare observer; AlertPlugin wraps it for workflow.Use
	return err
}
```

Four tenants fail at once. The cap lets the first two alerts out, holds `initech` back and never counts `loadtest`, which `When` filtered out before the limiter. An hour later the window has moved, and the next `initech` failure alerts with the one held back:

```text
-- per tenant, at most 2 alerts an hour: acme, globex, initech and loadtest fail
  ALERT charge/acme (and 0 more)
  ALERT charge/globex (and 0 more)
-- an hour later initech fails again
  ALERT charge/initech (and 1 more)
```

How the limiter decides, in order, for each failure that passed `When`:

1. A key that alerted less than `Dedup` ago is held back, and its count grows.
2. Otherwise, when `Max` alerts already went out (any key) within the last `Per`, it is held back too, and its count grows.
3. Otherwise the alert goes out with `Suppressed` set to the count for its key, and the count starts again from zero.

A held-back alert is not queued: nothing is sent until that key fails again after the window. A limiter of your own implements `middleware.AlertLimiter`, one method `Allow(ctx, key, at) (ok bool, suppressed int, err error)`, over the store your fleet shares.

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

Each failure becomes one Sentry event: the error with its type chain, level `error` for a run's end and `warning` for an attempt (`WithLevels`), the tags `workflow.name`, `workflow.step`, `workflow.status` and `workflow.final`, and a `workflow` context with the run ID, the attempt (for an attempt's report) and the time. Labels are sent only on opt-in, as `workflow.label.<key>` tags: `WithLabels(keys...)` for chosen keys or `WithAllLabels()`. The default fingerprint groups events by workflow, step and the type of the innermost error, so a failure that recurs is one issue, not one per run; `WithFingerprint` replaces it, and `WithEvent` sees each event and its failure before sending and may change or drop it.

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
context "workflow": at run_id
issue search: workflow.name:charge workflow.final:true workflow.label.tenant:acme
payload sent: false user label sent: false
```

The tag and context names are constants of the module (`sentry.TagWorkflow`, `TagStep`, `TagStatus`, `TagFinal`, `TagLabelPrefix` and `ContextName`; the full list is in [Reserved names](../reference/names.md)), so a saved search, an alert rule or a dashboard built in code never retypes them:

<!-- snippet: examples/sentry-reporting/main.go#tag-names -->
```go
// A saved issue search for one tenant's final failures of one workflow,
// built from the tag names the reporter sets rather than retyped.
search := fmt.Sprintf("%s:%s %s:true %s:%s", sentry.TagWorkflow, "charge", sentry.TagFinal, sentry.TagLabelPrefix+"tenant", "acme")
fmt.Println("issue search:", search)
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

The output of each is shown above: `failure-alerts` prints the first part under [Step 2](#step-2-add-them-as-plugins) and the second under [Deduplication and rate limits](#deduplication-and-rate-limits).

## Pitfalls

- **A held-back alert never arrives on its own.** The limiter only answers when a failure comes in; a key that failed once inside the window and never again is counted, not sent. Pair alerts with the [admin Monitor](../admin/alerts.md) or a dashboard when silence matters.
- **The cap is shared by every key.** With `Max` and `Per`, one noisy workflow can use up the window and hold back another workflow's first alert; give a critical workflow its own `AlertPlugin` with its own limiter.
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
