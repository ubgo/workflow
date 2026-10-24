# sentry-reporting

Send workflow failures to Sentry with the `sentry` module: one event per run that ends failed, with the workflow, step and status as tags, a chosen label as a tag (labels are opt-in), and never the payload. A transport that keeps events in memory stands in for sentry.io, so nothing leaves the machine and no account is needed.

```sh
cd examples
go run ./sentry-reporting
```

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

## The code

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

<!-- snippet: examples/sentry-reporting/main.go#flush -->
```go
hub.Flush(2 * time.Second) // before the process exits, as with any sentry-go event
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Failure alerts and error reporting › Report to Sentry](../../docs/observability/failure-alerts.md#report-to-sentry)
