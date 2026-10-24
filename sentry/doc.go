// Package sentry reports workflow failures to Sentry through
// github.com/getsentry/sentry-go: a workflow.Reporter that sends one
// Sentry event per failure it is handed.
//
//	r := sentry.New(nil, sentry.WithLabels("tenant"))
//	client, err := workflow.Use(client, middleware.Report(r, middleware.ReportConfig{}))
//
// middleware.Report (or ReportFailures alone) hands it one Failure per
// run that ended failed, and with ReportConfig.Attempts one per failed
// attempt that will be retried; ReportConfig.Discarded adds discarded
// runs. The Reporter itself sends whatever it is given.
//
// # What is sent
//
// Each event carries the error (its message and type chain, as
// sentry-go's CaptureException builds them), level LevelError for a final
// failure and LevelWarning for an attempt (WithLevels), the tags
// workflow.name, workflow.step, workflow.status and workflow.final, and a
// "workflow" context with the run ID, scheduled ID, attempt and time. The
// payload is never sent: workflow.Failure does not carry it. Job labels
// are not sent by default, because they can carry tenant or user ids into
// a third-party service; WithLabels (chosen keys) or WithAllLabels opts
// in, as tags named workflow.label.<key>. The default fingerprint groups
// events by workflow, step and error type (WithFingerprint replaces it),
// so one recurring failure is one Sentry issue rather than one per run.
//
// # Hub
//
// New takes the hub to send through. With a nil hub each Report uses the
// hub in its ctx (sentry.GetHubFromContext) or else sentry.CurrentHub.
// Every Report works on a clone of that hub, so concurrent reports never
// share a scope and the app's own scope is never changed. WithEvent is
// the escape hatch: it sees each event and the Failure just before
// sending, and may change or drop the event.
//
// Sending is sentry-go's: its transport batches in the background, so
// call sentry.Flush (or the hub client's Flush) before the process exits.
package sentry
