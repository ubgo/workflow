package sentry

import (
	"context"
	"errors"
	"reflect"
	"strconv"

	sentrygo "github.com/getsentry/sentry-go"

	"github.com/ubgo/workflow"
)

// Tag and context names set on every event. They are fixed so a Sentry
// search or alert rule written against them keeps working.
const (
	TagWorkflow    = "workflow.name"
	TagStep        = "workflow.step"
	TagStatus      = "workflow.status"
	TagFinal       = "workflow.final"
	TagLabelPrefix = "workflow.label."
	ContextName    = "workflow"
)

// Reporter is a workflow.Reporter that sends each Failure to Sentry as
// one event. Build it with New; it is safe for concurrent use.
type Reporter struct {
	hub         *sentrygo.Hub
	labels      map[string]bool
	allLabels   bool
	final       sentrygo.Level
	attempt     sentrygo.Level
	fingerprint func(workflow.Failure) []string
	event       func(*sentrygo.Event, workflow.Failure) *sentrygo.Event
}

var _ workflow.Reporter = (*Reporter)(nil)

// Option configures a Reporter.
type Option func(*Reporter)

// WithLabels forwards the job labels with these keys as tags
// (workflow.label.<key>). Labels are not sent unless chosen, because they
// can carry tenant or user ids.
func WithLabels(keys ...string) Option {
	return func(r *Reporter) {
		for _, k := range keys {
			r.labels[k] = true
		}
	}
}

// WithAllLabels forwards every job label as a tag. Prefer WithLabels: a
// label added later is then sent without anyone deciding it.
func WithAllLabels() Option { return func(r *Reporter) { r.allLabels = true } }

// WithLevels sets the event level of a final failure and of an attempt
// failure (LevelError and LevelWarning by default).
func WithLevels(final, attempt sentrygo.Level) Option {
	return func(r *Reporter) { r.final, r.attempt = final, attempt }
}

// WithFingerprint replaces the grouping of events into Sentry issues.
// The default is DefaultFingerprint.
func WithFingerprint(fn func(workflow.Failure) []string) Option {
	return func(r *Reporter) {
		if fn != nil {
			r.fingerprint = fn
		}
	}
}

// WithEvent runs fn on every event just before it is sent, with the
// Failure it came from; fn returns the event to send, or nil to drop it.
// It is the one escape hatch for anything the options do not cover.
func WithEvent(fn func(*sentrygo.Event, workflow.Failure) *sentrygo.Event) Option {
	return func(r *Reporter) { r.event = fn }
}

// New returns a Reporter that sends through hub; a nil hub means the hub
// in each Report's ctx, or sentry.CurrentHub when the ctx has none.
func New(hub *sentrygo.Hub, opts ...Option) *Reporter {
	r := &Reporter{
		hub:         hub,
		labels:      map[string]bool{},
		final:       sentrygo.LevelError,
		attempt:     sentrygo.LevelWarning,
		fingerprint: DefaultFingerprint,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// DefaultFingerprint groups events by workflow, step and the type of the
// innermost error, so a recurring failure is one issue whatever the run
// or the error text (which often holds ids).
func DefaultFingerprint(f workflow.Failure) []string {
	return []string{"ubgo-workflow", f.Workflow, f.Step, errorType(f.Err)}
}

// errorType is the Go type of the innermost error of err's chain.
func errorType(err error) string {
	if err == nil {
		return ""
	}
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return reflect.TypeOf(err).String()
		}
		err = next
	}
}

// Report sends f to Sentry as one event.
func (r *Reporter) Report(ctx context.Context, f workflow.Failure) {
	hub := r.hub
	if hub == nil {
		hub = sentrygo.GetHubFromContext(ctx)
	}
	if hub == nil {
		hub = sentrygo.CurrentHub()
	}
	client := hub.Client()
	if client == nil {
		return
	}
	err := f.Err
	if err == nil {
		err = errors.New("workflow: run " + string(f.Status))
	}
	event := client.EventFromException(err, r.level(f))
	event.Tags = r.tags(f)
	event.Contexts = map[string]sentrygo.Context{ContextName: workflowContext(f)}
	event.Fingerprint = r.fingerprint(f)
	if !f.At.IsZero() {
		event.Timestamp = f.At
	}
	if r.event != nil {
		if event = r.event(event, f); event == nil {
			return
		}
	}
	// A clone, so the event gets the app's scope data (user, release
	// tags) without this report touching the shared scope.
	hub.Clone().CaptureEvent(event)
}

// level is the event level of f.
func (r *Reporter) level(f workflow.Failure) sentrygo.Level {
	if f.Final {
		return r.final
	}
	return r.attempt
}

// tags are the fixed tags plus the labels the options let through.
func (r *Reporter) tags(f workflow.Failure) map[string]string {
	tags := map[string]string{
		TagWorkflow: f.Workflow,
		TagStatus:   string(f.Status),
		TagFinal:    strconv.FormatBool(f.Final),
	}
	if f.Step != "" {
		tags[TagStep] = f.Step
	}
	for k, v := range f.Labels {
		if r.allLabels || r.labels[k] {
			tags[TagLabelPrefix+k] = v
		}
	}
	return tags
}

// workflowContext is the per-run detail, kept out of tags because run IDs
// would make every tag value unique.
func workflowContext(f workflow.Failure) sentrygo.Context {
	c := sentrygo.Context{"run_id": string(f.Ref.RunID)}
	if !f.Ref.ScheduledID.IsZero() {
		c["scheduled_id"] = string(f.Ref.ScheduledID)
	}
	if f.Attempt > 0 {
		c["attempt"] = f.Attempt
	}
	if !f.At.IsZero() {
		c["at"] = f.At
	}
	return c
}
