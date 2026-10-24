package otel

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	gotel "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// ScopeName is the instrumentation scope of every tracer and meter this
// package creates.
const ScopeName = "github.com/ubgo/workflow/otel"

// LabelPrefix starts every job label this package writes to carry trace
// context; see the package doc for why this prefix. It is the core's
// workflow.LabelPrefixOTel, which this package owns.
const LabelPrefix = workflow.LabelPrefixOTel

// Attribute keys. Labels are recorded under LabelAttributePrefix + key.
const (
	AttrWorkflow = attribute.Key("workflow.name")
	AttrStep     = attribute.Key("workflow.step")
	AttrRunID    = attribute.Key("workflow.run_id")
	AttrAttempt  = attribute.Key("workflow.attempt")
	AttrOutcome  = attribute.Key("workflow.outcome")
	AttrQueue    = attribute.Key("workflow.queue")
	// AttrNode is StepInfo.Node, the process that ran the attempt, on
	// step spans when the driver sets it.
	AttrNode = attribute.Key("workflow.node")
	// AttrTenant is Job.Tenant (StepInfo.Tenant) on step and enqueue
	// spans, recorded only with WithTenant.
	AttrTenant = attribute.Key("workflow.tenant")
	// AttrRunStatus is the terminal workflow.Status of a run on
	// MetricRunsFinished: succeeded, failed, cancelled, or discarded on a
	// driver with CapRunStates.
	AttrRunStatus        = attribute.Key("workflow.run.status")
	LabelAttributePrefix = "workflow.label."
)

// Outcome is the value of the workflow.outcome attribute.
type Outcome string

// The outcomes a step attempt or an enqueue can have.
const (
	OutcomeOK        Outcome = "ok"
	OutcomeError     Outcome = "error"
	OutcomePermanent Outcome = "permanent"
	OutcomeCancelled Outcome = "cancelled"
	// OutcomeDiscarded is a step that returned workflow.Discard: the run
	// is dropped, not retried and not failed.
	OutcomeDiscarded Outcome = "discarded"
)

// Metric names.
const (
	MetricStepDuration = "workflow.step.duration"
	MetricStepOutcomes = "workflow.step.outcomes"
	MetricEnqueueCount = "workflow.enqueue.count"
	// MetricQueueWait is how long a step attempt waited for a worker:
	// from the moment it became ready (StepInfo.ReadyAt) to the moment it
	// started. For a run's first step that is enqueue to start.
	MetricQueueWait = "workflow.queue.wait"
	// MetricQueueDepth is how many runs wait in each queue, read through a
	// QueueDepthFunc at every collection.
	MetricQueueDepth = "workflow.queue.depth"
	// MetricRunsFinished counts run ends by workflow and AttrRunStatus,
	// fed by RunFinished.
	MetricRunsFinished = "workflow.runs.finished"
)

// QueueWaitBuckets returns the bucket boundaries, in seconds, of
// MetricQueueWait: from 5ms to an hour, because a queue wait ranges from
// an idle worker picking a job up at once to a backlog of an hour, and
// the SDK's default boundaries (0 to 10000, meant for milliseconds) would
// put almost every wait in the first two buckets. An SDK View replaces
// them. It returns a new slice on every call, so no caller can change
// another's boundaries.
func QueueWaitBuckets() []float64 {
	return []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}
}

// QueueDepthFunc returns how many runs wait in each queue, by queue name.
// admin.Admin.QueueDepths has this signature, so a method value plugs in
// directly; an app that can count with one SQL query passes its own.
type QueueDepthFunc func(ctx context.Context) (map[string]int64, error)

// Option configures New.
type Option func(*config)

type config struct {
	tp           trace.TracerProvider
	mp           metric.MeterProvider
	prop         propagation.TextMapPropagator
	labels       func(key string) bool
	clk          clock.Clock
	parenting    Parenting
	stepName     func(workflow.StepInfo) string
	enqueueName  func(workflow.Job) string
	metricLabels []string
	tenant       bool
}

// Parenting is how a step span relates to the span that enqueued its run,
// whose context the step reads from the job's labels.
type Parenting string

// The parentings WithParenting accepts.
const (
	// ParentChild makes the step span a child of the enqueue span, so the
	// whole run is one trace with its caller. The default.
	ParentChild Parenting = "child"
	// ParentLink keeps whatever span ctx already holds as the parent (an
	// engine SDK's span, or none) and links the enqueue span, as the OTel
	// messaging conventions do for async consumers.
	ParentLink Parenting = "link"
	// ParentNewRootLink starts every step span as the root of its own
	// trace, linked to the enqueue span, so a fan-out of thousands of jobs
	// does not build one trace too large for the backend to keep.
	ParentNewRootLink Parenting = "new_root_link"
)

// WithParenting picks how a step span relates to its enqueue span; the
// default is ParentChild. New refuses any other value with
// workflow.ErrInvalid.
func WithParenting(p Parenting) Option { return func(c *config) { c.parenting = p } }

// WithStepSpanName names step spans with fn instead of
// "workflow.step <workflow>/<step>". fn sees the driver's StepInfo (zero
// when ctx has none); an empty result falls back to the default. A span
// name is indexed by most backends, so keep it to a bounded set of values:
// never put a run ID or a label value with unbounded values in it.
func WithStepSpanName(fn func(workflow.StepInfo) string) Option {
	return func(c *config) { c.stepName = fn }
}

// WithEnqueueSpanName names enqueue spans with fn instead of
// "workflow.enqueue <workflow>". The same advice as WithStepSpanName
// applies; fn must not read the payload into the name.
func WithEnqueueSpanName(fn func(workflow.Job) string) Option {
	return func(c *config) { c.enqueueName = fn }
}

// WithMetricLabels adds the job labels named by keys to every metric as
// workflow.label.<key> attributes, for example a tenant label for
// per-tenant SLOs. Only the keys named are ever recorded, never all
// labels: each key multiplies the number of time series by its number of
// distinct values, so name only labels with a small, bounded set of
// values (a tier, a region, a few hundred tenants at most), never a user
// or order id. A job without a named label records it as "". Keys under
// LabelPrefix and empty keys are refused by New with workflow.ErrInvalid.
func WithMetricLabels(keys ...string) Option {
	return func(c *config) { c.metricLabels = append(c.metricLabels, keys...) }
}

// WithTracerProvider sets where spans go. Nil means otel.GetTracerProvider.
func WithTracerProvider(tp trace.TracerProvider) Option { return func(c *config) { c.tp = tp } }

// WithMeterProvider sets where metrics go. Nil means otel.GetMeterProvider.
func WithMeterProvider(mp metric.MeterProvider) Option { return func(c *config) { c.mp = mp } }

// WithPropagator sets how trace context is written into and read from job
// labels. Nil keeps the default, W3C TraceContext.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(c *config) { c.prop = p }
}

// WithLabelFilter records the job labels for which keep reports true as
// span attributes. By default no label is recorded: labels can carry
// tenant or user ids, and a trace backend is often a third party, so
// recording them is opt-in, as Logging never logs them either. Keys under
// LabelPrefix are never recorded. A nil keep records none.
func WithLabelFilter(keep func(key string) bool) Option {
	return func(c *config) { c.labels = keep }
}

// WithAllLabels records every job label (except LabelPrefix keys) as a
// span attribute. Use it only when no label holds personal data or
// secrets; WithLabelFilter picks labels by key instead.
func WithAllLabels() Option {
	return WithLabelFilter(func(string) bool { return true })
}

// WithTenant records Job.Tenant as the workflow.tenant attribute on step
// and enqueue spans. It is off by default, as labels are: a tenant ID can
// identify a customer. It is never a metric dimension (one series per
// tenant); WithMetricLabels is the way to opt into that, through a label.
func WithTenant() Option { return func(c *config) { c.tenant = true } }

// WithClock sets the clock step durations are measured with. Nil means
// clock.Real.
func WithClock(clk clock.Clock) Option { return func(c *config) { c.clk = clk } }

// Instrumentation holds the tracer, meter instruments and propagator the
// middleware share. Build it once with New and reuse it.
type Instrumentation struct {
	tracer       trace.Tracer
	prop         propagation.TextMapPropagator
	labels       func(key string) bool
	clk          clock.Clock
	parenting    Parenting
	stepName     func(workflow.StepInfo) string
	enqueueName  func(workflow.Job) string
	metricLabels []string
	tenant       bool
	stepDuration metric.Float64Histogram
	stepOutcomes metric.Int64Counter
	enqueues     metric.Int64Counter
	queueWait    metric.Float64Histogram
	queueDepth   metric.Int64ObservableGauge
	runsFinished metric.Int64Counter
	meter        metric.Meter
}

// New builds the instrumentation. It fails when an option is invalid
// (workflow.ErrInvalid) or the meter provider refuses to create an
// instrument.
func New(opts ...Option) (*Instrumentation, error) {
	cfg := config{}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if cfg.tp == nil {
		cfg.tp = gotel.GetTracerProvider()
	}
	if cfg.mp == nil {
		cfg.mp = gotel.GetMeterProvider()
	}
	if cfg.prop == nil {
		cfg.prop = propagation.TraceContext{}
	}
	if cfg.clk == nil {
		cfg.clk = clock.Real{}
	}
	switch cfg.parenting {
	case "":
		cfg.parenting = ParentChild
	case ParentChild, ParentLink, ParentNewRootLink:
	default:
		return nil, fmt.Errorf("%w: workflow/otel: unknown Parenting %q", workflow.ErrInvalid, cfg.parenting)
	}
	for _, k := range cfg.metricLabels {
		if k == "" || strings.HasPrefix(k, LabelPrefix) {
			return nil, fmt.Errorf("%w: workflow/otel: metric label key %q is empty or reserved", workflow.ErrInvalid, k)
		}
	}
	meter := cfg.mp.Meter(ScopeName)
	dur, err := meter.Float64Histogram(MetricStepDuration,
		metric.WithUnit("s"), metric.WithDescription("Duration of each workflow step attempt."))
	if err != nil {
		return nil, fmt.Errorf("workflow/otel: %s: %w", MetricStepDuration, err)
	}
	outcomes, err := meter.Int64Counter(MetricStepOutcomes,
		metric.WithUnit("{attempt}"), metric.WithDescription("Workflow step attempts by outcome."))
	if err != nil {
		return nil, fmt.Errorf("workflow/otel: %s: %w", MetricStepOutcomes, err)
	}
	enq, err := meter.Int64Counter(MetricEnqueueCount,
		metric.WithUnit("{job}"), metric.WithDescription("Workflow enqueues by outcome."))
	if err != nil {
		return nil, fmt.Errorf("workflow/otel: %s: %w", MetricEnqueueCount, err)
	}
	wait, err := meter.Float64Histogram(MetricQueueWait,
		metric.WithUnit("s"), metric.WithDescription("Time a workflow step attempt waited for a worker after it became ready."),
		metric.WithExplicitBucketBoundaries(QueueWaitBuckets()...))
	if err != nil {
		return nil, fmt.Errorf("workflow/otel: %s: %w", MetricQueueWait, err)
	}
	depth, err := meter.Int64ObservableGauge(MetricQueueDepth,
		metric.WithUnit("{run}"), metric.WithDescription("Workflow runs waiting for a worker, per queue."))
	if err != nil {
		return nil, fmt.Errorf("workflow/otel: %s: %w", MetricQueueDepth, err)
	}
	finished, err := meter.Int64Counter(MetricRunsFinished,
		metric.WithUnit("{run}"), metric.WithDescription("Workflow runs that ended, by status."))
	if err != nil {
		return nil, fmt.Errorf("workflow/otel: %s: %w", MetricRunsFinished, err)
	}
	return &Instrumentation{
		tracer:       cfg.tp.Tracer(ScopeName),
		prop:         cfg.prop,
		labels:       cfg.labels,
		clk:          cfg.clk,
		parenting:    cfg.parenting,
		stepName:     cfg.stepName,
		enqueueName:  cfg.enqueueName,
		metricLabels: cfg.metricLabels,
		tenant:       cfg.tenant,
		stepDuration: dur,
		stepOutcomes: outcomes,
		enqueues:     enq,
		queueWait:    wait,
		queueDepth:   depth,
		runsFinished: finished,
		meter:        meter,
	}, nil
}

// Step returns the step middleware: a span, a duration and an outcome per
// attempt. Put it outermost (first in middleware.Chain) so the span covers
// the other middleware and sees a recovered panic as an error. A ctx
// without a workflow.StepInfo still gets a span, with no workflow
// attributes.
func (i *Instrumentation) Step() workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			attrs := []attribute.KeyValue{
				AttrWorkflow.String(info.Workflow),
				AttrStep.String(info.Step),
				AttrRunID.String(info.RunID.String()),
				AttrAttempt.Int(info.Attempt),
				AttrQueue.String(info.Queue),
			}
			if info.Node != "" {
				attrs = append(attrs, AttrNode.String(info.Node))
			}
			if i.tenant && info.Tenant != "" {
				attrs = append(attrs, AttrTenant.String(info.Tenant))
			}
			attrs = append(attrs, i.labelAttrs(info.Labels)...)
			opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...)}
			if i.parenting == ParentNewRootLink {
				opts = append(opts, trace.WithNewRoot())
			}
			if len(info.Labels) > 0 {
				enq := i.prop.Extract(ctx, labelCarrier{labels: info.Labels})
				if sc := trace.SpanContextFromContext(enq); sc.IsValid() {
					if i.parenting == ParentChild {
						ctx = enq
					} else {
						opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
					}
				}
			}
			name := ""
			if i.stepName != nil {
				name = i.stepName(info)
			}
			if name == "" {
				name = "workflow.step " + info.Workflow + "/" + info.Step
			}
			ctx, span := i.tracer.Start(ctx, name, opts...)
			defer span.End()

			start := i.clk.Now()
			if !info.ReadyAt.IsZero() {
				i.queueWait.Record(ctx, max(start.Sub(info.ReadyAt), 0).Seconds(), metric.WithAttributes(append(i.metricLabelAttrs(info.Labels),
					AttrWorkflow.String(info.Workflow), AttrStep.String(info.Step), AttrQueue.String(info.Queue))...))
			}
			out, err := next(ctx, in)
			secs := i.clk.Now().Sub(start).Seconds()

			outcome := outcomeOf(err)
			recordErr(span, err)
			span.SetAttributes(AttrOutcome.String(string(outcome)))
			mattrs := metric.WithAttributes(append(i.metricLabelAttrs(info.Labels), AttrWorkflow.String(info.Workflow),
				AttrStep.String(info.Step), AttrQueue.String(info.Queue), AttrOutcome.String(string(outcome)))...)
			i.stepDuration.Record(ctx, secs, mattrs)
			i.stepOutcomes.Add(ctx, 1, mattrs)
			return out, err
		}
	}
}

// Enqueue returns the enqueue middleware: a span and a count per Enqueue,
// and the span's trace context written into a copy of job.Labels under
// LabelPrefix so the run's steps continue the trace. A job that arrives
// with a label under LabelPrefix is refused with workflow.ErrInvalid, before
// any span or count, rather than having it silently replaced: it would
// otherwise let a caller choose the trace the run's steps join. So put
// Enqueue in a chain once.
func (i *Instrumentation) Enqueue() workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			for _, k := range slices.Sorted(maps.Keys(job.Labels)) {
				if strings.HasPrefix(k, LabelPrefix) {
					return workflow.RunRef{}, fmt.Errorf("%w: workflow/otel: label %q uses the reserved prefix %q", workflow.ErrInvalid, k, LabelPrefix)
				}
			}
			attrs := append([]attribute.KeyValue{AttrWorkflow.String(job.Workflow)}, i.labelAttrs(job.Labels)...)
			if i.tenant && job.Tenant != "" {
				attrs = append(attrs, AttrTenant.String(job.Tenant))
			}
			// The enqueue side knows only the queue the job asked for; the
			// workflow's own queue is resolved by the driver, so an empty
			// Job.Queue records no attribute rather than a wrong one.
			var queueAttr []attribute.KeyValue
			if job.Queue != "" {
				queueAttr = []attribute.KeyValue{AttrQueue.String(job.Queue)}
				attrs = append(attrs, queueAttr...)
			}
			name := ""
			if i.enqueueName != nil {
				name = i.enqueueName(job)
			}
			if name == "" {
				name = "workflow.enqueue " + job.Workflow
			}
			ctx, span := i.tracer.Start(ctx, name,
				trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...))
			defer span.End()
			mlabels := i.metricLabelAttrs(job.Labels)

			labels := make(map[string]string, len(job.Labels)+2)
			maps.Copy(labels, job.Labels)
			i.prop.Inject(ctx, labelCarrier{labels: labels})
			job.Labels = labels

			ref, err := next(workflow.AllowReservedPrefix(ctx, workflow.LabelPrefixOTel), job)
			if !ref.RunID.IsZero() {
				span.SetAttributes(AttrRunID.String(ref.RunID.String()))
			}
			outcome := outcomeOf(err)
			recordErr(span, err)
			span.SetAttributes(AttrOutcome.String(string(outcome)))
			i.enqueues.Add(ctx, 1, metric.WithAttributes(append(append(mlabels, queueAttr...),
				AttrWorkflow.String(job.Workflow), AttrOutcome.String(string(outcome)))...))
			return ref, err
		}
	}
}

// ObserveQueueDepth reports fn's depths as the workflow.queue.depth gauge,
// one data point per queue with a workflow.queue attribute, read at every
// metric collection (no poller of its own). An error from fn skips that
// collection's points and is handed to the OpenTelemetry error handler.
// Unregister the returned registration to stop. A nil fn is refused with
// workflow.ErrInvalid.
func (i *Instrumentation) ObserveQueueDepth(fn QueueDepthFunc) (metric.Registration, error) {
	if fn == nil {
		return nil, fmt.Errorf("%w: workflow/otel: ObserveQueueDepth needs a QueueDepthFunc", workflow.ErrInvalid)
	}
	reg, err := i.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		depths, err := fn(ctx)
		if err != nil {
			return fmt.Errorf("workflow/otel: queue depth: %w", err)
		}
		for _, q := range slices.Sorted(maps.Keys(depths)) {
			o.ObserveInt64(i.queueDepth, depths[q], metric.WithAttributes(AttrQueue.String(q)))
		}
		return nil
	}, i.queueDepth)
	if err != nil {
		return nil, fmt.Errorf("workflow/otel: %s: %w", MetricQueueDepth, err)
	}
	return reg, nil
}

// labelAttrs turns labels into span attributes, skipping this package's
// own keys and any the filter rejects.
func (i *Instrumentation) labelAttrs(labels map[string]string) []attribute.KeyValue {
	if len(labels) == 0 || i.labels == nil {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(labels))
	for k, v := range labels {
		if strings.HasPrefix(k, LabelPrefix) || !i.labels(k) {
			continue
		}
		out = append(out, attribute.String(LabelAttributePrefix+k, v))
	}
	return out
}

// metricLabelAttrs returns the WithMetricLabels keys of labels as metric
// attributes, "" for a missing one so every series has the same keys.
func (i *Instrumentation) metricLabelAttrs(labels map[string]string) []attribute.KeyValue {
	if len(i.metricLabels) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(i.metricLabels)+3)
	for _, k := range i.metricLabels {
		out = append(out, attribute.String(LabelAttributePrefix+k, labels[k]))
	}
	return out
}

// RunFinished returns a run-finished observer that counts each run end on
// MetricRunsFinished with the workflow name and AttrRunStatus. Register
// it with the driver's workflow.RunNotifier (OnRunFinished) before Start.
// Delivery is at least once, so a run end delivered twice is counted
// twice; the counter is a rate, not a ledger. Only terminal statuses
// arrive here: a run in retry backoff (StatusRetrying) is not an end and
// is seen through RunQuery or admin instead.
func (i *Instrumentation) RunFinished() workflow.RunFinishedFunc {
	return func(ctx context.Context, ev workflow.RunEvent) {
		i.runsFinished.Add(ctx, 1, metric.WithAttributes(
			AttrWorkflow.String(ev.Workflow), AttrRunStatus.String(string(ev.Status))))
	}
}

// outcomeOf classifies an error the way the retry policy does. Discard is
// checked first: a discarded step is neither an error to retry nor a
// failure.
func outcomeOf(err error) Outcome {
	switch {
	case err == nil:
		return OutcomeOK
	case workflow.IsDiscarded(err):
		return OutcomeDiscarded
	case errors.Is(err, workflow.ErrCancelled):
		return OutcomeCancelled
	case workflow.IsPermanent(err):
		return OutcomePermanent
	default:
		return OutcomeError
	}
}

// recordErr marks span as failed. The error text is recorded because it is
// what an operator needs; steps that put secrets in error strings leak
// them to logs and run status already.
func recordErr(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// labelCarrier adapts job labels to propagation.TextMapCarrier, with every
// key under LabelPrefix so propagator keys never collide with app labels
// or with a driver's reserved names.
type labelCarrier struct{ labels map[string]string }

func (c labelCarrier) Get(key string) string { return c.labels[LabelPrefix+key] }

func (c labelCarrier) Set(key, value string) { c.labels[LabelPrefix+key] = value }

func (c labelCarrier) Keys() []string {
	keys := make([]string, 0, len(c.labels))
	for k := range c.labels {
		if rest, ok := strings.CutPrefix(k, LabelPrefix); ok {
			keys = append(keys, rest)
		}
	}
	return keys
}
