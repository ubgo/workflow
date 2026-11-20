package recorder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// Defaults of a Recorder.
const (
	// DefaultMaxPayload is the largest body (input, step input and
	// output, result) a Recorder stores; a larger one is recorded by size
	// only.
	//
	//ds:def id=rec-defaultmaxpayload-43z9t84h owner=@khanakia stability=stable
	DefaultMaxPayload = 64 << 10

	// DefaultWriteTimeout bounds one store write, so a slow store delays a
	// step by at most this much.
	//
	//ds:def id=rec-defaultwritetimeout-wbngg5vx owner=@khanakia stability=stable
	DefaultWriteTimeout = 5 * time.Second

	// DefaultPluginName is the name Plugin reports to workflow.Use.
	//
	//ds:def id=rec-defaultpluginname-wtsqd67u owner=@khanakia stability=stable
	DefaultPluginName = "recorder"
)

// WriteOp names the store write that failed, for an OnError hook.
type WriteOp string

// The writes a Recorder makes.
const (
	OpUpsertRun  WriteOp = "upsert-run"
	OpStartStep  WriteOp = "start-step"
	OpEndStep    WriteOp = "end-step"
	OpAddEvent   WriteOp = "add-event"
	OpFinishRun  WriteOp = "finish-run"
	OpReadResult WriteOp = "read-result"
)

// ErrorFunc is told about a failed write. The run goes on regardless: a
// recorder never fails a run. It runs on the goroutine that made the write
// (a step's, an enqueuer's, the driver's run-finished goroutine), so it
// must return quickly.
//
//ds:def id=rec-errorfunc-nrd8pzfd owner=@khanakia stability=stable
type ErrorFunc func(ctx context.Context, op WriteOp, ref workflow.RunRef, err error)

// Recorder turns the three seams every driver runs (enqueue middleware,
// step middleware, run-finished observers) into Store writes. Build one
// with New and apply it with workflow.Use(client, rec.Plugin()); a
// Recorder is safe for concurrent use and keeps no state of its own besides
// its configuration, so one Recorder may serve several clients.
//
//ds:def id=rec-recorder-dss7wy23 owner=@khanakia stability=stable
type Recorder struct {
	store         Store
	clk           clock.Clock
	redactor      Redactor
	maxPayload    int
	stepBodies    bool
	errorHistory  int
	defaultSource workflow.Source
	results       workflow.ResultReader
	onError       ErrorFunc
	writeTimeout  time.Duration
	name          string
}

// Option configures a Recorder.
type Option func(*Recorder) error

// WithClock sets the clock the recorder stamps writes with; clock.Real by
// default. Use the driver's clock so records and the driver agree.
//
//ds:def id=rec-withclock-w2ezubpp owner=@khanakia stability=stable
func WithClock(c clock.Clock) Option {
	return func(r *Recorder) error {
		if c == nil {
			return fmt.Errorf("%w: recorder.WithClock(nil)", workflow.ErrInvalid)
		}
		r.clk = c
		return nil
	}
}

// WithRedactor scrubs every body and event attribute before it is stored
// (see Redactor, JSONPaths). None by default.
//
//ds:def id=rec-withredactor-k5xefkav owner=@khanakia stability=stable
func WithRedactor(red Redactor) Option {
	return func(r *Recorder) error { r.redactor = red; return nil }
}

// WithMaxPayload sets the largest body stored, in bytes (DefaultMaxPayload
// by default). A larger body is recorded by its size only. Zero stores no
// bodies at all, only sizes, for apps whose payloads must never be copied;
// a negative value is invalid.
//
//ds:def id=rec-withmaxpayload-3f6m2zdh owner=@khanakia stability=stable
func WithMaxPayload(n int) Option {
	return func(r *Recorder) error {
		if n < 0 {
			return fmt.Errorf("%w: recorder.WithMaxPayload(%d)", workflow.ErrInvalid, n)
		}
		r.maxPayload = n
		return nil
	}
}

// WithStepBodies says whether step inputs and outputs are stored (true by
// default). The run's own input and result follow WithMaxPayload only.
//
//ds:def id=rec-withstepbodies-7dpfqzyb owner=@khanakia stability=stable
func WithStepBodies(keep bool) Option {
	return func(r *Recorder) error { r.stepBodies = keep; return nil }
}

// WithErrorHistory bounds the attempt errors kept per run
// (workflow.DefaultErrorHistory by default); zero keeps none.
//
//ds:def id=rec-witherrorhistory-3fap7agv owner=@khanakia stability=stable
func WithErrorHistory(n int) Option {
	return func(r *Recorder) error {
		if n < 0 {
			return fmt.Errorf("%w: recorder.WithErrorHistory(%d)", workflow.ErrInvalid, n)
		}
		r.errorHistory = n
		return nil
	}
}

// WithDefaultSource is the source recorded for a run whose labels say
// nothing (workflow.SourceOf returns ""). Empty by default, which records
// no source. Must pass workflow.ValidateSource when set.
//
//ds:def id=rec-withdefaultsource-x3dq62wb owner=@khanakia stability=stable
func WithDefaultSource(s workflow.Source) Option {
	return func(r *Recorder) error {
		if s != "" {
			if err := workflow.ValidateSource(s); err != nil {
				return fmt.Errorf("recorder.WithDefaultSource: %w", err)
			}
		}
		r.defaultSource = s
		return nil
	}
}

// WithResultReader reads a succeeded run's result from rr (usually the
// client, on a driver with workflow.CapResult) when the run ends, so the
// record holds it. Without it the record holds the step outputs only.
//
//ds:def id=rec-withresultreader-wrjsqy26 owner=@khanakia stability=stable
func WithResultReader(rr workflow.ResultReader) Option {
	return func(r *Recorder) error { r.results = rr; return nil }
}

// WithOnError is told about every failed write. By default a failure is
// logged with slog.Default at warn level.
//
//ds:def id=rec-withonerror-r4hkn36k owner=@khanakia stability=stable
func WithOnError(fn ErrorFunc) Option {
	return func(r *Recorder) error {
		if fn == nil {
			return fmt.Errorf("%w: recorder.WithOnError(nil)", workflow.ErrInvalid)
		}
		r.onError = fn
		return nil
	}
}

// WithWriteTimeout bounds each store write (DefaultWriteTimeout by
// default). Writes run on a context detached from the caller's
// cancellation, so a step whose ctx was cancelled still records its end.
//
//ds:def id=rec-withwritetimeout-rn35n8vw owner=@khanakia stability=stable
func WithWriteTimeout(d time.Duration) Option {
	return func(r *Recorder) error {
		if d <= 0 {
			return fmt.Errorf("%w: recorder.WithWriteTimeout(%s)", workflow.ErrInvalid, d)
		}
		r.writeTimeout = d
		return nil
	}
}

// WithName sets the plugin name (DefaultPluginName by default), so two
// recorders, say one per store, can be applied to one client.
//
//ds:def id=rec-withname-cuvqnswq owner=@khanakia stability=stable
func WithName(name string) Option {
	return func(r *Recorder) error {
		if name == "" {
			return fmt.Errorf("%w: recorder.WithName(\"\")", workflow.ErrInvalid)
		}
		r.name = name
		return nil
	}
}

// New returns a Recorder writing to s. It fails on a nil store or an
// invalid option, with an error wrapping workflow.ErrInvalid.
//
//ds:def id=rec-new-wrqg5nug owner=@khanakia stability=stable
func New(s Store, opts ...Option) (*Recorder, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: recorder.New: nil Store", workflow.ErrInvalid)
	}
	r := &Recorder{
		store: s, clk: clock.Real{}, maxPayload: DefaultMaxPayload, stepBodies: true,
		errorHistory: workflow.DefaultErrorHistory, writeTimeout: DefaultWriteTimeout, name: DefaultPluginName,
		onError: logError,
	}
	for _, o := range opts {
		if o == nil {
			continue
		}
		if err := o(r); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// logError is the default ErrorFunc.
func logError(ctx context.Context, op WriteOp, ref workflow.RunRef, err error) {
	slog.Default().WarnContext(ctx, "recorder: write failed", slog.String("op", string(op)), slog.String("run", KeyOf(ref)), slog.Any("error", err))
}

// Store returns the store the recorder writes to.
//
//ds:def id=rec-recorder-store-2gdjppvt owner=@khanakia stability=stable
func (r *Recorder) Store() Store { return r.store }

// Plugin returns the workflow.Plugin that applies the recorder to a client:
// workflow.Use(client, rec.Plugin()). It contributes one enqueue
// middleware, one step middleware and one run-finished observer, so the
// client must implement workflow.RunNotifier (every bundled driver does,
// except in enqueue-only mode: use EnqueuePlugin there).
//
//ds:def id=rec-recorder-plugin-u7hfc2yc owner=@khanakia stability=stable
func (r *Recorder) Plugin() workflow.Plugin { return plugin{r} }

// EnqueuePlugin returns a workflow.Plugin with the recorder's enqueue
// middleware only, for a client that enqueues but runs nothing: a web or
// API node built in a driver's enqueue-only mode (workflow.CapEnqueueOnly),
// which has no run-finished hook for Plugin to register on. It records each
// accepted run with its source and input; the worker processes, wrapped
// with Plugin over the same store, record the steps and the end into the
// same record (every write is an upsert keyed by the run).
//
//ds:def id=rec-recorder-enqueueplugin-rn5k8npv owner=@khanakia stability=stable
func (r *Recorder) EnqueuePlugin() workflow.Plugin { return enqueuePlugin{r} }

// enqueuePlugin is the workflow.Plugin EnqueuePlugin returns.
type enqueuePlugin struct{ r *Recorder }

func (p enqueuePlugin) Name() string { return p.r.name }

func (p enqueuePlugin) EnqueueMiddleware() []workflow.EnqueueMiddleware {
	return []workflow.EnqueueMiddleware{p.r.EnqueueMiddleware()}
}

var _ workflow.EnqueueMiddlewareProvider = enqueuePlugin{}

// plugin is the workflow.Plugin of a Recorder.
type plugin struct{ r *Recorder }

func (p plugin) Name() string { return p.r.name }

func (p plugin) EnqueueMiddleware() []workflow.EnqueueMiddleware {
	return []workflow.EnqueueMiddleware{p.r.EnqueueMiddleware()}
}

func (p plugin) StepMiddleware() []workflow.Middleware {
	return []workflow.Middleware{p.r.StepMiddleware()}
}

func (p plugin) RunFinishedObservers() []workflow.RunFinishedFunc {
	return []workflow.RunFinishedFunc{p.r.OnRunFinished}
}

var (
	_ workflow.StepMiddlewareProvider    = plugin{}
	_ workflow.EnqueueMiddlewareProvider = plugin{}
	_ workflow.RunFinishedProvider       = plugin{}
)

// write runs one store write on a ctx detached from the caller's
// cancellation and bounded by the write timeout, and hands a failure, or a
// panic in the store, to the error hook. It never returns an error: a
// recorder never fails a run.
func (r *Recorder) write(ctx context.Context, op WriteOp, ref workflow.RunRef, fn func(context.Context) error) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.writeTimeout)
	defer cancel()
	err := func() (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("recorder: store panicked: %v", p)
			}
		}()
		return fn(wctx)
	}()
	if err != nil {
		r.onError(ctx, op, ref, err)
	}
}

// body returns what may be stored of b, and its size before redaction: nil
// when b is nil or larger than the payload bound (before or after
// redaction), the redacted copy otherwise.
func (r *Recorder) body(info PayloadInfo, b []byte) ([]byte, int) {
	size := len(b)
	if b == nil || size > r.maxPayload {
		return nil, size
	}
	if r.redactor != nil {
		b = r.redactor.RedactPayload(info, b)
	}
	if len(b) > r.maxPayload {
		return nil, size
	}
	return slices.Clone(b), size
}

// sourceOf is the run's source: from its labels, else the default.
func (r *Recorder) sourceOf(labels map[string]string) (workflow.Source, string) {
	if s, ref := workflow.SourceOf(labels); s != "" {
		return s, ref
	}
	return r.defaultSource, ""
}

// EnqueueMiddleware records every run the client accepts. A delayed run is
// recorded under its ScheduledID with StatusScheduled; others under their
// RunID with StatusQueued. A failed enqueue records nothing, and a failed
// write never fails the enqueue. Chained jobs are recorded when their
// first step starts, since the driver enqueues them past the middleware.
//
//ds:def id=rec-recorder-enqueuemiddleware-tkqchzg3 owner=@khanakia stability=stable
func (r *Recorder) EnqueueMiddleware() workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			ref, err := next(ctx, job)
			if err != nil || ref.IsZero() {
				return ref, err
			}
			now := r.clk.Now()
			status := workflow.StatusQueued
			if ref.RunID.IsZero() {
				status = workflow.StatusScheduled
			}
			src, srcRef := r.sourceOf(job.Labels)
			payload, size := r.body(PayloadInfo{Kind: PayloadInput, Workflow: job.Workflow}, job.Payload)
			w := RunWrite{
				Ref: ref, Workflow: job.Workflow, Queue: job.Queue, Tenant: job.Tenant, Labels: job.Labels,
				Source: src, SourceRef: srcRef, Status: status, Payload: payload, PayloadSize: size,
				Priority: job.Priority, RunAt: job.RunAt, EnqueuedAt: now, At: now,
			}
			r.write(ctx, OpUpsertRun, ref, func(wctx context.Context) error { return r.store.UpsertRun(wctx, w) })
			return ref, nil
		}
	}
}

// StepMiddleware records each step attempt's start and end, and binds on
// the step's ctx a workflow.StepEventSink (workflow.RecordEvent lands on
// the attempt) and a workflow.OutcomeSetter (workflow.SetOutcome lands on
// the run). A ctx without workflow.StepInfo (a handler called outside a
// driver) passes through unrecorded. The handler's output and error are
// returned unchanged.
//
//ds:def id=rec-recorder-stepmiddleware-tbta9pzy owner=@khanakia stability=stable
func (r *Recorder) StepMiddleware() workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := workflow.StepInfoFrom(ctx)
			if !ok || info.RunID.IsZero() {
				return next(ctx, in)
			}
			ref := workflow.RunRef{RunID: info.RunID}
			src, srcRef := r.sourceOf(info.Labels)
			var inBody []byte
			inSize := len(in)
			if r.stepBodies {
				inBody, inSize = r.body(PayloadInfo{Kind: PayloadStepInput, Workflow: info.Workflow, Step: info.Step}, in)
			}
			start := StepStart{
				Ref: ref, Workflow: info.Workflow, Queue: info.Queue, Tenant: info.Tenant, Labels: info.Labels,
				Source: src, SourceRef: srcRef, Step: info.Step, Attempt: info.Attempt, Node: info.Node,
				Input: inBody, InputSize: inSize, At: r.clk.Now(),
			}
			r.write(ctx, OpStartStep, ref, func(wctx context.Context) error { return r.store.StartStep(wctx, start) })

			sctx := workflow.WithStepEventSink(ctx, stepSink{r: r, info: info})
			sctx = workflow.WithOutcomeSetter(sctx, outcomeSetter{r: r, ref: ref})
			out, err := next(sctx, in)

			end := StepEnd{Ref: ref, Step: info.Step, Attempt: info.Attempt, Node: info.Node, ErrorHistory: r.errorHistory, At: r.clk.Now()}
			_, snoozed := workflow.SnoozeDelay(err)
			switch {
			case err == nil:
				end.Status = StepSucceeded
				end.OutputSize = len(out)
				if r.stepBodies {
					end.Output, end.OutputSize = r.body(PayloadInfo{Kind: PayloadStepOutput, Workflow: info.Workflow, Step: info.Step}, out)
				}
			case snoozed:
				end.Status, end.Error = StepSnoozed, err.Error()
			default:
				end.Status, end.Error, end.Stack = StepFailed, err.Error(), workflow.StackOf(err)
			}
			r.write(ctx, OpEndStep, ref, func(wctx context.Context) error { return r.store.EndStep(wctx, end) })
			return out, err
		}
	}
}

// stepSink stores the events of one step attempt.
type stepSink struct {
	r    *Recorder
	info workflow.StepInfo
}

// RecordStepEvent implements workflow.StepEventSink.
func (s stepSink) RecordStepEvent(ctx context.Context, e workflow.StepEvent) {
	at := e.At
	if at.IsZero() {
		at = s.r.clk.Now()
	}
	attrs := e.Attrs
	if s.r.redactor != nil && len(attrs) > 0 {
		info := PayloadInfo{Kind: PayloadEvent, Workflow: s.info.Workflow, Step: s.info.Step}
		attrs = make([]workflow.EventAttr, len(e.Attrs))
		for i, a := range e.Attrs {
			attrs[i] = s.r.redactor.RedactAttr(info, a)
		}
	}
	ref := workflow.RunRef{RunID: s.info.RunID}
	w := EventWrite{Ref: ref, Step: s.info.Step, Attempt: s.info.Attempt, Message: e.Message, Level: e.Level, Attrs: attrs, At: at}
	s.r.write(ctx, OpAddEvent, ref, func(wctx context.Context) error { return s.r.store.AddEvent(wctx, w) })
}

// outcomeSetter stores the outcome a step sets. Unlike the other writes,
// its error goes back to the step: SetOutcome's caller asked for the
// outcome to be kept.
type outcomeSetter struct {
	r   *Recorder
	ref workflow.RunRef
}

// SetOutcome implements workflow.OutcomeSetter.
func (o outcomeSetter) SetOutcome(ctx context.Context, oc workflow.Outcome, note string) error {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.r.writeTimeout)
	defer cancel()
	return o.r.store.SetOutcome(wctx, OutcomeWrite{Ref: o.ref, Outcome: oc, Note: note, At: o.r.clk.Now()})
}

// OnRunFinished records a run's end; it is the run-finished observer the
// plugin registers, exported for an app that wires the seams by hand.
// Delivery is at least once and the write is an upsert, so a repeat is
// harmless. With WithResultReader it reads a succeeded run's result first.
//
//ds:def id=rec-recorder-onrunfinished-9a8agy3p owner=@khanakia stability=stable
func (r *Recorder) OnRunFinished(ctx context.Context, ev workflow.RunEvent) {
	now := r.clk.Now()
	src, srcRef := r.sourceOf(ev.Labels)
	f := Finish{
		Ref: ev.Ref, Workflow: ev.Workflow, Tenant: ev.Tenant, Labels: ev.Labels, Source: src, SourceRef: srcRef,
		Status: ev.Status, FailedStep: ev.Step, FinishedAt: ev.FinishedAt, At: now,
	}
	if f.FinishedAt.IsZero() {
		f.FinishedAt = now
	}
	if ev.Err != nil {
		f.Error = ev.Err.Error()
	}
	if r.results != nil && ev.Status == workflow.StatusSucceeded {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.writeTimeout)
		out, err := r.results.Result(rctx, ev.Ref)
		cancel()
		switch {
		case err == nil:
			f.Output, f.OutputSize = r.body(PayloadInfo{Kind: PayloadResult, Workflow: ev.Workflow}, out)
		case errors.Is(err, workflow.ErrUnsupported):
			// The client keeps no results; the record holds the step
			// outputs only. Not worth an error on every run.
		default:
			r.onError(ctx, OpReadResult, ev.Ref, err)
		}
	}
	r.write(ctx, OpFinishRun, ev.Ref, func(wctx context.Context) error { return r.store.FinishRun(wctx, f) })
}
