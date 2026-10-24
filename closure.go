package workflow

import (
	"context"
	"time"
)

// ClosureWorkflow is the workflow name a closure run carries in RunInfo,
// RunEvent and StepInfo, so run listings, run-finished observers and
// middleware see closures as runs of one name. Validate refuses it as the
// name of a registered workflow. The closure's own name is in the
// LabelClosure label.
const ClosureWorkflow = "ubgo.workflow.closure"

// LabelClosure is the label holding a closure run's ClosureJob.Name. It
// sits under the reserved internal prefix, so app labels cannot forge it.
const LabelClosure = LabelPrefixInternal + "closure"

// ClosureStep is the name of a closure run's one step.
const ClosureStep = "closure"

// ClosureJob is a Go function run as a job in this process, for small
// fire-and-forget work that does not deserve a registered workflow (send
// a log line elsewhere, warm a cache).
//
// A closure cannot be stored or sent: it lives only in the memory of the
// process that enqueued it, is lost on a restart or crash, and never runs
// on another machine. Anything that must survive a restart is a
// registered workflow with a payload. Only an in-process driver declares
// CapClosure; durable drivers and engines refuse closures.
type ClosureJob struct {
	// Name says what the closure is, for logs and run listings (the
	// LabelClosure label). Required.
	Name string

	// Fn is the work. Its ctx carries StepInfo and is cancelled when the
	// run is cancelled or the client stops. Returning an error retries it
	// under Retry. Required.
	Fn func(ctx context.Context) error

	// Queue is the queue it runs on; empty means DefaultQueue.
	Queue string

	// RunAt delays the start; zero means now.
	RunAt time.Time

	// Labels are copied onto the run, beside LabelClosure.
	Labels map[string]string

	// Priority orders it among waiting runs, as Job.Priority.
	Priority int

	// Retry is the retry policy; nil means no retry. Since the closure
	// never leaves the process, NoRetryOn and DelayFor are allowed.
	Retry *RetryPolicy

	// Timeout bounds each attempt; zero means none.
	Timeout time.Duration
}

// ValidateClosure checks c: a Name, a Fn, a valid queue and retry policy,
// a Timeout that is not negative, and no label under a reserved prefix.
// Errors wrap ErrInvalid.
func ValidateClosure(c ClosureJob) error {
	if c.Name == "" {
		return invalidf("closure has no Name")
	}
	if c.Fn == nil {
		return invalidf("closure %q has no Fn", c.Name)
	}
	if c.Queue != "" {
		if err := ValidateQueueName(c.Queue); err != nil {
			return err
		}
	}
	if c.Retry != nil {
		if err := validateRetry(*c.Retry); err != nil {
			return invalidf("closure %q: %v", c.Name, err)
		}
	}
	if c.Timeout < 0 {
		return invalidf("closure %q: Timeout is negative", c.Name)
	}
	return ValidateLabels(c.Labels)
}

// ClosureEnqueuer runs closures. A client implements it when it declares
// CapClosure. EnqueueClosure validates c (ValidateClosure), then starts a
// run of ClosureWorkflow whose one step (ClosureStep) calls c.Fn; the
// RunRef works with Status, Cancel and the run query family as any
// other. Before Start it returns ErrNotStarted. A closure run is never
// matched by an idempotency key and is never chained.
type ClosureEnqueuer interface {
	EnqueueClosure(ctx context.Context, c ClosureJob) (RunRef, error)
}

// EnqueueClosure runs c on client when it implements ClosureEnqueuer and
// declares CapClosure, and otherwise returns an UnsupportedError naming
// CapClosure, never silently dropping the work.
func EnqueueClosure(ctx context.Context, client Client, c ClosureJob) (RunRef, error) {
	ce, ok := client.(ClosureEnqueuer)
	if !ok || !client.Capabilities().Has(CapClosure) {
		return RunRef{}, &UnsupportedError{Field: "EnqueueClosure", Missing: CapClosure,
			Reason: "closures run only on an in-process driver; register a workflow instead"}
	}
	return ce.EnqueueClosure(ctx, c)
}
