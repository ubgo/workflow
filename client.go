package workflow

import "context"

// Client is the one API an app uses; every driver implements it. App code
// depends on this interface only, so moving from the memory driver to an
// engine changes one constructor call and nothing else.
//
// Lifecycle: Register every workflow, then Start once. Register after Start
// returns ErrAlreadyStarted; Enqueue, Signal and Schedule before Start
// return ErrNotStarted, so a forgotten Start fails loudly instead of
// queueing work nobody will run.
type Client interface {
	// Register adds a workflow definition. It validates wf (see Validate)
	// and fails with ErrAlreadyStarted after Start. Registering the same
	// name twice replaces the definition, and its declared Cron schedules
	// are replaced, never duplicated.
	Register(wf Workflow) error

	// Start checks every registered workflow against Capabilities (an error
	// wrapping ErrUnsupported names what is missing) and then starts the
	// workers. It is the only call that starts goroutines or opens
	// connections. A second Start returns ErrAlreadyStarted.
	//
	// Start returns once the workers are running; it does not block. The
	// workers live until ctx is cancelled, which is how an app stops a
	// client: no new step starts after that, and running steps see their
	// own ctx cancelled. There is no separate Stop.
	Start(ctx context.Context) error

	// Enqueue starts a run now, or later when job.RunAt is set (which
	// needs CapDelayedStart). A delayed run returns a RunRef with only
	// ScheduledID set until it starts. With an idempotency key held by an
	// unfinished run, it starts nothing and returns that run's RunRef.
	Enqueue(ctx context.Context, job Job) (RunRef, error)

	// Cancel stops a run or a delayed start. It is idempotent: cancelling
	// a finished or already cancelled run returns nil. An unknown ref
	// returns ErrNotFound.
	Cancel(ctx context.Context, ref RunRef) error

	// Status reports where a run is. An unknown ref returns ErrNotFound.
	Status(ctx context.Context, ref RunRef) (Status, error)

	// Signal delivers ev to runs whose wait step matches its name and key
	// (the fixed WaitSpec.Key, or the run's resolved KeyFrom). The
	// contract every driver keeps, and the conformance suite asserts:
	//
	//   - An event is kept from the run's enqueue: a run whose wait step
	//     begins after the Signal still takes it, so a fast reply that
	//     arrives before the run reaches its wait is not lost.
	//   - An event sent before a run was enqueued never resumes that run.
	//   - One run never takes the same event twice, so two wait steps on
	//     the same name and key need two events.
	//   - One event resumes every run waiting with that name and key, not
	//     only the first.
	//
	// A signal nobody waits for is not an error.
	Signal(ctx context.Context, ev Event) error

	// Schedule registers a recurring trigger and returns its ID, which the
	// app stores to Unschedule later. It needs CapCron, plus CapCronZone
	// for a Location other than UTC and the capability of its Misfire
	// policy (RequiresSchedule; drivers refuse with CheckScheduleCaps).
	Schedule(ctx context.Context, s Schedule) (ScheduleID, error)

	// Unschedule removes a schedule. An empty id is a no-op, so callers can
	// pass a stored ID without checking it first.
	Unschedule(ctx context.Context, id ScheduleID) error

	// Capabilities is what this driver can really do. It never changes
	// over the life of a client.
	Capabilities() Caps
}

// Handler runs one step. Its input follows StepInput: the run's payload
// for a first step, a single parent's output unchanged, or a JSON object
// keyed by parent name for several parents. It returns bytes the engine
// stores as the step's output. Returning Permanent(err) fails the step at once, skipping the
// remaining retries. Handlers read time from an injected clock, never from
// time.Now, so a replay or a test sees the same values.
type Handler func(ctx context.Context, in []byte) (out []byte, err error)

// Middleware wraps every step execution, the same way on every driver:
// logging, tracing, recovery, timeouts and codecs are middleware, never
// driver wrappers that would hide what the driver can do.
type Middleware func(next Handler) Handler

// EnqueueFunc is the shape of Client.Enqueue, so middleware can wrap it.
type EnqueueFunc func(ctx context.Context, job Job) (RunRef, error)

// EnqueueMiddleware wraps every enqueue: uniqueness, debouncing, routing
// and required-label checks live here, above any driver.
type EnqueueMiddleware func(next EnqueueFunc) EnqueueFunc
