package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// FuncStep is the name of the one step of a Func's workflow.
const FuncStep = "func"

// Func is a durable closure: a Go function registered under a name when
// the process starts and enqueued by that name with an encoded argument.
// A closure itself cannot be stored or sent to another machine, but a
// name and a JSON argument can, so a Func survives a restart and runs on
// any worker that registered the same name, on every driver (a Func is a
// one-step workflow underneath; no capability beyond the driver's own is
// needed). For a function that only ever runs in this process, the quick
// form is EnqueueClosure.
//
// Build one with NewFunc, register it with RegisterFunc(client, f) (or
// client.Register(f.Workflow())) in every process that runs workers, and
// enqueue with f.Enqueue or f.Job. The argument is encoded as JSON; input
// that does not decode is a Permanent error.
type Func[In any] struct {
	name string
	fn   func(ctx context.Context, in In) error
	cfg  FuncConfig
}

// FuncConfig holds what FuncOptions set: the same knobs a ClosureJob has
// that belong to the definition rather than to one enqueue. RunAt, Labels,
// Priority, IdempotencyKey and a per-run Queue are set on the Job (see
// Func.Job).
type FuncConfig struct {
	// Queue is the default queue of its runs; empty means DefaultQueue.
	Queue string

	// Retry is the retry policy of its one step; nil means no retry.
	Retry *RetryPolicy

	// Timeout bounds each attempt; zero means none.
	Timeout time.Duration

	// Catch runs once when a run fails for good, with the decoded
	// argument. Nil means nothing runs. Its error is reported the way the
	// driver reports an OnFailure error.
	Catch func(ctx context.Context, in []byte) error
}

// FuncOption configures a Func.
type FuncOption func(*FuncConfig)

// FuncQueue sets the Func's default queue.
func FuncQueue(q string) FuncOption { return func(c *FuncConfig) { c.Queue = q } }

// FuncRetry sets the retry policy of the Func's step. Nil means no retry.
func FuncRetry(p *RetryPolicy) FuncOption { return func(c *FuncConfig) { c.Retry = p } }

// FuncTimeout bounds each attempt.
func FuncTimeout(d time.Duration) FuncOption { return func(c *FuncConfig) { c.Timeout = d } }

// FuncCatch runs fn once when a run fails for good, with the argument the
// run was enqueued with, decoded. An argument that does not decode is
// passed as the zero In.
func FuncCatch[In any](fn func(ctx context.Context, in In) error) FuncOption {
	return func(c *FuncConfig) {
		if fn == nil {
			c.Catch = nil
			return
		}
		c.Catch = func(ctx context.Context, raw []byte) error {
			var in In
			if len(raw) > 0 {
				_ = json.Unmarshal(raw, &in) // a catch still runs on input that never decoded
			}
			return fn(ctx, in)
		}
	}
}

// NewFunc names fn. The name is the workflow name runs carry in RunInfo,
// RunEvent and StepInfo; Validate checks it at Register. Two Funcs with
// one name replace each other, as two workflows do.
func NewFunc[In any](name string, fn func(ctx context.Context, in In) error, opts ...FuncOption) Func[In] {
	f := Func[In]{name: name, fn: fn}
	for _, o := range opts {
		o(&f.cfg)
	}
	return f
}

// Name returns the name the Func was built with.
func (f Func[In]) Name() string { return f.name }

// Workflow returns the one-step workflow that runs the Func, for
// client.Register. Its step (FuncStep) decodes the argument and calls the
// function; OnFailure runs the Catch.
func (f Func[In]) Workflow() Workflow {
	wf := Workflow{Name: f.name, Queue: f.cfg.Queue}
	if f.fn != nil {
		fn := f.fn
		wf.Steps = []Step{{
			Name:    FuncStep,
			Retry:   f.cfg.Retry,
			Timeout: f.cfg.Timeout,
			Run: Define(func(ctx context.Context, in In) (struct{}, error) {
				return struct{}{}, fn(ctx, in)
			}),
		}}
	}
	if catch := f.cfg.Catch; catch != nil {
		wf.OnFailure = func(ctx context.Context, in []byte) ([]byte, error) {
			return nil, catch(ctx, in)
		}
	}
	return wf
}

// Job returns the Job that runs the Func with in, for a caller that sets
// RunAt, Labels, Priority, IdempotencyKey or Queue before Enqueue.
func (f Func[In]) Job(in In) (Job, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return Job{}, invalidf("func %q: encode argument: %v", f.name, err)
	}
	return Job{Workflow: f.name, Payload: b}, nil
}

// Enqueue starts a run of the Func with in on client.
func (f Func[In]) Enqueue(ctx context.Context, client Client, in In) (RunRef, error) {
	job, err := f.Job(in)
	if err != nil {
		return RunRef{}, err
	}
	return client.Enqueue(ctx, job)
}

// FuncDefinition is what RegisterFunc accepts: a Func of any argument type.
type FuncDefinition interface {
	Name() string
	Workflow() Workflow
}

// RegisterFunc registers f on client. A Func whose function is nil fails
// Validate (no steps) with ErrInvalid.
func RegisterFunc(client Client, f FuncDefinition) error {
	if err := client.Register(f.Workflow()); err != nil {
		return fmt.Errorf("func %q: %w", f.Name(), err)
	}
	return nil
}

// FuncJob returns the Job that runs the Func registered under name with
// arg encoded as JSON, for a caller that knows only the name (another
// service, an admin action). The driver refuses a name no process
// registered the way it refuses any unknown workflow.
func FuncJob(name string, arg any) (Job, error) {
	if name == "" {
		return Job{}, invalidf("func has no name")
	}
	b, err := json.Marshal(arg)
	if err != nil {
		return Job{}, invalidf("func %q: encode argument: %v", name, err)
	}
	return Job{Workflow: name, Payload: b}, nil
}
