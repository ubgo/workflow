package workflow

import (
	"context"
	"errors"
	"fmt"
)

// ErrDuplicatePlugin means two plugins handed to one Use call share a
// name.
var ErrDuplicatePlugin = errors.New("workflow: duplicate plugin name")

// Plugin is the only interface a plugin must satisfy: a feature (unique
// jobs, batches, tracing, an audit log) bundled so an app adds it with
// one Use call instead of wiring each piece by hand.
//
// Every contribution is an optional interface that Use type-asserts for:
// StepMiddlewareProvider, EnqueueMiddlewareProvider and
// RunFinishedProvider. A plugin implements only what it uses, so an
// absent contribution is absent rather than an empty method that looks
// the same as a forgotten one.
//
// Invariant: Name is non-empty and unique among the plugins of one Use
// call. It names the plugin in every refusal, so an operator knows which
// one to remove or reconfigure.
type Plugin interface {
	Name() string
}

// StepMiddlewareProvider contributes step middleware. It wraps every
// step's Run and the workflow's OnFailure for workflows registered
// through the client Use returns, inside any middleware the driver itself
// was built with (so a driver-level codec has already opened the input).
// The first entry is the outermost.
type StepMiddlewareProvider interface {
	StepMiddleware() []Middleware
}

// EnqueueMiddlewareProvider contributes enqueue middleware, run on every
// Enqueue through the client Use returns. The first entry sees the job
// first.
type EnqueueMiddlewareProvider interface {
	EnqueueMiddleware() []EnqueueMiddleware
}

// RunFinishedProvider contributes run-finished observers. Use registers
// them on the client's RunNotifier, so it must run before Start, and it
// refuses a client without RunNotifier rather than dropping them. The
// delivery contract, best-effort included, is RunNotifier's.
type RunFinishedProvider interface {
	RunFinishedObservers() []RunFinishedFunc
}

// Use applies plugins to c and returns the client to use from then on.
// Call it before Register and Start: step middleware reaches only
// workflows registered through the returned client, and observers can
// only be registered before Start.
//
// Order is the order given, across plugins and within each: the first
// plugin's middleware is outermost, and its observers are registered
// first. Use checks every plugin before registering any observer on c,
// and refuses:
//
//   - a nil plugin or an empty name (ErrInvalid);
//   - two plugins with one name (ErrDuplicatePlugin), within this call
//     only, since Use cannot see plugins applied by an earlier call;
//   - a RunFinishedProvider when c does not implement RunNotifier (an
//     UnsupportedError naming CapRunFinished and the plugin).
//
// An observer registration that fails (ErrAlreadyStarted after Start)
// returns the error naming the plugin; observers registered before it
// stay registered, because RunNotifier has no way to remove one.
//
// The returned client keeps c's Capabilities and forwards RunNotifier and
// every interface Forward covers to c, so wrappers stacked on it never
// hide the run-finished hook, run queries, pauses or drain. With no
// plugins Use returns c unchanged.
func Use(c Client, plugins ...Plugin) (Client, error) {
	if len(plugins) == 0 {
		return c, nil
	}
	seen := make(map[string]struct{}, len(plugins))
	var steps []Middleware
	var enqueue []EnqueueMiddleware
	notifier, hasNotifier := c.(RunNotifier)
	for i, p := range plugins {
		if p == nil {
			return nil, invalidf("plugin %d is nil", i)
		}
		name := p.Name()
		if name == "" {
			return nil, invalidf("plugin %d has an empty name", i)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicatePlugin, name)
		}
		seen[name] = struct{}{}
		if _, ok := p.(RunFinishedProvider); ok && !hasNotifier {
			return nil, &UnsupportedError{Field: "OnRunFinished", Missing: CapRunFinished, Reason: "plugin " + name + " observes run ends and the client does not implement workflow.RunNotifier"}
		}
		if sp, ok := p.(StepMiddlewareProvider); ok {
			steps = append(steps, sp.StepMiddleware()...)
		}
		if ep, ok := p.(EnqueueMiddlewareProvider); ok {
			enqueue = append(enqueue, ep.EnqueueMiddleware()...)
		}
	}
	for _, p := range plugins {
		rp, ok := p.(RunFinishedProvider)
		if !ok {
			continue
		}
		for _, fn := range rp.RunFinishedObservers() {
			if fn == nil {
				continue
			}
			if err := notifier.OnRunFinished(fn); err != nil {
				return nil, fmt.Errorf("workflow: plugin %q: %w", p.Name(), err)
			}
		}
	}
	next := EnqueueFunc(c.Enqueue)
	for i := len(enqueue) - 1; i >= 0; i-- {
		if enqueue[i] != nil {
			next = enqueue[i](next)
		}
	}
	return &pluginClient{Client: c, Forward: Forward{Inner: c}, steps: steps, enqueue: next}, nil
}

// pluginClient is the client Use returns. Every method not overridden
// here, Capabilities included, is the inner client's own.
type pluginClient struct {
	Client
	Forward // the optional interfaces, so Use never hides one
	steps   []Middleware
	enqueue EnqueueFunc
}

var _ RunNotifier = (*pluginClient)(nil)

// Register wraps wf's handlers with the plugins' step middleware. It
// copies Steps, so the caller's Workflow is never modified.
func (pc *pluginClient) Register(wf Workflow) error {
	if len(pc.steps) > 0 {
		steps := make([]Step, len(wf.Steps))
		copy(steps, wf.Steps)
		for i := range steps {
			steps[i].Run = pc.wrap(steps[i].Run)
		}
		wf.Steps = steps
		wf.OnFailure = pc.wrap(wf.OnFailure)
		wf.OnTimeout = pc.wrap(wf.OnTimeout)
	}
	return pc.Client.Register(wf)
}

// wrap applies the step middleware to h; nil stays nil, so a wait or
// sleep step still validates as one.
func (pc *pluginClient) wrap(h Handler) Handler {
	if h == nil {
		return nil
	}
	for i := len(pc.steps) - 1; i >= 0; i-- {
		if pc.steps[i] != nil {
			h = pc.steps[i](h)
		}
	}
	return h
}

func (pc *pluginClient) Enqueue(ctx context.Context, job Job) (RunRef, error) {
	return pc.enqueue(ctx, job)
}

// OnRunFinished forwards to the inner client. Its static type always has
// the method, so the honest answer for an inner client without the hook
// is an UnsupportedError, never a silent no-op.
func (pc *pluginClient) OnRunFinished(fn RunFinishedFunc) error {
	n, ok := pc.Client.(RunNotifier)
	if !ok {
		return &UnsupportedError{Field: "OnRunFinished", Missing: CapRunFinished, Reason: "the wrapped client does not implement workflow.RunNotifier"}
	}
	return n.OnRunFinished(fn)
}
