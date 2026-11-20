package workflow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// Run-addressed signals (WP-22 escalation, WP-23 event handlers). A run
// has an address, RunKey(id): the key of a Signal meant for that run
// alone. Everything below rides on the driver's Signal and its per-run
// record of taken signals, read in two ways: a wait step with
// WaitSpec.OwnRun blocks until one arrives, and the OnSignals plugin
// handles them between steps without blocking. A wait step and a handler
// on the same name share one queue: whichever looks first takes a signal,
// never both.

// runKeyPrefix starts every run address. It is under the reserved
// internal prefix, so an app's own event key never collides with one.
const runKeyPrefix = LabelPrefixInternal + "run:"

// RunKey returns the event key that addresses run id: a Signal with this
// key reaches only that run, through a wait step with WaitSpec.OwnRun or
// the OnSignals plugin. SignalRun builds it for you.
func RunKey(id RunID) string { return runKeyPrefix + id.String() }

// RunSignaller is implemented by clients whose RunRefs are not the run
// IDs their steps see, such as the wrappers that prefix refs with a
// backend or region name: SignalRun hands them the ref so they strip the
// prefix and signal the backend that holds the run. ev.Key is ignored;
// the run's address is the key. A ref without a known prefix (a RunID a
// step read from StepInfo or LabelParentRun) is signalled as the run's
// own ID wherever the run may be.
type RunSignaller interface {
	SignalRun(ctx context.Context, ref RunRef, ev Event) error
}

// SignalRun sends a signal named name to one run (the run ref names),
// addressed by its ID: a wait step with WaitSpec.OwnRun on that name
// takes it, or an OnSignals handler between two steps. It is a
// Client.Signal with Key RunKey(ref.RunID), so the Signal contract holds:
// the signal is kept until the run takes it (a run that reaches its wait
// later still gets it), and one run takes it once. A ref the client does
// not know is not an error, as with Signal. It needs CapWaitForEvent;
// the run's wait additionally needs CapWaitOwnRun, which Register checks.
func SignalRun(ctx context.Context, client Client, ref RunRef, name string, payload []byte) error {
	if client == nil || name == "" || ref.RunID.IsZero() {
		return invalidf("SignalRun needs a client, a run ID and a signal name")
	}
	if !client.Capabilities().Has(CapWaitForEvent) {
		return &UnsupportedError{Field: "SignalRun", Missing: CapWaitForEvent, Reason: "the run takes the signal through a wait step or the driver's signal store"}
	}
	ev := Event{Name: name, Key: RunKey(ref.RunID), Payload: payload}
	if rs, ok := client.(RunSignaller); ok {
		err := rs.SignalRun(ctx, ref, ev)
		if !errors.Is(err, errNoRunSignaller) {
			return err
		}
	}
	return client.Signal(ctx, ev)
}

// errNoRunSignaller is what Forward.SignalRun returns when its inner
// client has no RunSignaller of its own: SignalRun then signals the run's
// address through the outermost client's Signal, so a wrapper's own
// Signal routing is never bypassed.
var errNoRunSignaller = errors.New("workflow: no RunSignaller")

// SignalRun forwards to Inner's RunSignaller. Without one it signals
// nothing and lets SignalRun fall back to the outer client's Signal.
func (f Forward) SignalRun(ctx context.Context, ref RunRef, ev Event) error {
	if rs, ok := f.Inner.(RunSignaller); ok {
		return rs.SignalRun(ctx, ref, ev)
	}
	return errNoRunSignaller
}

// ParentRun returns the run that spawned the calling step's run
// (LabelParentRun, set by Spawn, AwaitChild, JoinChildren and the other
// child helpers), and false outside a step or for a run that is no
// child.
func ParentRun(ctx context.Context) (RunID, bool) {
	info, ok := StepInfoFrom(ctx)
	if !ok {
		return "", false
	}
	id := info.Labels[LabelParentRun]
	return RunID(id), id != ""
}

// SignalParent sends a signal named name from the calling step to the run
// that spawned its run (BPMN escalation, WP-22): the child goes on, and
// the parent takes the signal through a wait step with WaitSpec.OwnRun or
// an OnSignals handler, which also runs on every look of a JoinChildren
// or AwaitChild step, so a parent waiting for its children still hears
// them. client is the client the parent runs on (the child's own when
// both run on one backend). A run that is no child is refused with
// ErrInvalid (Permanent, so the step does not retry it).
//
// A retried step sends again: put an ID in the payload when the parent
// must count each escalation once.
func SignalParent(ctx context.Context, client Client, name string, payload []byte) error {
	parent, ok := ParentRun(ctx)
	if !ok {
		return Permanent(invalidf("SignalParent: the calling step's run has no parent (call it from a child run's step)"))
	}
	if err := SignalRun(ctx, client, RunRef{RunID: parent}, name, payload); err != nil {
		return fmt.Errorf("workflow: signal parent %s: %w", parent, err)
	}
	return nil
}

// RunSignal is one signal addressed to a run (SignalRun), as
// RunSignalReader lists it.
type RunSignal struct {
	// ID is the driver's identity of the signal, stable across reads, so
	// a handler can dedup on it.
	ID string

	// Name is the signal's name (Event.Name).
	Name string

	// Payload is the signal's payload.
	Payload []byte
}

// RunSignalReader reads a run's addressed signals without waiting
// (WP-23), for handlers that run between steps (OnSignals). A driver
// implements it when it keeps signals and the per-run record of taken
// ones in a store it can query: memory, sqlstore and Redis. The calls
// answer for themselves, so there is no capability bit; probe with
// Supports(c, OpRunSignals).
type RunSignalReader interface {
	// PendingSignals lists the signals addressed to run (Key
	// RunKey(run)) whose name is in names, sent after the run was
	// enqueued and not yet taken by it, oldest first, at most limit (a
	// limit of zero or less means no limit). An unknown or finished run
	// has none.
	PendingSignals(ctx context.Context, run RunID, names []string, limit int) ([]RunSignal, error)

	// TakeSignal records that run took the signal id, as a wait step
	// taking it would, and reports whether this call took it: false when
	// the run had already taken it (a wait step or another handler got
	// there first). An id that is not a pending signal addressed to run
	// returns ErrNotFound.
	TakeSignal(ctx context.Context, run RunID, id string) (bool, error)
}

// PendingSignals forwards to Inner's RunSignalReader.
func (f Forward) PendingSignals(ctx context.Context, run RunID, names []string, limit int) ([]RunSignal, error) {
	if r, ok := f.Inner.(RunSignalReader); ok {
		return r.PendingSignals(ctx, run, names, limit)
	}
	return nil, lacks("PendingSignals", Caps{})
}

// TakeSignal forwards to Inner's RunSignalReader.
func (f Forward) TakeSignal(ctx context.Context, run RunID, id string) (bool, error) {
	if r, ok := f.Inner.(RunSignalReader); ok {
		return r.TakeSignal(ctx, run, id)
	}
	return false, lacks("TakeSignal", Caps{})
}

// ClientBinder is an optional Plugin hook: Use calls BindClient with the
// client it wraps, before it collects any middleware, so a plugin that
// needs the driver (a reader, a store) gets it without the app passing
// the client twice, and can refuse a client that lacks what it needs. A
// BindClient error fails Use.
type ClientBinder interface {
	BindClient(c Client) error
}

// SignalHandler handles one signal addressed to a run, between two of
// its steps. ctx carries the StepInfo of the step about to run (the
// run's ID, workflow and labels). A nil error lets the signal be taken
// and the step run; an error fails the step's attempt with it (the
// signal is not taken, so the retry handles it again), and a Permanent
// error fails the step without a retry.
type SignalHandler func(ctx context.Context, sig RunSignal) error

// DefaultSignalBatch is how many signals OnSignals handles before one
// step when SignalHandlers.Batch is zero.
const DefaultSignalBatch = 64

// SignalHandlers configures OnSignals.
type SignalHandlers struct {
	// Workflow names the workflow whose runs these handlers serve; empty
	// serves every workflow registered through the client Use returns.
	Workflow string

	// On maps a signal name to its handler. Signals of other names are
	// left for wait steps (WaitSpec.OwnRun) or other handlers.
	On map[string]SignalHandler

	// Batch is the most signals handled before one step; the rest wait
	// for the next step. Zero means DefaultSignalBatch.
	Batch int
}

// signalsPlugin is OnSignals.
type signalsPlugin struct {
	h      SignalHandlers
	names  []string
	reader RunSignalReader
}

// OnSignals returns the plugin that runs handlers for the signals
// addressed to a run (SignalRun, SignalParent) between its steps: a BPMN
// non-interrupting event sub-process, or a signal handler inside a run,
// in a journal model (WP-23). Before each step attempt of a run of
// h.Workflow, it lists the run's pending signals whose name has a
// handler, calls each handler in the order the signals were sent, and
// takes each signal after its handler returns nil. The step then runs as
// usual.
//
// When it runs: at step boundaries. A step that snoozes (JoinChildren,
// AwaitChild, Poll) is looked at again every poll, so a parent waiting
// for its children handles their escalations while it waits. A run
// parked in a wait or sleep step handles nothing until the next step
// begins; use a wait step with WaitSpec.OwnRun to block on a signal.
// The plugin also wraps OnFailure and OnTimeout, as every step
// middleware does.
//
// To interrupt the run (an interrupting event sub-process), the handler
// cancels it (Client.Cancel), ends it early (EndRun with the EndRuns
// plugin), or returns a Permanent error to fail the step.
//
// Delivery is at least once, the promise a step body has: a worker that
// dies between a handler and its take, or two steps of one run starting
// at the same moment, run a handler twice for one signal. Make handlers
// idempotent; RunSignal.ID identifies the signal.
//
// Use refuses the plugin (an UnsupportedError) on a client without
// RunSignalReader (Supports(c, OpRunSignals)): memory, sqlstore and
// Redis have it, Hatchet has not. A plugin without handlers is refused
// with ErrInvalid. Give each OnSignals in one Use call a different
// Workflow, since a plugin's name includes it.
func OnSignals(h SignalHandlers) Plugin {
	return &signalsPlugin{h: h, names: slices.Sorted(maps.Keys(h.On))}
}

// Name names the plugin, per workflow, in refusals.
func (p *signalsPlugin) Name() string {
	if p.h.Workflow == "" {
		return "on-signals"
	}
	return "on-signals:" + p.h.Workflow
}

// BindClient keeps c's RunSignalReader, or refuses c.
func (p *signalsPlugin) BindClient(c Client) error {
	if len(p.h.On) == 0 {
		return invalidf("OnSignals: no handlers")
	}
	for name, fn := range p.h.On {
		if name == "" || fn == nil {
			return invalidf("OnSignals: a handler with an empty name or a nil func")
		}
	}
	if p.h.Batch < 0 {
		return invalidf("OnSignals: Batch %d is negative", p.h.Batch)
	}
	r, ok := c.(RunSignalReader)
	if !ok || !Supports(c, OpRunSignals) {
		return &UnsupportedError{Field: "OnSignals", Reason: "the driver cannot read a run's signals between steps (no RunSignalReader)"}
	}
	p.reader = r
	return nil
}

// StepMiddleware handles the run's pending signals before each attempt.
func (p *signalsPlugin) StepMiddleware() []Middleware {
	reader, h, names := p.reader, p.h, p.names
	batch := h.Batch
	if batch == 0 {
		batch = DefaultSignalBatch
	}
	return []Middleware{func(next Handler) Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := StepInfoFrom(ctx)
			if reader == nil || !ok || info.RunID.IsZero() || (h.Workflow != "" && info.Workflow != h.Workflow) {
				return next(ctx, in)
			}
			if err := handleRunSignals(ctx, reader, info.RunID, h.On, names, batch); err != nil {
				return nil, err
			}
			return next(ctx, in)
		}
	}}
}

// handleRunSignals runs the handlers of run's pending signals, oldest
// first, taking each one its handler accepted.
func handleRunSignals(ctx context.Context, reader RunSignalReader, run RunID, on map[string]SignalHandler, names []string, batch int) error {
	sigs, err := reader.PendingSignals(ctx, run, names, batch)
	if err != nil {
		return fmt.Errorf("workflow: run signals: %w", err)
	}
	for _, sig := range sigs {
		fn := on[sig.Name]
		if fn == nil {
			continue // the reader filters by name; a stray one is left alone
		}
		if err := fn(ctx, sig); err != nil {
			return fmt.Errorf("workflow: signal %q handler: %w", sig.Name, err)
		}
		if _, err := reader.TakeSignal(ctx, run, sig.ID); err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("workflow: take signal %q: %w", sig.Name, err)
		}
	}
	return nil
}
