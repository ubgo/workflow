package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow/clock"
)

// A durable state machine (XM-04): a statechart whose instances are keys,
// whose events are runs, and whose state is one record in a JournalStore.
//
// The model is the keyed virtual object of DefineObject: every event sent
// to a key is one run of the machine's workflow, and runs of one key are
// serialised on LabelObjectKey (Concurrency MaxRuns 1), so the machine
// handles one event at a time per key with no lock of its own. Cycles,
// long-lived instances and waits of any length come for free: an instance
// is a record, not a running run.
//
// The interpreter follows SCXML (the W3C statechart standard XState also
// implements) for compound and parallel states (WP-24), history (WP-26),
// entry and exit actions (WP-27), done events and conflict resolution
// between parallel regions, in plain Go: deterministic, no expressions,
// no hidden state.

// HistoryMode says how a transition enters a compound or parallel target
// (WP-26).
type HistoryMode int

const (
	// HistoryNone enters the target through its initial states.
	HistoryNone HistoryMode = iota

	// HistoryShallow re-enters the target's child (or, for a parallel
	// target, children) active when the target was last left, and that
	// child through its own initial states. Nothing remembered (the
	// target was never left) means the initial child.
	HistoryShallow

	// HistoryDeep re-enters the whole configuration below the target
	// that was active when it was last left.
	HistoryDeep
)

// UnhandledPolicy says what an event no active state handles does.
type UnhandledPolicy int

const (
	// MachineIgnore drops an unhandled event: the call succeeds with
	// MachineResult.Handled false and nothing changes (statechart
	// semantics).
	MachineIgnore UnhandledPolicy = iota

	// MachineReject fails the event's run with a Permanent
	// ErrUnhandledEvent, so a sender awaiting the result learns that the
	// event was not valid in the current state.
	MachineReject
)

// ErrUnhandledEvent is the error of an event no active state handles,
// under MachineReject, and of any event sent to a machine that reached a
// top-level final state. It matches ErrInvalid.
var ErrUnhandledEvent = fmt.Errorf("%w: event not handled in the machine's current state", ErrInvalid)

// MachineStartEvent is the event name entry actions see when an instance
// starts (StartMachine, or the first event sent to a new key).
const MachineStartEvent = LabelPrefixInternal + "start"

// DefaultMachineLog is how many handled events MachineStatus keeps in the
// log when MachineSpec.KeepLog is zero.
const DefaultMachineLog = 100

// MaxMachineMicrosteps bounds the transitions one event may cause,
// counting those of raised and done events, so a raise loop fails the
// event (Permanent) instead of spinning forever.
const MaxMachineMicrosteps = 100

// DoneEvent is the name of the event raised when compound state reaches
// a final child, or parallel state has every region in a final state
// (SCXML done.state.<id>). Handle it with a Transition whose Event is
// DoneEvent("payment") and whose From is "payment" or an ancestor.
func DoneEvent(state string) string { return "done.state." + state }

// MachineEvent is the event a guard or an action sees.
type MachineEvent struct {
	// Name is the event's name: the one sent, a timer's, a raised one,
	// a DoneEvent, or MachineStartEvent while the instance starts.
	Name string

	// Payload is the event's payload as sent; nil for done events and
	// timers.
	Payload []byte
}

// MachineCtx is what guards and actions get: the instance, the event, the
// state concerned and the instance's data.
type MachineCtx[D any] struct {
	// Machine is the machine's name; Key is the instance.
	Machine, Key string

	// Event is the event being handled.
	Event MachineEvent

	// State is the state whose entry or exit action runs, or the source
	// state of the transition whose guard or action runs.
	State string

	// Data is the instance's extended state. Actions may change it;
	// changes are saved with the new configuration when the event's
	// handling ends. A guard gets a shallow copy and must only read it.
	Data *D

	// Seq is the microstep sequence number (it grows by one per
	// transition set taken, starting at 1 when the instance starts), so
	// an action can build idempotency keys that stay the same when the
	// event is retried after a crash.
	Seq uint64

	run *machineRun[D]
}

// Raise queues an internal event, handled after the current transition
// set and before the sent event's handling ends (SCXML raise). Raised
// events that no state handles are dropped. Calling Raise from a guard
// does nothing.
func (m *MachineCtx[D]) Raise(event string, payload []byte) {
	if m.run != nil && !m.run.inGuard && event != "" {
		m.run.internal = append(m.run.internal, MachineEvent{Name: event, Payload: payload})
	}
}

// In reports whether state is active now (for a guard: before the
// transition; for an action: at the point of the microstep the action
// runs in).
func (m MachineCtx[D]) In(state string) bool {
	if m.run == nil {
		return false
	}
	n := m.run.m.nodes[state]
	return n != nil && m.run.active[n]
}

// IdempotencyKey returns a key unique to this instance, this microstep,
// this state and name, and identical when the event is handled again
// after a crash. Actions are at least once: give a job an action enqueues
// this key (Job.IdempotencyKey), or pass it to the remote side, so a
// repeat does nothing twice.
func (m MachineCtx[D]) IdempotencyKey(name string) string {
	sum := sha256.Sum256([]byte(m.Machine + "\x00" + m.Key + "\x00" + strconv.FormatUint(m.Seq, 10) + "\x00" + m.State + "\x00" + name))
	return "machine/" + hex.EncodeToString(sum[:16])
}

// MachineGuard decides whether a transition is taken. It should be a pure
// function of the data, the event and the configuration: it may be called
// more than once per event. m.Data points to a shallow copy of the data,
// so a change to a top-level field is dropped; maps and slices are shared,
// so never change those from a guard.
type MachineGuard[D any] func(m MachineCtx[D]) bool

// MachineAction runs on entry to a state, on exit from it, or when a transition
// is taken. An error fails the event's run (and the retry policy applies,
// running the event's whole handling again; state is saved only when it
// all succeeds), so actions are at least once: see
// MachineCtx.IdempotencyKey.
type MachineAction[D any] func(ctx context.Context, m *MachineCtx[D]) error

// StateTimer sends Event to the instance after its state has been active for
// Delay (a timeout per state, XState "after", a BPMN timer). A timer whose
// state was left before it fires is ignored, even when the state was
// entered again since (that entry has a timer of its own).
type StateTimer struct {
	Delay time.Duration
	Event string
}

// MachineState is one state of a machine.
type MachineState[D any] struct {
	// Name is unique within the machine.
	Name string

	// Parent is the compound or parallel state this one is a child of;
	// empty for a top-level state.
	Parent string

	// Initial is the child entered when this compound state is entered
	// without a more specific target. Required for a compound state,
	// ignored for a parallel one.
	Initial string

	// Parallel makes the state's children orthogonal regions: all of
	// them are active while it is (WP-24).
	Parallel bool

	// Final marks a final state: entering it raises DoneEvent(Parent),
	// and a top-level final state ends the instance. A final state has
	// no children.
	Final bool

	// Entry and Exit run when the state is entered and left (WP-27), in
	// order. Ancestors are entered before descendants and left after
	// them.
	Entry, Exit []MachineAction[D]

	// After are the state's timers.
	After []StateTimer
}

// MachineTransition moves the machine when Event arrives while From is active.
type MachineTransition[D any] struct {
	// From is the source state. A transition of an ancestor applies to
	// every descendant; the innermost active source with an enabled
	// transition wins.
	From string

	// Event is the event name that triggers it.
	Event string

	// To is the target state; empty makes a targetless transition, which
	// runs Actions without leaving or entering any state.
	To string

	// Guard, when set, must return true for the transition to be taken;
	// otherwise the next transition (in this slice's order, then the
	// source's ancestors) is tried.
	Guard MachineGuard[D]

	// Actions run after the exit actions and before the entry actions.
	Actions []MachineAction[D]

	// History enters a compound or parallel To where it was last left.
	History HistoryMode

	// Reenter makes a transition to From itself, or to one of its
	// descendants, leave and enter From again (running its exit and
	// entry actions and restarting its timers). Without it a
	// self-transition only runs Actions, and a transition to a
	// descendant leaves only From's active descendants.
	Reenter bool
}

// MachineSpec defines a durable state machine.
type MachineSpec[D any] struct {
	// Name is the machine's name and the name of the workflow
	// DefineMachine returns.
	Name string

	// Initial is the top-level state an instance starts in.
	Initial string

	// States lists every state; their order is the document order that
	// breaks ties (which region's transition is tried first, the order
	// of entry and exit actions among siblings).
	States []MachineState[D]

	// Transitions lists every transition; for one source, earlier
	// transitions are tried first.
	Transitions []MachineTransition[D]

	// Store keeps each instance's record. Use the driver's durable
	// JournalStore (sqlstore, Redis) so instances survive restarts and
	// every worker sees them; MemoryJournal keeps them in this process.
	// Records are never forgotten by the Journal plugin (their run id is
	// not a run's).
	Store JournalStore

	// Client sends the machine's timers (delayed runs of itself). It is
	// required when a state has timers, and must have CapDelayedStart
	// and CapIdempotencyKey; pass the client the machine is registered
	// on.
	Client Client

	// Clock times timers and the log; nil means clock.Real.
	Clock clock.Clock

	// Ordered handles a key's events in the order they were sent
	// (Concurrency.FIFO, CapKeyFIFO); without it events of one key are
	// still handled one at a time, in the order the driver claims them.
	Ordered bool

	// Retry is the event runs' retry policy, as Workflow.Retry.
	Retry RetryPolicy

	// KeepLog is how many handled events the instance's log keeps:
	// zero means DefaultMachineLog, negative keeps none.
	KeepLog int

	// Unhandled says what an event no active state handles does.
	Unhandled UnhandledPolicy
}

// MachineResult is an event run's result: what AwaitResult returns as
// JSON for a run of SendEvent or StartMachine.
type MachineResult struct {
	// Handled reports whether the event took a transition.
	Handled bool `json:"handled"`

	// Started reports that this run started the instance.
	Started bool `json:"started,omitempty"`

	// Stale reports a timer whose state was left before it fired.
	Stale bool `json:"stale,omitempty"`

	// States are the active atomic states after the event, in document
	// order.
	States []string `json:"states"`

	// Done reports that the instance reached a top-level final state.
	Done bool `json:"done,omitempty"`

	// Seq is the instance's microstep sequence number after the event.
	Seq uint64 `json:"seq"`
}

// MachineStep is one handled event in an instance's log.
type MachineStep struct {
	Seq   uint64    `json:"seq"`
	Event string    `json:"event"`
	From  []string  `json:"from,omitempty"`
	To    []string  `json:"to"`
	At    time.Time `json:"at"`
}

// MachineSnapshot is an instance as MachineStatus reads it.
type MachineSnapshot[D any] struct {
	// Done reports a top-level final state reached.
	Done bool

	// States are the active atomic states; Active every active state,
	// ancestors included; both in document order.
	States, Active []string

	// Data is the instance's extended state.
	Data D

	// Seq is the microstep sequence number.
	Seq uint64

	// Log is the last handled events, oldest first.
	Log []MachineStep
}

// In reports whether state is active in the snapshot.
func (s MachineSnapshot[D]) In(state string) bool {
	for _, a := range s.Active {
		if a == state {
			return true
		}
	}
	return false
}

// machineCall is an event run's payload.
type machineCall struct {
	Event   string          `json:"event,omitempty"`
	Payload []byte          `json:"payload,omitempty"`
	Start   bool            `json:"start,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Timer   *machineTimer   `json:"timer,omitempty"`
}

// machineTimer identifies the state entry a timer belongs to.
type machineTimer struct {
	State string `json:"state"`
	Seq   uint64 `json:"seq"`
	Index int    `json:"index"`
}

// machineRecord is an instance's stored record.
type machineRecord struct {
	Started bool                `json:"started"`
	Done    bool                `json:"done,omitempty"`
	Active  []string            `json:"active,omitempty"`
	Leaves  []string            `json:"leaves,omitempty"`
	Data    json.RawMessage     `json:"data,omitempty"`
	Seq     uint64              `json:"seq"`
	Entered map[string]uint64   `json:"entered,omitempty"`
	History map[string][]string `json:"history,omitempty"`
	Log     []MachineStep       `json:"log,omitempty"`
	LastRun RunID               `json:"last_run,omitempty"`
	LastOut []byte              `json:"last_out,omitempty"`
}

// DefineMachine checks spec and returns the workflow that handles its
// events: one step that loads the instance's record, starts the instance
// when it is new, takes the transitions the event enables (running exit,
// transition and entry actions and raising done and internal events),
// sends the timers of the states it entered, and saves the record with
// the run's id, all while no other event of the key is handled. Register
// it like any workflow; SendEvent and StartMachine enqueue events.
//
// An event is applied once: a run retried after its record was saved
// returns the saved result. The driver needs CapConcurrencyKey with a Go
// key function (memory, sqlstore, Redis; Hatchet refuses it at Register),
// and CapKeyFIFO with Ordered. A machine with timers needs a Client with
// CapDelayedStart and CapIdempotencyKey, checked here.
func DefineMachine[D any](spec MachineSpec[D]) (Workflow, error) {
	m, err := compileMachine(spec)
	if err != nil {
		return Workflow{}, err
	}
	return Workflow{
		Name:  spec.Name,
		Retry: spec.Retry,
		Concurrency: &Concurrency{
			Key:     KeyFunc(func(j Job) string { return j.Labels[LabelObjectKey] }),
			MaxRuns: 1,
			FIFO:    spec.Ordered,
		},
		Steps: []Step{{Name: "event", Run: m.step}},
	}, nil
}

// SendEvent enqueues event with payload for the instance key of machine,
// and returns the event's run; AwaitResult gives its MachineResult as
// JSON. A key never seen before starts in the machine's initial state
// with zero data, then handles the event. job may carry the other fields
// of the run (IdempotencyKey, RunAt, Labels, Tenant); its Workflow and
// Payload are set here.
func SendEvent(ctx context.Context, client Client, machine, key, event string, payload []byte, job Job) (RunRef, error) {
	if event == "" {
		return RunRef{}, invalidf("SendEvent needs an event name")
	}
	return sendMachine(ctx, client, machine, key, machineCall{Event: event, Payload: payload}, job)
}

// StartMachine enqueues the start of instance key with data as its
// initial extended state, running the initial states' entry actions. A
// key already started is left as it is (MachineResult.Started false), so
// StartMachine is safe to repeat.
func StartMachine[D any](ctx context.Context, client Client, machine, key string, data D, job Job) (RunRef, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return RunRef{}, fmt.Errorf("workflow: start machine %q: data: %w", machine, err)
	}
	return sendMachine(ctx, client, machine, key, machineCall{Start: true, Data: raw}, job)
}

func sendMachine(ctx context.Context, client Client, machine, key string, call machineCall, job Job) (RunRef, error) {
	if client == nil || machine == "" || key == "" {
		return RunRef{}, invalidf("a machine event needs a client, a machine and a key")
	}
	payload, err := json.Marshal(call)
	if err != nil {
		return RunRef{}, fmt.Errorf("workflow: machine %q: %w", machine, err)
	}
	job.Workflow, job.Payload = machine, payload
	labels := make(map[string]string, len(job.Labels)+1)
	for k, v := range job.Labels {
		labels[k] = v
	}
	labels[LabelObjectKey] = key
	job.Labels = labels
	return client.Enqueue(AllowReservedLabels(ctx, LabelObjectKey), job)
}

// MachineStatus reads instance key of machine from store, outside any
// event (a query endpoint, a test), and false for a key never started. It
// reads the last saved record; an event being handled now may replace it
// a moment later.
func MachineStatus[D any](ctx context.Context, store JournalStore, machine, key string) (MachineSnapshot[D], bool, error) {
	var snap MachineSnapshot[D]
	if store == nil || machine == "" || key == "" {
		return snap, false, invalidf("MachineStatus needs a store, a machine and a key")
	}
	rec, err := loadMachine(ctx, store, objectRun(machine, key))
	if err != nil {
		return snap, false, fmt.Errorf("workflow: machine %q key %q: %w", machine, key, err)
	}
	if !rec.Started {
		return snap, false, nil
	}
	if len(rec.Data) > 0 {
		if err := json.Unmarshal(rec.Data, &snap.Data); err != nil {
			return snap, false, fmt.Errorf("workflow: machine %q key %q: stored data does not decode: %w", machine, key, err)
		}
	}
	snap.Done, snap.States, snap.Active, snap.Seq, snap.Log = rec.Done, rec.Leaves, rec.Active, rec.Seq, rec.Log
	return snap, true, nil
}

// ForgetMachine deletes instance key of machine from store: its state,
// data, history and log. An event sent to the key later starts a new
// instance, and a timer of the old one finds nothing to act on. Call it
// for instances that reached a final state (MachineSnapshot.Done) and are
// no longer needed: records are never removed otherwise. It does not wait
// for an event being handled now, which may write the record again.
func ForgetMachine(ctx context.Context, store JournalStore, machine, key string) error {
	if store == nil || machine == "" || key == "" {
		return invalidf("ForgetMachine needs a store, a machine and a key")
	}
	if err := store.Forget(ctx, objectRun(machine, key)); err != nil {
		return fmt.Errorf("workflow: machine %q key %q: forget: %w", machine, key, err)
	}
	return nil
}

// IsUnhandledEvent reports whether err is an event refused under
// MachineReject: ErrUnhandledEvent itself, or the *RunFailedError that
// AwaitResult returns for the event's run, which carries the step's error
// as text only.
func IsUnhandledEvent(err error) bool {
	if errors.Is(err, ErrUnhandledEvent) {
		return true
	}
	var failed *RunFailedError
	return errors.As(err, &failed) && strings.Contains(failed.Err, ErrUnhandledEvent.Error())
}

func loadMachine(ctx context.Context, store JournalStore, run RunID) (machineRecord, error) {
	var rec machineRecord
	raw, ok, err := store.Load(ctx, run, objectStateKey)
	if err != nil || !ok {
		return rec, err
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		return rec, Permanent(fmt.Errorf("stored record does not decode: %w", err))
	}
	return rec, nil
}

// errMachineLoop is the error of an event whose handling passed
// MaxMachineMicrosteps.
var errMachineLoop = errors.New("workflow: machine event caused more than MaxMachineMicrosteps transitions (a raise loop?)")
