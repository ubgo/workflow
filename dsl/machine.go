package dsl

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// MachineDefinition is a durable state machine (workflow.DefineMachine)
// written as data: states, transitions and timers, with guards and
// actions named, never written, so the data can only call code the
// program registered in a MachineRegistry. It is a statechart, the model
// behind a state-machine language such as Step Functions' ASL, with
// hierarchy, parallel regions, history and timers.
//
//	{
//	  "name": "order",
//	  "initial": "new",
//	  "unhandled": "reject",
//	  "states": [
//	    {"name": "new", "after": [{"delay": "30m", "event": "expire"}]},
//	    {"name": "paid", "entry": ["send-receipt"]},
//	    {"name": "cancelled", "final": true}
//	  ],
//	  "transitions": [
//	    {"from": "new", "event": "pay", "to": "paid", "guard": "covers-total", "actions": ["record-payment"]},
//	    {"from": "new", "event": "expire", "to": "cancelled"}
//	  ]
//	}
//
//ds:def id=dsl-machinedefinition-tmpd4qeb owner=@khanakia stability=stable
type MachineDefinition struct {
	// Name is the machine's (and its workflow's) name.
	Name string `json:"name"`

	// Initial is the top-level state an instance starts in.
	Initial string `json:"initial"`

	// States lists every state, in document order.
	States []StateDef `json:"states"`

	// Transitions lists every transition; earlier ones are tried first.
	Transitions []TransitionDef `json:"transitions,omitempty"`

	// Ordered handles a key's events in the order they were sent.
	Ordered bool `json:"ordered,omitempty"`

	// KeepLog is how many handled events an instance's log keeps (zero:
	// the default, negative: none).
	KeepLog int `json:"keep_log,omitempty"`

	// Unhandled is "ignore" (the default) or "reject".
	Unhandled string `json:"unhandled,omitempty"`

	// Retry is the event runs' retry policy.
	Retry *RetryDef `json:"retry,omitempty"`
}

// StateDef is one state of a MachineDefinition.
//
//ds:def id=dsl-statedef-6whhsty9 owner=@khanakia stability=stable
type StateDef struct {
	Name     string `json:"name"`
	Parent   string `json:"parent,omitempty"`
	Initial  string `json:"initial,omitempty"`
	Parallel bool   `json:"parallel,omitempty"`
	Final    bool   `json:"final,omitempty"`

	// Entry and Exit name registered actions, run in order.
	Entry []string `json:"entry,omitempty"`
	Exit  []string `json:"exit,omitempty"`

	// After are the state's timers.
	After []TimerDef `json:"after,omitempty"`
}

// TimerDef is a timer of a state: Event after Delay (a Go duration
// string such as "30m").
//
//ds:def id=dsl-timerdef-gstrjz2d owner=@khanakia stability=stable
type TimerDef struct {
	Delay string `json:"delay"`
	Event string `json:"event"`
}

// TransitionDef is one transition of a MachineDefinition.
//
//ds:def id=dsl-transitiondef-jjhav7bf owner=@khanakia stability=stable
type TransitionDef struct {
	From  string `json:"from"`
	Event string `json:"event"`
	To    string `json:"to,omitempty"`

	// Guard names a registered guard; empty means always taken.
	Guard string `json:"guard,omitempty"`

	// Actions name registered actions, run in order.
	Actions []string `json:"actions,omitempty"`

	// History is "", "shallow" or "deep".
	History string `json:"history,omitempty"`

	// Reenter leaves and enters the source again on a self-transition
	// or a transition to a descendant.
	Reenter bool `json:"reenter,omitempty"`
}

// MachineRegistry holds the guards and actions a MachineDefinition may
// name, for machines whose data is a D. The zero value is ready to use;
// it is safe for concurrent use.
//
//ds:def id=dsl-machineregistry-k2z5pnpz owner=@khanakia stability=stable
type MachineRegistry[D any] struct {
	mu      sync.RWMutex
	guards  map[string]workflow.MachineGuard[D]
	actions map[string]workflow.MachineAction[D]
}

// NewMachineRegistry returns an empty registry.
//
//ds:def id=dsl-newmachineregistry-hst3eav7 owner=@khanakia stability=stable
func NewMachineRegistry[D any]() *MachineRegistry[D] { return &MachineRegistry[D]{} }

// Guard registers g under name; an empty name, a nil guard or a name
// taken is refused with ErrInvalid.
//
//ds:def id=dsl-machineregistry-guard-ag7tpere owner=@khanakia stability=stable
func (r *MachineRegistry[D]) Guard(name string, g workflow.MachineGuard[D]) error {
	if name == "" || g == nil {
		return fieldErr("", "a guard needs a name and a function")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.guards[name]; dup {
		return fieldErr("", "guard %q is registered twice", name)
	}
	if r.guards == nil {
		r.guards = map[string]workflow.MachineGuard[D]{}
	}
	r.guards[name] = g
	return nil
}

// Action registers a under name, as Guard does for guards.
//
//ds:def id=dsl-machineregistry-action-2d2f5jjj owner=@khanakia stability=stable
func (r *MachineRegistry[D]) Action(name string, a workflow.MachineAction[D]) error {
	if name == "" || a == nil {
		return fieldErr("", "an action needs a name and a function")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.actions[name]; dup {
		return fieldErr("", "action %q is registered twice", name)
	}
	if r.actions == nil {
		r.actions = map[string]workflow.MachineAction[D]{}
	}
	r.actions[name] = a
	return nil
}

//ds:def id=dsl-machineregistry-guardlookup-epekbcpd owner=@khanakia stability=stable
func (r *MachineRegistry[D]) guard(path, name string) (workflow.MachineGuard[D], error) {
	if r != nil {
		r.mu.RLock()
		g, ok := r.guards[name]
		r.mu.RUnlock()
		if ok {
			return g, nil
		}
	}
	return nil, fieldErr(path, "no guard is registered as %q (registered: %s)", name, r.names(true))
}

//ds:def id=dsl-machineregistry-actionlookup-ugtbmzng owner=@khanakia stability=stable
func (r *MachineRegistry[D]) action(path, name string) (workflow.MachineAction[D], error) {
	if r != nil {
		r.mu.RLock()
		a, ok := r.actions[name]
		r.mu.RUnlock()
		if ok {
			return a, nil
		}
	}
	return nil, fieldErr(path, "no action is registered as %q (registered: %s)", name, r.names(false))
}

func (r *MachineRegistry[D]) names(guards bool) string {
	if r == nil {
		return "none"
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var names []string
	if guards {
		for n := range r.guards {
			names = append(names, n)
		}
	} else {
		for n := range r.actions {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		return "none"
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}

// MachineEnv is what a machine needs besides its definition: where
// instances live, the client its timers are sent on, and the clock.
//
//ds:def id=dsl-machineenv-wqbuhqmk owner=@khanakia stability=stable
type MachineEnv struct {
	// Store keeps each instance's record (MachineSpec.Store).
	Store workflow.JournalStore

	// Client sends timers (MachineSpec.Client); required when a state
	// has timers.
	Client workflow.Client

	// Clock is the time source; nil means clock.Real.
	Clock clock.Clock
}

// ParseMachine reads a MachineDefinition from JSON strictly (unknown
// fields, wrong types and data after the document are refused with their
// JSON path) and checks what the data form can get wrong.
//
//ds:def id=dsl-parsemachine-mngjffec owner=@khanakia stability=stable
func ParseMachine(data []byte) (MachineDefinition, error) {
	def, err := parseDoc[MachineDefinition](data)
	if err != nil {
		return MachineDefinition{}, err
	}
	if err := def.Validate(); err != nil {
		return MachineDefinition{}, err
	}
	return def, nil
}

// Validate checks what only the data form can get wrong: names, an
// unhandled policy and history modes the machine knows, timer durations.
// BuildMachine then resolves names and runs workflow.DefineMachine's own
// checks (states, parents, initials, targets).
//
//ds:def id=dsl-machinedefinition-validate-w9g8h2br owner=@khanakia stability=stable
func (d MachineDefinition) Validate() error {
	if d.Name == "" {
		return fieldErr("name", "is empty")
	}
	if d.Unhandled != "" && d.Unhandled != "ignore" && d.Unhandled != "reject" {
		return fieldErr("unhandled", "%q is not \"ignore\" or \"reject\"", d.Unhandled)
	}
	if d.Retry != nil {
		if _, err := d.Retry.policy("retry"); err != nil {
			return err
		}
	}
	if len(d.States) == 0 {
		return fieldErr("states", "a machine needs at least one state")
	}
	for i, s := range d.States {
		for j, tm := range s.After {
			p := index(at(index("states", i), "after"), j)
			dl, err := duration(at(p, "delay"), tm.Delay)
			if err != nil {
				return err
			}
			if dl <= 0 {
				return fieldErr(at(p, "delay"), "must be positive")
			}
			if tm.Event == "" {
				return fieldErr(at(p, "event"), "is empty")
			}
		}
	}
	for i, t := range d.Transitions {
		if _, err := historyMode(at(index("transitions", i), "history"), t.History); err != nil {
			return err
		}
	}
	return nil
}

//ds:def id=dsl-historymode-bh8makju owner=@khanakia stability=stable
func historyMode(path, s string) (workflow.HistoryMode, error) {
	switch s {
	case "":
		return workflow.HistoryNone, nil
	case "shallow":
		return workflow.HistoryShallow, nil
	case "deep":
		return workflow.HistoryDeep, nil
	}
	return 0, fieldErr(path, "%q is not \"shallow\" or \"deep\"", s)
}

// BuildMachine turns def into the machine's workflow
// (workflow.DefineMachine), resolving guard and action names against reg
// (an unknown name is refused with its JSON path and the registered
// names). Register the workflow on the client like any other; SendEvent,
// StartMachine and MachineStatus work on it unchanged.
//
//ds:def id=dsl-buildmachine-pfc8h78c owner=@khanakia stability=stable
func BuildMachine[D any](def MachineDefinition, reg *MachineRegistry[D], env MachineEnv) (workflow.Workflow, error) {
	if err := def.Validate(); err != nil {
		return workflow.Workflow{}, err
	}
	spec := workflow.MachineSpec[D]{
		Name: def.Name, Initial: def.Initial, Ordered: def.Ordered, KeepLog: def.KeepLog,
		Store: env.Store, Client: env.Client, Clock: env.Clock,
	}
	if def.Unhandled == "reject" {
		spec.Unhandled = workflow.MachineReject
	}
	if def.Retry != nil {
		spec.Retry, _ = def.Retry.policy("retry")
	}
	actions := func(path string, names []string) ([]workflow.MachineAction[D], error) {
		var out []workflow.MachineAction[D]
		for i, n := range names {
			a, err := reg.action(index(path, i), n)
			if err != nil {
				return nil, err
			}
			out = append(out, a)
		}
		return out, nil
	}
	for i, s := range def.States {
		p := index("states", i)
		st := workflow.MachineState[D]{Name: s.Name, Parent: s.Parent, Initial: s.Initial, Parallel: s.Parallel, Final: s.Final}
		var err error
		if st.Entry, err = actions(at(p, "entry"), s.Entry); err != nil {
			return workflow.Workflow{}, err
		}
		if st.Exit, err = actions(at(p, "exit"), s.Exit); err != nil {
			return workflow.Workflow{}, err
		}
		for _, tm := range s.After {
			d, _ := duration("", tm.Delay) // Validate parsed it
			st.After = append(st.After, workflow.StateTimer{Delay: d, Event: tm.Event})
		}
		spec.States = append(spec.States, st)
	}
	for i, t := range def.Transitions {
		p := index("transitions", i)
		tr := workflow.MachineTransition[D]{From: t.From, Event: t.Event, To: t.To, Reenter: t.Reenter}
		tr.History, _ = historyMode("", t.History)
		if t.Guard != "" {
			g, err := reg.guard(at(p, "guard"), t.Guard)
			if err != nil {
				return workflow.Workflow{}, err
			}
			tr.Guard = g
		}
		var err error
		if tr.Actions, err = actions(at(p, "actions"), t.Actions); err != nil {
			return workflow.Workflow{}, err
		}
		spec.Transitions = append(spec.Transitions, tr)
	}
	wf, err := workflow.DefineMachine(spec)
	if err != nil {
		return workflow.Workflow{}, fmt.Errorf("dsl: machine %q: %w", def.Name, err)
	}
	return wf, nil
}

// LoadMachine is ParseMachine then BuildMachine.
//
//ds:def id=dsl-loadmachine-hyqt5332 owner=@khanakia stability=stable
func LoadMachine[D any](data []byte, reg *MachineRegistry[D], env MachineEnv) (workflow.Workflow, error) {
	def, err := ParseMachine(data)
	if err != nil {
		return workflow.Workflow{}, err
	}
	return BuildMachine(def, reg, env)
}

// MarshalMachine writes def as indented JSON, for an admin tool that
// reads, edits and stores definitions.
//
//ds:def id=dsl-marshalmachine-zgtjc9q9 owner=@khanakia stability=stable
func MarshalMachine(def MachineDefinition) ([]byte, error) {
	b, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: dsl: %w", workflow.ErrInvalid, err) // a NaN factor or jitter
	}
	return b, nil
}
