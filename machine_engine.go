package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/ubgo/workflow/clock"
)

// mnode is a compiled state. The root is a virtual compound state with an
// empty name, the parent of every top-level state.
type mnode struct {
	name     string
	parent   *mnode
	children []*mnode
	initial  *mnode
	parallel bool
	final    bool
	order    int // document order: pre-order over the tree
	spec     int // index in MachineSpec.States; -1 for the root
}

func (n *mnode) compound() bool { return len(n.children) > 0 && !n.parallel }

// descendantOf reports whether n is a strict descendant of a.
func (n *mnode) descendantOf(a *mnode) bool {
	for p := n.parent; p != nil; p = p.parent {
		if p == a {
			return true
		}
	}
	return false
}

// mtrans is a compiled transition.
type mtrans struct {
	src, dst *mnode // dst nil: targetless
	spec     int    // index in MachineSpec.Transitions
	event    string
	history  HistoryMode
	reenter  bool
}

// selfOnly reports a self-transition without Reenter: actions only.
func (t *mtrans) selfOnly() bool { return t.dst == t.src && !t.reenter }

// machine is a compiled MachineSpec.
type machine[D any] struct {
	spec    MachineSpec[D]
	root    *mnode
	nodes   map[string]*mnode
	ordered []*mnode // every state but the root, in document order
	trans   map[*mnode][]*mtrans
	history map[*mnode]bool // states some transition enters with history
	timers  bool
	clk     clock.Clock
	keep    int
}

//ds:def id=compilemachine-vd5zfvzw owner=@khanakia stability=stable
func compileMachine[D any](spec MachineSpec[D]) (*machine[D], error) {
	if spec.Name == "" || spec.Initial == "" || len(spec.States) == 0 || spec.Store == nil {
		return nil, invalidf("machine needs a Name, an Initial state, States and a Store")
	}
	m := &machine[D]{
		spec:    spec,
		root:    &mnode{spec: -1},
		nodes:   make(map[string]*mnode, len(spec.States)),
		trans:   map[*mnode][]*mtrans{},
		history: map[*mnode]bool{},
		clk:     spec.Clock,
		keep:    spec.KeepLog,
	}
	if m.clk == nil {
		m.clk = clock.Real{}
	}
	if m.keep == 0 {
		m.keep = DefaultMachineLog
	}
	fail := func(format string, args ...any) error {
		return invalidf("machine %q: "+format, append([]any{spec.Name}, args...)...)
	}
	for i, s := range spec.States {
		if s.Name == "" {
			return nil, fail("state %d has no name", i)
		}
		if _, dup := m.nodes[s.Name]; dup {
			return nil, fail("state %q is defined twice", s.Name)
		}
		m.nodes[s.Name] = &mnode{name: s.Name, parallel: s.Parallel, final: s.Final, spec: i}
		for j, tm := range s.After {
			if tm.Delay <= 0 || tm.Event == "" {
				return nil, fail("state %q timer %d needs a positive Delay and an Event", s.Name, j)
			}
			m.timers = true
		}
	}
	for _, s := range spec.States {
		n := m.nodes[s.Name]
		parent := m.root
		if s.Parent != "" {
			p, ok := m.nodes[s.Parent]
			if !ok {
				return nil, fail("state %q has an unknown parent %q", s.Name, s.Parent)
			}
			parent = p
		}
		n.parent = parent
		parent.children = append(parent.children, n)
	}
	// A parent chain that does not reach the root is a cycle.
	for _, n := range m.nodes {
		steps := 0
		for p := n.parent; p != m.root; p = p.parent {
			if p == nil || steps > len(m.nodes) {
				return nil, fail("state %q is in a parent cycle", n.name)
			}
			steps++
		}
	}
	m.root.initial = m.nodes[spec.Initial]
	if m.root.initial == nil || m.root.initial.parent != m.root {
		return nil, fail("Initial %q is not a top-level state", spec.Initial)
	}
	for _, s := range spec.States {
		n := m.nodes[s.Name]
		switch {
		case n.final && len(n.children) > 0:
			return nil, fail("final state %q has children", s.Name)
		case n.parallel && len(n.children) == 0:
			return nil, fail("parallel state %q has no children", s.Name)
		case n.compound():
			c, ok := m.nodes[s.Initial]
			if s.Initial == "" || !ok || c.parent != n {
				return nil, fail("compound state %q needs an Initial that is one of its children", s.Name)
			}
			n.initial = c
		case s.Initial != "" && !n.parallel:
			return nil, fail("state %q has an Initial but no children", s.Name)
		}
	}
	var walk func(n *mnode)
	walk = func(n *mnode) {
		for _, c := range n.children {
			c.order = len(m.ordered)
			m.ordered = append(m.ordered, c)
			walk(c)
		}
	}
	walk(m.root)
	for i, t := range spec.Transitions {
		src, ok := m.nodes[t.From]
		if !ok {
			return nil, fail("transition %d has an unknown From %q", i, t.From)
		}
		if t.Event == "" {
			return nil, fail("transition %d from %q has no Event", i, t.From)
		}
		mt := &mtrans{src: src, spec: i, event: t.Event, history: t.History, reenter: t.Reenter}
		if t.To != "" {
			if mt.dst, ok = m.nodes[t.To]; !ok {
				return nil, fail("transition %d from %q has an unknown To %q", i, t.From, t.To)
			}
		}
		if t.History != HistoryNone {
			if mt.dst == nil || len(mt.dst.children) == 0 {
				return nil, fail("transition %d from %q enters %q with history, which needs a compound or parallel target", i, t.From, t.To)
			}
			m.history[mt.dst] = true
		}
		m.trans[src] = append(m.trans[src], mt)
	}
	if m.timers {
		if spec.Client == nil {
			return nil, fail("states with timers need a Client")
		}
		need := CapsOf(CapDelayedStart, CapIdempotencyKey)
		if missing := spec.Client.Capabilities().Missing(need); !missing.IsZero() {
			return nil, &UnsupportedError{Field: "State.After", Missing: missing,
				Reason: "machine " + spec.Name + ": a timer is a delayed run of the machine, sent once per state entry"}
		}
	}
	return m, nil
}

// machineRun is the handling of one event run.
type machineRun[D any] struct {
	m        *machine[D]
	ctx      context.Context
	key      string
	rec      *machineRecord
	data     D
	active   map[*mnode]bool
	internal []MachineEvent
	steps    int
	inGuard  bool
	seq0     uint64 // the record's Seq before this event
}

// step is the machine workflow's one step.
//
//ds:def id=machine-step-vvjdn77u owner=@khanakia stability=stable
func (m *machine[D]) step(ctx context.Context, in []byte) ([]byte, error) {
	name := m.spec.Name
	info, ok := StepInfoFrom(ctx)
	if !ok {
		return nil, Permanent(invalidf("machine %q: not called from a step", name))
	}
	key := info.Labels[LabelObjectKey]
	if key == "" {
		return nil, Permanent(invalidf("machine %q: the event has no instance key; send it with SendEvent", name))
	}
	var call machineCall
	if err := json.Unmarshal(in, &call); err != nil {
		return nil, Permanent(fmt.Errorf("%w: machine %q: event payload: %v", ErrInvalid, name, err))
	}
	run := objectRun(name, key)
	rec, err := loadMachine(ctx, m.spec.Store, run)
	if err != nil {
		return nil, fmt.Errorf("workflow: machine %q key %q: %w", name, key, err)
	}
	if rec.LastRun == info.RunID {
		return rec.LastOut, nil // applied already; a retry after the save
	}
	r := &machineRun[D]{m: m, ctx: ctx, key: key, rec: &rec, active: map[*mnode]bool{}, seq0: rec.Seq}
	if len(rec.Data) > 0 {
		if err := json.Unmarshal(rec.Data, &r.data); err != nil {
			return nil, Permanent(fmt.Errorf("workflow: machine %q key %q: stored data does not decode: %w", name, key, err))
		}
	}
	for _, a := range rec.Active {
		n := m.nodes[a]
		if n == nil {
			return nil, Permanent(invalidf("machine %q key %q: stored state %q is not in the machine (renamed or removed since?)", name, key, a))
		}
		r.active[n] = true
	}
	if rec.Entered == nil {
		rec.Entered = map[string]uint64{}
	}
	from := slices.Clone(rec.Leaves)
	res := MachineResult{}
	event := call.Event
	var rejected error // MachineReject's refusal of an event that started the instance
	switch {
	case call.Timer != nil:
		n := m.nodes[call.Timer.State]
		if !rec.Started || rec.Done || n == nil || !r.active[n] || rec.Entered[n.name] != call.Timer.Seq {
			res.Stale = true
			break
		}
		if res.Handled, err = r.handle(MachineEvent{Name: event}); err != nil {
			return nil, err
		}
	case call.Start:
		if rec.Started {
			break
		}
		if len(call.Data) > 0 {
			if err := json.Unmarshal(call.Data, &r.data); err != nil {
				return nil, Permanent(fmt.Errorf("%w: machine %q: start data: %v", ErrInvalid, name, err))
			}
		}
		event = MachineStartEvent
		res.Started = true
		if err := r.start(); err != nil {
			return nil, err
		}
	default:
		if event == "" {
			return nil, Permanent(invalidf("machine %q: event has no name", name))
		}
		if !rec.Started {
			res.Started = true
			if err := r.start(); err != nil {
				return nil, err
			}
		}
		if !rec.Done {
			if res.Handled, err = r.handle(MachineEvent{Name: event, Payload: call.Payload}); err != nil {
				return nil, err
			}
		}
		if !res.Handled && m.spec.Unhandled == MachineReject {
			rejected = Permanent(fmt.Errorf("%w (machine %q key %q, event %q, states %v)", ErrUnhandledEvent, name, key, event, rec.Leaves))
			if !res.Started {
				return nil, rejected
			}
			// The event started the instance and was then refused: keep
			// the start (its entry actions ran and must not run again on
			// the next event), refuse the event.
			event = MachineStartEvent
		}
	}
	if !res.Handled && !res.Started {
		// Nothing changed: answer without a write, and without claiming
		// the run as applied (applying it again changes nothing either).
		res.States, res.Done, res.Seq = rec.Leaves, rec.Done, rec.Seq
		if res.States == nil {
			res.States = []string{}
		}
		return encodeJSON("machine result", res)
	}
	if err := r.sendTimers(); err != nil {
		return nil, err
	}
	rec.Active, rec.Leaves = r.configuration()
	raw, err := encodeJSON("machine "+name+" data", r.data)
	if err != nil {
		return nil, err
	}
	rec.Data = raw
	if m.keep > 0 {
		rec.Log = append(rec.Log, MachineStep{Seq: rec.Seq, Event: event, From: from, To: rec.Leaves, At: m.clk.Now().UTC()})
		if over := len(rec.Log) - m.keep; over > 0 {
			rec.Log = slices.Delete(rec.Log, 0, over)
		}
	}
	res.States, res.Done, res.Seq = rec.Leaves, rec.Done, rec.Seq
	out, err := encodeJSON("machine result", res)
	if err != nil {
		return nil, err
	}
	if rejected == nil {
		rec.LastRun, rec.LastOut = info.RunID, out
	}
	b, err := encodeJSON("machine "+name+" record", rec)
	if err != nil {
		return nil, err
	}
	if err := m.spec.Store.Save(ctx, run, objectStateKey, b); err != nil {
		return nil, fmt.Errorf("workflow: machine %q key %q: save: %w", name, key, err)
	}
	if rejected != nil {
		return nil, rejected
	}
	return out, nil
}

// configuration returns the active states and the active atomic states,
// in document order.
func (r *machineRun[D]) configuration() (active, leaves []string) {
	active, leaves = []string{}, []string{}
	for _, n := range r.m.ordered {
		if !r.active[n] {
			continue
		}
		active = append(active, n.name)
		leaf := true
		for _, c := range n.children {
			if r.active[c] {
				leaf = false
				break
			}
		}
		if leaf {
			leaves = append(leaves, n.name)
		}
	}
	return active, leaves
}

// start enters the initial configuration.
func (r *machineRun[D]) start() error {
	r.rec.Started = true
	set := map[*mnode]bool{}
	r.addDescendants(r.m.root.initial, set, HistoryNone)
	r.rec.Seq++
	if err := r.enter(set, MachineEvent{Name: MachineStartEvent}); err != nil {
		return err
	}
	return r.drain()
}

// handle processes an external event and every internal event it causes
// (a macrostep). It reports whether the external event took a
// transition.
func (r *machineRun[D]) handle(ev MachineEvent) (bool, error) {
	ts := r.selectTransitions(ev)
	if len(ts) == 0 {
		return false, nil
	}
	if err := r.microstep(ts, ev); err != nil {
		return false, err
	}
	return true, r.drain()
}

// drain processes queued internal events until none is left or the
// instance is done.
//
//ds:def id=machinerun-drain-rbqwphvs owner=@khanakia stability=stable
func (r *machineRun[D]) drain() error {
	for len(r.internal) > 0 && !r.rec.Done {
		ev := r.internal[0]
		r.internal = r.internal[1:]
		if ts := r.selectTransitions(ev); len(ts) > 0 {
			if err := r.microstep(ts, ev); err != nil {
				return err
			}
		}
	}
	r.internal = nil
	return nil
}

func (r *machineRun[D]) mctx(state string, ev MachineEvent) *MachineCtx[D] {
	return &MachineCtx[D]{Machine: r.m.spec.Name, Key: r.key, Event: ev, State: state, Data: &r.data, Seq: r.rec.Seq, run: r}
}

// selectTransitions picks the enabled transitions for ev: for each active
// atomic state in document order, the first transition (in spec order)
// of the innermost state on its path to the root whose event matches and
// whose guard passes; then conflicts between parallel regions are
// removed as SCXML does.
//
//ds:def id=machinerun-selecttransitions-cu4hj8az owner=@khanakia stability=stable
func (r *machineRun[D]) selectTransitions(ev MachineEvent) []*mtrans {
	var enabled []*mtrans
	_, leaves := r.configuration()
	for _, name := range leaves {
	path:
		for n := r.m.nodes[name]; n != r.m.root; n = n.parent {
			for _, t := range r.m.trans[n] {
				if t.event != ev.Name {
					continue
				}
				if g := r.m.spec.Transitions[t.spec].Guard; g != nil {
					mc := r.mctx(n.name, ev)
					data := r.data // a guard reads a copy: a change it makes is not kept
					mc.Data = &data
					r.inGuard = true
					ok := g(*mc)
					r.inGuard = false
					if !ok {
						continue
					}
				}
				if !slices.Contains(enabled, t) {
					enabled = append(enabled, t)
				}
				break path
			}
		}
	}
	var kept []*mtrans
	for _, t1 := range enabled {
		x1 := r.exitSet(t1)
		preempted := false
		var drop []*mtrans
		for _, t2 := range kept {
			if !intersects(x1, r.exitSet(t2)) {
				continue
			}
			if t1.src.descendantOf(t2.src) {
				drop = append(drop, t2)
			} else {
				preempted = true
				break
			}
		}
		if !preempted {
			kept = slices.DeleteFunc(kept, func(t *mtrans) bool { return slices.Contains(drop, t) })
			kept = append(kept, t1)
		}
	}
	return kept
}

func intersects(a, b map[*mnode]bool) bool {
	for n := range a {
		if b[n] {
			return true
		}
	}
	return false
}

// domain returns the state a transition does not leave: every active
// state below it is exited. Nil for a transition that exits nothing.
//
//ds:def id=machinerun-domain-8t7hkkbb owner=@khanakia stability=stable
func (r *machineRun[D]) domain(t *mtrans) *mnode {
	if t.dst == nil || t.selfOnly() {
		return nil
	}
	if !t.reenter && t.dst.descendantOf(t.src) && len(t.src.children) > 0 {
		return t.src
	}
	// The least common compound ancestor: the nearest proper ancestor
	// of the source that is the root or a compound state containing the
	// target.
	for a := t.src.parent; ; a = a.parent {
		if a == r.m.root || (!a.parallel && (t.dst == a || t.dst.descendantOf(a))) {
			if t.dst == a { // a target that is an ancestor of the source is left and entered again
				if a == r.m.root {
					return a
				}
				continue
			}
			return a
		}
	}
}

// exitSet is the active states a transition leaves.
func (r *machineRun[D]) exitSet(t *mtrans) map[*mnode]bool {
	set := map[*mnode]bool{}
	d := r.domain(t)
	if d == nil {
		return set
	}
	for n := range r.active {
		if n.descendantOf(d) {
			set[n] = true
		}
	}
	return set
}

// microstep takes a set of non-conflicting transitions.
//
//ds:def id=machinerun-microstep-33j5aqdq owner=@khanakia stability=stable
func (r *machineRun[D]) microstep(ts []*mtrans, ev MachineEvent) error {
	r.steps++
	if r.steps > MaxMachineMicrosteps {
		return Permanent(fmt.Errorf("%w (machine %q key %q, event %q)", errMachineLoop, r.m.spec.Name, r.key, ev.Name))
	}
	r.rec.Seq++
	exit := map[*mnode]bool{}
	for _, t := range ts {
		for n := range r.exitSet(t) {
			exit[n] = true
		}
	}
	exiting := make([]*mnode, 0, len(exit))
	for n := range exit {
		exiting = append(exiting, n)
	}
	slices.SortFunc(exiting, func(a, b *mnode) int { return b.order - a.order }) // descendants first
	for _, n := range exiting {
		if r.m.history[n] {
			var remembered []string
			for _, d := range r.m.ordered {
				if r.active[d] && d.descendantOf(n) {
					remembered = append(remembered, d.name)
				}
			}
			if r.rec.History == nil {
				r.rec.History = map[string][]string{}
			}
			r.rec.History[n.name] = remembered
		}
	}
	for _, n := range exiting {
		for i, a := range r.m.spec.States[n.spec].Exit {
			if err := a(r.ctx, r.mctx(n.name, ev)); err != nil {
				return fmt.Errorf("workflow: machine %q key %q: exit action %d of %q: %w", r.m.spec.Name, r.key, i, n.name, err)
			}
		}
		delete(r.active, n)
		delete(r.rec.Entered, n.name)
	}
	for _, t := range ts {
		for i, a := range r.m.spec.Transitions[t.spec].Actions {
			if err := a(r.ctx, r.mctx(t.src.name, ev)); err != nil {
				return fmt.Errorf("workflow: machine %q key %q: action %d of the %q transition from %q: %w", r.m.spec.Name, r.key, i, t.event, t.src.name, err)
			}
		}
	}
	entry := map[*mnode]bool{}
	for _, t := range ts {
		d := r.domain(t)
		if d == nil {
			continue
		}
		r.addDescendants(t.dst, entry, t.history)
		for a := t.dst.parent; a != nil && a != d; a = a.parent {
			entry[a] = true
			if a.parallel {
				for _, c := range a.children {
					if !hasInOrBelow(entry, c) {
						r.addDescendants(c, entry, HistoryNone)
					}
				}
			}
		}
	}
	return r.enter(entry, ev)
}

func hasInOrBelow(set map[*mnode]bool, s *mnode) bool {
	for n := range set {
		if n == s || n.descendantOf(s) {
			return true
		}
	}
	return false
}

// addDescendants adds s and the states entering s enters below it: the
// remembered ones under history, else the initial (compound) or every
// (parallel) child.
//
//ds:def id=machinerun-adddescendants-7p23ezrv owner=@khanakia stability=stable
func (r *machineRun[D]) addDescendants(s *mnode, set map[*mnode]bool, h HistoryMode) {
	set[s] = true
	if remembered, ok := r.rec.History[s.name]; ok && h != HistoryNone && len(remembered) > 0 {
		for _, name := range remembered {
			n := r.m.nodes[name]
			if n == nil {
				continue // removed from the machine since
			}
			if h == HistoryDeep {
				set[n] = true
			} else if n.parent == s {
				r.addDescendants(n, set, HistoryNone)
			}
		}
		if hasBelow(set, s) {
			return
		}
	}
	switch {
	case s.parallel:
		for _, c := range s.children {
			if !hasInOrBelow(set, c) {
				r.addDescendants(c, set, HistoryNone)
			}
		}
	case s.compound():
		r.addDescendants(s.initial, set, HistoryNone)
	}
}

func hasBelow(set map[*mnode]bool, s *mnode) bool {
	for n := range set {
		if n.descendantOf(s) {
			return true
		}
	}
	return false
}

// enter enters set's states in document order (ancestors first), runs
// their entry actions and raises done events for final states.
//
//ds:def id=machinerun-enter-w658d86g owner=@khanakia stability=stable
func (r *machineRun[D]) enter(set map[*mnode]bool, ev MachineEvent) error {
	entering := make([]*mnode, 0, len(set))
	for n := range set {
		if !r.active[n] {
			entering = append(entering, n)
		}
	}
	slices.SortFunc(entering, func(a, b *mnode) int { return a.order - b.order })
	for _, n := range entering {
		r.active[n] = true
		r.rec.Entered[n.name] = r.rec.Seq
		for i, a := range r.m.spec.States[n.spec].Entry {
			if err := a(r.ctx, r.mctx(n.name, ev)); err != nil {
				return fmt.Errorf("workflow: machine %q key %q: entry action %d of %q: %w", r.m.spec.Name, r.key, i, n.name, err)
			}
		}
		if !n.final {
			continue
		}
		p := n.parent
		if p == r.m.root {
			r.rec.Done = true
			continue
		}
		r.internal = append(r.internal, MachineEvent{Name: DoneEvent(p.name)})
		if g := p.parent; g != r.m.root && g.parallel && r.inFinal(g) {
			r.internal = append(r.internal, MachineEvent{Name: DoneEvent(g.name)})
		}
	}
	return nil
}

// inFinal reports whether compound s has a final child active, or every
// region of parallel s is in a final state.
func (r *machineRun[D]) inFinal(s *mnode) bool {
	switch {
	case s.parallel:
		for _, c := range s.children {
			if !r.inFinal(c) {
				return false
			}
		}
		return true
	case s.compound():
		for _, c := range s.children {
			if r.active[c] && c.final {
				return true
			}
		}
	}
	return false
}

// sendTimers enqueues the timers of every state entered by this event
// and still active at its end (a state entered and left within one
// event never times out). Each timer is a delayed event run under a key
// derived from the instance, the state, its entry and the timer's index,
// so a retried event does not send it twice: the timer's run cannot
// finish (and free its key) before this event's run ends, because both
// run under the instance's concurrency key.
//
//ds:def id=machinerun-sendtimers-kuc82nwf owner=@khanakia stability=stable
func (r *machineRun[D]) sendTimers() error {
	if !r.m.timers {
		return nil
	}
	now := r.m.clk.Now()
	for _, n := range r.m.ordered {
		seq, ok := r.rec.Entered[n.name]
		if !r.active[n] || !ok || seq <= r.seq0 {
			continue
		}
		for i, tm := range r.m.spec.States[n.spec].After {
			call := machineCall{Event: tm.Event, Timer: &machineTimer{State: n.name, Seq: seq, Index: i}}
			payload, err := encodeJSON("machine timer", call)
			if err != nil {
				return err
			}
			sum := sha256.Sum256([]byte(r.m.spec.Name + "\x00" + r.key + "\x00" + n.name + "\x00" + strconv.FormatUint(seq, 10) + "\x00" + strconv.Itoa(i)))
			job := Job{
				Workflow:       r.m.spec.Name,
				Payload:        payload,
				RunAt:          now.Add(tm.Delay),
				IdempotencyKey: "machine-timer/" + hex.EncodeToString(sum[:16]),
				Labels:         map[string]string{LabelObjectKey: r.key},
			}
			if _, err := r.m.spec.Client.Enqueue(AllowReservedLabels(r.ctx, LabelObjectKey), job); err != nil && !errors.Is(err, ErrDuplicate) {
				return fmt.Errorf("workflow: machine %q key %q: timer %d of %q: %w", r.m.spec.Name, r.key, i, n.name, err)
			}
		}
	}
	return nil
}
