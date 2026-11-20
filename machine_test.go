package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

type mdata struct {
	Trace []string `json:"trace,omitempty"`
	N     int      `json:"n"`
}

func mtrace(what string) []workflow.MachineAction[mdata] {
	return []workflow.MachineAction[mdata]{func(_ context.Context, m *workflow.MachineCtx[mdata]) error {
		m.Data.Trace = append(m.Data.Trace, what)
		return nil
	}}
}

// startMachine registers wf on a fresh memory client and starts it.
func startMachine(t *testing.T, c workflow.Client, wf workflow.Workflow) {
	t.Helper()
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
}

func sendAwait(t *testing.T, c workflow.Client, machine, key, event string) (workflow.MachineResult, error) {
	t.Helper()
	ref, err := workflow.SendEvent(context.Background(), c, machine, key, event, nil, workflow.Job{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 2 * time.Millisecond})
	var res workflow.MachineResult
	if err == nil {
		err = json.Unmarshal(out, &res)
	}
	return res, err
}

func TestDefineMachine_Refusals(t *testing.T) {
	store := workflow.NewMemoryJournal()
	base := func() workflow.MachineSpec[mdata] {
		return workflow.MachineSpec[mdata]{
			Name: "m", Initial: "a", Store: store,
			States:      []workflow.MachineState[mdata]{{Name: "a"}, {Name: "b"}},
			Transitions: []workflow.MachineTransition[mdata]{{From: "a", Event: "go", To: "b"}},
		}
	}
	cases := map[string]func(s *workflow.MachineSpec[mdata]){
		"no name":         func(s *workflow.MachineSpec[mdata]) { s.Name = "" },
		"no store":        func(s *workflow.MachineSpec[mdata]) { s.Store = nil },
		"no initial":      func(s *workflow.MachineSpec[mdata]) { s.Initial = "" },
		"unknown initial": func(s *workflow.MachineSpec[mdata]) { s.Initial = "z" },
		"nested initial": func(s *workflow.MachineSpec[mdata]) {
			s.States[1].Parent = "a"
			s.States[0].Initial = "b"
			s.Initial = "b"
		},
		"duplicate state":      func(s *workflow.MachineSpec[mdata]) { s.States[1].Name = "a" },
		"unnamed state":        func(s *workflow.MachineSpec[mdata]) { s.States[1].Name = "" },
		"unknown parent":       func(s *workflow.MachineSpec[mdata]) { s.States[1].Parent = "z" },
		"parent cycle":         func(s *workflow.MachineSpec[mdata]) { s.States[0].Parent = "b"; s.States[1].Parent = "a" },
		"compound no initial":  func(s *workflow.MachineSpec[mdata]) { s.States[1].Parent = "a" },
		"initial not a child":  func(s *workflow.MachineSpec[mdata]) { s.States[1].Parent = "a"; s.States[0].Initial = "a" },
		"initial without kids": func(s *workflow.MachineSpec[mdata]) { s.States[1].Initial = "a" },
		"final with children": func(s *workflow.MachineSpec[mdata]) {
			s.States[1].Parent = "a"
			s.States[0].Initial = "b"
			s.States[0].Final = true
		},
		"empty parallel":    func(s *workflow.MachineSpec[mdata]) { s.States[1].Parallel = true },
		"unknown from":      func(s *workflow.MachineSpec[mdata]) { s.Transitions[0].From = "z" },
		"unknown to":        func(s *workflow.MachineSpec[mdata]) { s.Transitions[0].To = "z" },
		"no event":          func(s *workflow.MachineSpec[mdata]) { s.Transitions[0].Event = "" },
		"history on atomic": func(s *workflow.MachineSpec[mdata]) { s.Transitions[0].History = workflow.HistoryDeep },
		"history targetless": func(s *workflow.MachineSpec[mdata]) {
			s.Transitions[0].To = ""
			s.Transitions[0].History = workflow.HistoryShallow
		},
		"timer without delay": func(s *workflow.MachineSpec[mdata]) { s.States[0].After = []workflow.StateTimer{{Event: "t"}} },
		"timer without event": func(s *workflow.MachineSpec[mdata]) { s.States[0].After = []workflow.StateTimer{{Delay: time.Second}} },
		"timer without a client": func(s *workflow.MachineSpec[mdata]) {
			s.States[0].After = []workflow.StateTimer{{Delay: time.Second, Event: "t"}}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := base()
			s.States = slices.Clone(s.States)
			s.Transitions = slices.Clone(s.Transitions)
			mutate(&s)
			if _, err := workflow.DefineMachine(s); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
		})
	}
	if _, err := workflow.DefineMachine(base()); err != nil {
		t.Fatalf("the base spec: %v", err)
	}
}

// noDelay is a client without CapDelayedStart.
type noDelay struct{ workflow.Client }

func (n noDelay) Capabilities() workflow.Caps {
	return n.Client.Capabilities().Without(workflow.CapDelayedStart)
}

func TestDefineMachine_TimersNeedDelayedStart(t *testing.T) {
	_, err := workflow.DefineMachine(workflow.MachineSpec[mdata]{
		Name: "m", Initial: "a", Store: workflow.NewMemoryJournal(), Client: noDelay{memory.New()},
		States: []workflow.MachineState[mdata]{{Name: "a", After: []workflow.StateTimer{{Delay: time.Second, Event: "t"}}}},
	})
	var unsup *workflow.UnsupportedError
	if !errors.As(err, &unsup) || !unsup.Missing.Has(workflow.CapDelayedStart) {
		t.Fatalf("got %v, want an UnsupportedError naming DelayedStart", err)
	}
}

// TestMachine_HatchetStyleRefusal: a driver that evaluates only key
// expressions refuses the machine at Register, through the ordinary
// concurrency check, as for virtual objects.
func TestMachine_ConcurrencyKeyIsAGoFunction(t *testing.T) {
	wf, err := workflow.DefineMachine(workflow.MachineSpec[mdata]{
		Name: "m", Initial: "a", Store: workflow.NewMemoryJournal(),
		States: []workflow.MachineState[mdata]{{Name: "a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if wf.Concurrency == nil || wf.Concurrency.Key.Func == nil || wf.Concurrency.MaxRuns != 1 || len(wf.Steps) != 1 {
		t.Fatalf("machine workflow %+v", wf)
	}
	if !workflow.Requires(wf).Has(workflow.CapConcurrencyKey) {
		t.Fatal("a machine must require CapConcurrencyKey")
	}
}

// TestMachine_ParallelConflicts: one event enabled in two regions takes
// both transitions when they do not conflict; a transition of the
// parallel state itself conflicts with the regions' and the descendant's
// wins (SCXML preemption: a descendant source preempts its ancestor's).
func TestMachine_ParallelConflicts(t *testing.T) {
	c := memory.New()
	wf, err := workflow.DefineMachine(workflow.MachineSpec[mdata]{
		Name: "par", Initial: "p", Store: workflow.NewMemoryJournal(),
		States: []workflow.MachineState[mdata]{
			{Name: "p", Parallel: true},
			{Name: "r1", Parent: "p", Initial: "a1"}, {Name: "a1", Parent: "r1"}, {Name: "b1", Parent: "r1"},
			{Name: "r2", Parent: "p", Initial: "a2"}, {Name: "a2", Parent: "r2"}, {Name: "b2", Parent: "r2"},
			{Name: "out"},
		},
		Transitions: []workflow.MachineTransition[mdata]{
			{From: "a1", Event: "e", To: "b1"},
			{From: "a2", Event: "e", To: "b2"},
			{From: "p", Event: "e", To: "out"},
			{From: "p", Event: "leave", To: "out"},
			{From: "b1", Event: "leave", To: "a1"},  // the descendant's transition wins over p's
			{From: "p", Event: "leave2", To: "out"}, // reached first, from region r1's leaf
			{From: "b2", Event: "leave2", To: "a2"}, // reached second, and preempts p's
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	startMachine(t, c, wf)
	if res, err := sendAwait(t, c, "par", "k", "e"); err != nil || !slices.Equal(res.States, []string{"b1", "b2"}) {
		t.Fatalf("e: %+v %v", res, err)
	}
	if res, err := sendAwait(t, c, "par", "k", "leave2"); err != nil || !slices.Equal(res.States, []string{"b1", "a2"}) {
		t.Fatalf("leave2: %+v %v", res, err)
	}
	if res, err := sendAwait(t, c, "par", "k", "leave"); err != nil || !slices.Equal(res.States, []string{"a1", "a2"}) {
		t.Fatalf("leave from b1: %+v %v", res, err)
	}
	if res, err := sendAwait(t, c, "par", "k", "leave"); err != nil || !slices.Equal(res.States, []string{"out"}) {
		t.Fatalf("leave from a1: %+v %v", res, err)
	}
}

// TestMachine_InternalAndReenter: a transition to a descendant leaves
// only the source's active descendants, a self-transition runs actions
// only, and Reenter leaves and enters the source again.
func TestMachine_InternalAndReenter(t *testing.T) {
	c := memory.New()
	store := workflow.NewMemoryJournal()
	wf, err := workflow.DefineMachine(workflow.MachineSpec[mdata]{
		Name: "int", Initial: "s", Store: store,
		States: []workflow.MachineState[mdata]{
			{Name: "s", Initial: "x", Entry: mtrace("in s"), Exit: mtrace("out s")},
			{Name: "x", Parent: "s", Entry: mtrace("in x"), Exit: mtrace("out x")},
			{Name: "y", Parent: "s", Entry: mtrace("in y"), Exit: mtrace("out y")},
		},
		Transitions: []workflow.MachineTransition[mdata]{
			{From: "s", Event: "to-y", To: "y"},
			{From: "s", Event: "self", To: "s", Actions: mtrace("self")},
			{From: "s", Event: "again", To: "s", Reenter: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	startMachine(t, c, wf)
	for _, ev := range []string{"to-y", "self", "again"} {
		if _, err := sendAwait(t, c, "int", "k", ev); err != nil {
			t.Fatal(ev, err)
		}
	}
	snap, _, err := workflow.MachineStatus[mdata](context.Background(), store, "int", "k")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"in s", "in x", "out x", "in y", "self", "out y", "out s", "in s", "in x"}
	if !slices.Equal(snap.Data.Trace, want) {
		t.Fatalf("trace\n got %v\nwant %v", snap.Data.Trace, want)
	}
}

// TestMachine_RaiseAndLoop: raised events are handled in the same event;
// a raise loop fails the event (Permanent) and leaves the instance as it
// was.
func TestMachine_RaiseAndLoop(t *testing.T) {
	c := memory.New()
	store := workflow.NewMemoryJournal()
	raise := func(ev string) []workflow.MachineAction[mdata] {
		return []workflow.MachineAction[mdata]{func(_ context.Context, m *workflow.MachineCtx[mdata]) error {
			m.Raise(ev, nil)
			return nil
		}}
	}
	wf, err := workflow.DefineMachine(workflow.MachineSpec[mdata]{
		Name: "raise", Initial: "a", Store: store,
		States: []workflow.MachineState[mdata]{{Name: "a"}, {Name: "b", Entry: raise("bounce")}, {Name: "c"}, {Name: "ping", Entry: raise("pong")}, {Name: "pong", Entry: raise("ping")}},
		Transitions: []workflow.MachineTransition[mdata]{
			{From: "a", Event: "go", To: "b"},
			{From: "b", Event: "bounce", To: "c"},
			{From: "c", Event: "loop", To: "ping"},
			{From: "ping", Event: "pong", To: "pong"},
			{From: "pong", Event: "ping", To: "ping"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	startMachine(t, c, wf)
	if res, err := sendAwait(t, c, "raise", "k", "go"); err != nil || !slices.Equal(res.States, []string{"c"}) {
		t.Fatalf("go: %+v %v", res, err)
	}
	if _, err := sendAwait(t, c, "raise", "k", "loop"); err == nil || !strings.Contains(err.Error(), "MaxMachineMicrosteps") {
		t.Fatalf("a raise loop: %v", err)
	}
	if snap, _, _ := workflow.MachineStatus[mdata](context.Background(), store, "raise", "k"); !slices.Equal(snap.States, []string{"c"}) {
		t.Fatalf("a failed event changed the instance: %+v", snap)
	}
}

// flakySave is a journal whose first Save of each record fails, as a
// crash after the actions and before the save would.
type flakySave struct {
	*workflow.MemoryJournal
	fail atomic.Int32
}

func (f *flakySave) Save(ctx context.Context, run workflow.RunID, key string, val []byte) error {
	if f.fail.Add(-1) >= 0 {
		return errors.New("save failed")
	}
	return f.MemoryJournal.Save(ctx, run, key, val)
}

// TestMachine_RetryAfterFailedSave: actions are at least once with the
// same idempotency keys, and the event is applied once.
func TestMachine_RetryAfterFailedSave(t *testing.T) {
	c := memory.New()
	store := &flakySave{MemoryJournal: workflow.NewMemoryJournal()}
	var keys []string
	wf, err := workflow.DefineMachine(workflow.MachineSpec[mdata]{
		Name: "retry", Initial: "a", Store: store,
		Retry:  workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Millisecond}},
		States: []workflow.MachineState[mdata]{{Name: "a"}, {Name: "b"}},
		Transitions: []workflow.MachineTransition[mdata]{{From: "a", Event: "go", To: "b", Actions: []workflow.MachineAction[mdata]{
			func(_ context.Context, m *workflow.MachineCtx[mdata]) error {
				keys = append(keys, m.IdempotencyKey("charge"))
				m.Data.N++
				return nil
			},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	startMachine(t, c, wf)
	startRef, err := workflow.StartMachine(context.Background(), c, "retry", "k", mdata{}, workflow.Job{})
	if err != nil {
		t.Fatal(err)
	}
	awaitCtx, cancelAwait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelAwait()
	if _, err := workflow.AwaitResult(awaitCtx, c, startRef, workflow.AwaitOptions{Poll: 2 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	store.fail.Store(1)
	res, err := sendAwait(t, c, "retry", "k", "go")
	if err != nil || !slices.Equal(res.States, []string{"b"}) {
		t.Fatalf("go: %+v %v", res, err)
	}
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("the action ran %d times with keys %v, want twice with one key", len(keys), keys)
	}
	snap, _, _ := workflow.MachineStatus[mdata](context.Background(), store, "retry", "k")
	if snap.Data.N != 1 || snap.Seq != 2 {
		t.Fatalf("the event was applied more than once: %+v", snap)
	}
}

// saveThenFail saves, then reports a failure once, as a process that died
// after the save and before the step's end was recorded.
type saveThenFail struct {
	*workflow.MemoryJournal
	fail atomic.Int32
}

func (f *saveThenFail) Save(ctx context.Context, run workflow.RunID, key string, val []byte) error {
	if err := f.MemoryJournal.Save(ctx, run, key, val); err != nil {
		return err
	}
	if f.fail.Add(-1) >= 0 {
		return errors.New("lost after the save")
	}
	return nil
}

// TestMachine_RetryAfterSave: a retry of an event whose record was saved
// returns the saved result without running the actions again.
func TestMachine_RetryAfterSave(t *testing.T) {
	c := memory.New()
	store := &saveThenFail{MemoryJournal: workflow.NewMemoryJournal()}
	var runs atomic.Int32
	wf, err := workflow.DefineMachine(workflow.MachineSpec[mdata]{
		Name: "saved", Initial: "a", Store: store,
		Retry:  workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Millisecond}},
		States: []workflow.MachineState[mdata]{{Name: "a"}, {Name: "b"}},
		Transitions: []workflow.MachineTransition[mdata]{
			{From: "a", Event: "go", To: "b", Actions: []workflow.MachineAction[mdata]{func(context.Context, *workflow.MachineCtx[mdata]) error {
				runs.Add(1)
				return nil
			}}},
			{From: "b", Event: "go", To: "a"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	startMachine(t, c, wf)
	startRef, err := workflow.StartMachine(context.Background(), c, "saved", "k", mdata{}, workflow.Job{})
	if err != nil {
		t.Fatal(err)
	}
	awaitCtx, cancelAwait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelAwait()
	if _, err := workflow.AwaitResult(awaitCtx, c, startRef, workflow.AwaitOptions{Poll: 2 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	store.fail.Store(1)
	res, err := sendAwait(t, c, "saved", "k", "go")
	if err != nil || !slices.Equal(res.States, []string{"b"}) || runs.Load() != 1 {
		t.Fatalf("go: %+v %v, action ran %d times", res, err, runs.Load())
	}
}
