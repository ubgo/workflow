package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// MachineHarness builds what the state-machine cases run on: Start
// registers the workflows on a fresh client of the driver and starts it
// (each case calls it once), and Store is the JournalStore instances live
// in (the driver's durable one where it has one). Client is called before
// Start, with the client Start will return, so a machine with timers can
// be defined against it; drivers whose Start builds the client return it
// from Client and reuse it in Start.
//
//ds:def id=workflowtest-machineharness-aghhe7m6 owner=@khanakia stability=stable
type MachineHarness struct {
	Client func(t *testing.T) workflow.Client
	Start  func(t *testing.T, c workflow.Client, wfs ...workflow.Workflow)
	Store  func(t *testing.T, c workflow.Client) workflow.JournalStore
}

// RunMachineFlows proves DefineMachine on a driver (XM-04, WP-24, WP-26,
// WP-27): guarded transitions with entry, exit and transition actions in
// SCXML order, extended data and the log; hierarchical and parallel
// states with done events; shallow and deep history; timers per state,
// stale after the state is left and restarted by a re-entry; events of
// one key handled one at a time and each applied once; and MachineReject.
// The driver needs CapConcurrencyKey with Go key functions, and
// CapDelayedStart and CapIdempotencyKey for the timer case.
//
//ds:def id=workflowtest-runmachineflows-6a9ymgxa owner=@khanakia stability=stable
func RunMachineFlows(t *testing.T, h MachineHarness) {
	t.Run("lifecycle", func(t *testing.T) { machineLifecycle(t, h) })
	t.Run("statechart", func(t *testing.T) { machineStatechart(t, h) })
	t.Run("history", func(t *testing.T) { machineHistory(t, h) })
	t.Run("timers", func(t *testing.T) { machineTimers(t, h) })
	t.Run("serial", func(t *testing.T) { machineSerial(t, h) })
	t.Run("reject", func(t *testing.T) { machineReject(t, h) })
}

type orderData struct {
	Total int      `json:"total"`
	Paid  int      `json:"paid"`
	Trace []string `json:"trace,omitempty"`
}

func traceAction(what string) workflow.MachineAction[orderData] {
	return func(_ context.Context, m *workflow.MachineCtx[orderData]) error {
		m.Data.Trace = append(m.Data.Trace, what)
		return nil
	}
}

func machineResult(t *testing.T, c workflow.Client, ref workflow.RunRef) workflow.MachineResult {
	t.Helper()
	out, err := flowAwait(t, c, ref)
	if err != nil {
		t.Fatal(err)
	}
	var res workflow.MachineResult
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("result %s: %v", out, err)
	}
	return res
}

func send(t *testing.T, c workflow.Client, machine, key, event string, payload any) workflow.MachineResult {
	t.Helper()
	var raw []byte
	if payload != nil {
		var err error
		if raw, err = json.Marshal(payload); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := workflow.SendEvent(context.Background(), c, machine, key, event, raw, workflow.Job{})
	if err != nil {
		t.Fatal(err)
	}
	return machineResult(t, c, ref)
}

func machineLifecycle(t *testing.T, h MachineHarness) {
	c := h.Client(t)
	store := h.Store(t, c)
	pay := func(_ context.Context, m *workflow.MachineCtx[orderData]) error {
		var amount int
		if err := json.Unmarshal(m.Event.Payload, &amount); err != nil {
			return workflow.Permanent(err)
		}
		m.Data.Paid += amount
		m.Data.Trace = append(m.Data.Trace, "pay")
		return nil
	}
	fullyPaid := func(m workflow.MachineCtx[orderData]) bool {
		var amount int
		_ = json.Unmarshal(m.Event.Payload, &amount)
		ok := m.Data.Paid+amount >= m.Data.Total
		m.Data.Total = -1 // a guard's change is dropped (it reads a copy)
		return ok
	}
	wf, err := workflow.DefineMachine(workflow.MachineSpec[orderData]{
		Name:    "sm-order",
		Initial: "new",
		Store:   store,
		States: []workflow.MachineState[orderData]{
			{Name: "new", Entry: []workflow.MachineAction[orderData]{traceAction("enter new")}, Exit: []workflow.MachineAction[orderData]{traceAction("exit new")}},
			{Name: "paid", Entry: []workflow.MachineAction[orderData]{traceAction("enter paid")}},
			{Name: "shipped", Final: true, Entry: []workflow.MachineAction[orderData]{traceAction("enter shipped")}},
		},
		Transitions: []workflow.MachineTransition[orderData]{
			{From: "new", Event: "pay", To: "paid", Guard: fullyPaid, Actions: []workflow.MachineAction[orderData]{pay}},
			{From: "new", Event: "pay", Actions: []workflow.MachineAction[orderData]{pay}}, // a part payment stays in new
			{From: "paid", Event: "ship", To: "shipped"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Start(t, c, wf)
	ctx := context.Background()
	ref, err := workflow.StartMachine(ctx, c, "sm-order", "o1", orderData{Total: 100}, workflow.Job{})
	if err != nil {
		t.Fatal(err)
	}
	if res := machineResult(t, c, ref); !res.Started || !slices.Equal(res.States, []string{"new"}) {
		t.Fatalf("start: %+v", res)
	}
	again, err := workflow.StartMachine(ctx, c, "sm-order", "o1", orderData{Total: 999}, workflow.Job{})
	if err != nil {
		t.Fatal(err)
	}
	if res := machineResult(t, c, again); res.Started || res.Handled {
		t.Fatalf("a second start must change nothing: %+v", res)
	}
	if res := send(t, c, "sm-order", "o1", "ship", nil); res.Handled || !slices.Equal(res.States, []string{"new"}) {
		t.Fatalf("ship before pay must be unhandled: %+v", res)
	}
	if res := send(t, c, "sm-order", "o1", "pay", 40); !res.Handled || !slices.Equal(res.States, []string{"new"}) {
		t.Fatalf("part payment: %+v", res)
	}
	if res := send(t, c, "sm-order", "o1", "pay", 60); !res.Handled || !slices.Equal(res.States, []string{"paid"}) {
		t.Fatalf("full payment: %+v", res)
	}
	if res := send(t, c, "sm-order", "o1", "ship", nil); !res.Done || !slices.Equal(res.States, []string{"shipped"}) {
		t.Fatalf("ship: %+v", res)
	}
	if res := send(t, c, "sm-order", "o1", "pay", 1); res.Handled || !res.Done {
		t.Fatalf("an event after the final state: %+v", res)
	}
	snap, ok, err := workflow.MachineStatus[orderData](ctx, store, "sm-order", "o1")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	// SCXML order: exit actions, then transition actions, then entry
	// actions; a targetless transition runs its actions only.
	want := []string{"enter new", "pay", "exit new", "pay", "enter paid", "enter shipped"}
	if !slices.Equal(snap.Data.Trace, want) || snap.Data.Paid != 100 || snap.Data.Total != 100 || !snap.Done || !snap.In("shipped") {
		t.Fatalf("snapshot %+v, want trace %v", snap, want)
	}
	var events []string
	for _, s := range snap.Log {
		events = append(events, s.Event)
	}
	if !slices.Equal(events, []string{workflow.MachineStartEvent, "pay", "pay", "ship"}) || snap.Log[0].At.IsZero() {
		t.Fatalf("log %+v", snap.Log)
	}
	// A key never started reads not found; an event to a new key starts it
	// with zero data first.
	if _, ok, err := workflow.MachineStatus[orderData](ctx, store, "sm-order", "never"); ok || err != nil {
		t.Fatalf("a key never started: %v %v", ok, err)
	}
	if res := send(t, c, "sm-order", "o2", "pay", 0); !res.Started || !slices.Equal(res.States, []string{"paid"}) {
		t.Fatalf("auto start then a zero payment of a zero total: %+v", res)
	}
	if err := workflow.ForgetMachine(ctx, store, "sm-order", "o1"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := workflow.MachineStatus[orderData](ctx, store, "sm-order", "o1"); ok || err != nil {
		t.Fatalf("a forgotten instance: %v %v", ok, err)
	}
	if res := send(t, c, "sm-order", "o1", "ship", nil); !res.Started || res.Handled || !slices.Equal(res.States, []string{"new"}) {
		t.Fatalf("an event after ForgetMachine starts a new instance: %+v", res)
	}
	if _, err := workflow.SendEvent(ctx, c, "sm-order", "", "pay", nil, workflow.Job{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("empty key: %v", err)
	}
}

type flowData struct {
	Trace []string `json:"trace,omitempty"`
}

func flowTrace(what string) workflow.MachineAction[flowData] {
	return func(_ context.Context, m *workflow.MachineCtx[flowData]) error {
		m.Data.Trace = append(m.Data.Trace, what)
		return nil
	}
}

// machineStatechart: a parallel "fulfil" state with a payment and a
// shipping region, each a compound state ending in a final state; when
// both are final, done.state.fulfil moves the order to "closed".
func machineStatechart(t *testing.T, h MachineHarness) {
	c := h.Client(t)
	store := h.Store(t, c)
	in := func(s string) []workflow.MachineAction[flowData] {
		return []workflow.MachineAction[flowData]{flowTrace("in " + s)}
	}
	out := func(s string) []workflow.MachineAction[flowData] {
		return []workflow.MachineAction[flowData]{flowTrace("out " + s)}
	}
	wf, err := workflow.DefineMachine(workflow.MachineSpec[flowData]{
		Name:    "sm-fulfil",
		Initial: "fulfil",
		Store:   store,
		States: []workflow.MachineState[flowData]{
			{Name: "fulfil", Parallel: true, Entry: in("fulfil"), Exit: out("fulfil")},
			{Name: "payment", Parent: "fulfil", Initial: "unpaid", Entry: in("payment"), Exit: out("payment")},
			{Name: "unpaid", Parent: "payment", Entry: in("unpaid"), Exit: out("unpaid")},
			{Name: "paid", Parent: "payment", Final: true, Entry: in("paid"), Exit: out("paid")},
			{Name: "shipping", Parent: "fulfil", Initial: "packing", Entry: in("shipping"), Exit: out("shipping")},
			{Name: "packing", Parent: "shipping", Entry: in("packing"), Exit: out("packing")},
			{Name: "sent", Parent: "shipping", Final: true, Entry: in("sent"), Exit: out("sent")},
			{Name: "closed", Final: true, Entry: in("closed")},
			{Name: "cancelled", Final: true, Entry: in("cancelled")},
		},
		Transitions: []workflow.MachineTransition[flowData]{
			{From: "unpaid", Event: "paid", To: "paid"},
			{From: "packing", Event: "sent", To: "sent"},
			{From: "payment", Event: workflow.DoneEvent("payment"), Actions: []workflow.MachineAction[flowData]{flowTrace("payment done")}},
			{From: "fulfil", Event: workflow.DoneEvent("fulfil"), To: "closed"},
			{From: "fulfil", Event: "cancel", To: "cancelled"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Start(t, c, wf)
	ctx := context.Background()
	if res := send(t, c, "sm-fulfil", "f1", "paid", nil); !res.Handled || !slices.Equal(res.States, []string{"paid", "packing"}) {
		t.Fatalf("paid: %+v", res)
	}
	if res := send(t, c, "sm-fulfil", "f1", "sent", nil); !res.Done || !slices.Equal(res.States, []string{"closed"}) {
		t.Fatalf("sent: %+v", res)
	}
	snap, _, err := workflow.MachineStatus[flowData](ctx, store, "sm-fulfil", "f1")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"in fulfil", "in payment", "in unpaid", "in shipping", "in packing", // ancestors first, regions in document order
		"out unpaid", "in paid", "payment done",
		"out packing", "in sent",
		"out sent", "out shipping", "out paid", "out payment", "out fulfil", "in closed", // descendants first
	}
	if !slices.Equal(snap.Data.Trace, want) {
		t.Fatalf("trace\n got %v\nwant %v", snap.Data.Trace, want)
	}
	// A transition of the parallel state leaves both regions.
	if res := send(t, c, "sm-fulfil", "f2", "cancel", nil); !res.Done || !slices.Equal(res.States, []string{"cancelled"}) {
		t.Fatalf("cancel: %+v", res)
	}
}

// machineHistory: an "editing" compound state with nested "review"
// (shallow history returns to review's initial child; deep history to
// the exact leaf).
func machineHistory(t *testing.T, h MachineHarness) {
	c := h.Client(t)
	store := h.Store(t, c)
	wf, err := workflow.DefineMachine(workflow.MachineSpec[flowData]{
		Name:    "sm-history",
		Initial: "editing",
		Store:   store,
		States: []workflow.MachineState[flowData]{
			{Name: "editing", Initial: "draft"},
			{Name: "draft", Parent: "editing"},
			{Name: "review", Parent: "editing", Initial: "legal"},
			{Name: "legal", Parent: "review"},
			{Name: "finance", Parent: "review"},
			{Name: "paused"},
		},
		Transitions: []workflow.MachineTransition[flowData]{
			{From: "draft", Event: "submit", To: "review"},
			{From: "legal", Event: "next", To: "finance"},
			{From: "editing", Event: "pause", To: "paused"},
			{From: "paused", Event: "resume", To: "editing", History: workflow.HistoryShallow},
			{From: "paused", Event: "resume-deep", To: "editing", History: workflow.HistoryDeep},
			{From: "paused", Event: "restart", To: "editing"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Start(t, c, wf)
	steps := []struct {
		event string
		want  []string
	}{
		{"resume", []string{"draft"}}, // unhandled in editing: stays
		{"submit", []string{"legal"}},
		{"next", []string{"finance"}},
		{"pause", []string{"paused"}},
		{"resume", []string{"legal"}}, // shallow: review, entered through its initial child
		{"next", []string{"finance"}},
		{"pause", []string{"paused"}},
		{"resume-deep", []string{"finance"}}, // deep: the exact leaf
		{"pause", []string{"paused"}},
		{"restart", []string{"draft"}}, // no history: the initial child
	}
	for i, s := range steps {
		if res := send(t, c, "sm-history", "h1", s.event, nil); !slices.Equal(res.States, s.want) {
			t.Fatalf("step %d %s: %+v, want %v", i, s.event, res, s.want)
		}
	}
}

// machineTimers: "waiting" times out to "expired" after the delay;
// leaving it first makes the timer stale; a Reenter self-transition
// restarts it.
func machineTimers(t *testing.T, h MachineHarness) {
	c := h.Client(t)
	needCaps(t, c, workflow.CapConcurrencyKey, workflow.CapDelayedStart, workflow.CapIdempotencyKey)
	store := h.Store(t, c)
	const delay = 400 * time.Millisecond
	define := func(name string, delay time.Duration) workflow.Workflow {
		t.Helper()
		wf, err := workflow.DefineMachine(workflow.MachineSpec[flowData]{
			Name:    name,
			Initial: "waiting",
			Store:   store,
			Client:  c,
			States: []workflow.MachineState[flowData]{
				{Name: "waiting", After: []workflow.StateTimer{{Delay: delay, Event: "timeout"}}},
				{Name: "answered"},
				{Name: "expired"},
			},
			Transitions: []workflow.MachineTransition[flowData]{
				{From: "waiting", Event: "timeout", To: "expired"},
				{From: "waiting", Event: "answer", To: "answered"},
				{From: "waiting", Event: "ping", To: "waiting", Reenter: true},
				{From: "answered", Event: "reopen", To: "waiting"},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return wf
	}
	// The Reenter case needs its ping applied before the first entry's
	// timer fires, which no delay guarantees on a loaded machine. Each
	// rung of this ladder is a machine with twice the delay of the one
	// before, tried when the ping of the rung before came too late.
	ladder := []time.Duration{delay, 2 * delay, 4 * delay, 8 * delay}
	wfs := []workflow.Workflow{define("sm-timer", delay)}
	for _, d := range ladder {
		wfs = append(wfs, define(fmt.Sprintf("sm-timer-%d", d.Milliseconds()), d))
	}
	h.Start(t, c, wfs...)
	ctx := context.Background()
	await := func(machine, key, state string, within time.Duration) workflow.MachineSnapshot[flowData] {
		t.Helper()
		deadline := time.Now().Add(within)
		for {
			snap, ok, err := workflow.MachineStatus[flowData](ctx, store, machine, key)
			if err != nil {
				t.Fatal(err)
			}
			if ok && snap.In(state) {
				return snap
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s key %s: not in %s after %s: %+v", machine, key, state, within, snap)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	// Times out.
	start := time.Now()
	if _, err := workflow.StartMachine(ctx, c, "sm-timer", "t1", flowData{}, workflow.Job{}); err != nil {
		t.Fatal(err)
	}
	await("sm-timer", "t1", "expired", 20*time.Second)
	if waited := time.Since(start); waited < delay {
		t.Fatalf("expired after %s, before the %s timer", waited, delay)
	}
	// Answered first: the timer fires into a state that was left, and
	// the state entered again since has a timer of its own.
	send(t, c, "sm-timer", "t2", "answer", nil)
	reopened := time.Now() // before the send: the entry, and its timer, come after it
	if res := send(t, c, "sm-timer", "t2", "reopen", nil); !slices.Equal(res.States, []string{"waiting"}) {
		t.Fatalf("reopen: %+v", res)
	}
	await("sm-timer", "t2", "expired", 20*time.Second)
	if waited := time.Since(reopened); waited < delay {
		t.Fatalf("the first entry's timer moved the second entry after %s", waited)
	}
	// A Reenter self-transition restarts the timer: the ping goes in
	// halfway through the first entry's timer, and the instance expires
	// no sooner than a whole delay after it. Had the first timer stayed
	// live, it would expire the instance half a delay after the ping
	// (plus the driver's firing latency, which must stay under half a
	// delay for the check to tell the two apart; it is a poll interval).
	// The instance's own log times the start and the expiry, so neither
	// this test's polling nor the scheduler's delay in starting the ping
	// skews the measure.
	for i, d := range ladder {
		machine, key := fmt.Sprintf("sm-timer-%d", d.Milliseconds()), fmt.Sprintf("t3-%d", i)
		ref, err := workflow.StartMachine(ctx, c, machine, key, flowData{}, workflow.Job{})
		if err != nil {
			t.Fatal(err)
		}
		if res := machineResult(t, c, ref); !res.Started {
			t.Fatalf("%s start: %+v", machine, res)
		}
		snap, _, err := workflow.MachineStatus[flowData](ctx, store, machine, key)
		if err != nil || len(snap.Log) != 1 {
			t.Fatalf("%s after the start: %+v %v", machine, snap, err)
		}
		time.Sleep(time.Until(snap.Log[0].At.Add(d / 2)))
		pinged := time.Now() // before the send: the re-entry, and its timer, come after it
		res := send(t, c, machine, key, "ping", nil)
		if !res.Handled && slices.Equal(res.States, []string{"expired"}) {
			// The first timer fired before the ping reached the
			// instance: a loaded worker, not a stale timer. Proves
			// nothing; the next rung leaves the ping more time.
			t.Logf("%s: the ping arrived %s after the start, after the %s timer; next rung", machine, time.Since(snap.Log[0].At), d)
			continue
		}
		if !res.Handled || !slices.Equal(res.States, []string{"waiting"}) {
			t.Fatalf("%s ping: %+v, want a re-entry of waiting", machine, res)
		}
		snap = await(machine, key, "expired", 20*time.Second+d)
		var events []string
		for _, s := range snap.Log {
			events = append(events, s.Event)
		}
		// Only the second entry's timer moved the instance: the first
		// one fired into a state entered again since, and was stale.
		if want := []string{workflow.MachineStartEvent, "ping", "timeout"}; !slices.Equal(events, want) {
			t.Fatalf("%s log %v, want %v", machine, events, want)
		}
		if waited := snap.Log[2].At.Sub(pinged); waited < d {
			t.Fatalf("%s expired %s after the re-entry, before its own %s timer", machine, waited, d)
		}
		return
	}
	t.Fatalf("no ping reached its instance before the first entry's timer, up to a %s delay", ladder[len(ladder)-1])
}

// machineSerial: concurrent events of one key are handled one at a time,
// each applied once.
func machineSerial(t *testing.T, h MachineHarness) {
	type counter struct {
		N int `json:"n"`
	}
	c := h.Client(t)
	store := h.Store(t, c)
	var mu sync.Mutex
	running, overlap := 0, false
	wf, err := workflow.DefineMachine(workflow.MachineSpec[counter]{
		Name:    "sm-counter",
		Initial: "on",
		Store:   store,
		States:  []workflow.MachineState[counter]{{Name: "on"}},
		Transitions: []workflow.MachineTransition[counter]{{From: "on", Event: "inc", Actions: []workflow.MachineAction[counter]{
			func(_ context.Context, m *workflow.MachineCtx[counter]) error {
				mu.Lock()
				running++
				overlap = overlap || running > 1
				mu.Unlock()
				time.Sleep(time.Millisecond)
				m.Data.N++
				mu.Lock()
				running--
				mu.Unlock()
				return nil
			},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Start(t, c, wf)
	ctx := context.Background()
	var refs []workflow.RunRef
	for range 20 {
		ref, err := workflow.SendEvent(ctx, c, "sm-counter", "k", "inc", nil, workflow.Job{})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	for _, r := range refs {
		if _, err := flowAwait(t, c, r); err != nil {
			t.Fatal(err)
		}
	}
	snap, _, err := workflow.MachineStatus[counter](ctx, store, "sm-counter", "k")
	if err != nil || snap.Data.N != 20 {
		t.Fatalf("counter %+v %v, want 20", snap.Data, err)
	}
	if overlap {
		t.Error("two events of one key were handled at once")
	}
}

func machineReject(t *testing.T, h MachineHarness) {
	c := h.Client(t)
	store := h.Store(t, c)
	wf, err := workflow.DefineMachine(workflow.MachineSpec[flowData]{
		Name:        "sm-strict",
		Initial:     "a",
		Store:       store,
		Unhandled:   workflow.MachineReject,
		States:      []workflow.MachineState[flowData]{{Name: "a", Entry: []workflow.MachineAction[flowData]{flowTrace("in a")}}, {Name: "b"}},
		Transitions: []workflow.MachineTransition[flowData]{{From: "a", Event: "go", To: "b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Start(t, c, wf)
	send(t, c, "sm-strict", "s", "go", nil)
	ref, err := workflow.SendEvent(context.Background(), c, "sm-strict", "s", "go", nil, workflow.Job{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flowAwait(t, c, ref); !workflow.IsUnhandledEvent(err) {
		t.Fatalf("an unhandled event under MachineReject: %v", err)
	}
	snap, _, _ := workflow.MachineStatus[flowData](context.Background(), store, "sm-strict", "s")
	if !slices.Equal(snap.States, []string{"b"}) || snap.Seq != 2 {
		t.Fatalf("a rejected event changed the instance: %+v", snap)
	}
	// A refused first event keeps the start it caused: the initial entry
	// actions ran once and do not run again on the next event.
	for i := range 2 {
		ref, err := workflow.SendEvent(context.Background(), c, "sm-strict", "fresh", "nope", nil, workflow.Job{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := flowAwait(t, c, ref); !workflow.IsUnhandledEvent(err) {
			t.Fatalf("refused first event %d: %v", i, err)
		}
	}
	fresh, ok, _ := workflow.MachineStatus[flowData](context.Background(), store, "sm-strict", "fresh")
	if !ok || !slices.Equal(fresh.States, []string{"a"}) || !slices.Equal(fresh.Data.Trace, []string{"in a"}) {
		t.Fatalf("an instance started by a refused event: %v %+v", ok, fresh)
	}
}
