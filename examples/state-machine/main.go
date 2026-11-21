// Command state-machine shows a durable state machine: an order whose
// lifecycle is a statechart, one instance per order key, each event a run
// and each instance's state one record in the journal store. It shows a
// guard, entry, exit and transition actions, compound states, a parallel
// state with a payment and a shipping region joined by a done event, deep
// history (an order put on hold resumes where it was), a timer per state,
// a rejected event and the instance's log.
//
// It runs on the in-memory driver with a fake clock, so the 30-minute
// payment timer fires when the program moves the clock, not after half an
// hour. It is the program behind docs/workflows/state-machines.md: the
// guide copies its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant: a Monday at 09:00 UTC.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "state-machine:", err)
		os.Exit(1)
	}
}

// snippet:data
// Order is each instance's extended state, saved with its configuration.
type Order struct {
	Total int `json:"total"`
	Paid  int `json:"paid"`
}

// amount reads a "pay" event's payload.
func amount(ev workflow.MachineEvent) int {
	var n int
	_ = json.Unmarshal(ev.Payload, &n)
	return n
}

// coversTotal is the guard of "pay": a payment must cover what is owed.
// A guard only reads.
func coversTotal(m workflow.MachineCtx[Order]) bool {
	return m.Data.Paid+amount(m.Event) >= m.Data.Total
}

// recordPayment is the action of "pay": it changes the instance's data.
func recordPayment(_ context.Context, m *workflow.MachineCtx[Order]) error {
	m.Data.Paid += amount(m.Event)
	fmt.Printf("  action: record payment of %d\n", amount(m.Event))
	return nil
}

// snippet:end

// say returns an action that prints what happened, so the output shows
// the order actions run in.
func say(what string) []workflow.MachineAction[Order] {
	return []workflow.MachineAction[Order]{
		func(context.Context, *workflow.MachineCtx[Order]) error {
			fmt.Println("  " + what)
			return nil
		},
	}
}

// snippet:states
func orderStates() []workflow.MachineState[Order] {
	return []workflow.MachineState[Order]{
		{Name: "cart", Exit: say("exit cart")},

		// fulfilment is parallel: its regions payment and shipping are
		// both active while it is.
		{Name: "fulfilment", Parallel: true, Entry: say("enter fulfilment"), Exit: say("exit fulfilment")},

		{Name: "payment", Parent: "fulfilment", Initial: "awaiting"},
		{Name: "awaiting", Parent: "payment", Exit: say("exit awaiting"),
			After: []workflow.StateTimer{{Delay: 30 * time.Minute, Event: "payment-timeout"}}},
		{Name: "paid", Parent: "payment", Final: true, Entry: say("enter paid")},

		{Name: "shipping", Parent: "fulfilment", Initial: "packing"},
		{Name: "packing", Parent: "shipping"},
		{Name: "in-transit", Parent: "shipping"},
		{Name: "delivered", Parent: "shipping", Final: true},

		{Name: "on-hold", Entry: say("enter on-hold"), Exit: say("exit on-hold")},
		{Name: "completed", Final: true, Entry: say("enter completed")},
		{Name: "cancelled", Final: true, Entry: say("enter cancelled")},
	}
}

// snippet:end

// snippet:transitions
func orderTransitions(c workflow.Client) []workflow.MachineTransition[Order] {
	return []workflow.MachineTransition[Order]{
		{From: "cart", Event: "checkout", To: "fulfilment"},

		// Payment region.
		{From: "awaiting", Event: "pay", To: "paid", Guard: coversTotal,
			Actions: []workflow.MachineAction[Order]{recordPayment}},
		{From: "awaiting", Event: "payment-timeout", To: "cancelled"},

		// Shipping region.
		{From: "packing", Event: "ship", To: "in-transit",
			Actions: []workflow.MachineAction[Order]{shippedEmail(c)}},
		{From: "in-transit", Event: "deliver", To: "delivered"},

		// Both regions final: the order is complete.
		{From: "fulfilment", Event: workflow.DoneEvent("fulfilment"), To: "completed"},

		// A transition of fulfilment applies in every state below it.
		{From: "fulfilment", Event: "hold", To: "on-hold"},
		{From: "on-hold", Event: "resume", To: "fulfilment", History: workflow.HistoryDeep},
		{From: "fulfilment", Event: "cancel", To: "cancelled"},
	}
}

// snippet:end

// snippet:idempotency
// shippedEmail enqueues the "shipped" email. Actions are at least once,
// so the job's IdempotencyKey is one that stays the same when the event
// is handled again after a crash: the email is sent once.
func shippedEmail(c workflow.Client) workflow.MachineAction[Order] {
	return func(ctx context.Context, m *workflow.MachineCtx[Order]) error {
		text, err := json.Marshal("order " + m.Key + " is on its way")
		if err != nil {
			return err
		}
		_, err = c.Enqueue(ctx, workflow.Job{
			Workflow:       "email",
			Payload:        text,
			IdempotencyKey: m.IdempotencyKey("shipped-email"),
		})
		return err
	}
}

// snippet:end

// snippet:define
func orderMachine(store workflow.JournalStore, c workflow.Client, clk clock.Clock) (workflow.Workflow, error) {
	return workflow.DefineMachine(workflow.MachineSpec[Order]{
		Name:        "order",
		Initial:     "cart",
		States:      orderStates(),
		Transitions: orderTransitions(c),
		Store:       store,                  // where each instance's record lives
		Client:      c,                      // sends the timers: delayed runs of the machine
		Clock:       clk,                    // times the timers and the log
		Unhandled:   workflow.MachineReject, // an event no state takes fails its run
	})
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))
	defer c.Stop()
	store := workflow.NewMemoryJournal()

	emails := make(chan string, 4)
	err := c.Register(workflow.Workflow{
		Name: "email",
		Steps: []workflow.Step{{
			Name: "send",
			Run: workflow.Define(func(_ context.Context, text string) (string, error) {
				emails <- text
				return "sent", nil
			}),
		}},
	})
	if err != nil {
		return err
	}
	order, err := orderMachine(store, c, clk)
	if err != nil {
		return err
	}
	if err := c.Register(order); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	fmt.Println("order o-1:")
	if err := startOrder(ctx, c, "o-1", 120); err != nil {
		return err
	}
	steps := []struct {
		event   string
		payload string
	}{
		{"checkout", ""},
		{"pay", "50"},  // less than the total: the guard refuses, nothing takes it
		{"pay", "120"}, // covers the total
		{"ship", ""},
		{"hold", ""},
		{"resume", ""}, // deep history: back to paid and in-transit
		{"deliver", ""},
	}
	for _, s := range steps {
		if err := send(ctx, c, "o-1", s.event, s.payload); err != nil {
			return err
		}
		if s.event == "ship" {
			fmt.Println("  email:", <-emails)
		}
	}
	if err := printStatus(ctx, store, "o-1"); err != nil {
		return err
	}

	fmt.Println("order o-2:")
	if err := startOrder(ctx, c, "o-2", 80); err != nil {
		return err
	}
	if err := send(ctx, c, "o-2", "checkout", ""); err != nil {
		return err
	}
	fmt.Println("> 30 minutes pass without a payment")
	clk.Advance(30 * time.Minute) // the awaiting state's timer fires
	if err := waitDone(ctx, store, "o-2"); err != nil {
		return err
	}
	return printStatus(ctx, store, "o-2")
}

// startOrder starts order key with its total and prints its states.
func startOrder(ctx context.Context, c workflow.Client, key string, total int) error {
	fmt.Println("> start, total", total)
	// snippet:start
	ref, err := workflow.StartMachine(ctx, c, "order", key, Order{Total: total}, workflow.Job{})
	if err != nil {
		return err
	}
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	var res workflow.MachineResult
	if err := json.Unmarshal(out, &res); err != nil {
		return err
	}
	fmt.Println("  states:", res.States)
	// snippet:end
	return nil
}

// send sends one event to order key, waits for its run and prints the
// states it left the order in, or why it was rejected.
func send(ctx context.Context, c workflow.Client, key, event, payload string) error {
	fmt.Println(">", strings.TrimSpace(event+" "+payload))
	var raw []byte
	if payload != "" {
		raw = []byte(payload)
	}
	// snippet:event
	ref, err := workflow.SendEvent(ctx, c, "order", key, event, raw, workflow.Job{})
	if err != nil {
		return err
	}
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	// snippet:end
	// snippet:reject
	if workflow.IsUnhandledEvent(err) {
		fmt.Println("  rejected: no transition takes it in the current state")
		return nil
	}
	// snippet:end
	if err != nil {
		return err
	}
	var res workflow.MachineResult
	if err := json.Unmarshal(out, &res); err != nil {
		return err
	}
	fmt.Println("  states:", res.States)
	return nil
}

// waitDone polls until order key reaches a final state: the timer's run
// is not one the program holds a RunRef for.
func waitDone(ctx context.Context, store workflow.JournalStore, key string) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		snap, _, err := workflow.MachineStatus[Order](ctx, store, "order", key)
		if err != nil {
			return err
		}
		if snap.Done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("order %s: not done after the timer: %v", key, snap.States)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// printStatus prints an instance as MachineStatus reads it.
func printStatus(ctx context.Context, store workflow.JournalStore, key string) error {
	// snippet:status
	snap, ok, err := workflow.MachineStatus[Order](ctx, store, "order", key)
	if err != nil || !ok {
		return fmt.Errorf("order %s: found %t: %w", key, ok, err)
	}
	fmt.Printf("status of %s: states %v, done %t, paid %d of %d\n",
		key, snap.States, snap.Done, snap.Data.Paid, snap.Data.Total)
	for _, step := range snap.Log {
		event := step.Event
		if event == workflow.MachineStartEvent {
			event = "(start)"
		}
		fmt.Printf("  %d %s %-15s %v -> %v\n",
			step.Seq, step.At.Format("15:04"), event, step.From, step.To)
	}
	// snippet:end
	return nil
}
