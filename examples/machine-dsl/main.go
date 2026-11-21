// Command machine-dsl runs a durable state machine whose definition is
// data: a JSON document lists the states of a support ticket (a compound
// review state with a nested escalation, a held state, a final closed
// state), its transitions with a guard and actions named by string, and a
// 48-hour timer. The program registers the Go guard and actions in a
// dsl.MachineRegistry, loads the document with dsl.LoadMachine and drives
// a ticket through it. It then edits the parsed definition in Go to build
// the same machine with no history and with shallow history
// (dsl.BuildMachine), so one hold and resume shows all three history
// modes; lets the timer expire a second ticket on a fake clock; shows a
// definition naming an unregistered guard refused with its JSON path; and
// round-trips the definition through dsl.MarshalMachine.
//
// It runs on the in-memory driver, so nothing needs to be installed. It is
// the program behind the "State machines as data" section of
// docs/workflows/definitions-as-data.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/dsl"
	"github.com/ubgo/workflow/memory"
)

// runTimeout bounds the whole program, generous for a slow CI host.
const runTimeout = 10 * time.Second

// expireAfter is how long a ticket may stay open. It must match the
// "delay" of the open state's timer in ticketJSON: the program moves the
// fake clock by exactly this much to make the timer fire.
const expireAfter = 48 * time.Hour

// machineName is the machine's name in ticketJSON; events and status
// reads address instances under it.
const machineName = "ticket"

// start is the fake clock's first instant: a Monday at 09:00 UTC.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// pollEvery is how often the program looks at a run or an instance again:
// short, so the program finishes quickly on the in-memory driver.
const pollEvery = 2 * time.Millisecond

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: pollEvery}

// snippet:definition
// ticketJSON is the machine as an app would keep it in a file or a
// database row. Guards and actions are names the program registers; the
// document holds no code.
const ticketJSON = `{
  "name": "ticket",
  "initial": "open",
  "unhandled": "reject",
  "keep_log": 20,
  "states": [
    {"name": "open", "after": [{"delay": "48h", "event": "expire"}]},
    {"name": "review", "initial": "first-line"},
    {"name": "first-line", "parent": "review"},
    {"name": "escalated", "parent": "review", "initial": "triage", "entry": ["note"]},
    {"name": "triage", "parent": "escalated"},
    {"name": "investigating", "parent": "escalated"},
    {"name": "held"},
    {"name": "closed", "final": true, "entry": ["note"]}
  ],
  "transitions": [
    {"from": "open", "event": "assign", "to": "review"},
    {"from": "open", "event": "expire", "to": "closed"},
    {"from": "first-line", "event": "escalate", "to": "escalated"},
    {"from": "triage", "event": "investigate", "to": "investigating"},
    {"from": "review", "event": "approve", "actions": ["approve"]},
    {"from": "review", "event": "close", "to": "closed", "guard": "approved-twice"},
    {"from": "review", "event": "hold", "to": "held"},
    {"from": "held", "event": "resume", "to": "review", "history": "deep"}
  ]
}`

// snippet:end

// snippet:data
// Ticket is each instance's extended state, saved as JSON with its
// configuration. The registry's guards and actions are typed on it.
type Ticket struct {
	Approvals int      `json:"approvals"`
	Notes     []string `json:"notes,omitempty"`
}

// snippet:end

// requiredApprovals is how many approvals the "approved-twice" guard
// wants before a ticket may close.
const requiredApprovals = 2

// ticketRegistry registers every guard and action ticketJSON may name.
func ticketRegistry() (*dsl.MachineRegistry[Ticket], error) {
	// snippet:registry
	reg := dsl.NewMachineRegistry[Ticket]()
	// An action may change the data; it runs at least once per event.
	if err := reg.Action("note", func(_ context.Context, m *workflow.MachineCtx[Ticket]) error {
		m.Data.Notes = append(m.Data.Notes, m.State) // the state being entered
		return nil
	}); err != nil {
		return nil, err
	}
	if err := reg.Action("approve", func(_ context.Context, m *workflow.MachineCtx[Ticket]) error {
		m.Data.Approvals++
		return nil
	}); err != nil {
		return nil, err
	}
	// A guard only reads: it may run more than once per event.
	if err := reg.Guard("approved-twice", func(m workflow.MachineCtx[Ticket]) bool {
		return m.Data.Approvals >= requiredApprovals
	}); err != nil {
		return nil, err
	}
	// snippet:end
	return reg, nil
}

// historyName is a value of dsl.TransitionDef.History, which the dsl
// package keeps as a plain string: one of the three constants below.
type historyName string

const (
	historyNone    historyName = ""        // resume enters the target's initial state
	historyShallow historyName = "shallow" // resume enters the child last active
	historyDeep    historyName = "deep"    // resume enters the whole configuration last active
)

// historyVariant is a copy of the ticket machine under another name whose
// "resume" transition uses another history mode.
type historyVariant struct {
	name    string      // the machine's name, unique on the client
	history historyName // the TransitionDef.History value
}

// variants are the machines the history comparison drives: the two built
// by editing the parsed definition, and the loaded one with deep history.
var variants = []historyVariant{
	{name: "ticket-none", history: historyNone},
	{name: "ticket-shallow", history: historyShallow},
	{name: machineName, history: historyDeep},
}

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "machine-dsl:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, writing to w, so the test drives
// the same code and checks every line.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))
	defer c.Stop()
	store := workflow.NewMemoryJournal()

	reg, err := ticketRegistry()
	if err != nil {
		return err
	}

	// snippet:load
	env := dsl.MachineEnv{
		Store:  store, // where each instance's record lives
		Client: c,     // sends the timers: delayed runs of the machine
		Clock:  clk,   // times the timers and the log
	}
	wf, err := dsl.LoadMachine([]byte(ticketJSON), reg, env) // ParseMachine, then BuildMachine
	if err != nil {
		return err // wraps workflow.ErrInvalid; a *dsl.FieldError names the place
	}
	if err := c.Register(wf); err != nil { // like a machine written in Go
		return err
	}
	// snippet:end

	if err := registerHistoryVariants(c, reg, env); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	fmt.Fprintln(w, "ticket T-1:")
	for _, event := range []string{"assign", "escalate", "investigate", "close", "hold", "resume", "approve", "approve", "close"} {
		if err := send(ctx, w, c, machineName, "T-1", event); err != nil {
			return err
		}
	}
	if err := printStatus(ctx, w, store, "T-1"); err != nil {
		return err
	}

	fmt.Fprintln(w, "history modes, after assign, escalate, investigate, hold:")
	for _, v := range variants {
		for _, event := range []string{"assign", "escalate", "investigate", "hold"} {
			if _, err := sendQuiet(ctx, c, v.name, "H-1", event); err != nil {
				return err
			}
		}
		states, err := sendQuiet(ctx, c, v.name, "H-1", "resume")
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "  %-7s resume enters %v\n", label(v.history), states)
	}

	fmt.Fprintln(w, "ticket T-2:")
	ref, err := workflow.StartMachine(ctx, c, machineName, "T-2", Ticket{}, workflow.Job{})
	if err != nil {
		return err
	}
	if _, err := workflow.AwaitResult(ctx, c, ref, await); err != nil {
		return err
	}
	fmt.Fprintln(w, "> 48 hours pass while it is open")
	clk.Advance(expireAfter) // the open state's timer fires
	if err := waitDone(ctx, store, "T-2"); err != nil {
		return err
	}
	if err := printStatus(ctx, w, store, "T-2"); err != nil {
		return err
	}

	// snippet:refusal
	bad := strings.Replace(ticketJSON, `"guard": "approved-twice"`, `"guard": "approved-once"`, 1)
	_, err = dsl.LoadMachine([]byte(bad), reg, env)
	var fe *dsl.FieldError
	if errors.As(err, &fe) {
		fmt.Fprintf(w, "refused at %s: %s\n", fe.Path, fe.Reason)
	}
	// snippet:end
	if fe == nil {
		return fmt.Errorf("a definition naming an unregistered guard was not refused with a FieldError: %v", err)
	}

	// snippet:round-trip
	def, err := dsl.ParseMachine([]byte(ticketJSON))
	if err != nil {
		return err
	}
	stored, err := dsl.MarshalMachine(def) // indented JSON, ready to store
	if err != nil {
		return err
	}
	back, err := dsl.ParseMachine(stored)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "round trip equal:", reflect.DeepEqual(def, back))
	// snippet:end

	// snippet:forget
	if err := workflow.ForgetMachine(ctx, store, machineName, "T-1"); err != nil {
		return err
	}
	_, found, err := workflow.MachineStatus[Ticket](ctx, store, machineName, "T-1")
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "T-1 after ForgetMachine, found:", found)
	// snippet:end
	return nil
}

// registerHistoryVariants builds the ticket machine again under the
// names in variants whose history differs from the document's, by editing
// the parsed definition as an admin form would, and registers each.
func registerHistoryVariants(c workflow.Client, reg *dsl.MachineRegistry[Ticket], env dsl.MachineEnv) error {
	// snippet:edit
	def, err := dsl.ParseMachine([]byte(ticketJSON))
	if err != nil {
		return err
	}
	resume := slices.IndexFunc(def.Transitions, func(t dsl.TransitionDef) bool {
		return t.From == "held" && t.Event == "resume"
	})
	for _, v := range variants {
		if v.name == def.Name {
			continue // the loaded machine already has this mode
		}
		variant := def
		variant.Name = v.name
		variant.Transitions = slices.Clone(def.Transitions) // keep def unchanged
		variant.Transitions[resume].History = string(v.history)
		wf, err := dsl.BuildMachine(variant, reg, env) // Validate, resolve names, DefineMachine
		if err != nil {
			return err
		}
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	// snippet:end
	return nil
}

// label names a history mode for the output; the empty mode is "none".
func label(history historyName) string {
	if history == historyNone {
		return "none"
	}
	return string(history)
}

// send sends one event to instance key of machine, waits for its run and
// prints the states it left the ticket in, or that it was rejected.
func send(ctx context.Context, w io.Writer, c workflow.Client, machine, key, event string) error {
	fmt.Fprintln(w, ">", event)
	states, err := sendQuiet(ctx, c, machine, key, event)
	if workflow.IsUnhandledEvent(err) {
		fmt.Fprintln(w, "  rejected: no transition takes it in the current state")
		return nil
	}
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "  states:", states)
	return nil
}

// sendQuiet sends one event, waits for its run and returns the active
// atomic states after it.
func sendQuiet(ctx context.Context, c workflow.Client, machine, key, event string) ([]string, error) {
	ref, err := workflow.SendEvent(ctx, c, machine, key, event, nil, workflow.Job{})
	if err != nil {
		return nil, err
	}
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return nil, err
	}
	var res workflow.MachineResult
	if err := json.Unmarshal(out, &res); err != nil {
		return nil, err
	}
	return res.States, nil
}

// waitDone polls until ticket key reaches a final state: the timer's run
// is not one the program holds a RunRef for.
func waitDone(ctx context.Context, store workflow.JournalStore, key string) error {
	for {
		snap, _, err := workflow.MachineStatus[Ticket](ctx, store, machineName, key)
		if err != nil {
			return err
		}
		if snap.Done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ticket %s: not done after the timer: %v: %w", key, snap.States, ctx.Err())
		case <-time.After(await.Poll):
		}
	}
}

// printStatus prints an instance as MachineStatus reads it, with its log.
func printStatus(ctx context.Context, w io.Writer, store workflow.JournalStore, key string) error {
	snap, ok, err := workflow.MachineStatus[Ticket](ctx, store, machineName, key)
	if err != nil || !ok {
		return fmt.Errorf("ticket %s: found %t: %w", key, ok, err)
	}
	fmt.Fprintf(w, "status of %s: states %v, done %t, approvals %d, notes %v\n",
		key, snap.States, snap.Done, snap.Data.Approvals, snap.Data.Notes)
	for _, step := range snap.Log {
		event := step.Event
		if event == workflow.MachineStartEvent {
			event = "(start)"
		}
		fmt.Fprintf(w, "  %2d %s %-11s %v -> %v\n", step.Seq, step.At.Format("Mon 15:04"), event, step.From, step.To)
	}
	return nil
}
