package dsl_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/dsl"
	"github.com/ubgo/workflow/memory"
)

type ticket struct {
	Approvals int      `json:"approvals"`
	Notes     []string `json:"notes,omitempty"`
}

// ticketMachine is a support ticket: open, then in review (a compound
// state with a nested escalation), then closed; "hold" leaves review and
// "resume" comes back with deep history; an open ticket expires.
const ticketMachine = `{
  "name": "ticket",
  "initial": "open",
  "unhandled": "reject",
  "keep_log": 5,
  "states": [
    {"name": "open", "entry": ["note"], "after": [{"delay": "200ms", "event": "expire"}]},
    {"name": "review", "initial": "first-line", "exit": ["note"]},
    {"name": "first-line", "parent": "review"},
    {"name": "escalated", "parent": "review", "entry": ["note"]},
    {"name": "held"},
    {"name": "closed", "final": true, "entry": ["note"]}
  ],
  "transitions": [
    {"from": "open", "event": "assign", "to": "review"},
    {"from": "open", "event": "expire", "to": "closed"},
    {"from": "first-line", "event": "escalate", "to": "escalated"},
    {"from": "review", "event": "approve", "actions": ["approve"]},
    {"from": "review", "event": "close", "to": "closed", "guard": "approved-twice"},
    {"from": "review", "event": "hold", "to": "held"},
    {"from": "held", "event": "resume", "to": "review", "history": "deep"}
  ]
}`

func ticketRegistry(t *testing.T) *dsl.MachineRegistry[ticket] {
	t.Helper()
	reg := dsl.NewMachineRegistry[ticket]()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(reg.Action("note", func(_ context.Context, m *workflow.MachineCtx[ticket]) error {
		m.Data.Notes = append(m.Data.Notes, m.State)
		return nil
	}))
	must(reg.Action("approve", func(_ context.Context, m *workflow.MachineCtx[ticket]) error {
		m.Data.Approvals++
		return nil
	}))
	must(reg.Guard("approved-twice", func(m workflow.MachineCtx[ticket]) bool { return m.Data.Approvals >= 2 }))
	return reg
}

func TestMachineFromJSON(t *testing.T) {
	c := memory.New()
	store := workflow.NewMemoryJournal()
	wf, err := dsl.LoadMachine([]byte(ticketMachine), ticketRegistry(t), dsl.MachineEnv{Store: store, Client: c})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	send := func(key, event string) ([]string, error) {
		ref, err := workflow.SendEvent(ctx, c, "ticket", key, event, nil, workflow.Job{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 2 * time.Millisecond})
		if err != nil {
			return nil, err
		}
		var res workflow.MachineResult
		if err := json.Unmarshal(out, &res); err != nil {
			t.Fatal(err)
		}
		return res.States, nil
	}
	steps := []struct {
		event  string
		want   []string
		reject bool
	}{
		{"assign", []string{"first-line"}, false},
		{"escalate", []string{"escalated"}, false},
		{"close", nil, true}, // the guard refuses: no approvals yet
		{"hold", []string{"held"}, false},
		{"resume", []string{"escalated"}, false}, // deep history
		{"approve", []string{"escalated"}, false},
		{"approve", []string{"escalated"}, false},
		{"close", []string{"closed"}, false},
	}
	for i, s := range steps {
		got, err := send("T-1", s.event)
		if s.reject {
			if !workflow.IsUnhandledEvent(err) {
				t.Fatalf("step %d %s: %v, want a rejection", i, s.event, err)
			}
			continue
		}
		if err != nil || !slices.Equal(got, s.want) {
			t.Fatalf("step %d %s: %v %v, want %v", i, s.event, got, err, s.want)
		}
	}
	snap, _, err := workflow.MachineStatus[ticket](ctx, store, "ticket", "T-1")
	if err != nil || snap.Data.Approvals != 2 || len(snap.Log) != 5 || !snap.Done {
		t.Fatalf("snapshot %+v %v", snap, err)
	}
	want := []string{"open", "escalated", "review", "escalated", "review", "closed"}
	if !slices.Equal(snap.Data.Notes, want) {
		t.Fatalf("notes %v, want %v", snap.Data.Notes, want)
	}
	// The timer from the JSON: an open ticket expires.
	if _, err := workflow.StartMachine(ctx, c, "ticket", "T-2", ticket{}, workflow.Job{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		snap, _, err := workflow.MachineStatus[ticket](ctx, store, "ticket", "T-2")
		if err != nil {
			t.Fatal(err)
		}
		if snap.Done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("T-2 never expired: %+v", snap)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMachineDefinitionRefusals(t *testing.T) {
	reg := ticketRegistry(t)
	env := dsl.MachineEnv{Store: workflow.NewMemoryJournal(), Client: memory.New()}
	cases := map[string]struct{ old, new, path string }{
		"unknown field":   {`"keep_log": 5`, `"keep_logs": 5`, "keep_logs"},
		"wrong type":      {`"keep_log": 5`, `"keep_log": "5"`, "keep_log"},
		"unknown guard":   {`"guard": "approved-twice"`, `"guard": "approved-once"`, "transitions[4].guard"},
		"unknown action":  {`"actions": ["approve"]`, `"actions": ["aprove"]`, "transitions[3].actions[0]"},
		"unknown entry":   {`"entry": ["note"], "after"`, `"entry": ["nope"], "after"`, "states[0].entry[0]"},
		"bad history":     {`"history": "deep"`, `"history": "deepest"`, "transitions[6].history"},
		"bad unhandled":   {`"unhandled": "reject"`, `"unhandled": "drop"`, "unhandled"},
		"bad delay":       {`"delay": "200ms"`, `"delay": "soon"`, "states[0].after[0].delay"},
		"zero delay":      {`"delay": "200ms"`, `"delay": "0s"`, "states[0].after[0].delay"},
		"unknown target":  {`"to": "held"`, `"to": "hold"`, ""},
		"no name":         {`"name": "ticket"`, `"name": ""`, "name"},
		"timer no client": {`"name": "ticket"`, `"name": "ticket"`, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := env
			if name == "timer no client" {
				e.Client = nil
			}
			_, err := dsl.LoadMachine([]byte(strings.Replace(ticketMachine, tc.old, tc.new, 1)), reg, e)
			if !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("got %v, want ErrInvalid", err)
			}
			var fe *dsl.FieldError
			if tc.path != "" && (!errors.As(err, &fe) || fe.Path != tc.path) {
				t.Fatalf("got %v, want a FieldError at %s", err, tc.path)
			}
		})
	}
	def, err := dsl.ParseMachine([]byte(ticketMachine))
	if err != nil {
		t.Fatal(err)
	}
	b, err := dsl.MarshalMachine(def)
	if err != nil {
		t.Fatal(err)
	}
	back, err := dsl.ParseMachine(b)
	if err != nil {
		t.Fatal(err)
	}
	if b2, _ := dsl.MarshalMachine(back); string(b2) != string(b) {
		t.Fatalf("round trip changed the definition:\n%s\n%s", b, b2)
	}
	if err := reg.Guard("approved-twice", func(workflow.MachineCtx[ticket]) bool { return true }); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a duplicate guard: %v", err)
	}
}
