package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/workflowtest"
)

// TestRun runs the program and checks every line the guide shows: an
// order behind each wrapper is paid by its prefixed ref while the other
// order keeps waiting, the gauge plugin sees the inbox's pending signals
// before each step, the program takes one of them once, and Use refuses
// the gauge on a client without RunSignalReader.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `signal a run by its ID behind a wrapper:
  regional ref prefix "us"       paid "card 4242", other order waiting
  failover ref prefix "primary"  paid "card 4242", other order waiting
  cutover  ref prefix "new"      paid "card 4242", other order waiting
  hybrid   ref prefix ""         paid "card 4242", other order waiting
a plugin that binds the client:
  pending while working: 2, took "first": true, took it again: false
  gauge before work: 0 pending
  gauge before report: 1 pending
  without RunSignalReader: unsupported true
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// snippet:suite
// TestEscalationFlows runs the run-addressed signal suite over the
// regional client the program builds: OwnRun waits, SignalParent, OnSignals
// handlers, the RunSignalReader contract and ApproveRun, RejectRun,
// SignalChoiceRun and UpdateRun. A case whose capability the client lacks
// skips.
func TestEscalationFlows(t *testing.T) {
	workflowtest.RunEscalationFlows(t, workflowtest.EscalationHarness{
		Start: func(t *testing.T, plugins []workflow.Plugin, wfs ...workflow.Workflow) workflow.Client {
			base, err := newRegional()
			if err != nil {
				t.Fatal(err)
			}
			// The Journal plugin first, then the suite's plugins (OnSignals).
			c, err := workflow.Use(base, append([]workflow.Plugin{workflow.Journal(workflow.NewMemoryJournal())}, plugins...)...)
			if err != nil {
				t.Fatal(err)
			}
			for _, wf := range wfs {
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			return c
		},
	})
}

// snippet:end
