package workflowtest

import (
	"errors"
	"time"

	"github.com/ubgo/workflow"
)

// editCases prove workflow.RunEditor (AD-04): a scheduled run's payload,
// priority and start change before it runs, and nothing else can be
// edited.
var editCases = []testCase{
	{"EditRun changes a scheduled run's payload and start", func(e *env) {
		e.need(workflow.CapDelayedStart)
		ed := asOpt[workflow.RunEditor](e, "workflow.RunEditor")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, Payload: []byte(`{"to":"old@example.com"}`), RunAt: e.now().Add(24 * time.Hour)})
		ctx, cancel := e.callCtx()
		defer cancel()
		payload, prio, at := []byte(`{"to":"new@example.com"}`), 7, e.now().Add(e.tm.unit)
		got, err := ed.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload, Priority: &prio, RunAt: &at})
		e.skipUnsupported(err, "EditRun")
		if err != nil {
			e.t.Fatalf("EditRun: %v", err)
		}
		e.waitStatus(got, workflow.StatusSucceeded, e.tm.unit)
		if string(c.input()) != string(payload) || c.count() != 1 {
			e.t.Fatalf("the step ran %d time(s) with %s, want once with the edited payload", c.count(), c.input())
		}
	}},
	{"EditRun refuses an empty edit and a run that is not scheduled", func(e *env) {
		ed := asOpt[workflow.RunEditor](e, "workflow.RunEditor")
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		ctx, cancel := e.callCtx()
		defer cancel()
		p := []byte(`1`)
		_, err := ed.EditRun(ctx, ref, workflow.JobEdit{Payload: &p})
		e.skipUnsupported(err, "EditRun")
		if !errors.Is(err, workflow.ErrWrongState) {
			e.t.Fatalf("EditRun of a finished run = %v, want ErrWrongState", err)
		}
		if _, err := ed.EditRun(ctx, ref, workflow.JobEdit{}); !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("EditRun with nothing to change = %v, want ErrInvalid", err)
		}
	}},
}

// p3Groups are the conformance groups of the P3 contracts, in Run order.
var p3Groups = []struct {
	name  string
	cases []testCase
}{
	{"edit", editCases},
}
