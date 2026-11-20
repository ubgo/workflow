package workflowtest

import (
	"errors"

	"github.com/ubgo/workflow"
)

// workflowVersionCases check CapWorkflowVersion (LO-11) on one client: a
// run carries its definition's Version to the step and the run listing;
// a driver without the capability refuses a versioned workflow. That a
// worker never takes a version it does not run needs two clients with
// two definitions over one store, so each durable driver proves it in its
// own tests (sqlstore enginetest, redis TestWorkflowVersions).
var workflowVersionCases = []testCase{
	{"a run carries its workflow's Version", func(e *env) {
		e.need(workflow.CapWorkflowVersion)
		var s stepInfos
		wf := oneStep(e.name("w"), s.handler())
		wf.Version, wf.MinVersion = 3, 2
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := s.last().Version; got != 3 {
			e.t.Errorf("StepInfo.Version = %d, want 3", got)
		}
		if e.c.Capabilities().Has(workflow.CapRunQuery) {
			if got := e.runInfo(ref).Version; got != 3 {
				e.t.Errorf("RunInfo.Version = %d, want 3", got)
			}
		}
	}},
	{"an unversioned run reports version 0", func(e *env) {
		var s stepInfos
		wf := oneStep(e.name("w"), s.handler())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		if got := s.last().Version; got != 0 {
			e.t.Errorf("StepInfo.Version = %d, want 0", got)
		}
	}},
	{"a driver without CapWorkflowVersion refuses a versioned workflow", func(e *env) {
		e.lacks(workflow.CapWorkflowVersion)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Version = 2
		err := e.registerAndStart(wf)
		var ue *workflow.UnsupportedError
		if !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapWorkflowVersion) {
			e.t.Fatalf("Register of a versioned workflow = %v; want an UnsupportedError missing CapWorkflowVersion", err)
		}
	}},
}
