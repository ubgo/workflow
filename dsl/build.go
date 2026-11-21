package dsl

import (
	"slices"
	"strings"

	"github.com/ubgo/workflow"
)

// Build turns def into a workflow.Workflow: it validates def
// (Definition.Validate), resolves every handler name against reg, converts
// durations, builds the label-reading key functions, and finally runs
// workflow.Validate, the same check a driver's Register runs, so a cycle in
// After, a step After a step that does not exist or a value out of range is
// refused here with the error the same workflow written in Go gets.
//
// A handler name reg does not know is refused with a *FieldError naming
// the step's path and listing the names reg knows. A nil reg knows none.
// Capability checks stay with the driver: Build accepts a wait step, and a
// driver without CapWaitForEvent refuses the workflow at Register, as it
// would refuse the Go form.
func Build(def Definition, reg *Registry) (workflow.Workflow, error) {
	if err := def.Validate(); err != nil {
		return workflow.Workflow{}, err
	}
	wf := workflow.Workflow{
		Name:       def.Name,
		Version:    def.Version,
		MinVersion: def.MinVersion,
		Queue:      def.Queue,
		Cron:       slices.Clone(def.Cron), // the Workflow never aliases the Definition
	}
	// Validate has parsed every duration and the zone, so these cannot fail.
	wf.Timeout, _ = duration("timeout", def.Timeout)
	wf.RunTimeout, _ = duration("run_timeout", def.RunTimeout)
	wf.CronLocation, _ = location(def.CronLocation)
	if def.Retry != nil {
		wf.Retry, _ = def.Retry.policy("retry")
	}
	if c := def.Concurrency; c != nil {
		wf.Concurrency = &workflow.Concurrency{
			MaxRuns:  c.MaxRuns,
			Strategy: workflow.ConcurrencyStrategy(c.Strategy),
			FIFO:     c.FIFO,
		}
		if c.KeyLabel != "" {
			wf.Concurrency.Key = labelKey(c.KeyLabel)
		}
	}
	if def.OnFailure != "" {
		h, err := resolve(reg, "on_failure", def.OnFailure)
		if err != nil {
			return workflow.Workflow{}, err
		}
		wf.OnFailure = h
	}
	wf.Steps = make([]workflow.Step, len(def.Steps))
	for i, s := range def.Steps {
		step, err := s.build(index("steps", i), reg)
		if err != nil {
			return workflow.Workflow{}, err
		}
		wf.Steps[i] = step
	}
	if err := workflow.Validate(wf); err != nil {
		return workflow.Workflow{}, err
	}
	return wf, nil
}

// build converts one validated step at path.
func (s StepDef) build(path string, reg *Registry) (workflow.Step, error) {
	step := workflow.Step{Name: s.Name, After: slices.Clone(s.After)}
	step.Timeout, _ = duration(at(path, "timeout"), s.Timeout)
	step.Sleep, _ = duration(at(path, "sleep"), s.Sleep)
	if s.Retry != nil {
		p, _ := s.Retry.policy(at(path, "retry"))
		step.Retry = &p
	}
	if s.Handler != "" {
		h, err := resolve(reg, at(path, "handler"), s.Handler)
		if err != nil {
			return step, err
		}
		step.Run = h
	}
	if w := s.Wait; w != nil {
		spec := &workflow.WaitSpec{Event: w.Event, Key: w.Key}
		spec.Timeout, _ = duration(at(path, "wait.timeout"), w.Timeout)
		if w.KeyLabel != "" {
			spec.KeyFrom = labelKey(w.KeyLabel)
		}
		step.Wait = spec
	}
	return step, nil
}

// resolve looks name up in reg, refusing an unknown one at path with the
// names reg knows, so a typo reads next to the right spelling.
func resolve(reg *Registry, path, name string) (workflow.Handler, error) {
	if h, ok := reg.Handler(name); ok {
		return h, nil
	}
	known := "none"
	if names := reg.Names(); len(names) > 0 {
		known = strings.Join(names, ", ")
	}
	return nil, fieldErr(path, "no handler is registered as %q (registered: %s)", name, known)
}

// labelKey is a key function reading one job label: the per-run key of a
// wait or a concurrency cap, taken from Job.Labels, which the enqueuer
// sets. A missing label reads as the empty key.
func labelKey(label string) workflow.ConcurrencyKey {
	return workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[label] })
}
