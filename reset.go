package workflow

import (
	"context"
	"slices"
)

// RunResetter re-runs a failed run from a step the caller picks (DE-14,
// restart from a step), where RunRetrier always resumes at the step that
// failed. Use it when the failure was caused upstream: bad data fixed in
// the source, a transform step deployed with a fix. A client implements it
// when its driver can; the call answers for itself, so there is no
// capability bit (UnsupportedError from a wrapper whose inner client
// lacks it).
//
// ResetRun drops the outputs of step and of every step downstream of it
// (StepsFrom), keeps the outputs of the other steps that succeeded, and
// puts the run back to StatusQueued under its own RunRef, exactly as
// Retry does otherwise: fresh attempt counts for every step that runs
// again, its idempotency key taken again (ErrDuplicate when another
// unfinished run holds it), its end reported again to run-finished
// observers. A wait step that runs again waits for a new event: the
// events it took before stay taken. A run that is not StatusFailed returns
// ErrWrongState, a step the workflow does not have ErrInvalid, an unknown
// ref ErrNotFound.
type RunResetter interface {
	ResetRun(ctx context.Context, ref RunRef, step string) (RunRef, error)
}

// StepsFrom returns step and every step of wf downstream of it (a step
// whose After reaches step, directly or through other steps), in wf's step
// order: the steps ResetRun runs again besides those that had not
// succeeded. It returns nil when wf has no step named step.
func StepsFrom(wf Workflow, step string) []string {
	if !slices.ContainsFunc(wf.Steps, func(s Step) bool { return s.Name == step }) {
		return nil
	}
	reached := map[string]bool{step: true}
	for changed := true; changed; {
		changed = false
		for _, s := range wf.Steps {
			if reached[s.Name] {
				continue
			}
			if slices.ContainsFunc(s.After, func(p string) bool { return reached[p] }) {
				reached[s.Name] = true
				changed = true
			}
		}
	}
	out := make([]string, 0, len(reached))
	for _, s := range wf.Steps {
		if reached[s.Name] {
			out = append(out, s.Name)
		}
	}
	return out
}

// ResetRun forwards to Inner's RunResetter.
func (f Forward) ResetRun(ctx context.Context, ref RunRef, step string) (RunRef, error) {
	if i, ok := f.Inner.(RunResetter); ok {
		return i.ResetRun(ctx, ref, step)
	}
	return RunRef{}, lacks("ResetRun", Caps{})
}
