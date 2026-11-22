package workflow

import "context"

// PauseTarget names what Pauser holds: one workflow, one queue, or
// everything. Exactly one field is set; build it with PauseWorkflow,
// PauseQueue or PauseAll.
//
//ds:def id=pausetarget-2g5zg3um owner=@khanakia stability=stable
type PauseTarget struct {
	// Workflow holds the runs of one workflow. Needs CapPauseWorkflow.
	Workflow string

	// Queue holds the runs on one queue (EffectiveQueue). Needs
	// CapPauseQueue.
	Queue string

	// All holds every run. Needs CapPauseWorkflow.
	All bool
}

// PauseWorkflow targets the runs of one workflow.
func PauseWorkflow(name string) PauseTarget { return PauseTarget{Workflow: name} }

// PauseQueue targets the runs on one queue.
//
//ds:def id=pausequeue-xg7ky8xh owner=@khanakia stability=stable
func PauseQueue(name string) PauseTarget { return PauseTarget{Queue: name} }

// PauseAll targets every run. Resume(PauseAll()) clears every pause.
//
//ds:def id=pauseall-ntt6srtz owner=@khanakia stability=stable
func PauseAll() PauseTarget { return PauseTarget{All: true} }

// ValidatePauseTarget refuses a target that sets none or more than one
// field, or names an invalid queue. Errors wrap ErrInvalid.
func ValidatePauseTarget(t PauseTarget) error {
	n := 0
	if t.Workflow != "" {
		n++
	}
	if t.Queue != "" {
		n++
		if err := ValidateQueueName(t.Queue); err != nil {
			return err
		}
	}
	if t.All {
		n++
	}
	if n != 1 {
		return invalidf("pause target must set exactly one of Workflow, Queue and All")
	}
	return nil
}

// RequiresPause is the capability a target needs: CapPauseQueue for a
// queue, CapPauseWorkflow otherwise.
//
//ds:def id=requirespause-bpxg35v9 owner=@khanakia stability=stable
func RequiresPause(t PauseTarget) Caps {
	if t.Queue != "" {
		return CapPauseQueue
	}
	return CapPauseWorkflow
}

// CheckPause validates t and refuses it with an UnsupportedError when have
// lacks what it needs. Drivers call it first in Pause and Resume.
//
//ds:def id=checkpause-9fyvd7wv owner=@khanakia stability=stable
func CheckPause(have Caps, t PauseTarget) error {
	if err := ValidatePauseTarget(t); err != nil {
		return err
	}
	if missing := have.Missing(RequiresPause(t)); !missing.IsZero() {
		field := "PauseTarget.Workflow"
		if t.Queue != "" {
			field = "PauseTarget.Queue"
		}
		return &UnsupportedError{Field: field, Missing: missing}
	}
	return nil
}

// Holds reports whether a paused target t holds a run of workflow wf on
// queue q.
func (t PauseTarget) Holds(wf, q string) bool {
	return t.All || (t.Workflow != "" && t.Workflow == wf) || (t.Queue != "" && t.Queue == q)
}

// Pauser holds runs without cancelling them, and resumes them. A client implements it when it
// declares CapPauseWorkflow or CapPauseQueue, and refuses the targets of
// the one it lacks (CheckPause).
//
// While a target is paused, no step attempt of a run it holds is claimed:
// new runs and the next steps of running runs alike stay StatusQueued.
// Attempts already executing finish. Enqueue is still accepted. Timers
// (RunAt, sleeps, wait deadlines, retry backoffs) keep running, and what
// they make ready waits. A run is held while any paused target holds it.
// On a durable driver a pause is stored, so every node obeys it and it
// survives a restart.
//
// Pause is idempotent. Resume removes exactly that target, except
// Resume(PauseAll()), which removes every pause; resuming a target that is
// not paused is a no-op. Paused lists the targets in force, All first,
// then workflows, then queues, each by name.
//
//ds:def id=pauser-5p4t8yg6 owner=@khanakia stability=stable
type Pauser interface {
	Pause(ctx context.Context, t PauseTarget) error
	Resume(ctx context.Context, t PauseTarget) error
	Paused(ctx context.Context) ([]PauseTarget, error)
}
