package workflow

import (
	"context"
	"maps"
	"slices"
	"strings"
)

// Labels a reset or a fork stamps on a run, so observers and operators
// can tell a redo from a first run (DE-14).
const (
	// LabelRedo marks a run that redoes work which already succeeded
	// once: a run reset with ResetOptions.Succeeded after it had
	// succeeded, and a fork of a run that had succeeded or carried this
	// label. Its value is "true". The Then plugin skips the ends of such
	// runs (ThenAfterRedo turns that off), so a next job that already
	// started once for the first success is not started a second time.
	//ds:def id=labelredo-7f5kcp9k owner=@khanakia stability=stable
	LabelRedo = LabelPrefixInternal + "redo"

	// LabelForkOf is the RunID of the run a fork was made from.
	//ds:def id=labelforkof-xac2k37t owner=@khanakia stability=stable
	LabelForkOf = LabelPrefixInternal + "fork-of"

	// LabelForkStep is the step a fork starts from.
	//ds:def id=labelforkstep-6p5fzb5h owner=@khanakia stability=stable
	LabelForkStep = LabelPrefixInternal + "fork-step"
)

// ResetOptions widen ResetRunWith beyond what ResetRun accepts. The zero
// value is ResetRun exactly.
//
//ds:def id=resetoptions-fgtq654p owner=@khanakia stability=stable
type ResetOptions struct {
	// Succeeded also accepts a run that ended StatusSucceeded: the
	// explicit opt-in for redoing work that already succeeded. The run
	// then gets LabelRedo. Its chain (Job.Chain) was handed on when it
	// succeeded and is never handed on again: the jobs it started are
	// runs of their own, reset those if they must run again. A driver
	// that holds a chain handoff still pending refuses with
	// ErrWrongState.
	Succeeded bool `json:"succeeded,omitempty"`
}

// RunResetterWith is RunResetter with options (DE-14): ResetRunWith with
// the zero ResetOptions is ResetRun; with Succeeded it also resets a run
// that succeeded, in place, keeping its RunRef.
//
// What happens around the run, so nothing runs twice by accident: the
// steps that run again (step, every step downstream of it, and the steps
// that had not succeeded) do all their work again, so a child they spawn
// is a new run (Spawn gets back a child that is still unfinished, since
// it holds its key); the steps that are kept do not run, so their
// children are untouched. The run's stored result is cleared and
// replaced by its next end. Run-finished observers see that end, as after
// Retry; the batch package ignores a repeated end, and the Then plugin
// skips runs carrying LabelRedo. The chain is not handed on again.
//
//ds:def id=runresetterwith-792722gn owner=@khanakia stability=stable
type RunResetterWith interface {
	ResetRunWith(ctx context.Context, ref RunRef, step string, opts ResetOptions) (RunRef, error)
}

// ForkOptions tune ForkRun.
//
//ds:def id=forkoptions-6g2ac7wq owner=@khanakia stability=stable
type ForkOptions struct {
	// IdempotencyKey is the fork's own key; empty means none. The
	// original's key is never copied: it belongs to the original.
	IdempotencyKey string `json:"idempotency_key,omitempty"`

	// Labels are added to the fork's labels, over the ones it keeps from
	// the original. They are app labels: a reserved prefix is refused
	// (ValidateLabels).
	Labels map[string]string `json:"labels,omitempty"`
}

// RunForker starts a new run from a finished one (DE-14, fork): same
// workflow, payload, tenant, queue, priority, job retry policy and
// workflow version, a new RunID, and the outputs of every step the
// original succeeded in except step and the steps downstream of it
// (StepsFrom), so the fork starts at step with the original's journal up
// to there. The original is not changed.
//
// Only a finished run (succeeded, failed, cancelled or discarded) can be
// forked, since a step still running in the original could otherwise run
// in both: anything else returns ErrWrongState. A step the workflow does
// not have returns ErrInvalid, an unknown ref ErrNotFound.
//
// The fork never carries the original's chain (it would run once for the
// fork and again when the original is retried) or its idempotency key; it
// keeps the original's labels except the membership labels ForkLabels
// drops, and gets LabelForkOf, LabelForkStep and, when the original had
// succeeded or carried it, LabelRedo. Its steps that run spawn new
// children (its RunID is new), and its wait steps wait for events sent
// after the fork. A client implements it when its driver can; the call
// answers for itself, so there is no capability bit.
//
//ds:def id=runforker-9cpe2f7f owner=@khanakia stability=stable
type RunForker interface {
	ForkRun(ctx context.Context, ref RunRef, step string, opts ForkOptions) (RunRef, error)
}

// forkDrops are the reserved labels that make a run a member of
// something (a batch, a parent's children, a schedule's ticks, a
// partition set, an object's calls, a backfill, a Then or continue-as-new
// lineage). A fork is a member of none of them.
//
//ds:def id=forkdrops-k5wk4h5m owner=@khanakia stability=stable
var forkDrops = []string{LabelParentRun, LabelParentStep, LabelSchedule, LabelTickAt, LabelTrigger,
	LabelPartitionSet, LabelPartitionKey, LabelObjectKey, LabelThenFrom, LabelContinuedFrom, LabelRunKey,
	LabelForkOf, LabelForkStep, LabelRedo}

// ForkLabels returns the labels a fork of a run with labels gets: the
// original's labels without everything under LabelPrefixBatch and the
// membership labels (parent run and step, schedule, tick time, trigger,
// partition, object key, Then and continue-as-new lineage, backfill key),
// then extra, then LabelForkOf, LabelForkStep and, when redo is true,
// LabelRedo. Claim-check and trace labels are kept: the payload and the
// kept outputs may be references under the original's claim token.
// Drivers call it; it is exported so a driver outside this module builds
// the same labels.
//
//ds:def id=forklabels-479w9npr owner=@khanakia stability=stable
func ForkLabels(labels map[string]string, extra map[string]string, of RunID, step string, redo bool) map[string]string {
	out := make(map[string]string, len(labels)+len(extra)+3)
	for k, v := range labels {
		if strings.HasPrefix(k, LabelPrefixBatch) || slices.Contains(forkDrops, k) {
			continue
		}
		out[k] = v
	}
	maps.Copy(out, extra)
	out[LabelForkOf] = of.String()
	out[LabelForkStep] = step
	if redo {
		out[LabelRedo] = "true"
	}
	return out
}

// ForkRedo reports whether a fork of a run with this status and these
// labels redoes work that already succeeded (LabelRedo).
//
//ds:def id=forkredo-k36kx3xp owner=@khanakia stability=stable
func ForkRedo(st Status, labels map[string]string) bool {
	return st == StatusSucceeded || labels[LabelRedo] == "true"
}

// CheckFork validates a ForkRun call before a driver looks the run up:
// a step is needed and opts.Labels must be app labels.
//
//ds:def id=checkfork-a9wp7kp8 owner=@khanakia stability=stable
func CheckFork(step string, opts ForkOptions) error {
	if step == "" {
		return invalidf("ForkRun needs a step")
	}
	return ValidateLabels(opts.Labels)
}

// ForkStatusOK reports whether a run in st may be forked: it has
// finished.
//
//ds:def id=forkstatusok-x544ybnq owner=@khanakia stability=stable
func ForkStatusOK(st Status) bool { return st.IsTerminal() }

// ResetStatusOK reports whether a run in st may be reset with opts:
// failed always, succeeded with opts.Succeeded.
//
//ds:def id=resetstatusok-pdwz2avg owner=@khanakia stability=stable
func ResetStatusOK(st Status, opts ResetOptions) bool {
	return st == StatusFailed || (opts.Succeeded && st == StatusSucceeded)
}

// ResetRunWith forwards to Inner's RunResetterWith.
func (f Forward) ResetRunWith(ctx context.Context, ref RunRef, step string, opts ResetOptions) (RunRef, error) {
	if i, ok := f.Inner.(RunResetterWith); ok {
		return i.ResetRunWith(ctx, ref, step, opts)
	}
	return RunRef{}, lacks("ResetRunWith", Caps{})
}

// ForkRun forwards to Inner's RunForker.
func (f Forward) ForkRun(ctx context.Context, ref RunRef, step string, opts ForkOptions) (RunRef, error) {
	if i, ok := f.Inner.(RunForker); ok {
		return i.ForkRun(ctx, ref, step, opts)
	}
	return RunRef{}, lacks("ForkRun", Caps{})
}
