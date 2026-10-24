package workflow

import (
	"context"
	"fmt"
	"maps"
	"strconv"
)

// Labels Spawn writes on every job it enqueues, so a run listing can find
// the children of a run. Spawn sets them itself, overwriting any value a
// job carried under the same key, and enqueues under AllowReservedLabels
// for these two keys only: they are refused on a public Enqueue unless
// the owner opts in (CheckJobLabels, on drivers that enforce it), so they
// cannot arrive by accident or from untrusted input. Like every reserved
// label they are a guard, not a sandbox (see AllowReservedLabels).
const (
	// LabelParentRun is the RunID of the run whose step spawned the job.
	LabelParentRun = LabelPrefixInternal + "parent-run"

	// LabelParentStep is the name of that step.
	LabelParentStep = LabelPrefixInternal + "parent-step"
)

// SpawnOption configures Spawn.
type SpawnOption func(*spawnConfig)

type spawnConfig struct{ allowDuplicates bool }

// SpawnAllowDuplicates lets Spawn run on a client without
// CapIdempotencyKey. The journal still keeps a retried step from
// enqueueing a job twice, except when the process dies between an enqueue
// and the journal's save of its RunRef; with this option that window can
// start a job twice.
func SpawnAllowDuplicates() SpawnOption { return func(c *spawnConfig) { c.allowDuplicates = true } }

// Spawn enqueues jobs from inside a step as runs of their own and returns
// their RunRefs in order, without waiting for them: multiple instances
// without synchronization (WC-12). Each child is its own run, with its own
// retries and its own worker, and carries LabelParentRun and
// LabelParentStep.
//
// A retried step does not spawn twice: each job's RunRef is memoised
// under name and the job's index (Spawn needs the Journal plugin, as Memo
// does), and a job without an IdempotencyKey gets one derived from the
// run, the step, name and the index, which covers a crash between the
// enqueue and the save. That key needs CapIdempotencyKey on client; a
// client without it is refused with an UnsupportedError unless
// SpawnAllowDuplicates is given. A job that already has a key keeps it.
//
// client is the client the app enqueues with (a step has no client of its
// own); pass the same one the step's workflow is registered on, or
// another one to spawn onto another backend.
func Spawn(ctx context.Context, client Client, name string, jobs []Job, opts ...SpawnOption) ([]RunRef, error) {
	var cfg spawnConfig
	for _, o := range opts {
		o(&cfg)
	}
	if client == nil {
		return nil, Permanent(invalidf("spawn %q: client is nil", name))
	}
	if !cfg.allowDuplicates && !client.Capabilities().Has(CapIdempotencyKey) {
		return nil, Permanent(&UnsupportedError{Field: "Spawn", Missing: CapIdempotencyKey,
			Reason: "a retried step could start a child twice; pass SpawnAllowDuplicates to accept that"})
	}
	info, ok := StepInfoFrom(ctx)
	if !ok {
		return nil, Permanent(invalidf("spawn %q: not called from a step", name))
	}
	j, err := journalFrom(ctx)
	if err != nil {
		return nil, err
	}
	key, err := j.claim(name)
	if err != nil {
		return nil, err
	}
	refs := make([]RunRef, len(jobs))
	for i, job := range jobs {
		idx := strconv.Itoa(i)
		ref, err := memoKey(ctx, j, key+"#"+idx, name+"#"+idx, func(ctx context.Context) (RunRef, error) {
			job.Labels = maps.Clone(job.Labels)
			if job.Labels == nil {
				job.Labels = map[string]string{}
			}
			job.Labels[LabelParentRun] = info.RunID.String()
			job.Labels[LabelParentStep] = info.Step
			if job.IdempotencyKey == "" {
				job.IdempotencyKey = "spawn/" + info.RunID.String() + "/" + key + "/" + idx
			}
			return client.Enqueue(AllowReservedLabels(ctx, LabelParentRun, LabelParentStep), job)
		})
		if err != nil {
			return nil, fmt.Errorf("workflow: spawn %q: job %d: %w", name, i, err)
		}
		refs[i] = ref
	}
	return refs, nil
}
