package recorder

import (
	"context"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// Retention is how long a store keeps finished runs, in two stages, so the
// heavy part of a run goes first and the summary row a runs page lists
// stays longer:
//
//   - Stage one, after Steps: the run's step records and their events are
//     dropped; the run record (status, source, outcome, error, timings,
//     input and result) stays.
//   - Stage two, after Runs: the run record itself is deleted, after
//     Archive (when set) accepted it.
//
// Ages are measured from the run's FinishedAt; unfinished runs are never
// pruned. A zero duration keeps that stage forever. PerWorkflow overrides
// both ages for a workflow name.
type Retention struct {
	RetentionRule

	// PerWorkflow overrides RetentionRule for the named workflows.
	PerWorkflow map[string]RetentionRule

	// Archive, when set, is handed every record before stage two deletes
	// it (with its steps, when stage one left any). An error stops the
	// prune and deletes nothing of that batch, so nothing is lost when an
	// archive is down; the next prune tries again.
	Archive func(ctx context.Context, runs []RunRecord) error

	// Batch is how many runs one prune step handles at a time, so a big
	// backlog never holds one long transaction. Zero means
	// DefaultPruneBatch.
	Batch int
}

// RetentionRule is the two ages of a Retention.
type RetentionRule struct {
	// Steps is the age after which a finished run's steps and events are
	// dropped; zero keeps them.
	Steps time.Duration

	// Runs is the age after which a finished run is deleted; zero keeps
	// it.
	Runs time.Duration
}

// DefaultPruneBatch is Retention.Batch when zero.
const DefaultPruneBatch = 500

// MaxPruneBatch bounds Retention.Batch.
const MaxPruneBatch = 10000

// Rule returns the rule for workflow wf: its PerWorkflow entry, else the
// default.
func (r Retention) Rule(wf string) RetentionRule {
	if rule, ok := r.PerWorkflow[wf]; ok {
		return rule
	}
	return r.RetentionRule
}

// BatchSize returns Batch with its default applied.
func (r Retention) BatchSize() int {
	if r.Batch == 0 {
		return DefaultPruneBatch
	}
	return r.Batch
}

// IsZero reports whether r prunes nothing.
func (r Retention) IsZero() bool {
	if r.Steps != 0 || r.Runs != 0 {
		return false
	}
	for _, rule := range r.PerWorkflow {
		if rule.Steps != 0 || rule.Runs != 0 {
			return false
		}
	}
	return true
}

// ValidateRetention refuses negative ages and a Batch outside [0,
// MaxPruneBatch]. Errors wrap workflow.ErrInvalid.
func ValidateRetention(r Retention) error {
	check := func(name string, rule RetentionRule) error {
		if rule.Steps < 0 || rule.Runs < 0 {
			return fmt.Errorf("%w: recorder retention %s: negative age", workflow.ErrInvalid, name)
		}
		return nil
	}
	if err := check("default", r.RetentionRule); err != nil {
		return err
	}
	for wf, rule := range r.PerWorkflow {
		if err := check(fmt.Sprintf("for %q", wf), rule); err != nil {
			return err
		}
	}
	if r.Batch < 0 || r.Batch > MaxPruneBatch {
		return fmt.Errorf("%w: recorder retention: Batch %d is outside [0, %d]", workflow.ErrInvalid, r.Batch, MaxPruneBatch)
	}
	return nil
}

// PruneResult is what one Prune did.
type PruneResult struct {
	// StepsCleared counts runs whose steps stage one dropped.
	StepsCleared int

	// RunsDeleted counts runs stage two deleted.
	RunsDeleted int

	// Archived counts runs handed to Archive (equal to RunsDeleted when
	// Archive is set).
	Archived int
}

// PruneEvery calls p.Prune every interval until ctx is cancelled, handing
// each error to onError (nil ignores them), and returns nil when ctx ends.
// It is the loop an app runs in a goroutine next to its client; a store's
// Prune is also safe to call from a scheduled job instead.
func PruneEvery(ctx context.Context, p Pruner, interval time.Duration, onError func(error)) error {
	if p == nil || interval <= 0 {
		return fmt.Errorf("%w: recorder.PruneEvery needs a Pruner and a positive interval", workflow.ErrInvalid)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := p.Prune(ctx); err != nil && onError != nil && ctx.Err() == nil {
			onError(err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
