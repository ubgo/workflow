package batch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// DefaultAwaitPoll is how long an await step waits between two looks at
// its batch: each look is one retry of the step, so it trades the
// engine's retry traffic against how soon the workflow resumes after the
// batch ends. Five seconds matches DefaultPollInterval, the pace at which
// the batch itself learns of job ends by polling. Change it with
// AwaitPoll.
const DefaultAwaitPoll = 5 * time.Second

// DefaultAwaitPolls is how many looks an await step makes before it
// gives up: 720 at DefaultAwaitPoll is one hour. Change it with
// AwaitPolls; an engine may cap a step's retries lower than the value
// asked for, so check the engine's limit for a long batch.
const DefaultAwaitPolls = 720

// ErrBatchPending is the error an await step returns while its batch has
// not finished, so the step's retry policy looks again after the poll
// interval. It shows up as a failed attempt in an engine's run history and
// in step metrics; that is the polling, not a fault.
var ErrBatchPending = errors.New("batch: still running")

// ErrBatchFailed is the permanent error an await step ends with when its
// batch finished with a failed or cancelled job, or was cancelled, unless
// AwaitAllowFailures was given.
var ErrBatchFailed = errors.New("batch: finished with failed or cancelled jobs")

// AwaitOption configures Manager.AwaitStep.
type AwaitOption func(*awaitConfig)

type awaitConfig struct {
	poll          time.Duration
	polls         int
	allowFailures bool
}

// AwaitPoll sets the wait between two looks at the batch (see
// DefaultAwaitPoll). A value that is not positive keeps the default. An
// engine may round it (Hatchet uses whole seconds).
func AwaitPoll(d time.Duration) AwaitOption {
	return func(c *awaitConfig) {
		if d > 0 {
			c.poll = d
		}
	}
}

// AwaitPolls sets how many looks the step makes before it fails with
// ErrBatchPending (see DefaultAwaitPolls). A value below 1 keeps the
// default.
func AwaitPolls(n int) AwaitOption {
	return func(c *awaitConfig) {
		if n > 0 {
			c.polls = n
		}
	}
}

// AwaitAllowFailures makes the step succeed whatever the batch's outcome,
// so the next step reads the Progress and decides; by default a batch
// with a failed or cancelled job fails the step with ErrBatchFailed.
func AwaitAllowFailures() AwaitOption { return func(c *awaitConfig) { c.allowFailures = true } }

// AwaitStep returns a run step that dispatches a batch and completes when
// the batch has finished, so a workflow can fan out to many jobs and
// resume after them: a batch inside a chain of steps.
// Put it in a workflow like any step; its successors run after the batch.
//
// On its first attempt the step calls build with its input and dispatches
// the batch build returns. Every later attempt looks at that batch: while
// it runs, the step returns ErrBatchPending and its retry policy (set by
// AwaitStep: AwaitPolls retries, AwaitPoll apart, constant) looks again;
// once it has finished, the step returns the batch's Progress as JSON, or
// fails permanently with ErrBatchFailed when a job failed or was
// cancelled (see AwaitAllowFailures). It is polling built on retries
// because every driver has retries; a driver without durable retries
// (memory) loses the wait with the process, as it loses any retry.
//
// The batch ID is derived from the run and step (workflow.StepInfo), not
// from WithIDFunc, so a retried attempt finds the batch the first one
// dispatched instead of dispatching another. That needs a driver that
// sets StepInfo (every driver in this module does); a ctx without it
// fails the step permanently. build should be quick and deterministic; an
// error from it, or from Dispatch, is retried like ErrBatchPending.
//
// The Manager must be attached and running in the worker process, since
// the step reads the batch from the Manager's Store; with a shared Store
// (sqlstore's BatchStore) any process may run the step.
func (m *Manager) AwaitStep(name string, build func(ctx context.Context, in []byte) (Batch, error), opts ...AwaitOption) workflow.Step {
	cfg := awaitConfig{poll: DefaultAwaitPoll, polls: DefaultAwaitPolls}
	for _, o := range opts {
		o(&cfg)
	}
	return workflow.Step{
		Name: name,
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			return m.await(ctx, cfg, build, in)
		},
		Retry: &workflow.RetryPolicy{Max: cfg.polls, Backoff: workflow.Backoff{Initial: cfg.poll, Max: cfg.poll}},
	}
}

// await is one attempt of an AwaitStep.
func (m *Manager) await(ctx context.Context, cfg awaitConfig, build func(context.Context, []byte) (Batch, error), in []byte) ([]byte, error) {
	info, ok := workflow.StepInfoFrom(ctx)
	if !ok || info.RunID.IsZero() {
		return nil, workflow.Permanent(errors.New("batch: AwaitStep needs workflow.StepInfo with a RunID on its ctx"))
	}
	id := AwaitID(info.RunID, info.Step)
	st, err := m.store.Get(ctx, id)
	if errors.Is(err, workflow.ErrNotFound) {
		b, err := build(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("batch: AwaitStep %q: build: %w", info.Step, err)
		}
		if err := checkBatch(b); err != nil {
			return nil, workflow.Permanent(err)
		}
		if _, err := m.dispatch(ctx, b, id); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("batch %s: %w", id, ErrBatchPending)
	}
	if err != nil {
		return nil, err
	}
	p := progressOf(st)
	if !st.Finished {
		return nil, fmt.Errorf("batch %s: %d of %d processed: %w", id, p.Processed(), p.Total, ErrBatchPending)
	}
	out, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if !cfg.allowFailures && (p.Failed > 0 || p.CancelledJobs > 0 || p.Cancelled) {
		return nil, workflow.Permanent(fmt.Errorf("batch %s: %d failed, %d cancelled: %w", id, p.Failed, p.CancelledJobs, ErrBatchFailed))
	}
	return out, nil
}

// AwaitID is the ID of the batch an AwaitStep named step dispatches in
// run: a hash of the two, so it is the same on every attempt, unique per
// run and step, and made of characters every engine accepts in a label.
// A test or an operator uses it to find that batch with Manager.Progress.
func AwaitID(run workflow.RunID, step string) ID {
	sum := sha256.Sum256([]byte(string(run) + "\x00" + step))
	return ID("await-" + hex.EncodeToString(sum[:16]))
}
