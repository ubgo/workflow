package memory

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// WithMaxCheckpoint sets the largest checkpoint a step may save
// (workflow.SaveCheckpoint), in bytes; a bigger one is refused with a
// *workflow.CheckpointTooLargeError. Zero or less means
// workflow.DefaultMaxCheckpoint, the default.
func WithMaxCheckpoint(n int) Option { return func(m *Client) { m.maxCheckpoint = max(n, 0) } }

// checkpoint is one step's saved checkpoint.
type checkpoint struct {
	data    []byte
	expires time.Time // zero: until the run ends
}

// attemptState is what this driver puts on the ctx of one step attempt:
// its checkpointer and the completion CompleteInTx records. Every field is
// guarded by Client.mu.
type attemptState struct {
	c       *Client
	r       *run
	step    string
	done    bool   // the handler returned
	settled bool   // CompleteInTx recorded the step's success
	output  []byte // CompleteInTx's output
}

type attemptKey struct{}

var _ workflow.Checkpointer = (*attemptState)(nil)

// withAttemptState puts a new attemptState for t on actx, the attempt's
// cancellable ctx, with its Checkpointer.
func (c *Client) withAttemptState(actx context.Context, t *task) (context.Context, *attemptState) {
	st := &attemptState{c: c, r: t.r, step: t.step.Name}
	actx = context.WithValue(actx, attemptKey{}, st)
	return workflow.WithCheckpointer(actx, st), st
}

// finish marks the attempt's handler returned and reports the output
// CompleteInTx recorded, if it did.
func (st *attemptState) finish() ([]byte, bool) {
	st.c.mu.Lock()
	defer st.c.mu.Unlock()
	st.done = true
	return st.output, st.settled
}

// lostLocked is the ErrAttemptLost error of an attempt that no longer owns
// its step, or nil: its handler returned or its run ended. This driver
// starts the next attempt of a step only after the handler of the last one
// returned, so a handler still running owns its step even after its ctx
// was cancelled, as a sqlstore attempt owns it until its lease is lost.
func (st *attemptState) lostLocked() error {
	switch {
	case st.r.status.IsTerminal():
		return fmt.Errorf("memory: step %q of run %s: the run has ended: %w", st.step, st.r.id, workflow.ErrAttemptLost)
	case st.done:
		return fmt.Errorf("memory: step %q of run %s: the attempt has returned: %w", st.step, st.r.id, workflow.ErrAttemptLost)
	}
	return nil
}

// SaveCheckpoint implements workflow.Checkpointer.
func (st *attemptState) SaveCheckpoint(_ context.Context, data []byte, ttl time.Duration) error {
	if err := workflow.CheckCheckpointSize(st.step, data, st.c.maxCheckpoint, "memory.WithMaxCheckpoint"); err != nil {
		return err
	}
	c := st.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := st.lostLocked(); err != nil {
		return err
	}
	cp := checkpoint{data: bytes.Clone(data)}
	if ttl > 0 {
		cp.expires = c.clk.Now().Add(ttl)
	}
	if c.checkpoints == nil {
		c.checkpoints = map[*run]map[string]checkpoint{}
	}
	if c.checkpoints[st.r] == nil {
		c.checkpoints[st.r] = map[string]checkpoint{}
	}
	c.checkpoints[st.r][st.step] = cp
	return nil
}

// LoadCheckpoint implements workflow.Checkpointer.
func (st *attemptState) LoadCheckpoint(context.Context) ([]byte, bool, error) {
	c := st.c
	c.mu.Lock()
	defer c.mu.Unlock()
	cp, ok := c.checkpoints[st.r][st.step]
	if !ok || (!cp.expires.IsZero() && !c.clk.Now().Before(cp.expires)) {
		return nil, false, nil
	}
	return bytes.Clone(cp.data), true, nil
}

// ClearCheckpoint implements workflow.Checkpointer.
func (st *attemptState) ClearCheckpoint(context.Context) error {
	c := st.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := st.lostLocked(); err != nil {
		return err
	}
	delete(c.checkpoints[st.r], st.step)
	return nil
}

var _ workflow.TxCompleter = (*Client)(nil)

// CompleteInTx implements workflow.TxCompleter with a function-scoped unit
// of work: fn runs with no transaction handle on its ctx
// (workflow.CompletionTx reports ok false), and the step's success with
// output is recorded only when fn returns nil and the attempt still owns
// its step, both before and after fn; otherwise nothing is recorded and
// the error is returned. After a recorded completion the handler's return
// value is ignored, as on sqlstore, so a test on this driver takes the
// same path as production on a SQL database.
func (c *Client) CompleteInTx(ctx context.Context, output []byte, fn func(ctx context.Context) error) error {
	if fn == nil {
		return fmt.Errorf("%w: CompleteInTx needs a function", workflow.ErrInvalid)
	}
	st, ok := ctx.Value(attemptKey{}).(*attemptState)
	if !ok || st.c != c {
		return fmt.Errorf("%w: CompleteInTx must be called with the context of a step this client is running", workflow.ErrInvalid)
	}
	check := func() error {
		if st.settled {
			return fmt.Errorf("%w: step %q of run %s was already completed by this attempt", workflow.ErrInvalid, st.step, st.r.id)
		}
		return st.lostLocked()
	}
	c.mu.Lock()
	err := check()
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if err := fn(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := check(); err != nil {
		return err
	}
	st.settled, st.output = true, bytes.Clone(output)
	return nil
}

// dropCheckpointsLocked forgets every checkpoint of r, which has ended.
func (c *Client) dropCheckpointsLocked(r *run) {
	delete(c.checkpoints, r)
}
