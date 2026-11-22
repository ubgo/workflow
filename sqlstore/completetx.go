package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/ubgo/workflow"
)

// attemptKey is the context key under which the worker puts the attempt a
// step handler runs as, so CompleteTx can find the step's lease token. It is
// unexported: only this package's workers set it.
type attemptKey struct{}

// attemptCtx is the value under attemptKey: the client that claimed the
// attempt and the attempt itself.
type attemptCtx struct {
	c *Client
	a *attempt
}

// completion is what CompleteTx staged for one attempt: the decisions its
// transaction made that must be carried out after the commit (txOut). The
// worker reads it when the handler returns; it applies only when the
// app's transaction committed, which the worker checks in the database.
type completion struct {
	mu  sync.Mutex
	out *txOut // the latest CompleteTx's decisions; nil: none was called
}

func (p *completion) set(o txOut) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.out = &o
}

func (p *completion) get() *txOut {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out
}

// withAttempt puts a on the step context ctx for CompleteTx.
func (c *Client) withAttempt(ctx context.Context, a *attempt) context.Context {
	a.completion = &completion{}
	return context.WithValue(ctx, attemptKey{}, attemptCtx{c: c, a: a})
}

// CompleteTx records the success of the step attempt running on ctx, with
// output as the step's output, inside the app's transaction tx: the app's
// side effect and the step's completion commit together, or neither does.
// Call it from a step handler run by this client, with the handler's ctx,
// then commit tx and return.
//
// It writes what a returned success writes: the step succeeds with output,
// the steps that waited on it become ready, and when it was the run's last
// step the run succeeds (its output, chain hand-off and idempotency key
// release included). Chain edits the handler made before the call
// (workflow.PrependToChain, AppendToChain) are written with it.
//
// The write is fenced by the attempt's lease: it locks the step row and
// changes it only while the step is still running under this attempt's
// lease token, so a step another client took over (after this attempt's
// lease expired) or that a cancel or failure ended elsewhere is refused
// with an error matching ErrLeaseLost, and so is an attempt whose own
// heartbeat already gave the lease up. Until tx ends, no other client can
// claim, renew or end the step: they wait on the row lock (Postgres,
// MySQL) or the writer lock (SQLite).
//
// After the commit the step is done. What the handler returns then is
// ignored: no second completion, no retry, no failure, and the run-end
// hooks fire once. The after-commit work (lifecycle Finished events, waking
// the workers for the next step) runs when the handler returns. If tx rolls
// back instead, nothing was recorded and the handler's return value is
// recorded as usual. The step's context may be cancelled shortly after the
// commit (the lease renewal finds the step no longer running): return
// promptly. Chain edits made after the call are not written.
//
// output is stored as given: step middleware that rewrites a step's output
// on its way out of the handler (a codec, encryption, the claim check) does
// not see it, so with such middleware pass output in its stored form, or
// return the output instead of completing in the transaction.
//
// tx must be open on the *sql.DB this client was built with. It refuses
// with workflow.ErrInvalid a nil tx, a ctx that is not a step handler's
// context of this client (another client's step, RunInline, a test that
// calls the handler directly) and a second completion of the same attempt.
// An error leaves tx open; the caller decides whether to roll it back.
// Runs on every engine sqlstore supports: SQLite, Postgres and MySQL.
//
//ds:def id=sql-client-completetx-q8d5sbtc owner=@khanakia stability=stable
func (c *Client) CompleteTx(ctx context.Context, tx *sql.Tx, output []byte) error {
	if tx == nil {
		return fmt.Errorf("%w: CompleteTx needs a transaction", workflow.ErrInvalid)
	}
	v, ok := ctx.Value(attemptKey{}).(attemptCtx)
	if !ok || v.c != c {
		return fmt.Errorf("%w: CompleteTx must be called with the context of a step this client is running", workflow.ErrInvalid)
	}
	a := v.a
	if errors.Is(context.Cause(ctx), ErrLeaseLost) {
		return fmt.Errorf("sqlstore: complete step %q of run %s: %w", a.name, a.runID, ErrLeaseLost)
	}
	reg := c.reg(a.workflow)
	if reg == nil {
		return fmt.Errorf("%w: workflow %q is not registered", workflow.ErrNotFound, a.workflow)
	}
	var o txOut
	row, err := c.touch(ctx, tx, a.runID)
	if errors.Is(err, workflow.ErrNotFound) {
		return fmt.Errorf("sqlstore: complete step %q: run %s is gone: %w", a.name, a.runID, ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("sqlstore: complete step %q of run %s: %w", a.name, a.runID, err)
	}
	if row.status != runActive {
		return fmt.Errorf("sqlstore: complete step %q: run %s is %s: %w", a.name, a.runID, row.status, ErrLeaseLost)
	}
	// The fence: the step row changes only while it is this attempt's, and
	// the change locks it until tx ends. Moving status off running is a
	// real change on every engine, so MySQL's changed-rows count is 1.
	n, err := c.st.execN(ctx, tx, "UPDATE {steps} SET status = ? WHERE id = ? AND lease_token = ? AND status = ?",
		stepSucceeded, a.stepID, a.token, stepRunning)
	if err != nil {
		return fmt.Errorf("sqlstore: complete step %q of run %s: %w", a.name, a.runID, err)
	}
	if n == 0 {
		if done, err := c.completedBy(ctx, tx, a); err != nil {
			return err
		} else if done {
			return fmt.Errorf("%w: step %q of run %s was already completed by this attempt", workflow.ErrInvalid, a.name, a.runID)
		}
		return fmt.Errorf("sqlstore: complete step %q: the step is no longer this attempt's: %w", a.name, ErrLeaseLost)
	}
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {step_cancel} WHERE step_id = ?", a.stepID); err != nil {
		return err
	}
	if err := c.applyChainEdits(ctx, tx, a); err != nil {
		return err
	}
	if err := c.completeStep(ctx, tx, reg, row, a.name, output, &o); err != nil {
		return err
	}
	// completeStep cleared the lease token; keeping this attempt's token on
	// the succeeded row is how the worker later tells that this attempt's
	// completion committed (completedBy). Nothing else reads the token of a
	// step that is not running.
	if _, err := c.st.exec(ctx, tx, "UPDATE {steps} SET lease_token = ? WHERE id = ?", a.token, a.stepID); err != nil {
		return err
	}
	a.completion.set(o)
	return nil
}

// completedBy reports whether a's step is succeeded under a's lease token,
// which only CompleteTx writes: true once the app's transaction that
// completed it committed (or, read in that transaction, before).
func (c *Client) completedBy(ctx context.Context, tx *sql.Tx, a *attempt) (bool, error) {
	var n int64
	if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {steps} WHERE id = ? AND lease_token = ? AND status = ?",
		[]any{a.stepID, a.token, stepSucceeded}, &n); err != nil {
		return false, fmt.Errorf("sqlstore: read step %q of run %s: %w", a.name, a.runID, err)
	}
	return n == 1, nil
}

// stagedCompletion returns, inside the worker's transaction tx that has
// touched a's run, the after-commit decisions of a CompleteTx whose
// transaction committed; nil when the handler did not call it or its
// transaction rolled back, so the worker records the handler's result.
func (c *Client) stagedCompletion(ctx context.Context, tx *sql.Tx, a *attempt) (*txOut, error) {
	staged := a.completion.get()
	if staged == nil {
		return nil, nil
	}
	done, err := c.completedBy(ctx, tx, a)
	if err != nil || !done {
		return nil, err
	}
	return staged, nil
}

// recordCompleted carries out the after-commit work of a's CompleteTx when
// its transaction committed, for an attempt whose outcome is otherwise not
// written (its lease was lost after the commit).
func (c *Client) recordCompleted(a *attempt) {
	if a.completion.get() == nil {
		return
	}
	ctx := c.ctx
	var staged *txOut
	err := c.st.tx(ctx, func(tx *sql.Tx) error {
		staged = nil
		if _, err := c.touch(ctx, tx, a.runID); err != nil {
			if errors.Is(err, workflow.ErrNotFound) {
				return nil
			}
			return err
		}
		var err error
		staged, err = c.stagedCompletion(ctx, tx, a)
		return err
	})
	if err != nil {
		if ctx.Err() == nil {
			c.log.Error("sqlstore: read a step completed in a transaction", "run", a.runID, "step", a.name, "error", err)
		}
		return
	}
	if staged != nil {
		c.afterTx(ctx, *staged)
	}
}
