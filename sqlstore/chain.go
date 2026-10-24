package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// A run's chain (workflow.Job.Chain, then the edits its steps made) is a
// row of run_chain, written with the run and rewritten in the transaction
// that records the step success making an edit. The run's success hands the
// chain on in the transaction that marks the run succeeded: the next job is
// inserted as a new run carrying the rest of the chain, and the row is
// deleted. So the chain is data at every moment, and a crash between two of
// its jobs loses nothing.
//
// A chained job whose workflow is not registered on the client that
// finishes the run cannot be inserted there (its concurrency and rate keys
// come from the registration). The row is then marked with that workflow's
// name (next_wf), and the dispatcher of any client that registers it hands
// the chain on (handoffChains).
//
// A run that ends failed, discarded or cancelled keeps its row and does not
// hand on: the chain stops. Retrying a failed run (Client.Retry) resumes it,
// since the chain is still there when that run succeeds. Delete and Prune
// remove the row with the run, except a row waiting for a handoff.

// chainDoc is one workflow.Job as stored in run_chain.chain. Only data
// fields exist: workflow.ValidateJob refuses a Job.Retry with functions on
// every chained job, so nothing is lost. RunAt is Unix nanoseconds (0:
// none).
type chainDoc struct {
	Workflow       string            `json:"workflow"`
	Payload        []byte            `json:"payload,omitempty"`
	RunAt          int64             `json:"run_at,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	Queue          string            `json:"queue,omitempty"`
	Retry          *retryDoc         `json:"retry,omitempty"`
	Chain          []chainDoc        `json:"chain,omitempty"`
}

func toChainDocs(jobs []workflow.Job) []chainDoc {
	if len(jobs) == 0 {
		return nil
	}
	out := make([]chainDoc, len(jobs))
	for i, j := range jobs {
		d := chainDoc{Workflow: j.Workflow, Payload: j.Payload, IdempotencyKey: j.IdempotencyKey,
			Labels: j.Labels, Priority: j.Priority, Queue: j.Queue, Chain: toChainDocs(j.Chain)}
		if !j.RunAt.IsZero() {
			d.RunAt = j.RunAt.UnixNano()
		}
		if j.Retry != nil {
			d.Retry = toRetryDoc(j.Retry)
		}
		out[i] = d
	}
	return out
}

func fromChainDocs(docs []chainDoc) []workflow.Job {
	if len(docs) == 0 {
		return nil
	}
	out := make([]workflow.Job, len(docs))
	for i, d := range docs {
		j := workflow.Job{Workflow: d.Workflow, Payload: d.Payload, IdempotencyKey: d.IdempotencyKey,
			Labels: d.Labels, Priority: d.Priority, Queue: d.Queue, Chain: fromChainDocs(d.Chain)}
		if d.RunAt != 0 {
			j.RunAt = unixTime(d.RunAt)
		}
		if d.Retry != nil {
			j.Retry = fromRetryDoc(*d.Retry)
		}
		out[i] = j
	}
	return out
}

// loadChain reads run runID's stored chain; nil when it has none.
func (c *Client) loadChain(ctx context.Context, x sqlb.DBTX, runID string) ([]workflow.Job, error) {
	var raw string
	err := c.st.one(ctx, x, "SELECT chain FROM {run_chain} WHERE run_id = ?", []any{runID}, &raw)
	if errors.Is(err, sqlb.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read the run's chain: %w", err)
	}
	return decodeChain(raw)
}

func decodeChain(raw string) ([]workflow.Job, error) {
	var docs []chainDoc
	if err := json.Unmarshal([]byte(raw), &docs); err != nil {
		return nil, fmt.Errorf("sqlstore: stored chain: %w", err)
	}
	return fromChainDocs(docs), nil
}

// saveChain replaces run runID's chain with chain in tx; an empty chain
// removes the row.
func (c *Client) saveChain(ctx context.Context, tx *sql.Tx, runID string, chain []workflow.Job) error {
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_chain} WHERE run_id = ?", runID); err != nil {
		return err
	}
	if len(chain) == 0 {
		return nil
	}
	b, err := json.Marshal(toChainDocs(chain))
	if err != nil {
		return fmt.Errorf("sqlstore: encode the chain: %w", err)
	}
	_, err = c.st.exec(ctx, tx, "INSERT INTO {run_chain} (run_id, chain, next_wf) VALUES (?, ?, '')", runID, string(b))
	return err
}

// chainEditor is the workflow.ChainEditor on a step attempt's ctx. It reads
// the run's chain only when the step first edits, on the step's goroutine
// while no transaction is open, so attempts that never edit cost no query;
// the edits are buffered in a workflow.ChainEdits and applied by
// applyChainEdits once the attempt succeeds.
//
// Invariant: nothing here reads the pool once the record transaction is
// open. changed runs inside it, so it must never be the first get: used
// records that Prepend or Append ran (and so loaded), and changed answers
// "no edits" without loading when they did not. A load from inside the
// transaction would hold one pool connection while asking for another,
// which deadlocks once every connection is held by such a transaction.
type chainEditor struct {
	once  sync.Once
	used  atomic.Bool
	load  func() ([]workflow.Job, error)
	edits *workflow.ChainEdits
	err   error
}

func (e *chainEditor) get() (*workflow.ChainEdits, error) {
	e.once.Do(func() {
		chain, err := e.load()
		e.err = err
		e.edits = workflow.NewChainEdits(caps, chain)
	})
	return e.edits, e.err
}

// Prepend implements workflow.ChainEditor.
func (e *chainEditor) Prepend(jobs ...workflow.Job) error {
	e.used.Store(true)
	ed, err := e.get()
	if err != nil {
		return err
	}
	return ed.Prepend(jobs...)
}

// Append implements workflow.ChainEditor.
func (e *chainEditor) Append(jobs ...workflow.Job) error {
	e.used.Store(true)
	ed, err := e.get()
	if err != nil {
		return err
	}
	return ed.Append(jobs...)
}

// changed returns the edits when the step made any. It never loads: a step
// that did not edit was never loaded and has none (see chainEditor).
func (e *chainEditor) changed() (*workflow.ChainEdits, bool) {
	if !e.used.Load() {
		return nil, false
	}
	ed, err := e.get()
	if err != nil || !ed.Changed() {
		return nil, false
	}
	return ed, true
}

// newChainEditor is the editor of one attempt of run runID.
func (c *Client) newChainEditor(runID string) *chainEditor {
	return &chainEditor{load: func() ([]workflow.Job, error) {
		return c.loadChain(c.ctx, c.st.db, runID)
	}}
}

// applyChainEdits writes attempt a's chain edits, inside the transaction
// that records its success, so an attempt that fails (and is retried)
// never adds its jobs. The run is touched, so no other step of it edits
// the chain at the same time.
func (c *Client) applyChainEdits(ctx context.Context, tx *sql.Tx, a *attempt) error {
	if a.edits == nil {
		return nil
	}
	ed, ok := a.edits.changed()
	if !ok {
		return nil
	}
	chain, err := c.loadChain(ctx, tx, a.runID)
	if err != nil {
		return err
	}
	return c.saveChain(ctx, tx, a.runID, ed.Apply(chain))
}

// handoffChain hands on the chain of row, which has just succeeded in tx:
// the next job is inserted as a new run carrying the rest, or, when its
// workflow is not registered here, the row is marked for a client that
// registers it.
func (c *Client) handoffChain(ctx context.Context, tx *sql.Tx, row *runRow, o *txOut) error {
	chain, err := c.loadChain(ctx, tx, row.id)
	if err != nil || len(chain) == 0 {
		return err
	}
	next, _ := workflow.NextInChain(chain)
	reg := c.reg(next.Workflow)
	if reg == nil {
		_, err := c.st.exec(ctx, tx, "UPDATE {run_chain} SET next_wf = ? WHERE run_id = ?", next.Workflow, row.id)
		return err
	}
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_chain} WHERE run_id = ?", row.id); err != nil {
		return err
	}
	return c.insertChained(ctx, tx, reg, row.id, next, o)
}

// insertChained inserts next, the job a chain hands on from run from, in
// tx. A job its workflow refuses (a key it requires, a queue it does not
// allow) cannot be enqueued by anyone, so the chain ends there with an
// error in the log rather than failing the run that already succeeded. A
// key an unfinished run holds starts nothing, as Enqueue does.
func (c *Client) insertChained(ctx context.Context, tx *sql.Tx, reg *registered, from string, next workflow.Job, o *txOut) error {
	key, err := idemKey(reg, next)
	if err == nil {
		err = c.checkJob(reg, next)
	}
	if err != nil {
		c.log.Error("sqlstore: the chain ends: its next job is refused", "run", from, "workflow", next.Workflow, "error", err)
		return nil
	}
	o.changed = true
	if key == "" {
		_, err := c.insertRun(ctx, tx, reg, next, "", "")
		return err
	}
	for range maxIdemAttempts {
		_, done, err := c.insertKeyed(ctx, tx, reg, next, key)
		if err != nil || done {
			return err
		}
	}
	return fmt.Errorf("%w: key %q of workflow %q kept changing hands", workflow.ErrDuplicate, key, reg.wf.Name)
}

// handoffChains hands on the chains of succeeded runs whose next job's
// workflow is registered here but was not on the client that finished
// them. Each row is taken by deleting it, so two clients never both hand
// one on.
func (c *Client) handoffChains(ctx context.Context) error {
	type pending struct{ runID, chain string }
	var list []pending
	var p pending
	err := c.st.each(ctx, c.st.db, "SELECT run_id, chain FROM {run_chain} WHERE next_wf IN ("+inList(len(c.names))+") LIMIT ?",
		c.nameArgs(c.fetch), func() error { list = append(list, p); return nil }, &p.runID, &p.chain)
	if err != nil {
		return err
	}
	for _, p := range list {
		chain, err := decodeChain(p.chain)
		if err != nil {
			return err
		}
		next, ok := workflow.NextInChain(chain)
		if !ok {
			continue
		}
		reg := c.reg(next.Workflow)
		if reg == nil {
			continue
		}
		var out txOut
		err = c.st.tx(ctx, func(tx *sql.Tx) error {
			out = txOut{}
			n, err := c.st.execN(ctx, tx, "DELETE FROM {run_chain} WHERE run_id = ? AND next_wf <> ''", p.runID)
			if err != nil || n == 0 {
				return err // another client took it
			}
			return c.insertChained(ctx, tx, reg, p.runID, next, &out)
		})
		if err != nil {
			return err
		}
		c.afterTx(ctx, out)
	}
	return nil
}

// toRetryDoc and fromRetryDoc convert a Job.Retry to and from its stored
// form (data fields only).
func toRetryDoc(p *workflow.RetryPolicy) *retryDoc {
	return &retryDoc{
		Max: p.Max, Initial: int64(p.Backoff.Initial), Factor: p.Backoff.Factor, MaxDelay: int64(p.Backoff.Max),
		Jitter: p.Backoff.Jitter, Until: int64(p.Until), MaxErrors: p.MaxErrors, DiscardOnExhaust: p.DiscardOnExhaust,
	}
}

func fromRetryDoc(d retryDoc) *workflow.RetryPolicy {
	return &workflow.RetryPolicy{
		Max:              d.Max,
		Backoff:          workflow.Backoff{Initial: time.Duration(d.Initial), Factor: d.Factor, Max: time.Duration(d.MaxDelay), Jitter: d.Jitter},
		Until:            time.Duration(d.Until),
		MaxErrors:        d.MaxErrors,
		DiscardOnExhaust: d.DiscardOnExhaust,
	}
}
