package enginetest

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// ctxFixture is one started client whose workflow "pay" has a step "charge"
// that runs body, then a step "receipt" that records its input, and whose
// workflow "solo" has one step "only" that runs body too, plus an app table
// (<prefix>orders) beside the queue.
type ctxFixture struct {
	c        *sqlstore.Client
	db       *sql.DB
	orders   string
	stepsTbl string
	attempts atomic.Int64 // executions of "charge"
	solo     atomic.Int64 // executions of "only"
	finished atomic.Int64 // run-finished events
	// soloEnds counts the lifecycle Finished events of "solo" runs: a run
	// whose last step completed in the app's transaction reports its end
	// from the after-commit work the worker carries out.
	soloEnds atomic.Int64
	mu       sync.Mutex
	receipt  []byte // the input "receipt" ran with
}

func newCompleteFixture(t *testing.T, e engine, body func(f *ctxFixture, ctx context.Context, attempt int64) ([]byte, error)) *ctxFixture {
	t.Helper()
	db, prefix := e.open(t)
	f := &ctxFixture{db: db, orders: e.dialect.QuoteIdent(prefix + "orders"), stepsTbl: e.dialect.QuoteIdent(prefix + "steps")}
	if _, err := db.Exec("CREATE TABLE " + f.orders + " (id VARCHAR(64) NOT NULL PRIMARY KEY)"); err != nil {
		t.Fatalf("create app table: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + f.orders) })
	f.c = newClient(t, e, db, prefix)
	retry := workflow.RetryPolicy{Max: 2}
	if err := f.c.Register(workflow.Workflow{
		Name: "pay",
		Steps: []workflow.Step{
			{Name: "charge", Retry: &retry, Run: func(ctx context.Context, in []byte) ([]byte, error) {
				return body(f, ctx, f.attempts.Add(1))
			}},
			{Name: "receipt", After: []string{"charge"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
				f.mu.Lock()
				f.receipt = append([]byte(nil), in...)
				f.mu.Unlock()
				return in, nil
			}},
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := f.c.Register(workflow.Workflow{Name: "solo", Steps: []workflow.Step{{Name: "only", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		return body(f, ctx, f.solo.Add(1))
	}}}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := f.c.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) {
		if ev.Kind == workflow.LifecycleFinished && ev.Workflow == "solo" {
			f.soloEnds.Add(1)
		}
	}); err != nil {
		t.Fatalf("OnLifecycle: %v", err)
	}
	if err := f.c.OnRunFinished(func(context.Context, workflow.RunEvent) { f.finished.Add(1) }); err != nil {
		t.Fatalf("OnRunFinished: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := f.c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return f
}

// orderRows counts the app's committed orders.
func (f *ctxFixture) orderRows(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.db.QueryRow("SELECT COUNT(*) FROM " + f.orders).Scan(&n); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	return n
}

// awaitEnd waits for the run to end and returns its status.
func (f *ctxFixture) awaitEnd(t *testing.T, ref workflow.RunRef) workflow.Status {
	t.Helper()
	var st workflow.Status
	waitFor(t, 10*time.Second, "the run to end", func() bool {
		var err error
		st, err = f.c.Status(context.Background(), ref)
		return err == nil && st.IsTerminal()
	})
	return st
}

// chargeInTx is the handler body the commit and rollback cases share: an
// app write and CompleteTx in one transaction, ended by end.
func chargeInTx(f *ctxFixture, ctx context.Context, output string, end func(*sql.Tx) error) error {
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+f.orders+" (id) VALUES ('"+randHex(8)+"')"); err != nil {
		return err
	}
	if err := f.c.CompleteTx(ctx, tx, []byte(output)); err != nil {
		return err
	}
	return end(tx)
}

// TestCompleteTxCommit proves a step completed inside the app's committed
// transaction is done exactly once (TX-04): the handler's later error is
// ignored (no retry, no failure), the next step gets the output written in
// the transaction, the run-finished hook fires once, and the app's row is
// there.
//
//ds:def id=enginetest-testcompletetxcommit-t6a53up6 owner=@khanakia stability=stable
func TestCompleteTxCommit(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			var completeErr atomic.Value
			f := newCompleteFixture(t, e, func(f *ctxFixture, ctx context.Context, attempt int64) ([]byte, error) {
				if err := chargeInTx(f, ctx, `"charged-in-tx"`, func(tx *sql.Tx) error { return tx.Commit() }); err != nil {
					completeErr.Store(err)
					return nil, err
				}
				// After the commit the step is done: this error must not
				// fail or retry it, and this output must not replace it.
				return []byte(`"returned"`), errors.New("boom after the commit")
			})
			ref, err := f.c.Enqueue(context.Background(), workflow.Job{Workflow: "pay"})
			if err != nil {
				t.Fatal(err)
			}
			st := f.awaitEnd(t, ref)
			if v := completeErr.Load(); v != nil {
				t.Fatalf("CompleteTx: %v", v)
			}
			if st != workflow.StatusSucceeded {
				t.Fatalf("run %s, want succeeded", st)
			}
			if n := f.attempts.Load(); n != 1 {
				t.Fatalf("charge ran %d times, want 1 (a retry after the commit)", n)
			}
			f.mu.Lock()
			got := string(f.receipt)
			f.mu.Unlock()
			if got != `"charged-in-tx"` {
				t.Fatalf("receipt input %q, want the output CompleteTx wrote", got)
			}
			if n := f.orderRows(t); n != 1 {
				t.Fatalf("%d app rows, want 1", n)
			}
			waitFor(t, 5*time.Second, "the run-finished hook", func() bool { return f.finished.Load() >= 1 })
			time.Sleep(10 * testPoll) // a second delivery would land here
			if n := f.finished.Load(); n != 1 {
				t.Fatalf("run-finished hook fired %d times, want 1", n)
			}

			// The run's last step: the run ends in the app's transaction,
			// and its Finished event comes from the after-commit work.
			solo, err := f.c.Enqueue(context.Background(), workflow.Job{Workflow: "solo"})
			if err != nil {
				t.Fatal(err)
			}
			if st := f.awaitEnd(t, solo); st != workflow.StatusSucceeded {
				t.Fatalf("solo run %s, want succeeded", st)
			}
			out, err := f.c.Result(context.Background(), solo)
			if err != nil || string(out) != `"charged-in-tx"` {
				t.Fatalf("solo result %q, %v, want the output CompleteTx wrote", out, err)
			}
			waitFor(t, 5*time.Second, "the solo run's Finished event", func() bool { return f.soloEnds.Load() >= 1 })
			time.Sleep(10 * testPoll)
			if n, m := f.solo.Load(), f.soloEnds.Load(); n != 1 || m != 1 {
				t.Fatalf("solo step ran %d times with %d Finished events, want 1 and 1", n, m)
			}
		})
	}
}

// TestCompleteTxRollback proves a rolled-back CompleteTx records nothing:
// the handler's own return value is recorded as usual, a success with its
// output, or a failure that is retried.
//
//ds:def id=enginetest-testcompletetxrollback-hyf5acd3 owner=@khanakia stability=stable
func TestCompleteTxRollback(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name+"/returns output", func(t *testing.T) {
			f := newCompleteFixture(t, e, func(f *ctxFixture, ctx context.Context, attempt int64) ([]byte, error) {
				if err := chargeInTx(f, ctx, `"lost"`, func(tx *sql.Tx) error { return tx.Rollback() }); err != nil {
					return nil, err
				}
				return []byte(`"returned"`), nil
			})
			ref, err := f.c.Enqueue(context.Background(), workflow.Job{Workflow: "pay"})
			if err != nil {
				t.Fatal(err)
			}
			if st := f.awaitEnd(t, ref); st != workflow.StatusSucceeded {
				t.Fatalf("run %s, want succeeded", st)
			}
			f.mu.Lock()
			got := string(f.receipt)
			f.mu.Unlock()
			if got != `"returned"` {
				t.Fatalf("receipt input %q, want the handler's returned output", got)
			}
			if n := f.orderRows(t); n != 0 {
				t.Fatalf("%d app rows after a rollback, want 0", n)
			}
		})
		t.Run(e.name+"/returns an error", func(t *testing.T) {
			f := newCompleteFixture(t, e, func(f *ctxFixture, ctx context.Context, attempt int64) ([]byte, error) {
				if attempt == 1 {
					if err := chargeInTx(f, ctx, `"lost"`, func(tx *sql.Tx) error { return tx.Rollback() }); err != nil {
						return nil, err
					}
					return nil, errors.New("declined")
				}
				return nil, chargeInTx(f, ctx, `"second"`, func(tx *sql.Tx) error { return tx.Commit() })
			})
			ref, err := f.c.Enqueue(context.Background(), workflow.Job{Workflow: "pay"})
			if err != nil {
				t.Fatal(err)
			}
			if st := f.awaitEnd(t, ref); st != workflow.StatusSucceeded {
				t.Fatalf("run %s, want succeeded", st)
			}
			if n := f.attempts.Load(); n != 2 {
				t.Fatalf("charge ran %d times, want 2 (the rolled-back attempt's error is retried)", n)
			}
			f.mu.Lock()
			got := string(f.receipt)
			f.mu.Unlock()
			if got != `"second"` {
				t.Fatalf("receipt input %q, want the second attempt's output", got)
			}
			if n := f.orderRows(t); n != 1 {
				t.Fatalf("%d app rows, want 1", n)
			}
		})
	}
}

// TestCompleteTxRefusals proves CompleteTx refuses with ErrInvalid outside a
// step of this client and on a second completion of one attempt, and with
// ErrLeaseLost once the step is no longer the attempt's (taken over by
// another lease, or its run cancelled); each refusal leaves the app's
// transaction usable.
//
//ds:def id=enginetest-testcompletetxrefusals-86xvqrf7 owner=@khanakia stability=stable
func TestCompleteTxRefusals(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name+"/outside a step", func(t *testing.T) {
			f := newCompleteFixture(t, e, func(*ctxFixture, context.Context, int64) ([]byte, error) { return nil, nil })
			ctx := context.Background()
			tx, err := f.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if err := f.c.CompleteTx(ctx, tx, nil); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("outside a step: %v, want ErrInvalid", err)
			}
			if err := f.c.CompleteTx(ctx, nil, nil); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("nil tx: %v, want ErrInvalid", err)
			}
		})
		t.Run(e.name+"/another client's step and twice", func(t *testing.T) {
			var other atomic.Pointer[sqlstore.Client]
			var foreign, again atomic.Value
			f := newCompleteFixture(t, e, func(f *ctxFixture, ctx context.Context, attempt int64) ([]byte, error) {
				tx, err := f.db.BeginTx(ctx, nil)
				if err != nil {
					return nil, err
				}
				defer func() { _ = tx.Rollback() }()
				foreign.Store(errBox{other.Load().CompleteTx(ctx, tx, nil)})
				if err := f.c.CompleteTx(ctx, tx, []byte(`"once"`)); err != nil {
					return nil, err
				}
				again.Store(errBox{f.c.CompleteTx(ctx, tx, []byte(`"twice"`))})
				return nil, tx.Commit()
			})
			oc, err := sqlstore.New(f.db, e.dialect, sqlstore.WithTablePrefix("other_"))
			if err != nil {
				t.Fatal(err)
			}
			other.Store(oc)
			ref, err := f.c.Enqueue(context.Background(), workflow.Job{Workflow: "pay"})
			if err != nil {
				t.Fatal(err)
			}
			if st := f.awaitEnd(t, ref); st != workflow.StatusSucceeded {
				t.Fatalf("run %s, want succeeded", st)
			}
			if b, _ := foreign.Load().(errBox); !errors.Is(b.err, workflow.ErrInvalid) {
				t.Fatalf("another client's CompleteTx: %v, want ErrInvalid", b.err)
			}
			if b, _ := again.Load().(errBox); !errors.Is(b.err, workflow.ErrInvalid) {
				t.Fatalf("second CompleteTx of one attempt: %v, want ErrInvalid", b.err)
			}
			f.mu.Lock()
			got := string(f.receipt)
			f.mu.Unlock()
			if got != `"once"` {
				t.Fatalf("receipt input %q, want the first completion's", got)
			}
		})
		t.Run(e.name+"/lease taken over", func(t *testing.T) {
			var lost atomic.Value
			f := newCompleteFixture(t, e, func(f *ctxFixture, ctx context.Context, attempt int64) ([]byte, error) {
				if attempt > 1 {
					return []byte(`"retaken"`), nil
				}
				// Another client's claim, as it would land after this
				// attempt's lease expired: a new token on the step.
				if _, err := f.db.ExecContext(ctx, "UPDATE "+f.stepsTbl+" SET lease_token = 'lease_other', lease_until = 0 WHERE name = 'charge'"); err != nil {
					return nil, err
				}
				err := chargeInTx(f, context.WithoutCancel(ctx), `"stale"`, func(tx *sql.Tx) error { return tx.Commit() })
				lost.Store(errBox{err})
				return nil, err
			})
			ref, err := f.c.Enqueue(context.Background(), workflow.Job{Workflow: "pay"})
			if err != nil {
				t.Fatal(err)
			}
			if st := f.awaitEnd(t, ref); st != workflow.StatusSucceeded {
				t.Fatalf("run %s, want succeeded once the step was retaken", st)
			}
			waitFor(t, 5*time.Second, "the first attempt's CompleteTx", func() bool { return lost.Load() != nil })
			if b, _ := lost.Load().(errBox); !errors.Is(b.err, sqlstore.ErrLeaseLost) {
				t.Fatalf("CompleteTx after a takeover: %v, want ErrLeaseLost", b.err)
			}
			f.mu.Lock()
			got := string(f.receipt)
			f.mu.Unlock()
			if got != `"retaken"` {
				t.Fatalf("receipt input %q, want the retaken attempt's output", got)
			}
			if n := f.orderRows(t); n != 0 {
				t.Fatalf("%d app rows, want 0 (the refused tx was rolled back)", n)
			}
		})
		t.Run(e.name+"/run cancelled", func(t *testing.T) {
			var lost atomic.Value
			var ref workflow.RunRef
			refSet := make(chan struct{})
			f := newCompleteFixture(t, e, func(f *ctxFixture, ctx context.Context, attempt int64) ([]byte, error) {
				<-refSet
				if err := f.c.Cancel(context.WithoutCancel(ctx), ref); err != nil {
					return nil, err
				}
				err := chargeInTx(f, context.WithoutCancel(ctx), `"late"`, func(tx *sql.Tx) error { return tx.Commit() })
				lost.Store(errBox{err})
				return nil, err
			})
			var err error
			ref, err = f.c.Enqueue(context.Background(), workflow.Job{Workflow: "pay"})
			if err != nil {
				t.Fatal(err)
			}
			close(refSet)
			if st := f.awaitEnd(t, ref); st != workflow.StatusCancelled {
				t.Fatalf("run %s, want cancelled", st)
			}
			waitFor(t, 5*time.Second, "the handler's CompleteTx", func() bool { return lost.Load() != nil })
			if b, _ := lost.Load().(errBox); !errors.Is(b.err, sqlstore.ErrLeaseLost) {
				t.Fatalf("CompleteTx on a cancelled run: %v, want ErrLeaseLost", b.err)
			}
		})
	}
}

// errBox lets an atomic.Value hold a nil error.
type errBox struct{ err error }
