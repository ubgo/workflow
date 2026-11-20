// Command complete-in-transaction completes a running step inside the
// app's own database transaction with sqlstore.Client.CompleteTx: the
// payment row and the step's success commit together, so a crash between
// "payment stored" and "step done" can no longer charge twice.
//
// It is the program behind docs/reliability/complete-in-transaction.md:
// the guide copies its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// statusPoll is how often waitTerminal re-reads a run's status: short, so
// the demo finishes quickly; a real app waits on the run-finished hook.
const statusPoll = 10 * time.Millisecond

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "complete-in-transaction:", err)
		os.Exit(1)
	}
}

// errDeclined is the card processor's answer in the second run.
var errDeclined = errors.New("card declined")

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) (err error) {
	db, err := driversqlite.Memory()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE payments (order_id TEXT PRIMARY KEY, amount INTEGER NOT NULL)"); err != nil {
		return err
	}
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(10*time.Millisecond))
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil {
		return err
	}

	var attempts atomic.Int64
	emailed := make(chan string, 2)
	if err := c.Register(workflow.Workflow{Name: "checkout", Steps: []workflow.Step{
		{Name: "charge", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			attempts.Add(1)
			return nil, charge(ctx, db, c, in)
		}},
		{Name: "email", After: []string{"charge"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			emailed <- string(in)
			return nil, nil
		}},
	}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	fmt.Println("-- commit")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "checkout", Payload: []byte("o1")})
	if err != nil {
		return err
	}
	fmt.Println("email got", <-emailed)
	st, err := waitTerminal(ctx, c, ref)
	if err != nil {
		return err
	}
	fmt.Println("run:", st)
	fmt.Println("charge attempts:", attempts.Load())

	fmt.Println("-- roll back")
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "checkout", Payload: []byte("o2")})
	if err != nil {
		return err
	}
	if st, err = waitTerminal(ctx, c, ref); err != nil {
		return err
	}
	fmt.Println("run:", st)
	var payments int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM payments").Scan(&payments); err != nil {
		return err
	}
	fmt.Println("payments stored:", payments)

	fmt.Println("-- outside a step")
	// snippet:refusal
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	err = c.CompleteTx(ctx, tx, nil) // ctx is not a step's context
	fmt.Println("refused with ErrInvalid:", errors.Is(err, workflow.ErrInvalid))
	// snippet:end
	return tx.Rollback()
}

// charge stores the payment and completes the step in one transaction. Order
// o2 is declined after the payment row was written, so the transaction
// rolls back and the step fails for good instead.
func charge(ctx context.Context, db *sql.DB, c *sqlstore.Client, in []byte) (err error) {
	// snippet:complete
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(); !errors.Is(rbErr, sql.ErrTxDone) {
			err = errors.Join(err, rbErr)
		}
	}()
	order := string(in)
	if _, err := tx.ExecContext(ctx, "INSERT INTO payments (order_id, amount) VALUES (?, 4200)", order); err != nil {
		return err
	}
	if order == "o2" {
		return workflow.Permanent(errDeclined) // the deferred Rollback drops the payment row
	}
	// The step succeeds with this output if and only if tx commits.
	if err := c.CompleteTx(ctx, tx, []byte(`{"order":"`+order+`","paid":4200}`)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// snippet:end
	// From here the step is done: an error now is ignored, never retried.
	return errors.New("lost connection to the receipt printer")
}

// waitTerminal polls a run's status until it is terminal.
func waitTerminal(ctx context.Context, c workflow.Client, ref workflow.RunRef) (workflow.Status, error) {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st.IsTerminal() {
			return st, err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(statusPoll):
		}
	}
}
