// Command transactional-enqueue writes a run inside the app's own database
// transaction with sqlstore.Client.EnqueueTx: the run exists if and only if
// the app's writes commit.
//
// It is the program behind docs/reliability/transactional-enqueue.md: the
// guide copies its regions (marked "snippet:") and task docs:snippets fails
// when they drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "transactional-enqueue:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	dir, err := os.MkdirTemp("", "transactional-enqueue-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT NOT NULL)"); err != nil {
		return err
	}

	shipped := make(chan string, 4)
	// An hour between polls, so only Wake gets a committed run going
	// quickly: the default is a second.
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(time.Hour))
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil {
		return err
	}
	if err := c.Register(workflow.Workflow{Name: "ship", Steps: []workflow.Step{{
		Name: "ship",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			shipped <- string(in)
			return nil, nil
		},
	}}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	fmt.Fprintln(out, "-- commit")
	// snippet:commit
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // a no-op once committed
	if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id, item) VALUES (1, 'book')"); err != nil {
		return err
	}
	ref, err := c.EnqueueTx(ctx, tx, workflow.Job{
		Workflow:       "ship",
		Payload:        []byte(`{"order":1}`),
		IdempotencyKey: "order-1",
	})
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	c.Wake() // admit it now instead of at the next poll
	// snippet:end
	fmt.Fprintln(out, "shipped", <-shipped)
	st, err := waitTerminal(ctx, c, ref)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "run:", st)

	fmt.Fprintln(out, "-- roll back")
	// snippet:rollback
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id, item) VALUES (2, 'lamp')"); err != nil {
		return err
	}
	ref, err = c.EnqueueTx(ctx, tx, workflow.Job{Workflow: "ship", Payload: []byte(`{"order":2}`)})
	if err != nil {
		return err
	}
	if err := tx.Rollback(); err != nil { // the app decides not to commit
		return err
	}
	_, err = c.Status(ctx, ref)
	fmt.Fprintln(out, "run after rollback is ErrNotFound:", errors.Is(err, workflow.ErrNotFound))
	// snippet:end
	var orders int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders").Scan(&orders); err != nil {
		return err
	}
	fmt.Fprintln(out, "orders stored:", orders)

	fmt.Fprintln(out, "-- same key twice in one transaction")
	if err := sameKeyTwice(ctx, out, db, c); err != nil {
		return err
	}
	fmt.Fprintln(out, "shipped", <-shipped)
	return nil
}

// sameKeyTwice enqueues one key twice in one transaction: the second call
// returns the first run and the transaction stays usable.
func sameKeyTwice(ctx context.Context, out io.Writer, db *sql.DB, c *sqlstore.Client) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// snippet:same-key
	job := workflow.Job{Workflow: "ship", Payload: []byte(`{"order":3}`), IdempotencyKey: "order-3"}
	first, err := c.EnqueueTx(ctx, tx, job)
	if err != nil {
		return err
	}
	second, err := c.EnqueueTx(ctx, tx, job) // returns the run written above
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintln(out, "second call returned the first run:", first == second)
	if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id, item) VALUES (3, 'desk')"); err != nil {
		return err
	}
	fmt.Fprintln(out, "transaction still usable: true")
	if err := tx.Commit(); err != nil {
		return err
	}
	c.Wake()
	return nil
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
