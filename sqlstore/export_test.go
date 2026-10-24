package sqlstore

import (
	"context"
	"database/sql"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// Crash abandons c the way a killed process would (see Client.abandon),
// for the conformance suite's crash case and the lease tests.
func Crash(c *Client) { c.abandon() }

// TxOnRuns runs one transaction that touches the runs table through the
// client's retry loop and reports how many times its body ran, for the
// transaction-attempt tests.
func TxOnRuns(ctx context.Context, c *Client) (int, error) {
	attempts := 0
	err := c.st.tx(ctx, func(tx *sql.Tx) error {
		attempts++
		_, err := c.st.exec(ctx, tx, "DELETE FROM {runs} WHERE id = ?", "none")
		return err
	})
	return attempts, err
}

// CancelMidTx opens a transaction through the client's own path, writes
// (so it holds the write lock), starts a query that returns several rows
// and cancels ctx while those rows are still open, the shape of the
// modernc.org/sqlite v1.39.0 leak: a cancelled query whose rows were never
// released kept its transaction, and with it the database's lock, alive.
// It returns the transaction's error: the cancel, or the commit finding
// the transaction already rolled back.
func CancelMidTx(c *Client) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return c.st.tx(ctx, func(tx *sql.Tx) error {
		if _, err := c.st.exec(ctx, tx, "INSERT INTO {locks} (name, n) VALUES ('hang:a', 0), ('hang:b', 0), ('hang:c', 0)"); err != nil {
			return err
		}
		var name string
		return c.st.each(ctx, tx, "SELECT name FROM {locks}", nil, func() error {
			cancel()
			return nil
		}, &name)
	})
}

// RunsPlan is SQLite's EXPLAIN QUERY PLAN of the statement Runs reads its
// first page of q with, one line per plan step, so a test can prove a
// filter reads an index.
func RunsPlan(ctx context.Context, c *Client, q workflow.RunQuery) ([]string, error) {
	where, args := c.runFilter(q)
	var out []string
	var id, parent, unused int
	var detail string
	err := c.st.each(ctx, c.st.db, "EXPLAIN QUERY PLAN "+infoQuery(where), append(args, q.PageLimit()+1),
		func() error { out = append(out, detail); return nil }, &id, &parent, &unused, &detail)
	return out, err
}

// SetDatabaseClockLocal replaces the local clock a DatabaseClock advances
// on between readings, so a test can move it with a fake.
func SetDatabaseClockLocal(k *DatabaseClock, c clock.Clock) { k.local = c }
