// Package probe carries the few views of sqlstore's unexported state that
// its test module (sqlstore/enginetest) needs: abandoning a client the way
// a crash would, counting transaction attempts, cancelling mid-transaction,
// reading a query plan and swapping a clock.
//
// It lives under internal/, so only packages under sqlstore/ can import
// it; an app cannot reach these. sqlstore sets every variable in its init.
// The client and clock arrive as any because probe cannot import sqlstore
// (sqlstore imports probe); enginetest wraps each one with the concrete
// types, and sqlstore asserts them back, so a wrong type fails the test
// that passed it.
package probe

import (
	"context"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

var (
	// Crash abandons a *sqlstore.Client the way a killed process would.
	Crash func(client any)
	// TxOnRuns runs one transaction on the runs table through the client's
	// retry loop and reports how many times its body ran.
	TxOnRuns func(ctx context.Context, client any) (int, error)
	// CancelMidTx writes in a transaction, then cancels a query while its
	// rows are still open, and returns the transaction's error.
	CancelMidTx func(client any) error
	// RunsPlan is SQLite's EXPLAIN QUERY PLAN of the first-page statement
	// Runs reads q with.
	RunsPlan func(ctx context.Context, client any, q workflow.RunQuery) ([]string, error)
	// SetDatabaseClockLocal replaces the local clock a *sqlstore.DatabaseClock
	// advances on between readings.
	SetDatabaseClockLocal func(k any, c clock.Clock)
)
