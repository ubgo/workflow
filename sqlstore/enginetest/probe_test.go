package enginetest

import (
	"context"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/sqlstore/internal/probe"
)

// The typed side of sqlstore/internal/probe: each wrapper hands the probe
// the concrete value sqlstore's init expects.

// crash abandons c the way a killed process would, for the conformance
// suite's crash case and the lease tests.
func crash(c *sqlstore.Client) { probe.Crash(c) }

// txOnRuns runs one transaction that touches the runs table through the
// client's retry loop and reports how many times its body ran.
func txOnRuns(ctx context.Context, c *sqlstore.Client) (int, error) { return probe.TxOnRuns(ctx, c) }

// cancelMidTx cancels a query while its rows are open inside a write
// transaction; see probe.CancelMidTx.
func cancelMidTx(c *sqlstore.Client) error { return probe.CancelMidTx(c) }

// runsPlan is SQLite's EXPLAIN QUERY PLAN of the statement Runs reads its
// first page of q with, one line per plan step.
func runsPlan(ctx context.Context, c *sqlstore.Client, q workflow.RunQuery) ([]string, error) {
	return probe.RunsPlan(ctx, c, q)
}

// setDatabaseClockLocal replaces the local clock k advances on between
// readings.
func setDatabaseClockLocal(k *sqlstore.DatabaseClock, c clock.Clock) {
	probe.SetDatabaseClockLocal(k, c)
}
