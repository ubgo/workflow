package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ubgo/sqlb"
)

// Workflow versions for rolling deploys (LO-11). A run records the
// Workflow.Version of the client that enqueued it in <prefix>run_version
// (migration 010; no row means version zero, so runs written before 010
// and unversioned runs cost nothing). A client admits, claims and
// advances only runs whose version its own definition runs
// (Workflow.RunsVersion): admission and claims filter in SQL
// (versionWhere), and every transition that applies the client's step
// graph to a run it did not claim (a sleep ending, a signal delivered, a
// run timeout, a failure handler taken over) checks first (runsRun). A
// run no live worker may run waits, as a run of an unregistered workflow
// does.

// tblRunVersion is migration 010's table.
const tblRunVersion = "run_version"

// runVersionDefs is migration 010's table. Its rendered text is frozen
// once released, like ddl.
func runVersionDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblRunVersion, lines: []string{
		"run_id " + t.id + " NOT NULL PRIMARY KEY",
		// version is the run's Workflow.Version, never zero here (zero is
		// no row).
		"version " + t.int + " NOT NULL",
	}}}
}

// insertVersion records a run's version in tx; nothing for version zero.
func (c *Client) insertVersion(ctx context.Context, tx *sql.Tx, reg *registered, id string) error {
	if reg.stub || reg.wf.Version == 0 {
		return nil
	}
	_, err := c.st.exec(ctx, tx, "INSERT INTO {run_version} (run_id, version) VALUES (?, ?)", id, reg.wf.Version)
	return err
}

// runVersion reads run id's version; zero when it has none.
func (c *Client) runVersion(ctx context.Context, x sqlb.DBTX, id string) (int, error) {
	var v int64
	err := c.st.one(ctx, x, "SELECT version FROM {run_version} WHERE run_id = ?", []any{id}, &v)
	if errors.Is(err, sqlb.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("sqlstore: read run version: %w", err)
	}
	return int(v), nil
}

// runsRun returns this client's definition of row's workflow when that
// definition runs row's version, and nil otherwise (or when the workflow
// is not registered here), so the caller leaves the run to a client that
// runs it.
func (c *Client) runsRun(ctx context.Context, x sqlb.DBTX, row *runRow) (*registered, error) {
	reg := c.reg(row.workflow)
	if reg == nil {
		return nil, nil
	}
	v, err := c.runVersion(ctx, x, row.id)
	if err != nil {
		return nil, err
	}
	if !reg.wf.RunsVersion(v) {
		return nil, nil
	}
	return reg, nil
}

// versionFilter is the SQL that limits runs to the versions this client's
// definitions run, built once at Start from the registered workflows.
type versionFilter struct {
	// pinned are the workflows that do not run version zero
	// (MinVersion > 0).
	pinned []any
	// ranges are (workflow, from, to) for every workflow with a Version.
	ranges []any
}

// buildVersionFilter reads the registered definitions. Call with c.mu
// held or before Start returns.
func (c *Client) buildVersionFilter() versionFilter {
	names := make([]string, 0, len(c.wfs))
	for n := range c.wfs {
		names = append(names, n)
	}
	sort.Strings(names)
	var f versionFilter
	for _, n := range names {
		wf := c.wfs[n].wf
		if wf.MinVersion > 0 {
			f.pinned = append(f.pinned, n)
		}
		if wf.Version > 0 {
			f.ranges = append(f.ranges, n, max(wf.MinVersion, 1), wf.Version)
		}
	}
	return f
}

// versionWhere returns the predicate (and its arguments) that a run whose
// id is the expression runID, of the workflow in the column wfCol, has a
// version this client runs: version zero (no row) for a workflow that
// runs it, or a row within a versioned workflow's range.
func (c *Client) versionWhere(runID, wfCol string) (string, []any) {
	f := c.verFilter
	var b strings.Builder
	b.WriteString("((NOT EXISTS (SELECT 1 FROM {run_version} rv WHERE rv.run_id = " + runID + ")")
	if len(f.pinned) > 0 {
		b.WriteString(" AND " + wfCol + " NOT IN (" + inList(len(f.pinned)) + ")")
	}
	b.WriteString(")")
	args := append([]any(nil), f.pinned...)
	if n := len(f.ranges) / 3; n > 0 {
		b.WriteString(" OR EXISTS (SELECT 1 FROM {run_version} rv WHERE rv.run_id = " + runID + " AND (")
		for i := range n {
			if i > 0 {
				b.WriteString(" OR ")
			}
			b.WriteString("(" + wfCol + " = ? AND rv.version BETWEEN ? AND ?)")
		}
		b.WriteString("))")
		args = append(args, f.ranges...)
	}
	b.WriteString(")")
	return b.String(), args
}
