package sqlitetest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
)

// The fault-injecting wrapper: a database/sql driver.Connector over the
// real SQLite driver that, per rule, fails a transaction's begin or commit,
// a statement, a result set's row read or close, or rewrites one column of
// a result set to a value the store cannot read. Every other call reaches
// SQLite unchanged, so a test drives the store over real data and fails
// exactly the step it names.

// op is the call a fault fires on.
type op int

const (
	opBegin   op = iota // BeginTx fails
	opCommit            // Commit fails (the transaction is rolled back)
	opExec              // a statement's Exec fails
	opQuery             // a statement's Query fails
	opNext              // the first row read of a result set fails
	opClose             // closing a result set fails
	opCorrupt           // every row of a result set has one column replaced
)

// fault is one rule. A fault matches a statement holding match ("" matches
// every statement, and begin and commit, which have none); the first skip
// matching calls pass, then it fires times times (0: on every later call).
type fault struct {
	op    op
	match string
	skip  int
	times int
	err   error
	// col and val are the column opCorrupt replaces and its new value.
	col int
	val driver.Value

	seen  int // matching calls
	fired int // calls it failed
}

// injector holds the rules of one database.
type injector struct {
	mu     sync.Mutex
	faults []*fault
	log    []string // every statement run, in order
}

// add installs f and returns it, so a test can read its counters.
func (in *injector) add(f fault) *fault {
	in.mu.Lock()
	defer in.mu.Unlock()
	p := &f
	in.faults = append(in.faults, p)
	return p
}

// reset removes every rule and clears the statement log.
func (in *injector) reset() {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.faults, in.log = nil, nil
}

// hits returns how many times f fired.
func (in *injector) hits(f *fault) int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return f.fired
}

// statements returns the statements run since the last reset.
func (in *injector) statements() []string {
	in.mu.Lock()
	defer in.mu.Unlock()
	return slices.Clone(in.log)
}

// fire returns the rule that fails this call, or nil.
func (in *injector) fire(o op, stmt string) *fault {
	in.mu.Lock()
	defer in.mu.Unlock()
	if o == opExec || o == opQuery {
		in.log = append(in.log, stmt)
	}
	for _, f := range in.faults {
		if f.op != o || !strings.Contains(stmt, f.match) {
			continue
		}
		f.seen++
		if f.seen <= f.skip || (f.times > 0 && f.fired >= f.times) {
			continue
		}
		f.fired++
		return f
	}
	return nil
}

type connector struct {
	dsn string
	drv driver.Driver
	in  *injector
}

func (c *connector) Connect(context.Context) (driver.Conn, error) {
	cn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &conn{Conn: cn, in: c.in}, nil
}

func (c *connector) Driver() driver.Driver { return c.drv }

type conn struct {
	driver.Conn
	in *injector
}

var (
	_ driver.ConnBeginTx    = (*conn)(nil)
	_ driver.ExecerContext  = (*conn)(nil)
	_ driver.QueryerContext = (*conn)(nil)
)

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if f := c.in.fire(opBegin, ""); f != nil {
		return nil, f.err
	}
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &txw{Tx: tx, in: c.in}, nil
}

func (c *conn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if f := c.in.fire(opExec, q); f != nil {
		return nil, f.err
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}

func (c *conn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if f := c.in.fire(opQuery, q); f != nil {
		return nil, f.err
	}
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	if err != nil {
		return nil, err
	}
	return &rowsw{Rows: rows, next: c.in.fire(opNext, q), close: c.in.fire(opClose, q), corrupt: c.in.fire(opCorrupt, q)}, nil
}

type txw struct {
	driver.Tx
	in *injector
}

func (t *txw) Commit() error {
	if f := t.in.fire(opCommit, ""); f != nil {
		_ = t.Rollback()
		return f.err
	}
	return t.Tx.Commit()
}

type rowsw struct {
	driver.Rows
	next, close, corrupt *fault
}

func (r *rowsw) Next(dest []driver.Value) error {
	if r.next != nil {
		return r.next.err
	}
	if err := r.Rows.Next(dest); err != nil {
		return err
	}
	if r.corrupt != nil {
		dest[r.corrupt.col] = r.corrupt.val
	}
	return nil
}

func (r *rowsw) Close() error {
	err := r.Rows.Close()
	if r.close != nil {
		return r.close.err
	}
	return err
}

// openDB opens a fresh SQLite database file through the fault wrapper,
// with the pragmas the sqlb SQLite opener sets (WAL, a busy timeout).
func openDB(t *testing.T) (*sql.DB, *injector) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rec.db")
	base, err := driversqlite.Open(path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	drv := base.Driver()
	_ = base.Close()
	in := &injector{}
	db := sql.OpenDB(&connector{dsn: path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", drv: drv, in: in})
	t.Cleanup(func() { _ = db.Close() })
	return db, in
}

// dump renders every row of the store's tables, so a test can prove a
// failed write left the database as it was.
func dump(t *testing.T, db *sql.DB, prefix string) string {
	t.Helper()
	var b strings.Builder
	for _, tbl := range []string{"runs", "steps", "events", "labels", "schema"} {
		rows, err := db.Query(`SELECT * FROM "` + prefix + tbl + `"`)
		if err != nil {
			t.Fatalf("dump %s: %v", tbl, err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, fmt.Sprintf("%v", vals))
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		slices.Sort(lines)
		fmt.Fprintf(&b, "%s:\n%s\n", tbl, strings.Join(lines, "\n"))
	}
	return b.String()
}

// count is the number of rows of one table.
func count(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "` + table + `"`).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// TestFaultWrapperPassesThrough: with no rule, the wrapper is SQLite.
func TestFaultWrapperPassesThrough(t *testing.T) {
	db, in := openDB(t)
	if _, err := db.Exec(`CREATE TABLE x (a INTEGER)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO x (a) VALUES (?), (?)`, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, "x"); n != 2 {
		t.Fatalf("rows = %d", n)
	}
	f := in.add(fault{op: opNext, match: "FROM x", err: io.ErrUnexpectedEOF})
	rows, err := db.Query(`SELECT a FROM x`)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("a row read through a failing result set")
	}
	if err := rows.Err(); err != io.ErrUnexpectedEOF || in.hits(f) != 1 {
		t.Fatalf("rows.Err = %v, hits %d", err, in.hits(f))
	}
	_ = rows.Close()
	if got := in.statements(); len(got) != 4 || got[3] != `SELECT a FROM x` {
		t.Fatalf("statement log = %q", got)
	}
}
