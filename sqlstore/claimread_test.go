package sqlstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// faultyConnector opens SQLite connections whose next query containing
// match fails once armed: the way a pool that runs out of server
// connections fails one statement between two others.
type faultyConnector struct {
	base  driver.Driver
	dsn   string
	match string
	armed atomic.Bool
	hits  atomic.Int64
}

func (f *faultyConnector) Connect(context.Context) (driver.Conn, error) {
	c, err := f.base.Open(f.dsn)
	if err != nil {
		return nil, err
	}
	return &faultyConn{Conn: c, f: f}, nil
}

func (f *faultyConnector) Driver() driver.Driver { return f.base }

type faultyConn struct {
	driver.Conn
	f *faultyConnector
}

var errInjected = errors.New("injected: the database refused the connection")

func (c *faultyConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, c.f.match) && c.f.armed.CompareAndSwap(true, false) {
		c.f.hits.Add(1)
		return nil, errInjected
	}
	qc, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return qc.QueryContext(ctx, q, args)
}

func (c *faultyConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	ec, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return ec.ExecContext(ctx, q, args)
}

func (c *faultyConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	bt, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New("faultyConn: the SQLite driver has no BeginTx")
	}
	return bt.BeginTx(ctx, opts)
}

func (c *faultyConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	pc, ok := c.Conn.(driver.ConnPrepareContext)
	if !ok {
		return nil, errors.New("faultyConn: the SQLite driver has no PrepareContext")
	}
	return pc.PrepareContext(ctx, q)
}

// TestClaimReadFailureLeavesTheStepClaimable fails the read of a claimed
// step once. The claim and the read are one transaction, so the failure
// rolls the claim back and the next claim runs the step at once; were the
// claim committed on its own, the step would stay leased to nobody until
// its lease (an hour here) ran out.
func TestClaimReadFailureLeavesTheStepClaimable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wf.db")
	seed, err := driversqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	fc := &faultyConnector{base: seed.Driver(), dsn: path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", match: "WHERE st.lease_token = "}
	_ = seed.Close()
	db := sql.OpenDB(fc)
	t.Cleanup(func() { _ = db.Close() })
	e := engine{name: "sqlite", dialect: sqlb.SQLiteDialect{}}
	ran := make(chan struct{}, 1)
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		ran <- struct{}{}
		return nil, nil
	}}}}
	c := newClient(t, e, db, "wf_", sqlstore.WithLease(time.Hour))
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fc.armed.Store(true)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the step never ran: a failed read left it leased to nobody until its lease expires")
	}
	if fc.hits.Load() != 1 {
		t.Fatalf("injected %d failures, want 1 (the read is not where the test expects)", fc.hits.Load())
	}
}
