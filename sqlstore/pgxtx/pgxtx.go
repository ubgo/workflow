// Package pgxtx lets an app whose transactions are pgx transactions (not
// database/sql ones) write workflow runs inside them: Run hands a
// database/sql *sql.Tx whose every statement runs on the app's pgx.Tx, so
// sqlstore.Client.EnqueueTx and outbox.Outbox.Write commit or roll back
// with the app's own pgx writes.
//
//	tx, _ := pool.Begin(ctx) // a pgx transaction
//	defer tx.Rollback(ctx)
//	// ... the app's own writes on tx ...
//	err := pgxtx.Run(ctx, tx, func(stx *sql.Tx) error {
//		_, err := client.EnqueueTx(ctx, stx, job)
//		return err
//	})
//	// ... then tx.Commit(ctx), and client.Wake()
//
// The *sql.Tx is a view, not a transaction: its Commit and Rollback do
// nothing, and it is valid only inside fn. Committing or rolling back is
// the app's, on its pgx transaction, as always. The pgx transaction must be
// open on the database the sqlstore client's tables live in.
//
// It works through a minimal database/sql driver over the pgx transaction
// (one connection, no prepared statements), so the store's SQL, its
// savepoints and its error classification (pgconn.PgError carries the
// SQLSTATE) run unchanged. Postgres only, as pgx is.
package pgxtx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is what Run needs from a pgx transaction: pgx.Tx satisfies it,
// and so do *pgx.Conn and a pgxpool connection, for an app that manages
// its transaction with BEGIN itself.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// ErrUnsupported is returned for a database/sql feature the view does not
// provide (prepared statements, a nested BeginTx).
var ErrUnsupported = errors.New("pgxtx: not supported on a pgx transaction view")

// Run calls fn with a *sql.Tx whose statements run on tx, and returns fn's
// error. It neither commits nor rolls back tx.
func Run(ctx context.Context, tx Querier, fn func(*sql.Tx) error) error {
	if tx == nil || fn == nil {
		return errors.New("pgxtx: Run needs a transaction and a function")
	}
	db := sql.OpenDB(connector{q: tx})
	defer db.Close()
	db.SetMaxOpenConns(1)
	stx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pgxtx: open the view: %w", err)
	}
	ferr := fn(stx)
	// Ends the view's database/sql transaction so its connection returns to
	// the throwaway pool; nothing reaches tx.
	_ = stx.Rollback()
	return ferr
}

// connector hands out the one connection over q.
type connector struct{ q Querier }

func (c connector) Connect(context.Context) (driver.Conn, error) { return &conn{q: c.q}, nil }
func (c connector) Driver() driver.Driver                        { return drv{} }

// drv exists because driver.Connector must name one; it opens nothing.
type drv struct{}

func (drv) Open(string) (driver.Conn, error) { return nil, ErrUnsupported }

// conn runs every statement on q and treats Begin as the view's start.
type conn struct {
	q    Querier
	open bool
}

var (
	_ driver.ExecerContext                  = (*conn)(nil)
	_ driver.QueryerContext                 = (*conn)(nil)
	_ driver.ConnBeginTx                    = (*conn)(nil)
	_ driver.NamedValueChecker              = (*conn)(nil)
	_ driver.Conn                           = (*conn)(nil)
	_ driver.Rows                           = (*rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
)

func (c *conn) Prepare(string) (driver.Stmt, error) { return nil, ErrUnsupported }
func (c *conn) Close() error                        { return nil }
func (c *conn) Begin() (driver.Tx, error)           { return c.BeginTx(context.Background(), driver.TxOptions{}) }

// BeginTx starts the view once; the transaction is the app's pgx one.
func (c *conn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if c.open {
		return nil, ErrUnsupported
	}
	c.open = true
	return view{c: c}, nil
}

// CheckNamedValue passes every argument to pgx as it is, which encodes
// more types than database/sql's default converter accepts.
func (c *conn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	tag, err := c.q.Exec(ctx, query, values(args)...)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(tag.RowsAffected()), nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	r, err := c.q.Query(ctx, query, values(args)...)
	if err != nil {
		return nil, err
	}
	return &rows{r: r}, nil
}

// view is the database/sql transaction handle: ending it ends nothing.
type view struct{ c *conn }

func (v view) Commit() error   { v.c.open = false; return nil }
func (v view) Rollback() error { v.c.open = false; return nil }

func values(args []driver.NamedValue) []any {
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

// rows adapts pgx.Rows to driver.Rows.
type rows struct{ r pgx.Rows }

func (r *rows) Columns() []string {
	fds := r.r.FieldDescriptions()
	out := make([]string, len(fds))
	for i, fd := range fds {
		out[i] = fd.Name
	}
	return out
}

func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	return fmt.Sprint(r.r.FieldDescriptions()[i].DataTypeOID)
}

func (r *rows) Close() error {
	r.r.Close()
	return r.r.Err()
}

// Next copies the next row into dest, as values database/sql can assign:
// pgx's own decodings, with narrower integers and floats widened and any
// driver.Valuer (numeric, intervals) asked for its value.
func (r *rows) Next(dest []driver.Value) error {
	if !r.r.Next() {
		if err := r.r.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	vals, err := r.r.Values()
	if err != nil {
		return err
	}
	for i := range dest {
		v, err := toDriver(vals[i])
		if err != nil {
			return fmt.Errorf("pgxtx: column %d: %w", i, err)
		}
		dest[i] = v
	}
	return nil
}

// toDriver turns one pgx-decoded value into a driver.Value.
func toDriver(v any) (driver.Value, error) {
	switch x := v.(type) {
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case float32:
		return float64(x), nil
	case driver.Valuer:
		return x.Value()
	}
	if driver.IsValue(v) {
		return v, nil
	}
	return nil, fmt.Errorf("%w: a value of type %T", ErrUnsupported, v)
}
