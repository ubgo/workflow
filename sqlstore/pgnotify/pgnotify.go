// Package pgnotify is a sqlstore.Notifier on Postgres LISTEN/NOTIFY: an
// enqueue on one machine wakes idle workers on every other machine at once,
// instead of at their next poll.
//
//	db, _ := driverpgx.Open(dsn)
//	c, _ := sqlstore.New(db, sqlb.PostgresDialect{}, sqlstore.WithNotifier(pgnotify.New(db)))
//
// Notify is a plain pg_notify statement and works through any Postgres
// driver. Listen needs the pgx stdlib driver (github.com/jackc/pgx/v5/stdlib,
// which driverpgx.Open uses), because database/sql has no way to wait for a
// notification; on any other driver Listen returns ErrDriver and the client
// keeps polling.
//
// Listen holds one pool connection for as long as the client runs, so the
// pool needs one connection more than the client's workers use. That
// connection is discarded, never returned to the pool, when Listen ends, so
// no other query ever runs on a connection that is still listening.
//
// Behind a transaction-mode connection pooler (PgBouncer in transaction or
// statement mode) LISTEN does not work; point the pool at Postgres itself or
// leave the notifier off.
package pgnotify

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// ErrDriver is returned by Listen when the pool's driver is not the pgx
// stdlib driver, which is the only one that exposes notifications.
var ErrDriver = errors.New("pgnotify: Listen needs a pool opened with the pgx stdlib driver")

// Notifier sends and receives wake-ups over one Postgres pool. Build it with
// New; it holds no state besides the pool.
type Notifier struct {
	db *sql.DB
}

// New returns a Notifier over db, the same pool the sqlstore client uses
// (or any pool on the same Postgres database).
func New(db *sql.DB) *Notifier { return &Notifier{db: db} }

// Notify sends an empty notification on channel. Postgres delivers it to
// every session listening on the channel, this one's listener included.
func (n *Notifier) Notify(ctx context.Context, channel string) error {
	if _, err := n.db.ExecContext(ctx, "SELECT pg_notify($1, '')", channel); err != nil {
		return fmt.Errorf("pgnotify: notify %s: %w", channel, err)
	}
	return nil
}

// Listen takes a pool connection, runs LISTEN on channel, calls wake once
// (to cover anything sent before the subscription) and then once per
// notification, until ctx ends (it returns nil) or the connection fails.
func (n *Notifier) Listen(ctx context.Context, channel string, wake func()) error {
	conn, err := n.db.Conn(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("pgnotify: take a connection: %w", err)
	}
	defer func() { _ = conn.Close() }()
	var listenErr error
	// Raw always returns driver.ErrBadConn, which makes database/sql
	// discard the connection: it is either still subscribed or in the
	// unknown state a cancelled wait leaves it in.
	_ = conn.Raw(func(dc any) error {
		sc, ok := dc.(*stdlib.Conn)
		if !ok {
			listenErr = fmt.Errorf("%w (got %T)", ErrDriver, dc)
			return driver.ErrBadConn
		}
		listenErr = listen(ctx, sc.Conn(), channel, wake)
		return driver.ErrBadConn
	})
	return listenErr
}

// listen is the subscription loop on one pgx connection.
func listen(ctx context.Context, pc *pgx.Conn, channel string, wake func()) error {
	if _, err := pc.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("pgnotify: listen %s: %w", channel, err)
	}
	wake()
	for {
		if _, err := pc.WaitForNotification(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("pgnotify: wait on %s: %w", channel, err)
		}
		wake()
	}
}
