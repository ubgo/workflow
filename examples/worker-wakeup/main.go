// Command worker-wakeup starts a job on an idle worker as soon as another
// node enqueues it, instead of at the worker's next poll, on the SQL
// driver: a web client and a worker share one database and a
// sqlstore.Notifier. Without PG_DSN it runs on a SQLite file with a small
// in-process notifier (the seam any bus fits: Redis pub/sub, NATS); with
// PG_DSN it runs on Postgres with pgnotify, LISTEN/NOTIFY.
//
// It is the program behind the "Wake workers at once" section of
// docs/drivers/sqlstore.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/sqlstore/pgnotify"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "worker-wakeup:", err)
		os.Exit(1)
	}
}

// pollInterval is long on purpose: a job that starts well before it was
// woken up, not found by a poll.
const pollInterval = 5 * time.Second

// localBus is an in-process Notifier: every Listen on a channel is woken
// by every Notify on it. Across machines the same two methods sit on
// Postgres (pgnotify), Redis pub/sub or NATS.
type localBus struct {
	mu   sync.Mutex
	subs map[string][]func()
}

// snippet:notifier
func (b *localBus) Notify(_ context.Context, channel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, wake := range b.subs[channel] {
		wake()
	}
	return nil
}

func (b *localBus) Listen(ctx context.Context, channel string, wake func()) error {
	b.mu.Lock()
	b.subs[channel] = append(b.subs[channel], wake)
	b.mu.Unlock()
	wake() // cover anything sent before the subscription
	<-ctx.Done()
	return nil
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	db, dialect, notifier, prefix, cleanup, err := open(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	started := make(chan time.Time, 1)
	wf := workflow.Workflow{Name: "welcome-email", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
		started <- time.Now()
		return nil, nil
	}}}}

	web, err := sqlstore.New(db, dialect, sqlstore.WithTablePrefix(prefix), sqlstore.WithEnqueueOnly(), sqlstore.WithNotifier(notifier))
	if err != nil {
		return err
	}
	if err := web.Migrate(ctx); err != nil {
		return err
	}

	// snippet:worker
	worker, err := sqlstore.New(db, dialect,
		sqlstore.WithTablePrefix(prefix),
		sqlstore.WithPollInterval(pollInterval), // the fallback; a lost wake-up only delays work
		sqlstore.WithNotifier(notifier),         // pgnotify.New(db) on Postgres
	)
	// snippet:end
	if err != nil {
		return err
	}
	if err := worker.Register(wf); err != nil {
		return err
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := worker.Start(wctx); err != nil {
		return err
	}
	defer worker.Stop()
	// The worker's first poll happens at Start; wait past it so the job is
	// found only by a wake-up or by the poll after next.
	time.Sleep(200 * time.Millisecond)

	at := time.Now()
	if _, err := web.Enqueue(ctx, workflow.Job{Workflow: "welcome-email"}); err != nil {
		return err
	}
	select {
	case s := <-started:
		fmt.Fprintf(out, "started before the next poll (%s): %t\n", pollInterval, s.Sub(at) < pollInterval/2)
	case <-time.After(2 * pollInterval):
		return errors.New("the job did not start")
	}
	return nil
}

// open returns Postgres with pgnotify when PG_DSN is set, else a SQLite
// file with the in-process bus.
func open(_ context.Context) (*sql.DB, sqlb.Dialect, sqlstore.Notifier, string, func(), error) {
	if dsn := os.Getenv("PG_DSN"); dsn != "" {
		db, err := driverpgx.Open(dsn)
		if err != nil {
			return nil, nil, nil, "", nil, err
		}
		// A fresh table prefix, dropped at the end.
		prefix := "wakeup" + strconv.FormatInt(time.Now().UnixNano()%1e9, 36) + "_"
		cleanup := func() {
			if err := dropPrefix(context.Background(), db, prefix); err != nil {
				fmt.Fprintln(os.Stderr, "worker-wakeup: drop tables:", err)
			}
			_ = db.Close()
		}
		return db, sqlb.PostgresDialect{}, pgnotify.New(db), prefix, cleanup, nil
	}
	dir, err := os.MkdirTemp("", "worker-wakeup-")
	if err != nil {
		return nil, nil, nil, "", nil, err
	}
	db, err := driversqlite.Open(filepath.Join(dir, "jobs.db"))
	if err != nil {
		os.RemoveAll(dir)
		return nil, nil, nil, "", nil, err
	}
	bus := &localBus{subs: map[string][]func(){}}
	return db, sqlb.SQLiteDialect{}, bus, "wf_", func() { _ = db.Close(); os.RemoveAll(dir) }, nil
}

// dropPrefix drops every table under prefix in the current schema, so a
// run against a shared Postgres leaves nothing behind.
func dropPrefix(ctx context.Context, db *sql.DB, prefix string) error {
	rows, err := db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE $1`, prefix+"%")
	if err != nil {
		return err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return err
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var errs []error
	for _, n := range names {
		if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS "`+n+`" CASCADE`); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
