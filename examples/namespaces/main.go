// Command namespaces keeps two environments apart on one database: a
// staging and a production client of the SQL driver share one SQLite file,
// each under its own table prefix, and neither sees the other's runs or
// schedules. A second staging client with the same prefix sees everything
// staging has, which is how a fleet of workers is built. With REDIS_URL set
// it shows the same on Redis with two key prefixes.
//
// It is the program behind docs/operations/namespaces.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
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
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/redis"
	"github.com/ubgo/workflow/sqlstore"
)

const (
	// envURL names the Redis to run the Redis part against, such as
	// redis://localhost:6379/0; without it that part is skipped.
	envURL = "REDIS_URL"
	// stagingPrefix and prodPrefix are the two namespaces' table prefixes.
	stagingPrefix = "staging_"
	prodPrefix    = "prod_"
	// delay keeps the enqueued run waiting for its start, so its status
	// reads the same however fast the host is.
	delay = time.Hour
	// redisPoll is the Redis clients' poll interval in this program.
	redisPoll = 50 * time.Millisecond
	// suffixBase is the base the per-run key suffix is written in: base 36
	// keeps a nanosecond timestamp short enough to read in a key name.
	suffixBase = 36
)

// report is the one workflow every client registers.
var report = workflow.Workflow{Name: "report", Steps: []workflow.Step{{
	Name: "build",
	Run:  func(_ context.Context, in []byte) ([]byte, error) { return in, nil },
}}}

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "namespaces:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) (err error) {
	dir, err := os.MkdirTemp("", "namespaces-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	db, err := driversqlite.Open(filepath.Join(dir, "shared.db"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := sqlNamespaces(ctx, w, db); err != nil {
		return err
	}
	if err := refused(w, db); err != nil {
		return err
	}
	url := os.Getenv(envURL)
	if url == "" {
		fmt.Fprintln(w, envURL, "is not set: the Redis part is skipped")
		return nil
	}
	return redisNamespaces(ctx, w, url)
}

// sqlNamespaces builds two namespaces on one database and shows what each
// client sees.
func sqlNamespaces(ctx context.Context, w io.Writer, db *sql.DB) error {
	// snippet:sql
	staging, err := start(ctx, db, sqlstore.WithTablePrefix(stagingPrefix))
	if err != nil {
		return err
	}
	prod, err := start(ctx, db, sqlstore.WithTablePrefix(prodPrefix))
	if err != nil {
		return err
	}
	// A second staging process: same prefix, same namespace.
	stagingWorker, err := start(ctx, db, sqlstore.WithTablePrefix(stagingPrefix))
	if err != nil {
		return err
	}
	// snippet:end
	defer staging.Stop()
	defer prod.Stop()
	defer stagingWorker.Stop()

	tables, err := tableCounts(ctx, db, stagingPrefix, prodPrefix)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "tables: %s\n", tables)

	// snippet:isolated
	ref, err := staging.Enqueue(ctx, workflow.Job{Workflow: "report", RunAt: time.Now().Add(delay)})
	if err != nil {
		return err
	}
	_, err = staging.Schedule(ctx, workflow.Schedule{
		Name: "nightly", Workflow: "report", Cron: "0 2 * * *", Location: time.UTC,
	})
	if err != nil {
		return err
	}
	for _, c := range []struct {
		name   string
		client *sqlstore.Client
	}{{"staging worker", stagingWorker}, {"prod", prod}} {
		st, err := c.client.Status(ctx, ref)
		if errors.Is(err, workflow.ErrNotFound) {
			st = "not found"
		} else if err != nil {
			return err
		}
		scheds, err := c.client.Schedules(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s: staging run %s, %d schedule(s)\n", c.name, st, len(scheds))
	}
	// snippet:end
	return nil
}

// refused shows the two namespace settings New refuses.
func refused(w io.Writer, db *sql.DB) error {
	// snippet:refused
	_, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithTablePrefix("Staging-"))
	fmt.Fprintf(w, "bad prefix: invalid %t\n", errors.Is(err, workflow.ErrInvalid))
	_, err = sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithSchema("staging"))
	fmt.Fprintf(w, "schema on SQLite: unsupported %t\n", errors.Is(err, workflow.ErrUnsupported))
	// snippet:end
	return nil
}

// start builds, migrates, registers and starts one SQL client.
func start(ctx context.Context, db *sql.DB, opts ...sqlstore.Option) (*sqlstore.Client, error) {
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, opts...)
	if err != nil {
		return nil, err
	}
	if err := c.Migrate(ctx); err != nil { // the migration ledger is per prefix: <prefix>migrations
		return nil, err
	}
	if err := c.Register(report); err != nil {
		return nil, err
	}
	return c, c.Start(ctx)
}

// tableCounts counts the tables under each prefix.
func tableCounts(ctx context.Context, db *sql.DB, prefixes ...string) (string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table'")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", err
		}
		for _, p := range prefixes {
			if strings.HasPrefix(name, p) {
				counts[p]++
			}
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	parts := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		parts = append(parts, fmt.Sprintf("%d %s*", counts[p], p))
	}
	return strings.Join(parts, ", "), nil
}

// redisNamespaces shows two Redis key prefixes on one server.
func redisNamespaces(ctx context.Context, w io.Writer, url string) (err error) {
	opts, err := goredis.ParseURL(url)
	if err != nil {
		return err
	}
	rdb := goredis.NewClient(opts)
	defer func() { err = errors.Join(err, rdb.Close()) }()
	// A unique suffix keeps repeated runs apart on a shared server.
	run := strconv.FormatInt(time.Now().UnixNano(), suffixBase)

	// snippet:redis
	// newClient builds, registers and starts one Redis client under prefix.
	newClient := func(prefix string) (*redis.Client, error) {
		c, err := redis.New(rdb,
			redis.WithPrefix(prefix), // keys look like <prefix>:run:<id>
			redis.WithPollInterval(redisPoll),
		)
		if err != nil {
			return nil, err
		}
		if err := c.Register(report); err != nil {
			return nil, err
		}
		return c, c.Start(ctx)
	}
	staging, err := newClient("staging-" + run)
	if err != nil {
		return err
	}
	prod, err := newClient("prod-" + run)
	if err != nil {
		return err
	}
	// snippet:end
	defer staging.Stop()
	defer prod.Stop()

	ref, err := staging.Enqueue(ctx, workflow.Job{Workflow: "report", RunAt: time.Now().Add(delay)})
	if err != nil {
		return err
	}
	_, err = prod.Status(ctx, ref)
	fmt.Fprintf(w, "redis prod: staging run not found %t\n", errors.Is(err, workflow.ErrNotFound))
	return nil
}
