// Command unique-jobs refuses a second job for the same key while the first
// holds a lock: for a fixed time (Unique), until the run finishes
// (UniqueUntilFinished), keyed by the workflow and its arguments
// (UniqueByArgs), and across processes through a SQL lock table
// (sqlstore.UniqueStore on SQLite).
//
// It is the program behind docs/reliability/unique-jobs.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "unique-jobs:", err)
		os.Exit(1)
	}
}

// byUser is the uniqueness key: one job per user at a time. A job without
// the label returns "" and is not unique.
func byUser(j workflow.Job) string { return j.Labels["user"] }

// report is a compact verdict for one enqueue.
func report(err error) string {
	switch {
	case err == nil:
		return "accepted"
	case errors.Is(err, workflow.ErrDuplicate):
		return "refused (ErrDuplicate)"
	}
	return "error: " + err.Error()
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := forATime(ctx, out); err != nil {
		return err
	}
	if err := untilFinished(ctx, out); err != nil {
		return err
	}
	if err := byArgs(ctx, out); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "unique-jobs-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	return acrossProcesses(ctx, out, filepath.Join(dir, "locks.db"))
}

// export is a one-step workflow that does nothing; the lock is the point.
func export(ran chan<- string, gate <-chan struct{}) workflow.Workflow {
	return workflow.Workflow{Name: "export", Steps: []workflow.Step{{
		Name: "build",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if gate != nil {
				<-gate
			}
			if ran != nil {
				ran <- "export ran"
			}
			return nil, nil
		},
	}}}
}

// forATime holds the lock for one hour from the enqueue, whatever the run
// does. A fake clock moves the hour on without waiting.
func forATime(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "-- Unique for a time")
	mc := memory.New()
	if err := mc.Register(export(nil, nil)); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := mc.Start(ctx); err != nil {
		return err
	}

	// snippet:unique
	clk := clock.NewFake(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))
	locks := middleware.NewMemoryUnique(clk)
	c := middleware.WrapClient(mc, middleware.Unique(byUser, locks, time.Hour))

	job := workflow.Job{Workflow: "export", Labels: map[string]string{"user": "u-7"}}
	_, err1 := c.Enqueue(ctx, job)
	_, err2 := c.Enqueue(ctx, job) // same user within the hour
	clk.Advance(time.Hour)
	_, err3 := c.Enqueue(ctx, job) // the lock has expired
	// snippet:end

	fmt.Fprintln(out, "first:", report(err1))
	fmt.Fprintln(out, "second, same hour:", report(err2))
	fmt.Fprintln(out, "third, an hour later:", report(err3))
	return nil
}

// untilFinished frees the lock as soon as the run ends, with the ttl as a
// safety net for a lost run-finished event.
func untilFinished(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "-- Unique until the run finishes")
	ran, gate := make(chan string, 2), make(chan struct{})
	mc := memory.New()
	if err := mc.Register(export(ran, gate)); err != nil {
		return err
	}

	// snippet:until-finished
	locks := middleware.NewMemoryUnique(nil)
	// Before Start: the release is a run-finished observer on mc.
	unique, err := middleware.UniqueUntilFinished(mc, byUser, locks, 24*time.Hour)
	if err != nil {
		return err
	}
	c := middleware.WrapClient(mc, unique)
	// snippet:end

	finished := make(chan workflow.RunEvent, 2)
	// Observers run in registration order, so by the time this one sees the
	// event the unique release before it has run.
	if err := mc.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { finished <- ev }); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := mc.Start(ctx); err != nil {
		return err
	}

	job := workflow.Job{Workflow: "export", Labels: map[string]string{"user": "u-7"}}
	if _, err := c.Enqueue(ctx, job); err != nil {
		return err
	}
	fmt.Fprintln(out, "first: accepted")
	_, err = c.Enqueue(ctx, job)
	fmt.Fprintln(out, "second, while the first runs:", report(err))

	close(gate)
	fmt.Fprintln(out, <-ran)
	ev := <-finished
	fmt.Fprintln(out, "run finished:", ev.Status, "key label:", ev.Labels[middleware.UniqueKeyLabel])

	_, err = c.Enqueue(ctx, job)
	fmt.Fprintln(out, "third, after it finished:", report(err))
	fmt.Fprintln(out, <-ran)
	<-finished
	return nil
}

// byArgs keys the lock on the workflow and its payload instead of a
// label, so the same report for the same account is refused while a
// different account, or the same one asked in another format, is not.
func byArgs(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "-- Unique by workflow and arguments")
	mc := memory.New()
	if err := mc.Register(workflow.Workflow{Name: "report", Steps: []workflow.Step{{
		Name: "build", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil },
	}}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := mc.Start(ctx); err != nil {
		return err
	}

	// snippet:by-args
	// Only the account and the month count: a different requested_by is
	// still the same report.
	key := middleware.UniqueByArgs(middleware.ArgsFields("account", "month"))
	c := middleware.WrapClient(mc, middleware.Unique(key, middleware.NewMemoryUnique(nil), time.Hour))

	_, err1 := c.Enqueue(ctx, workflow.Job{Workflow: "report", Payload: []byte(`{"account":"a-1","month":"2026-01","requested_by":"ann"}`)})
	// Same fields in another order, asked by someone else: the same key.
	_, err2 := c.Enqueue(ctx, workflow.Job{Workflow: "report", Payload: []byte(`{"requested_by":"bob","month":"2026-01","account":"a-1"}`)})
	_, err3 := c.Enqueue(ctx, workflow.Job{Workflow: "report", Payload: []byte(`{"account":"a-2","month":"2026-01"}`)})
	// snippet:end

	fmt.Fprintln(out, "a-1, ann:", report(err1))
	fmt.Fprintln(out, "a-1, bob, keys reordered:", report(err2))
	fmt.Fprintln(out, "a-2:", report(err3))
	fmt.Fprintln(out, "key:", key(workflow.Job{Workflow: "report", Payload: []byte(`{"account":"a-1","month":"2026-01"}`)})[:len("args:report:")+12]+"...")
	return nil
}

// acrossProcesses shares one lock table between two "processes": two
// connection pools on one SQLite file, each with its own client. In
// production these are two servers on one Postgres or MySQL database.
func acrossProcesses(ctx context.Context, out io.Writer, path string) error {
	fmt.Fprintln(out, "-- A store shared by every process")
	clk := clock.NewFake(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))

	// newProcess opens its own pool and lock store, as a separate server
	// would, and wraps its own client with Unique.
	newProcess := func() (workflow.Client, *sqlstore.UniqueStore, func(), error) {
		// snippet:sql-store
		db, err := driversqlite.Open(path)
		if err != nil {
			return nil, nil, nil, err
		}
		// No sqlstore queue needed: the lock table works beside any driver.
		locks, err := sqlstore.NewUniqueStore(db, sqlb.SQLiteDialect{}, sqlstore.WithClock(clk))
		if err != nil {
			return nil, nil, nil, err
		}
		if err := locks.Migrate(ctx); err != nil { // creates wf_unique_locks only
			return nil, nil, nil, err
		}
		mc := memory.New()
		c := middleware.WrapClient(mc, middleware.Unique(byUser, locks, 10*time.Minute))
		// snippet:end
		if err := mc.Register(export(nil, nil)); err != nil {
			return nil, nil, nil, err
		}
		pctx, cancel := context.WithCancel(ctx)
		if err := mc.Start(pctx); err != nil {
			cancel()
			return nil, nil, nil, err
		}
		return c, locks, func() { cancel(); _ = db.Close() }, nil
	}

	a, locksA, closeA, err := newProcess()
	if err != nil {
		return err
	}
	defer closeA()
	b, _, closeB, err := newProcess()
	if err != nil {
		return err
	}
	defer closeB()

	job := workflow.Job{Workflow: "export", Labels: map[string]string{"user": "u-7"}}
	_, err = a.Enqueue(ctx, job)
	fmt.Fprintln(out, "process A:", report(err))
	_, err = b.Enqueue(ctx, job)
	fmt.Fprintln(out, "process B, same user:", report(err))

	// snippet:prune
	clk.Advance(10 * time.Minute)
	n, err := locksA.PruneExpired(ctx) // a running sqlstore.Client does this itself
	// snippet:end
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "expired locks pruned:", n)
	_, err = b.Enqueue(ctx, job)
	fmt.Fprintln(out, "process B, after expiry:", report(err))
	return nil
}
