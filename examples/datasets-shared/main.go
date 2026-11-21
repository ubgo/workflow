// Command datasets-shared shows where events.Datasets keeps what its
// triggers already used. First the in-memory store, events.MemoryLineage,
// used directly as a test would: a record saved twice is kept once, and
// Find returns the newest first. Then two processes (two sqlstore clients
// over two connections to one SQLite file) sharing a sqlstore.LineageStore:
// a version the first process announced and consumed starts nothing when
// the second announces it again. Last, the same two processes each with
// their own MemoryLineage, where the second process starts the trigger a
// second time for the same version.
//
// SQLite runs in a temporary file, so nothing needs to be installed.
//
// It is the program behind docs/events/datasets.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/sqlstore"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "datasets-shared:", err)
		os.Exit(1)
	}
}

// orders is the dataset both processes announce.
const orders = "warehouse.orders"

// The workflow and trigger names.
const (
	wfAudit      = "orders-audit"
	triggerAudit = "audit"
)

// pollInterval is how often each sqlstore client looks for runs the other
// one queued. The default suits a fleet; the program wants its runs to
// start at once.
const pollInterval = 10 * time.Millisecond

// awaitPoll is how often announce looks at a run it started: fast for the
// same reason, so each announcement returns as soon as its run ends.
const awaitPoll = 5 * time.Millisecond

// awaitOpts polls at awaitPoll.
var awaitOpts = workflow.AwaitOptions{Poll: awaitPoll}

// t0 is the fixed time of the hand-written lineage records, so the output
// is the same on every run.
var t0 = time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)

// run is main without the process exit, so the test drives the same code
// and compares what it prints.
func run(ctx context.Context, w io.Writer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := memoryLineage(ctx, w); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "datasets-shared-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	path := filepath.Join(dir, "app.db")
	a, err := startProcess(ctx, path)
	if err != nil {
		return err
	}
	defer a.close()
	b, err := startProcess(ctx, path)
	if err != nil {
		return err
	}
	defer b.close()
	if err := shared(ctx, w, a, b); err != nil {
		return err
	}
	return perProcess(ctx, w, a, b)
}

// memoryLineage uses MemoryLineage on its own, as a unit test of code that
// reads lineage would.
func memoryLineage(ctx context.Context, w io.Writer) error {
	fmt.Fprintln(w, "memory lineage:")
	// snippet:memory-lineage
	store := events.NewMemoryLineage() // in-process; what NewDatasets uses without WithLineage
	v09 := events.Lineage{Kind: events.LineageProduced, Dataset: orders, Version: "2026-11-20T09",
		Workflow: "load-orders", Step: "write", Meta: map[string]string{"rows": "1200"}, At: t0}
	v10 := v09
	v10.Version, v10.At = "2026-11-20T10", t0.Add(time.Hour)
	for _, l := range []events.Lineage{v09, v09, v10} { // the second v09 is the same fact: ignored
		if err := store.Record(ctx, l); err != nil {
			return err
		}
	}
	all, err := events.Producers(ctx, store, orders) // newest first
	if err != nil {
		return err
	}
	newest, err := store.Find(ctx, events.LineageQuery{Kind: events.LineageProduced, Dataset: orders, Limit: 1})
	if err != nil {
		return err
	}
	_, refused := store.Find(ctx, events.LineageQuery{Limit: -1})
	// snippet:end
	fmt.Fprintf(w, "  records: %d, newest: %s, negative Limit is ErrInvalid: %v\n",
		len(all), newest[0].Version, errors.Is(refused, workflow.ErrInvalid))
	return nil
}

// process is one app process: its own connection pool to the shared file,
// its own sqlstore client and bus.
type process struct {
	db  *sql.DB
	c   *sqlstore.Client
	bus *events.Bus
}

// startProcess opens path, migrates the queue and starts a client with the
// audit workflow registered.
func startProcess(ctx context.Context, path string) (*process, error) {
	db, err := driversqlite.Open(path)
	if err != nil {
		return nil, err
	}
	p := &process{db: db}
	p.c, err = sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	// noop is the audit step: the example only counts that it ran.
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	if err := p.c.Migrate(ctx); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := p.c.Register(workflow.Workflow{Name: wfAudit, Steps: []workflow.Step{{Name: "audit", Run: noop}}}); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := p.c.Start(ctx); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	p.bus, err = events.New(p.c)
	if err != nil {
		p.close()
		return nil, err
	}
	return p, nil
}

// close stops the client and closes its connections.
func (p *process) close() {
	p.c.Stop()
	_ = p.db.Close()
}

// datasets returns a Datasets on p's bus over store, with the audit
// trigger on orders.
func (p *process) datasets(store events.LineageStore) (*events.Datasets, error) {
	ds, err := events.NewDatasets(p.bus, events.WithLineage(store))
	if err != nil {
		return nil, err
	}
	return ds, ds.Trigger(events.DatasetTrigger{Name: triggerAudit, Datasets: []string{orders}, Workflow: wfAudit})
}

// shared runs both processes on one SQL lineage store: the first through
// Client.LineageStore, the second through NewLineageStore on its own
// connection.
func shared(ctx context.Context, w io.Writer, a, b *process) error {
	fmt.Fprintln(w, "shared SQL lineage store:")
	// snippet:shared-store
	la := a.c.LineageStore() // the lineage table in the client's database and prefix
	if err := la.Migrate(ctx); err != nil {
		return err // Client.Migrate does not create this table: the store's own Migrate does
	}
	lb, err := sqlstore.NewLineageStore(b.db, sqlb.SQLiteDialect{}) // any *sql.DB, no Client needed
	if err != nil {
		return err
	}
	dsA, err := a.datasets(la) // events.NewDatasets(bus, events.WithLineage(la)) plus the trigger
	if err != nil {
		return err
	}
	dsB, err := b.datasets(lb)
	if err != nil {
		return err
	}
	// snippet:end
	steps := []struct {
		name    string
		ds      *events.Datasets
		version string
	}{
		{"process A", dsA, "2026-11-20T09"},
		{"process B", dsB, "2026-11-20T09"}, // the same version, from another replica
		{"process B", dsB, "2026-11-20T10"},
	}
	for _, s := range steps {
		started, err := announce(ctx, s.ds, a.c, s.version)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "  %s announces %s: started %s\n", s.name, s.version, started)
	}
	consumed, err := events.Consumers(ctx, la, orders)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  audit runs recorded in the shared store: %d\n", len(consumed))
	return nil
}

// perProcess gives each process its own MemoryLineage: neither sees what
// the other consumed.
func perProcess(ctx context.Context, w io.Writer, a, b *process) error {
	fmt.Fprintln(w, "a MemoryLineage per process:")
	dsA, err := a.datasets(events.NewMemoryLineage())
	if err != nil {
		return err
	}
	dsB, err := b.datasets(events.NewMemoryLineage())
	if err != nil {
		return err
	}
	for _, s := range []struct {
		name string
		ds   *events.Datasets
	}{{"process A", dsA}, {"process B", dsB}} {
		started, err := announce(ctx, s.ds, a.c, "2026-11-20T11")
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "  %s announces 2026-11-20T11: started %s\n", s.name, started)
	}
	return nil
}

// announce calls Updated outside a step, waits for every run it started
// (so the next announcement sees it finished, as a later one would), and
// names the triggers it started.
func announce(ctx context.Context, ds *events.Datasets, c workflow.Client, version string) (string, error) {
	res, err := ds.Updated(ctx, orders, version, nil)
	if err != nil {
		return "", err
	}
	var started []string
	for _, t := range res.Triggered {
		if _, err := workflow.Await(ctx, c, t.Ref, awaitOpts); err != nil {
			return "", err
		}
		started = append(started, t.Trigger)
	}
	if len(started) == 0 {
		return "nothing", nil
	}
	return strings.Join(started, ", "), nil
}
