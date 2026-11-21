// Command sql-leader-election shows how the SQL driver keeps the nodes of
// one database in agreement when their clocks and their health differ:
//
//   - leader election: cron fires and housekeeping on one elected node
//     (WithLeaderElection, NewLeaderElector, WithElector, Client.IsLeader);
//   - the database server's clock as the one "now" (NewDatabaseClock,
//     WithDatabaseClock) and the margin other nodes wait past a lease
//     (WithLeaseSkew);
//   - leases: a holder that cannot renew stops itself (ErrLeaseLost), a
//     node whose clock is past the lease end takes the step over, and a
//     step that keeps losing its workers fails (WithMaxReclaims,
//     ErrAbandoned).
//
// Each part runs on its own SQLite file in a temporary directory, so it
// needs nothing installed. Fake lease clocks stand in for a stalled node
// and for time passing; the clients live in one process so the program
// can print what each did, where production runs each in its own.
//
// It is the program behind the "Leases and crashed workers" and "Clocks,
// skew and one fire node" sections of docs/drivers/sqlstore.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
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
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

const (
	// leaderLease is short so a handover takes a fraction of a second; the
	// default is 15 seconds. A node campaigns every third of it.
	leaderLease = 600 * time.Millisecond
	// stepLease is the step lease of the lease demo (the default is 30s).
	// The fake lease clocks jump past it, so its size only sets how far.
	stepLease = time.Minute
	// holderSkew keeps the stalled holder from taking its own step back:
	// it waits this long past a lease's end, the others none.
	holderSkew = time.Hour
	// takerAhead is how far node-b's lease clock reads ahead of node-a's:
	// far past node-a's lease end, so node-b sees the step abandoned.
	takerAhead = 2 * time.Hour
	// clusterSkew is the WithLeaseSkew of the database-clock client: the
	// largest clock difference expected between nodes.
	clusterSkew = 2 * time.Second
	// clockResync is how often the database clock is read again.
	clockResync = time.Minute
	// trustBound is what the program checks the database clock against: a
	// SQLite file shares this machine's clock, so offset and uncertainty
	// must both be far below it.
	trustBound = time.Second
	// waitTimeout bounds every wait, generous for a slow CI host.
	waitTimeout = 10 * time.Second
	// waitPoll is how often a wait re-checks its condition.
	waitPoll = 5 * time.Millisecond
)

// base is the fake lease clocks' first reading, fixed so runs repeat.
var base = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sql-leader-election:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) (err error) {
	dir, err := os.MkdirTemp("", "sql-leader-election-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	for _, part := range []struct {
		file string
		fn   func(context.Context, io.Writer, *sql.DB) error
	}{
		{"election.db", election},
		{"elector.db", elector},
		{"clock.db", databaseClock},
		{"takeover.db", func(ctx context.Context, w io.Writer, db *sql.DB) error { return takeover(ctx, w, db, false) }},
		{"poison.db", func(ctx context.Context, w io.Writer, db *sql.DB) error { return takeover(ctx, w, db, true) }},
	} {
		if err := withDB(filepath.Join(dir, part.file), func(db *sql.DB) error { return part.fn(ctx, w, db) }); err != nil {
			return fmt.Errorf("%s: %w", part.file, err)
		}
	}
	return nil
}

// noop is the workflow every client of the election registers.
var noop = workflow.Workflow{Name: "noop", Steps: []workflow.Step{{Name: "a",
	Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}

// election starts two clients with WithLeaderElection: the first leads,
// the second follows, and when the first stops it resigns and the second
// takes over.
func election(ctx context.Context, w io.Writer, db *sql.DB) error {
	// snippet:election
	// newNode builds one node's client over the shared database.
	newNode := func(node string) (*sqlstore.Client, error) {
		return sqlstore.New(db, sqlb.SQLiteDialect{},
			// Cron fires and housekeeping only while this node leads the role
			// "scheduler"; every node still runs steps.
			sqlstore.WithLeaderElection(node, sqlstore.LeaderLease(leaderLease)),
		)
	}
	// snippet:end
	a, err := newNode("node-a")
	if err != nil {
		return err
	}
	if err := a.Migrate(ctx); err != nil { // creates wf_leaders (migration 007)
		return err
	}
	if err := startClient(ctx, a, noop); err != nil {
		return err
	}
	defer a.Stop()
	if err := waitFor("node-a to lead", a.IsLeader); err != nil {
		return err
	}
	b, err := newNode("node-b")
	if err != nil {
		return err
	}
	if err := startClient(ctx, b, noop); err != nil {
		return err
	}
	defer b.Stop()

	// snippet:handover
	fmt.Fprintf(w, "node-a leads: %v, node-b leads: %v\n", a.IsLeader(), b.IsLeader())
	a.Stop() // resigns: node-b takes the role at its next campaign, not after the lease
	if err := waitFor("node-b to take over", b.IsLeader); err != nil {
		return err
	}
	fmt.Fprintf(w, "after node-a stopped: node-a leads: %v, node-b leads: %v\n", a.IsLeader(), b.IsLeader())
	// snippet:end
	return nil
}

// snippet:custom-elector
// flagElector is an Elector of the app's own: a Redis lock, a Kubernetes
// Lease or, here, a flag. IsLeader is called on every dispatcher pass, so
// it answers from memory.
type flagElector struct{ leads atomic.Bool }

// IsLeader implements sqlstore.Elector.
func (f *flagElector) IsLeader() bool { return f.leads.Load() }

// snippet:end

// elector campaigns with LeaderElectors of its own for a role of the app,
// and gates a client on an Elector of the app's own.
func elector(ctx context.Context, w io.Writer, db *sql.DB) error {
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil { // the elector campaigns in the client's leader table
		return err
	}

	// snippet:elector
	// mk builds one node's elector for the app's role "nightly-export".
	mk := func(node string) (*sqlstore.LeaderElector, error) {
		return sqlstore.NewLeaderElector(db, sqlb.SQLiteDialect{}, "nightly-export", node,
			sqlstore.LeaderLease(leaderLease),
			sqlstore.LeaderTablePrefix("wf_"), // the prefix of the client that migrated
		)
	}
	n1, err := mk("node-1")
	if err != nil {
		return err
	}
	n2, err := mk("node-2")
	if err != nil {
		return err
	}
	won1, err := n1.Campaign(ctx) // one attempt; Run(ctx) campaigns every third of the lease
	if err != nil {
		return err
	}
	won2, err := n2.Campaign(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "node-1 won: %v (term %d), node-2 won: %v\n", won1, n1.Term(), won2)
	if err := n1.Resign(ctx); err != nil { // hand the role on now, not after the lease
		return err
	}
	won2, err = n2.Campaign(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "after node-1 resigned: node-2 won: %v (term %d)\n", won2, n2.Term())
	// snippet:end

	// snippet:with-elector
	flag := &flagElector{}
	gated, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithElector(flag))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "client with a follower elector may fire cron: %v\n", gated.IsLeader())
	flag.leads.Store(true)
	fmt.Fprintf(w, "and once it leads: %v\n", gated.IsLeader())
	// snippet:end
	return nil
}

// databaseClock reads the database server's clock and runs a client on
// it.
func databaseClock(ctx context.Context, w io.Writer, db *sql.DB) error {
	// snippet:dbclock
	dbc, err := sqlstore.NewDatabaseClock(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	if err := dbc.Sync(ctx); err != nil { // one read; Now then runs on the local monotonic clock
		return err
	}
	// Offset: how far the database is ahead of this machine. Uncertainty:
	// half the read's round trip. A SQLite file shares this machine's clock,
	// so both are tiny here; on Postgres or MySQL they are what the nodes'
	// clocks disagree by.
	fmt.Fprintf(w, "database clock: offset under %s: %v, uncertainty under %s: %v\n",
		trustBound, dbc.Offset().Abs() < trustBound, trustBound, dbc.Uncertainty() < trustBound)

	c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
		sqlstore.WithDatabaseClock(clockResync), // due times and leases on the database's clock
		sqlstore.WithLeaseSkew(clusterSkew),     // others wait this long past a lease's end
	)
	// snippet:end
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil {
		return err
	}
	if err := startClient(ctx, c, noop); err != nil { // Start reads the database clock first
		return err
	}
	defer c.Stop()
	fmt.Fprintf(w, "client runs on the database clock: %v\n", c.DatabaseClock() != nil)
	return nil
}

// takeover runs one step on node-a, stalls node-a past its lease, and lets
// node-b, whose clock is past the lease end, take the step over. With
// poison set, node-b allows no takeover (WithMaxReclaims(0)), so the
// attempt fails with ErrAbandoned instead of running again.
func takeover(ctx context.Context, w io.Writer, db *sql.DB, poison bool) error {
	started := make(chan struct{})
	fenced := make(chan bool, 1)
	clkA := clock.NewFake(base)

	// snippet:lease-holder
	// charge is node-a's step: it starts, then hangs until its lease is lost.
	charge := func(ctx context.Context, _ []byte) ([]byte, error) {
		close(started)
		<-ctx.Done() // the database stalls: no renewal lands
		// The holder fenced itself: its outcome is not written, because
		// another node may already hold the step.
		fenced <- errors.Is(context.Cause(ctx), sqlstore.ErrLeaseLost)
		return nil, ctx.Err()
	}
	a, err := sqlstore.New(db, sqlb.SQLiteDialect{},
		sqlstore.WithNodeID("node-a"),
		sqlstore.WithLease(stepLease),      // renewed every third; taken over after it lapses
		sqlstore.WithLeaseClock(clkA),      // wall time for leases, apart from WithClock
		sqlstore.WithLeaseSkew(holderSkew), // this node waits an hour past a lease's end
	)
	// snippet:end
	if err != nil {
		return err
	}
	if err := a.Migrate(ctx); err != nil {
		return err
	}
	if err := startClient(ctx, a, workflow.Workflow{Name: "charge", Steps: []workflow.Step{{Name: "charge", Run: charge}}}); err != nil {
		return err
	}
	defer a.Stop()
	ref, err := a.Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`"order-7"`)})
	if err != nil {
		return err
	}
	if err := waitChan("node-a to start the step", started); err != nil {
		return err
	}
	if !poison { // the fence is the same both times; print it once
		clkA.Advance(2 * stepLease) // a whole lease without a renewal
		select {
		case lost := <-fenced:
			fmt.Fprintf(w, "node-a stopped its attempt with ErrLeaseLost: %v\n", lost)
		case <-time.After(waitTimeout):
			return errors.New("node-a never fenced itself")
		}
	}

	ranOn := make(chan string, 1)
	// snippet:lease-taker
	reclaims := 5 // the default: takeovers of one attempt before ErrAbandoned
	if poison {
		reclaims = 0 // fail at the first takeover
	}
	b, err := sqlstore.New(db, sqlb.SQLiteDialect{},
		sqlstore.WithNodeID("node-b"),
		sqlstore.WithLease(stepLease),
		sqlstore.WithLeaseClock(clock.NewFake(base.Add(takerAhead))), // past node-a's lease end
		sqlstore.WithMaxReclaims(reclaims),
	)
	// snippet:end
	if err != nil {
		return err
	}
	if err := startClient(ctx, b, workflow.Workflow{Name: "charge", Steps: []workflow.Step{{Name: "charge",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			ranOn <- info.Node
			return in, nil
		}}}}); err != nil {
		return err
	}
	defer b.Stop()
	b.Wake() // its fake lease clock never fires a poll timer: look now
	if err := waitFor("the run to end", func() bool {
		st, err := b.Status(ctx, ref)
		return err == nil && st.IsTerminal()
	}); err != nil {
		return err
	}
	st, err := b.Status(ctx, ref)
	if err != nil {
		return err
	}
	if !poison {
		fmt.Fprintf(w, "%s took the step over and ran it: run %s\n", <-ranOn, st)
		return nil
	}
	page, err := b.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	if len(page.Runs) != 1 {
		return fmt.Errorf("found %d charge runs, want 1", len(page.Runs))
	}
	msg := page.Runs[0].Error
	fmt.Fprintf(w, "WithMaxReclaims(0): run %s, abandoned: %v\n", st, strings.Contains(msg, sqlstore.ErrAbandoned.Error()))
	fmt.Fprintf(w, "error: %s\n", msg)
	return nil
}

// startClient registers wf on c and starts it for the lifetime of ctx.
func startClient(ctx context.Context, c *sqlstore.Client, wf workflow.Workflow) error {
	if err := c.Register(wf); err != nil {
		return err
	}
	return c.Start(ctx)
}

// withDB opens a SQLite file, runs fn and closes it.
func withDB(path string, fn func(*sql.DB) error) (err error) {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	return fn(db)
}

// waitFor polls cond until it holds or waitTimeout passes.
func waitFor(what string, cond func() bool) error {
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s", what)
		}
		time.Sleep(waitPoll)
	}
	return nil
}

// waitChan waits until ch is closed or waitTimeout passes.
func waitChan(what string, ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-time.After(waitTimeout):
		return fmt.Errorf("timed out waiting for %s", what)
	}
}
