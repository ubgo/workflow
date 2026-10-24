package sqlstore_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

// TestLeaderElectorTerms: one node wins, a second loses while the lease
// holds, renewing keeps the term, resigning hands the role on with the
// next term, and the old leader no longer leads.
func TestLeaderElectorTerms(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			newClient(t, e, db, prefix) // migrates the leader table
			ctx := context.Background()
			mk := func(node string, opts ...sqlstore.LeaderOption) *sqlstore.LeaderElector {
				el, err := sqlstore.NewLeaderElector(db, e.dialect, "r", node, append([]sqlstore.LeaderOption{sqlstore.LeaderTablePrefix(prefix)}, opts...)...)
				if err != nil {
					t.Fatal(err)
				}
				return el
			}
			a, b := mk("a"), mk("b")
			if ok, err := a.Campaign(ctx); err != nil || !ok {
				t.Fatalf("a's first campaign = %v, %v; want it to lead", ok, err)
			}
			if ok, err := b.Campaign(ctx); err != nil || ok {
				t.Fatalf("b's campaign while a holds the lease = %v, %v; want false", ok, err)
			}
			if ok, _ := a.Campaign(ctx); !ok || a.Term() != 1 || !a.IsLeader() {
				t.Fatalf("a's renewal: lead %v term %d; want true, term 1", ok, a.Term())
			}
			if err := a.Resign(ctx); err != nil {
				t.Fatal(err)
			}
			if a.IsLeader() {
				t.Fatal("a still leads after Resign")
			}
			if ok, err := b.Campaign(ctx); err != nil || !ok || b.Term() != 2 {
				t.Fatalf("b after a resigned: lead %v (%v) term %d; want true, term 2", ok, err, b.Term())
			}
			if ok, _ := a.Campaign(ctx); ok {
				t.Fatal("a took the role back while b holds it")
			}
		})
	}
}

// TestLeaderElectorExpiry: a leader that stops renewing stops leading
// after one lease on its own clock; a node with a skew margin waits past
// the lease end, one without takes over.
func TestLeaderElectorExpiry(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			newClient(t, e, db, prefix)
			ctx := context.Background()
			clk := clock.NewFake(start)
			mk := func(node string, skew time.Duration) *sqlstore.LeaderElector {
				el, err := sqlstore.NewLeaderElector(db, e.dialect, "r", node, sqlstore.LeaderTablePrefix(prefix),
					sqlstore.LeaderClock(clk), sqlstore.LeaderLease(time.Minute), sqlstore.LeaderSkew(skew))
				if err != nil {
					t.Fatal(err)
				}
				return el
			}
			a, b, c := mk("a", 0), mk("b", time.Hour), mk("c", 0)
			if ok, _ := a.Campaign(ctx); !ok {
				t.Fatal("a did not win")
			}
			clk.Advance(59 * time.Second)
			if !a.IsLeader() {
				t.Fatal("a stopped leading inside its lease")
			}
			clk.Advance(2 * time.Second)
			if a.IsLeader() {
				t.Fatal("a still leads a whole lease after its last renewal")
			}
			if ok, _ := b.Campaign(ctx); ok {
				t.Fatal("b, with an hour of skew margin, took over a second after the lease ended")
			}
			if ok, _ := c.Campaign(ctx); !ok || c.Term() != 2 {
				t.Fatalf("c did not take over the expired lease (term %d)", c.Term())
			}
		})
	}
}

// flagElector is an Elector a test flips.
type flagElector struct{ on atomic.Bool }

func (f *flagElector) IsLeader() bool { return f.on.Load() }

// TestElectorGatesCron: a client whose elector says it does not lead
// fires no cron tick; once it leads, the due tick fires.
func TestElectorGatesCron(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			el := &flagElector{}
			var runs atomic.Int64
			wf := workflow.Workflow{Name: "tick", Cron: []string{"* * * * *"}, Steps: []workflow.Step{{Name: "a",
				Run: func(context.Context, []byte) ([]byte, error) { runs.Add(1); return nil, nil }}}}
			c := newClient(t, e, db, prefix, sqlstore.WithClock(clk), sqlstore.WithElector(el))
			startWith(t, c, wf)
			if c.IsLeader() {
				t.Fatal("IsLeader with a follower elector")
			}
			clk.Advance(time.Minute)
			time.Sleep(30 * testPoll)
			if n := runs.Load(); n != 0 {
				t.Fatalf("a follower fired %d cron runs", n)
			}
			el.on.Store(true)
			waitFor(t, 10*time.Second, "the leader's fire", func() bool { return runs.Load() >= 1 })
		})
	}
}

// TestLeaderElectionHandsOver: of two clients with WithLeaderElection one
// leads; when it stops it resigns and the other takes over.
func TestLeaderElectionHandsOver(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			wf := workflow.Workflow{Name: "noop", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
			lease := sqlstore.LeaderLease(time.Hour) // a takeover can only come from the resignation
			a := newClient(t, e, db, prefix, sqlstore.WithLeaderElection("a", lease))
			startWith(t, a, wf)
			waitFor(t, 10*time.Second, "a to lead", a.IsLeader)
			b := newClient(t, e, db, prefix, sqlstore.WithLeaderElection("b", lease, sqlstore.LeaderLease(300*time.Millisecond)))
			startWith(t, b, wf)
			time.Sleep(500 * time.Millisecond)
			if b.IsLeader() {
				t.Fatal("b leads while a holds the role")
			}
			a.Stop()
			if a.IsLeader() {
				t.Fatal("a still leads after Stop")
			}
			waitFor(t, 10*time.Second, "b to take over after a resigned", b.IsLeader)
		})
	}
}

// TestNewLeaderElectorRefuses bad arguments.
func TestNewLeaderElectorRefuses(t *testing.T) {
	db, _ := openSQLite(t)
	e := engines(t)[0]
	for name, f := range map[string]func() error{
		"no role": func() error { _, err := sqlstore.NewLeaderElector(db, e.dialect, "", "n"); return err },
		"no node": func() error { _, err := sqlstore.NewLeaderElector(db, e.dialect, "r", ""); return err },
		"bad prefix": func() error {
			_, err := sqlstore.NewLeaderElector(db, e.dialect, "r", "n", sqlstore.LeaderTablePrefix("X;"))
			return err
		},
		"nil db": func() error { _, err := sqlstore.NewLeaderElector(nil, e.dialect, "r", "n"); return err },
	} {
		if err := f(); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
}
