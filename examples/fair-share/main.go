// Command fair-share shows round robin across concurrency keys and
// weighted shares (FC-05, FC-06): one worker serves a tenant that
// enqueued a backlog first and tenants that enqueued later, and the
// tenants take turns instead of waiting behind the backlog; with
// Concurrency.Weight, a tenant of weight 3 gets three turns for each turn
// of a tenant of weight 1.
//
// It runs the same workflows on the in-memory driver and on SQLite
// (sqlstore), with one worker, and prints the order the worker started
// the runs in; both drivers print the same order. It is the program behind
// the fair-share section of docs/jobs/concurrency.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "fair-share:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) (err error) {
	dir, err := os.MkdirTemp("", "fair-share-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	for _, d := range []struct {
		name string
		open func(i int) (workflow.Client, func() error, error)
	}{
		{"memory", func(int) (workflow.Client, func() error, error) {
			c := memory.New(memory.WithWorkers(1))
			return c, func() error { c.Stop(); return nil }, nil
		}},
		{"sqlite", func(i int) (workflow.Client, func() error, error) {
			return openSQLite(ctx, filepath.Join(dir, fmt.Sprintf("app-%d.db", i)))
		}},
	} {
		fmt.Println(d.name + ":")
		for i, demo := range []struct {
			title   string
			conc    *workflow.Concurrency
			backlog []string
		}{
			{"round robin", turns(), []string{"acme-1", "acme-2", "acme-3", "acme-4", "globex-1", "globex-2", "initech-1"}},
			{"weights", weights(), []string{"free-1", "free-2", "free-3", "free-4", "gold-1", "gold-2", "gold-3", "gold-4", "gold-5", "gold-6"}},
		} {
			c, stop, err := d.open(i)
			if err != nil {
				return err
			}
			order, err := startOrder(ctx, c, demo.conc, demo.backlog)
			if serr := stop(); err == nil {
				err = serr
			}
			if err != nil {
				return fmt.Errorf("%s, %s: %w", d.name, demo.title, err)
			}
			fmt.Printf("  %s: %s\n", demo.title, strings.Join(order, " "))
		}
	}
	return nil
}

// tenant buckets a job by the tenant named before the dash in its payload.
var tenant = workflow.KeyFunc(func(j workflow.Job) string {
	t, _, _ := strings.Cut(string(j.Payload), "-")
	return t
})

func turns() *workflow.Concurrency {
	// snippet:turns
	conc := &workflow.Concurrency{
		Key:      tenant,
		MaxRuns:  10,                             // per tenant at once
		Strategy: workflow.ConcurrencyRoundRobin, // tenants take turns for the workers
	}
	// snippet:end
	return conc
}

func weights() *workflow.Concurrency {
	// snippet:weights
	conc := &workflow.Concurrency{
		Key:      tenant,
		MaxRuns:  10,
		Strategy: workflow.ConcurrencyRoundRobin,
		// gold gets three turns for each turn of a tenant of weight 1
		Weight: workflow.KeyWeights(map[string]int{"gold": 3}, 1),
	}
	// snippet:end
	return conc
}

// startOrder registers a one-step workflow with conc on c, holds c's one
// worker with a first run while backlog is enqueued, then releases it and
// returns the order the worker started the backlog in.
func startOrder(ctx context.Context, c workflow.Client, conc *workflow.Concurrency, backlog []string) ([]string, error) {
	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	wf := workflow.Workflow{Name: "sync", Concurrency: conc, Steps: []workflow.Step{{Name: "pull", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		if string(in) == "hold-0" {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, nil
		}
		mu.Lock()
		order = append(order, string(in))
		mu.Unlock()
		return nil, nil
	}}}}
	if err := c.Register(wf); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return nil, err
	}
	hold, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte("hold-0")})
	if err != nil {
		return nil, err
	}
	if err := waitFor(ctx, c, hold, workflow.StatusRunning); err != nil {
		return nil, err
	}
	var refs []workflow.RunRef
	for _, p := range backlog {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(p)})
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	time.Sleep(200 * time.Millisecond) // the SQL driver admits on its next pass
	close(release)
	for _, ref := range refs {
		if err := waitFor(ctx, c, ref, workflow.StatusSucceeded); err != nil {
			return nil, err
		}
	}
	mu.Lock()
	defer mu.Unlock()
	return order, nil
}

// waitFor polls ref until it reaches want, for up to ten seconds.
func waitFor(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s is %s, want %s", ref.RunID, st, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// openSQLite opens a migrated sqlstore client with one worker on a SQLite
// file.
func openSQLite(ctx context.Context, path string) (workflow.Client, func() error, error) {
	db, err := driversqlite.Open(path)
	if err != nil {
		return nil, nil, err
	}
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithWorkers(1), sqlstore.WithPollInterval(20*time.Millisecond))
	if err == nil {
		err = c.Migrate(ctx)
	}
	if err != nil {
		return nil, nil, errors.Join(err, db.Close())
	}
	return c, func() error { c.Stop(); return db.Close() }, nil
}
