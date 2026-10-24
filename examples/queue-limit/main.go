// Command queue-limit shows QueueConfig.Limit, a cap on the in-flight step
// attempts of one queue across every client sharing the store. Workers caps
// one client; Limit caps the queue for the whole cluster.
//
// It runs the same workload twice: on the in-memory driver (one process is
// the whole cluster) and on two SQL clients over one SQLite file (two
// "nodes", each with room for four workers, sharing a limit of three).
// The output is deterministic: it reports whether the peak number of
// attempts running at once stayed within the limit and reached it.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

const (
	// importLimit is the queue's cluster-wide ceiling.
	importLimit = 3
	// runCount is how many runs each demo enqueues: more than the limit, so
	// the cap is what holds the rest back.
	runCount = 9
	// holdFor is how long each step holds its slot, long enough for every
	// worker to be busy at once if the limit allowed it.
	holdFor = 150 * time.Millisecond
	// runTimeout bounds each demo, generous for a slow host.
	runTimeout = 30 * time.Second
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "queue-limit:", err)
		os.Exit(1)
	}
}

// gauge counts attempts running now and the peak, across every client of a
// demo, so it sees the cluster the way the limit does.
type gauge struct{ now, peak atomic.Int64 }

func (g *gauge) enter() {
	n := g.now.Add(1)
	for {
		p := g.peak.Load()
		if n <= p || g.peak.CompareAndSwap(p, n) {
			return
		}
	}
}

func (g *gauge) leave() { g.now.Add(-1) }

// importWorkflow is a one-step workflow on the "imports" queue whose step
// records itself in g and holds its slot for holdFor.
func importWorkflow(g *gauge) workflow.Workflow {
	return workflow.Workflow{
		Name:  "import",
		Queue: "imports",
		Steps: []workflow.Step{{Name: "load", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			g.enter()
			defer g.leave()
			time.Sleep(holdFor)
			return nil, nil
		}}},
	}
}

func run(ctx context.Context, out io.Writer) error {
	if err := runMemory(ctx, out); err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	dir, err := os.MkdirTemp("", "queue-limit-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := runSQL(ctx, out, filepath.Join(dir, "app.db")); err != nil {
		return fmt.Errorf("sqlstore: %w", err)
	}
	return nil
}

// report prints the two facts the limit promises.
func report(out io.Writer, driver string, g *gauge) {
	peak := int(g.peak.Load())
	fmt.Fprintf(out, "%s: peak within limit %d: %v\n", driver, importLimit, peak <= importLimit)
	fmt.Fprintf(out, "%s: peak reached the limit: %v\n", driver, peak == importLimit)
}

// wait blocks until n runs have finished.
func wait(ctx context.Context, done <-chan struct{}, n int) error {
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	for range n {
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("runs did not finish: %w", ctx.Err())
		}
	}
	return nil
}

func runMemory(ctx context.Context, out io.Writer) error {
	g := &gauge{}
	// snippet:memory
	c := memory.New(
		memory.WithWorkers(8),
		memory.WithQueues(workflow.QueueConfig{Name: "imports", Limit: importLimit}),
	)
	// snippet:end
	defer c.Stop()
	done := make(chan struct{}, runCount)
	if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) { done <- struct{}{} }); err != nil {
		return err
	}
	if err := c.Register(importWorkflow(g)); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	for range runCount {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "import"}); err != nil {
			return err
		}
	}
	if err := wait(ctx, done, runCount); err != nil {
		return err
	}
	report(out, "memory", g)
	return nil
}

func runSQL(ctx context.Context, out io.Writer, path string) error {
	g := &gauge{}
	done := make(chan struct{}, runCount)

	// snippet:sql
	// Two clients over one database: two nodes. Each has four workers, so
	// eight attempts could run; Limit lets only three of the queue run at
	// once across both.
	newNode := func() (*sqlstore.Client, error) {
		db, err := driversqlite.Open(path)
		if err != nil {
			return nil, err
		}
		return sqlstore.New(db, sqlb.SQLiteDialect{},
			sqlstore.WithWorkers(4),
			sqlstore.WithPollInterval(50*time.Millisecond),
			sqlstore.WithQueues(workflow.QueueConfig{Name: "imports", Workers: 4, Limit: importLimit}),
		)
	}
	// snippet:end
	var nodes []*sqlstore.Client
	for i := range 2 {
		c, err := newNode()
		if err != nil {
			return err
		}
		if i == 0 {
			if err := c.Migrate(ctx); err != nil {
				return err
			}
		}
		if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) { done <- struct{}{} }); err != nil {
			return err
		}
		if err := c.Register(importWorkflow(g)); err != nil {
			return err
		}
		if err := c.Start(ctx); err != nil {
			return err
		}
		defer c.Stop()
		nodes = append(nodes, c)
	}
	for range runCount {
		if _, err := nodes[0].Enqueue(ctx, workflow.Job{Workflow: "import"}); err != nil {
			return err
		}
	}
	if err := wait(ctx, done, runCount); err != nil {
		return err
	}
	fmt.Fprintf(out, "sqlstore: queue limit is cluster-wide: %v\n", nodes[0].Capabilities().Has(workflow.CapQueueLimitGlobal))
	report(out, "sqlstore", g)
	return nil
}
