// Command accelerator-queue runs one step of a workflow on the machines
// that have the hardware for it. Two worker clients share one SQLite
// database, the way two kinds of node share a store in production: the
// CPU node serves queue "cpu" with four workers, the GPU node serves queue
// "gpu" with one worker per card. The photo workflow runs on "cpu"; its
// label step calls the infer workflow, which lives on "gpu", as a child
// run and waits for its result without holding a CPU worker.
//
// It is the program behind docs/jobs/queues.md (Run one step on an
// accelerator): the guide copies its regions (marked "snippet:") and task
// docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// The two queues and the node names the handlers report.
const (
	cpuQueue = "cpu"
	gpuQueue = "gpu"
	cpuNode  = "cpu-node"
	gpuNode  = "gpu-node"
)

// cpuWorkers is the CPU node's pool; gpuWorkers is one attempt per card on
// a one-card GPU node, so the model never shares the card.
const (
	cpuWorkers = 4
	gpuWorkers = 1
)

// poll is how often each client looks for work the other made due, and
// how often a waiting parent step looks at its child: short so the demo
// finishes quickly.
const poll = 10 * time.Millisecond

// modelTime is how long one inference holds the card, long enough that
// three photos would overlap on the GPU if its queue allowed it.
const modelTime = 30 * time.Millisecond

// waitLimit bounds the wait for each photo, generous for a slow CI host.
const waitLimit = 20 * time.Second

// ranBuffer holds one report per step attempt without blocking a handler:
// three photos make six attempts, and the spare room covers retries.
const ranBuffer = 16

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "accelerator-queue:", err)
		os.Exit(1)
	}
}

// gauge counts the model attempts running at once and keeps the peak.
type gauge struct{ now, peak atomic.Int64 }

// enter records one more attempt on the card and raises the peak if the
// count passed it.
func (g *gauge) enter() {
	n := g.now.Add(1)
	for {
		old := g.peak.Load()
		if n <= old || g.peak.CompareAndSwap(old, n) {
			return
		}
	}
}

// leave records that one attempt released the card.
func (g *gauge) leave() { g.now.Add(-1) }

// run is main without the process exit, so the test drives the same code
// and checks every line it writes to w.
func run(ctx context.Context, w io.Writer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dir, err := os.MkdirTemp("", "accelerator-queue-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	db, err := driversqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	// snippet:nodes
	cpu, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(poll),
		sqlstore.WithQueues(workflow.QueueConfig{Name: cpuQueue, Workers: cpuWorkers}))
	if err != nil {
		return err
	}
	gpu, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(poll),
		sqlstore.WithQueues(workflow.QueueConfig{Name: gpuQueue, Workers: gpuWorkers})) // one attempt per card
	if err != nil {
		return err
	}
	// snippet:end
	defer cpu.Stop()
	defer gpu.Stop()
	if err := cpu.Migrate(ctx); err != nil {
		return err
	}
	journal := cpu.JournalStore() // durable: a parent step on any CPU node sees what it spawned
	if err := journal.Migrate(ctx); err != nil {
		return err
	}

	var onGPU gauge
	// ran is one step attempt and the node that ran it.
	type ran struct{ step, node string }
	ranOn := make(chan ran, ranBuffer)
	// workflows builds both definitions for one node. Every node registers
	// both, so any of them can enqueue either; each runs only the steps of
	// the queues it serves.
	workflows := func(node string, c workflow.Client) []workflow.Workflow {
		// resize is the CPU step; it reports where it ran and passes the
		// photo on unchanged.
		resize := func(_ context.Context, in []byte) ([]byte, error) {
			ranOn <- ran{"resize", node}
			return in, nil
		}
		// runModel is the GPU step; it counts itself on the card, holds it
		// for modelTime and returns the label.
		runModel := func(_ context.Context, in []byte) ([]byte, error) {
			onGPU.enter()
			defer onGPU.leave()
			ranOn <- ran{"model", node}
			time.Sleep(modelTime)
			return []byte("label of " + string(in)), nil
		}
		// snippet:workflows
		infer := workflow.Workflow{Name: "infer", Queue: gpuQueue, Steps: []workflow.Step{{Name: "model", Run: runModel}}}
		photo := workflow.Workflow{Name: "photo", Queue: cpuQueue, Steps: []workflow.Step{
			{Name: "resize", Run: resize},
			{Name: "label", After: []string{"resize"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
				// Spawns infer once (the Journal remembers it), then snoozes
				// until it ends: the CPU worker is free while the GPU works.
				return workflow.AwaitChild(ctx, c, "label", workflow.Job{Workflow: "infer", Payload: in},
					workflow.ChildOptions{Poll: poll})
			}},
		}}
		// snippet:end
		return []workflow.Workflow{infer, photo}
	}

	// snippet:journal
	cpuClient, err := workflow.Use(cpu, workflow.Journal(journal)) // AwaitChild needs the Journal plugin
	if err != nil {
		return err
	}
	// snippet:end
	gpuClient, err := workflow.Use(gpu, workflow.Journal(journal))
	if err != nil {
		return err
	}
	for node, c := range map[string]workflow.Client{cpuNode: cpuClient, gpuNode: gpuClient} {
		for _, wf := range workflows(node, c) {
			if err := c.Register(wf); err != nil {
				return err
			}
		}
	}
	for _, c := range []workflow.Client{cpuClient, gpuClient} {
		if err := c.Start(ctx); err != nil {
			return err
		}
	}

	photos := []string{"p1", "p2", "p3"}
	refs := make([]workflow.RunRef, len(photos))
	for i, p := range photos {
		if refs[i], err = cpuClient.Enqueue(ctx, workflow.Job{Workflow: "photo", Payload: []byte(p)}); err != nil {
			return err
		}
	}
	wctx, stop := context.WithTimeout(ctx, waitLimit)
	defer stop()
	results := cpuClient.(workflow.ResultReader) // sqlstore keeps each run's result (CapResult)
	for i, ref := range refs {
		st, err := workflow.Await(wctx, cpuClient, ref, workflow.AwaitOptions{Poll: poll})
		if err != nil {
			return err
		}
		out, err := results.Result(wctx, ref)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "photo %s: %s, result %q\n", photos[i], st, out)
	}
	close(ranOn)
	count := map[ran]int{}
	for r := range ranOn {
		count[r]++
	}
	fmt.Fprintf(w, "resize on %s: %d, resize on %s: %d\n", cpuNode, count[ran{"resize", cpuNode}], gpuNode, count[ran{"resize", gpuNode}])
	fmt.Fprintf(w, "model on %s: %d, model on %s: %d\n", gpuNode, count[ran{"model", gpuNode}], cpuNode, count[ran{"model", cpuNode}])
	fmt.Fprintf(w, "model attempts at once on the GPU node: at most %d\n", onGPU.peak.Load())
	return nil
}
