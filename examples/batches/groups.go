package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
)

// chunkRuns and chunkBytes are the Hatchet bulk-trigger limits the
// example sets: half of each default, as for an engine whose gRPC message
// limit was lowered. Neither may exceed hatchet.DefaultGroupChunkRuns or
// hatchet.DefaultGroupChunkBytes.
const (
	chunkRuns  = hatchet.DefaultGroupChunkRuns / 2
	chunkBytes = hatchet.DefaultGroupChunkBytes / 2
)

// wfGroupFinally is the Finally callback of the group runner batches.
const wfGroupFinally = "group-finally"

// snippet:group-runner
// groupClient is a driver that can start a whole batch in one call: it
// implements batch.GroupRunner, so Manager.Dispatch hands it every
// eligible batch at once. A real driver makes one engine call per chunk
// (Hatchet's RunGroup is one bulk trigger per 1000 runs); this one loops
// over Enqueue and counts the calls, so the output can show which path a
// batch took. Embedding *memory.Client keeps the run-finished hook, so
// the batch hears of every child's end as for an enqueued job.
type groupClient struct {
	*memory.Client
	groups atomic.Int32 // RunGroup calls
}

// RunGroup starts jobs and returns one RunRef per job, in order. Each job
// keeps its labels, which carry the batch labels the batch needs.
func (g *groupClient) RunGroup(ctx context.Context, group string, jobs []workflow.Job) ([]workflow.RunRef, error) {
	g.groups.Add(1)
	refs := make([]workflow.RunRef, len(jobs))
	for i, job := range jobs {
		ref, err := g.Client.Enqueue(ctx, job)
		if err != nil {
			return refs, fmt.Errorf("group %s job %d: %w", group, i, err) // the refs so far let Dispatch cancel what started
		}
		refs[i] = ref
	}
	return refs, nil
}

// snippet:end

// The build fails here if groupClient stops satisfying batch.GroupRunner.
var _ batch.GroupRunner = (*groupClient)(nil)

// runGroups dispatches two batches through a GroupRunner client: one
// whose jobs are all eligible, started in one RunGroup call, and one with
// an idempotency key, enqueued one job at a time. Then it shows the
// Hatchet driver refusing a chunk limit over its ceiling.
func runGroups(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g := &groupClient{Client: memory.New()}
	finished := make(chan batch.Progress, 1)
	if err := registerGroupWorkflows(g.Client, finished); err != nil {
		return err
	}
	store := batch.NewMemoryStore()
	m := batch.New(g, store, batch.WithIDFunc(numbered("group-")))
	if err := m.Attach(ctx); err != nil {
		return err
	}
	if err := g.Start(ctx); err != nil {
		return err
	}

	fmt.Println("-- a client that starts a group in one call")
	jobs := []workflow.Job{{Workflow: wfResize}, {Workflow: wfResize}, {Workflow: wfResize}}
	// snippet:group-dispatch
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs, Finally: "group-finally"}) // eligible: one RunGroup call
	if err != nil {
		return err
	}
	st, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("%s: grouped=%v, RunGroup calls=%d\n", id, st.Grouped, g.groups.Load())
	// snippet:end
	p := <-finished
	fmt.Printf("%s: finally succeeded=%d of %d\n", p.ID, p.Succeeded, p.Total)

	// snippet:group-ineligible
	keyed := append([]workflow.Job{{Workflow: "resize", IdempotencyKey: "resize:cover"}}, jobs[1:]...)
	id, err = m.Dispatch(ctx, batch.Batch{Jobs: keyed, Finally: "group-finally"}) // one key: one Enqueue per job
	if err != nil {
		return err
	}
	st, err = store.Get(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("%s: grouped=%v, RunGroup calls=%d\n", id, st.Grouped, g.groups.Load())
	// snippet:end
	p = <-finished
	fmt.Printf("%s: finally succeeded=%d of %d\n", p.ID, p.Succeeded, p.Total)

	fmt.Println("-- Hatchet bulk-trigger chunks")
	// snippet:chunks
	bulk := hatchet.New(hatchet.Config{
		WorkerName:      "thumbnails",
		GroupChunkRuns:  chunkRuns,  // runs per bulk trigger; at most hatchet.DefaultGroupChunkRuns (1000)
		GroupChunkBytes: chunkBytes, // JSON input and metadata per bulk trigger; at most hatchet.DefaultGroupChunkBytes (2 MiB)
	})
	var _ batch.GroupRunner = bulk // Dispatch on bulk starts an eligible batch with RunGroup
	// snippet:end
	fmt.Printf("chunks of at most %d runs and %d bytes\n", chunkRuns, chunkBytes)
	// snippet:chunk-ceiling
	// Twice the ceiling: refused before it connects to any engine.
	tooBig := hatchet.New(hatchet.Config{GroupChunkRuns: 2 * hatchet.DefaultGroupChunkRuns})
	err = tooBig.Start(ctx)
	fmt.Println("over the ceiling refused:", errors.Is(err, workflow.ErrInvalid))
	// snippet:end
	return nil
}

// registerGroupWorkflows registers the job workflow and a Finally that
// hands its Progress to finished.
func registerGroupWorkflows(c *memory.Client, finished chan<- batch.Progress) error {
	if err := c.Register(workflow.Workflow{
		Name:  wfResize,
		Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	}); err != nil {
		return err
	}
	return c.Register(workflow.Workflow{
		Name: wfGroupFinally,
		Steps: []workflow.Step{{Name: "run", Run: workflow.Define(func(_ context.Context, p batch.Progress) (string, error) {
			finished <- p
			return "", nil
		})}},
	})
}

// numbered returns a batch ID function that numbers IDs after prefix, so
// the output is the same on every run.
func numbered(prefix string) func() (batch.ID, error) {
	n := 0
	return func() (batch.ID, error) {
		n++
		return batch.ID(fmt.Sprint(prefix, n)), nil
	}
}
