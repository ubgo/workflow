// Command batch-lifecycle shows what a batch can do after Dispatch, on the
// in-memory driver: Before and Progress callbacks, a job that adds more
// jobs to its own batch, RetryFailed on a batch with a failed job, Prune
// of old batch state, a workflow step that dispatches a batch and waits
// for it, and EnqueueMany for many jobs without a batch.
//
// It is the program behind docs/batches/lifecycle.md and
// docs/batches/await.md: the guides copy its regions (marked "snippet:")
// and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// awaitPoll is how often the import workflow's await step looks at its
// batch; the memory driver finishes these jobs in microseconds.
const awaitPoll = 10 * time.Millisecond

var errFlaky = errors.New("upstream timed out")

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "batch-lifecycle:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := memory.New()
	lines := make(chan string, 32)
	var m *batch.Manager // the split job below adds jobs through it
	if err := registerWorkflows(c, lines, &m); err != nil {
		return err
	}

	seq := 0
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	// snippet:retryable
	m = batch.New(c, batch.NewMemoryStore(),
		batch.WithClock(clk),
		batch.WithRetryableJobs(), // keeps each job's spec, which RetryFailed needs
		batch.WithManualCallbackRetry(),
		batch.WithIDFunc(func() (batch.ID, error) { seq++; return batch.ID("b" + strconv.Itoa(seq)), nil }),
	)
	// snippet:end
	if err := registerImport(c, m, lines); err != nil {
		return err
	}
	if err := m.Attach(ctx); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// print waits for n lines and prints them sorted: callbacks are
	// separate runs and may finish in any order.
	print := func(n int) {
		got := make([]string, 0, n)
		for range n {
			got = append(got, <-lines)
		}
		slices.Sort(got)
		for _, l := range got {
			fmt.Fprintln(out, l)
		}
	}

	fmt.Fprintln(out, "-- before and progress")
	// snippet:before
	_, err := m.Dispatch(ctx, batch.Batch{
		Name:     "thumbnails",
		Jobs:     []workflow.Job{{Workflow: "resize"}, {Workflow: "resize"}, {Workflow: "resize"}},
		Before:   "before",   // once, when the batch is created
		Progress: "progress", // once per job end
		Finally:  "finally",
	})
	// snippet:end
	if err != nil {
		return err
	}
	print(5) // before, three progress, finally

	fmt.Fprintln(out, "-- a job adds jobs to its batch")
	if _, err := m.Dispatch(ctx, batch.Batch{Name: "split", Jobs: []workflow.Job{{Workflow: "split"}}, Then: "then", Finally: "finally"}); err != nil {
		return err
	}
	print(2)

	fmt.Fprintln(out, "-- retry the failed jobs")
	// snippet:retry
	id, err := m.Dispatch(ctx, batch.Batch{
		Name:          "sync",
		Jobs:          []workflow.Job{{Workflow: "flaky"}, {Workflow: "resize"}},
		AllowFailures: true,
		Then:          "then",
		Catch:         "catch",
		Finally:       "finally",
	})
	if err != nil {
		return err
	}
	print(2) // catch and finally: the flaky job failed once
	n, err := m.RetryFailed(ctx, id)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "retried", n)
	print(2) // then and finally: this time every job succeeded
	// snippet:end

	fmt.Fprintln(out, "-- a workflow waits for a batch")
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "import", Payload: []byte("4")}); err != nil {
		return err
	}
	print(1)

	fmt.Fprintln(out, "-- many jobs without a batch")
	// snippet:bulk
	refs, err := batch.EnqueueMany(ctx, c, []workflow.Job{
		{Workflow: "resize"}, {Workflow: "resize"}, {Workflow: "resize"},
	})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "started", len(refs), "runs")
	// snippet:end

	fmt.Fprintln(out, "-- prune")
	clk.Advance(48 * time.Hour)
	// snippet:prune
	pruned, err := m.Prune(ctx, batch.PruneFilter{
		FinishedBefore:   clk.Now().Add(-24 * time.Hour), // closed batches, a day after they finished
		UnfinishedBefore: clk.Now().Add(-72 * time.Hour), // stuck ones, three days after they were created
	})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "pruned", pruned)
	// snippet:end
	return nil
}

// registerWorkflows registers the batch jobs and the callbacks, which
// report on lines.
func registerWorkflows(c *memory.Client, lines chan<- string, m **batch.Manager) error {
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	var failedOnce atomic.Bool
	defs := []workflow.Workflow{
		{Name: "resize", Steps: []workflow.Step{{Name: "run", Run: noop}}},
		{Name: "flaky", Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) {
			if failedOnce.CompareAndSwap(false, true) {
				return nil, workflow.Permanent(errFlaky)
			}
			return nil, nil
		}}}},
		// snippet:add
		{Name: "split", Steps: []workflow.Step{{Name: "run", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			id := batch.ID(info.Labels[batch.LabelBatch]) // the batch this job belongs to
			return nil, (*m).Add(ctx, id, workflow.Job{Workflow: "resize"}, workflow.Job{Workflow: "resize"})
		}}}},
		// snippet:end
	}
	for _, kind := range []string{"before", "progress", "then", "catch", "finally"} {
		defs = append(defs, workflow.Workflow{Name: kind, Steps: []workflow.Step{{Name: "run", Run: workflow.Define(func(_ context.Context, p batch.Progress) (string, error) {
			switch kind {
			case "before", "progress": // the other counts depend on which runs ended first
				lines <- fmt.Sprintf("%s %s total=%d", kind, p.ID, p.Total)
			case "catch":
				lines <- fmt.Sprintf("%s %s total=%d failed=%d", kind, p.ID, p.Total, p.Failed)
			default:
				lines <- fmt.Sprintf("%s %s total=%d succeeded=%d failed=%d", kind, p.ID, p.Total, p.Succeeded, p.Failed)
			}
			return "", nil
		})}}})
	}
	for _, wf := range defs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	return nil
}

// registerImport registers a workflow that fans out to a batch of resize
// jobs, one per row of its payload, and reports once they all ended.
func registerImport(c *memory.Client, m *batch.Manager, lines chan<- string) error {
	// snippet:await
	fan := m.AwaitStep("fan", func(_ context.Context, in []byte) (batch.Batch, error) {
		rows, err := strconv.Atoi(string(in))
		if err != nil {
			return batch.Batch{}, workflow.Permanent(err)
		}
		b := batch.Batch{Name: "import"}
		for range rows {
			b.Jobs = append(b.Jobs, workflow.Job{Workflow: "resize"})
		}
		return b, nil
	}, batch.AwaitPoll(awaitPoll))
	report := workflow.Step{Name: "report", After: []string{"fan"}, Run: workflow.Define(func(_ context.Context, p batch.Progress) (string, error) {
		lines <- fmt.Sprintf("import done: %d of %d succeeded", p.Succeeded, p.Total)
		return "", nil
	})}
	return c.Register(workflow.Workflow{Name: "import", Steps: []workflow.Step{fan, report}})
	// snippet:end
}
