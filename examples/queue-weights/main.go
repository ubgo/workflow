// Command queue-weights shows two ways to order work: weighted queues,
// where one worker shares its turns between queues of equal priority in
// proportion to their weights, and FIFO per concurrency key, where the
// runs of one key start one at a time in the order they were enqueued.
//
// It runs on the in-memory driver with one worker and a seeded random
// source, so the weighted draws, and therefore the output, are the same
// on every run. It is the program behind the weighted-queues section of
// docs/jobs/queues.md and the FIFO section of docs/jobs/concurrency.md:
// the guides copy its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"sync"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "queue-weights:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := weighted(ctx, out); err != nil {
		return err
	}
	return fifo(ctx, out)
}

// recorder returns a handler that reports its payload when it starts and,
// for the payload "hold", waits for release, so the program can queue work
// behind it.
func recorder(started chan<- string, release <-chan struct{}) workflow.Handler {
	return func(_ context.Context, in []byte) ([]byte, error) {
		started <- string(in)
		if string(in) == "hold" {
			<-release
		}
		return nil, nil
	}
}

// weighted queues eight imports and eight exports behind one busy worker
// and prints the order the worker takes them in.
func weighted(ctx context.Context, out io.Writer) error {
	started := make(chan string, 32)
	release := make(chan struct{})
	h := recorder(started, release)

	seeded := rand.New(rand.NewPCG(1, 2)) // pinned, so the draws repeat
	var mu sync.Mutex
	draw := func() float64 { mu.Lock(); defer mu.Unlock(); return seeded.Float64() }

	// snippet:weights
	c := memory.New(
		memory.WithWorkers(1),
		memory.WithJitterSource(draw), // the weighted draw uses it; production leaves the default
		memory.WithQueues(
			workflow.QueueConfig{Name: "exports", Weight: 3}, // about three turns in four
			workflow.QueueConfig{Name: "imports", Weight: 1}, // still gets one in four
		),
	)
	// snippet:end
	for _, wf := range []workflow.Workflow{
		{Name: "export", Queue: "exports", Steps: []workflow.Step{{Name: "run", Run: h}}},
		{Name: "import", Queue: "imports", Steps: []workflow.Step{{Name: "run", Run: h}}},
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "import", Payload: []byte("hold")}); err != nil {
		return err
	}
	<-started
	for i := range 8 {
		for _, wf := range []string{"export", "import"} {
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf, Payload: fmt.Appendf(nil, "%c%d", wf[0]-32, i+1)}); err != nil {
				return err
			}
		}
	}
	close(release)
	order := make([]string, 16)
	for i := range order {
		order[i] = <-started
	}
	fmt.Fprintln(out, "weighted (E = export, I = import):")
	fmt.Fprintf(out, "  %s\n", strings.Join(order[:8], " "))
	fmt.Fprintf(out, "  %s\n", strings.Join(order[8:], " "))
	return nil
}

// fifo queues three syncs of one account behind a running one and
// prints the order they start in.
func fifo(ctx context.Context, out io.Writer) error {
	started := make(chan string, 8)
	release := make(chan struct{})
	h := recorder(started, release)

	// snippet:fifo
	wf := workflow.Workflow{
		Name: "sync-account",
		Concurrency: &workflow.Concurrency{
			Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["account"] }),
			MaxRuns: 1,    // one sync per account at a time
			FIFO:    true, // and in the order they were enqueued
		},
		Steps: []workflow.Step{{Name: "sync", Run: h}},
	}
	// snippet:end
	c := memory.New(memory.WithWorkers(4))
	if err := c.Register(wf); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	acct := map[string]string{"account": "a-1"}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync-account", Labels: acct, Payload: []byte("hold")}); err != nil {
		return err
	}
	<-started
	for i := range 3 {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync-account", Labels: acct, Payload: fmt.Appendf(nil, "sync-%d", i+1)}); err != nil {
			return err
		}
	}
	close(release)
	fmt.Fprintln(out, "fifo per key:")
	for range 3 {
		fmt.Fprintf(out, "  started %s\n", <-started)
	}
	return nil
}
