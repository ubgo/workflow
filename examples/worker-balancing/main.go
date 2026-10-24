// Command worker-balancing shows workers moved between named queues as
// their load changes: the two built-in strategies (by depth, by wait)
// on fixed loads, per-queue bounds and a cap on how far a round moves,
// then a balance loop on the in-memory driver giving a backed-up import
// queue the workers an idle email queue does not need.
//
// It is the program behind docs/operations/balancing.md. The loop runs on
// a fake clock, so its round happens when the program says.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// loadPoll is how often the program re-reads queue loads while workers
// pick up jobs in their own goroutines.
const loadPoll = time.Millisecond

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:strategies
	loads := []workflow.QueueLoad{
		{Queue: "emails", Depth: 2, Running: 1, Workers: 4, OldestWait: 3 * time.Second},
		{Queue: "imports", Depth: 40, Running: 4, Workers: 4, OldestWait: 9 * time.Second},
	}
	cfg := workflow.BalanceConfig{Total: 8} // eight workers to share; each queue keeps at least one
	fmt.Fprintf(out, "by depth: %s\n", show(workflow.ByDepth(cfg).Balance(loads)))
	fmt.Fprintf(out, "by wait:  %s\n", show(workflow.ByWait(cfg).Balance(loads)))
	// snippet:end

	// snippet:bounds
	bounded := workflow.BalanceConfig{
		Total:    8,
		Bounds:   map[string]workflow.QueueBounds{"emails": {Min: 3}, "imports": {Max: 6}},
		MaxShift: 1, // move at most one worker per queue per round
	}
	fmt.Fprintf(out, "bounded:  %s\n", show(workflow.ByDepth(bounded).Balance(loads)))
	// snippet:end

	// snippet:client
	c := memory.New(
		memory.WithWorkers(8), // the pool: at least the balancer's total
		memory.WithQueues(
			workflow.QueueConfig{Name: "emails", Workers: 4},
			workflow.QueueConfig{Name: "imports", Workers: 4},
		),
	)
	// snippet:end
	gate := make(chan struct{})
	if err := c.Register(workflow.Workflow{
		Name:  "import",
		Queue: "imports",
		Steps: []workflow.Step{{Name: "load", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			select {
			case <-gate:
			case <-ctx.Done():
			}
			return nil, nil
		}}},
	}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	defer close(gate)
	for range 12 {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "import"}); err != nil {
			return err
		}
	}
	if err := waitRunning(ctx, c, "imports", 4); err != nil {
		return err
	}
	if err := printLoads(ctx, out, c, "before"); err != nil {
		return err
	}

	balanceClock := clock.NewFake(start)
	// snippet:loop
	loop := workflow.BalanceLoop{
		Scaler:   c, // the memory and SQL drivers implement workflow.WorkerScaler
		Balancer: workflow.ByDepth(workflow.BalanceConfig{Total: 8}),
		Every:    10 * time.Second,
		Clock:    balanceClock, // nil means the real clock
	}
	go func() { _ = loop.Run(ctx) }() // stops when ctx ends
	// snippet:end
	if err := balanceClock.BlockUntil(ctx, 1); err != nil { // the loop is waiting for its first round
		return err
	}
	balanceClock.Advance(10 * time.Second)
	if err := waitRunning(ctx, c, "imports", 7); err != nil {
		return err
	}
	return printLoads(ctx, out, c, "after one round")
}

// show prints a balancer's answer in queue order.
func show(m map[string]int) string {
	return fmt.Sprintf("emails %d, imports %d", m["emails"], m["imports"])
}

// printLoads prints every queue's load.
func printLoads(ctx context.Context, out io.Writer, c *memory.Client, when string) error {
	loads, err := c.QueueLoads(ctx)
	if err != nil {
		return err
	}
	parts := make([]string, 0, len(loads))
	for _, l := range loads {
		parts = append(parts, fmt.Sprintf("%s workers %d running %d waiting %d", l.Queue, l.Workers, l.Running, l.Depth))
	}
	fmt.Fprintf(out, "%s: %s\n", when, strings.Join(parts, "; "))
	return nil
}

// waitRunning polls until n attempts of queue are running.
func waitRunning(ctx context.Context, c *memory.Client, queue string, n int) error {
	for {
		loads, err := c.QueueLoads(ctx)
		if err != nil {
			return err
		}
		for _, l := range loads {
			if l.Queue == queue && l.Running == n {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(loadPoll):
		}
	}
}
