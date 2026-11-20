// Command partitioned-runs shows one logical job split into partitions: a
// backfill that runs a workflow once per day, with a status per day, a
// re-run that starts again only the days that failed, and a per-tenant
// set enqueued twice while its runs are unfinished, which starts each
// tenant once. It runs on the in-memory driver, so nothing needs to be
// installed.
//
// It is the program behind docs/workflows/partitions.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "partitioned-runs:", err)
		os.Exit(1)
	}
}

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := memory.New()
	var sourceDown atomic.Bool
	sourceDown.Store(true)
	gate := make(chan struct{})
	wfs := []workflow.Workflow{
		{Name: "daily-totals", Steps: []workflow.Step{{Name: "load", Run: func(_ context.Context, in []byte) ([]byte, error) {
			var day string
			if err := json.Unmarshal(in, &day); err != nil {
				return nil, workflow.Permanent(err)
			}
			if day == "2026-11-02" && sourceDown.Load() {
				return nil, workflow.Permanent(errors.New("source down for " + day))
			}
			return nil, nil
		}}}},
		{Name: "tenant-sync", Steps: []workflow.Step{{Name: "sync", Run: func(context.Context, []byte) ([]byte, error) {
			<-gate
			return nil, nil
		}}}},
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	fmt.Println("backfill:")
	// snippet:enqueue
	set := workflow.PartitionSet{Name: "totals-backfill", Workflow: "daily-totals"}
	refs, err := workflow.EnqueuePartitions(ctx, c, set, []string{"2026-11-01", "2026-11-02", "2026-11-03"})
	if err != nil {
		return err
	}
	// snippet:end
	if err := wait(ctx, c, refs); err != nil {
		return err
	}
	if err := printStatus(ctx, c, set.Name); err != nil {
		return err
	}

	fmt.Println("retry failed:")
	sourceDown.Store(false)
	// snippet:retry
	keys, refs, err := workflow.RetryFailedPartitions(ctx, c, set)
	if err != nil {
		return err
	}
	fmt.Printf("  retried: %v\n", keys)
	// snippet:end
	if err := wait(ctx, c, refs); err != nil {
		return err
	}
	if err := printStatus(ctx, c, set.Name); err != nil {
		return err
	}

	fmt.Println("per tenant, enqueued twice while running:")
	// snippet:again
	tenants := workflow.PartitionSet{Name: "nightly-sync", Workflow: "tenant-sync"}
	first, err := workflow.EnqueuePartitions(ctx, c, tenants, []string{"acme", "globex"})
	if err != nil {
		return err
	}
	again, err := workflow.EnqueuePartitions(ctx, c, tenants, []string{"acme", "globex"})
	if err != nil {
		return err
	}
	fmt.Printf("  same runs: %v\n", first[0] == again[0] && first[1] == again[1])
	// snippet:end
	close(gate)
	if err := wait(ctx, c, first); err != nil {
		return err
	}
	return printStatus(ctx, c, tenants.Name)
}

// printStatus prints the latest run of every partition of set.
func printStatus(ctx context.Context, c workflow.Client, set string) error {
	// snippet:status
	parts, err := workflow.PartitionStatus(ctx, c, set)
	if err != nil {
		return err
	}
	for _, p := range parts {
		line := fmt.Sprintf("  %s %s", p.Key, p.Run.Status)
		if p.Run.Error != "" {
			line += ": " + p.Run.Error
		}
		fmt.Println(line)
	}
	// snippet:end
	return nil
}

// wait waits for every run to end, whatever its end.
func wait(ctx context.Context, c workflow.Client, refs []workflow.RunRef) error {
	for _, r := range refs {
		if _, err := workflow.Await(ctx, c, r, await); err != nil {
			return err
		}
	}
	return nil
}
