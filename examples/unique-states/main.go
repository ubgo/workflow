// Command unique-states shows the finer controls of unique jobs: a lock
// that counts only while its run is waiting to start (UniqueWith States),
// a key taken from the payload fields tagged `unique:"true"` (ArgsTagged),
// and a duplicate answered with the existing run instead of an error
// (ReturnExisting), told apart from a new run by WithEnqueueOutcome.
//
// It runs on the in-memory driver with a fake clock, so the delayed run
// starts when the program moves the clock and the output is the same on
// every run. It is the program behind the unique-by-state, tagged-fields
// and return-existing sections of docs/reliability/unique-jobs.md: the
// guide copies its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// snippet:tagged
// SyncArgs is the payload of "sync-shop". Only the shop decides whether two
// syncs are the same job; the cursor does not.
type SyncArgs struct {
	ShopID string `json:"shop_id" unique:"true"`
	Cursor string `json:"cursor"`
}

// snippet:end

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "unique-states:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(start)
	started := make(chan string, 4)
	release := make(chan struct{})
	sync := func(_ context.Context, in []byte) ([]byte, error) {
		started <- string(in)
		<-release
		return nil, nil
	}
	mc := memory.New(memory.WithClock(clk))
	for _, wf := range []workflow.Workflow{
		{Name: "sync-shop", Steps: []workflow.Step{{Name: "sync", Run: sync}}},
		{Name: "export", Steps: []workflow.Step{{Name: "build", Run: sync}}},
	} {
		if err := mc.Register(wf); err != nil {
			return err
		}
	}
	if err := mc.Start(ctx); err != nil {
		return err
	}
	defer mc.Stop()
	defer close(release)

	fmt.Fprintln(out, "unique while waiting to start:")
	// snippet:states
	waiting, err := middleware.UniqueWith(middleware.UniqueConfig{
		By:     middleware.UniqueByArgs(middleware.ArgsTagged[SyncArgs]()),
		Store:  middleware.NewMemoryUnique(clk),
		TTL:    24 * time.Hour, // the safety net
		States: []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued},
		Client: mc, // asked for the holder's status
	})
	if err != nil {
		return err
	}
	c := middleware.WrapClient(mc, waiting)
	// snippet:end

	enqueue := func(label string, args SyncArgs, runAt time.Time) error {
		job, err := workflow.JobOf("sync-shop", args)
		if err != nil {
			return err
		}
		job.RunAt = runAt
		_, err = c.Enqueue(ctx, job)
		switch {
		case errors.Is(err, workflow.ErrDuplicate):
			fmt.Fprintf(out, "  %s: refused (ErrDuplicate)\n", label)
		case err != nil:
			return err
		default:
			fmt.Fprintf(out, "  %s: accepted\n", label)
		}
		return nil
	}
	if err := enqueue("s-1 in an hour", SyncArgs{ShopID: "s-1", Cursor: "a"}, start.Add(time.Hour)); err != nil {
		return err
	}
	if err := enqueue("s-1 now, other cursor, first one scheduled", SyncArgs{ShopID: "s-1", Cursor: "b"}, time.Time{}); err != nil {
		return err
	}
	if err := enqueue("s-2 now", SyncArgs{ShopID: "s-2"}, time.Time{}); err != nil {
		return err
	}
	fmt.Fprintf(out, "  started %s\n", <-started)
	if err := clk.BlockUntil(ctx, 1); err != nil { // the delayed start is armed
		return err
	}
	clk.Advance(time.Hour)
	fmt.Fprintf(out, "  started %s\n", <-started)
	if err := enqueue("s-1 again, first one running", SyncArgs{ShopID: "s-1", Cursor: "c"}, time.Time{}); err != nil {
		return err
	}
	fmt.Fprintf(out, "  started %s\n", <-started)

	fmt.Fprintln(out, "return the existing run:")
	// snippet:return-existing
	existing, err := middleware.UniqueWith(middleware.UniqueConfig{
		By:             func(j workflow.Job) string { return "export:" + j.Labels["user"] },
		Store:          middleware.NewMemoryUnique(clk),
		TTL:            time.Hour,
		ReturnExisting: true, // a duplicate gets the holder's RunRef and no error
	})
	if err != nil {
		return err
	}
	exports := middleware.WrapClient(mc, existing)
	job := workflow.Job{Workflow: "export", Labels: map[string]string{"user": "u-7"}}
	var first workflow.RunRef
	for _, try := range []string{"first", "second"} {
		ctx, outcome := workflow.WithEnqueueOutcome(ctx) // a fresh one per Enqueue
		ref, err := exports.Enqueue(ctx, job)
		if err != nil {
			return err
		}
		if first.IsZero() {
			first = ref
		}
		fmt.Fprintf(out, "  %s: same run %t, duplicate %t (reported %t)\n", try, ref == first, outcome.Duplicate, outcome.Reported)
	}
	// snippet:end
	<-started

	// snippet:idempotency-outcome
	keyed := workflow.Job{Workflow: "export", IdempotencyKey: "export:u-8"}
	for _, try := range []string{"first", "second"} {
		ctx, outcome := workflow.WithEnqueueOutcome(ctx)
		if _, err := mc.Enqueue(ctx, keyed); err != nil { // an idempotency key, no middleware
			return err
		}
		fmt.Fprintf(out, "  key %s: duplicate %t\n", try, outcome.Duplicate)
	}
	// snippet:end
	<-started
	return nil
}
