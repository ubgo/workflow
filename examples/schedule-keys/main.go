// Command schedule-keys gives a schedule an idempotency key, so its fired
// runs, a manual TriggerSchedule and a job enqueued by hand for the same
// work are one run while it is unfinished; a refusing trigger reports the
// held key instead; and a period template gives one key per day.
//
// It is the program behind docs/schedules/schedule-keys.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "schedule-keys:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	release := make(chan struct{})
	c := memory.New()
	if err := c.Register(workflow.Workflow{Name: "sync", Steps: []workflow.Step{{Name: "pull", Run: func(context.Context, []byte) ([]byte, error) {
		<-release // keep the run unfinished until the program says so
		return nil, nil
	}}}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}

	// snippet:fixed
	id, err := c.Schedule(ctx, workflow.Schedule{
		Name: "sync-acme", Workflow: "sync", Cron: "0 * * * *", Location: time.UTC,
		IdempotencyKey: "sync:acme", // every fire and every trigger carries this key
	})
	if err != nil {
		return err
	}
	first, err := c.TriggerSchedule(ctx, id)
	if err != nil {
		return err
	}
	octx, outcome := workflow.WithEnqueueOutcome(ctx)
	again, err := c.TriggerSchedule(octx, id) // the first run is still running
	if err != nil {
		return err
	}
	fmt.Println("second trigger returned the running run:", again == first, "duplicate:", outcome.Duplicate)
	byHand, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync", IdempotencyKey: "sync:acme"})
	if err != nil {
		return err
	}
	fmt.Println("a hand enqueue under the same key joined it:", byHand == first)
	// snippet:end

	// snippet:refuse
	strict, err := c.Schedule(ctx, workflow.Schedule{
		Name: "sync-acme-strict", Workflow: "sync", Cron: "0 * * * *", Location: time.UTC,
		IdempotencyKey: "sync:acme", OnHeld: workflow.HeldRefuse,
	})
	if err != nil {
		return err
	}
	_, err = c.TriggerSchedule(ctx, strict)
	fmt.Println("a refusing trigger while the key is held:", errors.Is(err, workflow.ErrDuplicate))
	// snippet:end

	// snippet:period
	key, err := workflow.ExpandScheduleKey("digest:"+workflow.KeyDay, time.Date(2026, 11, 28, 9, 0, 0, 0, time.UTC), time.UTC)
	if err != nil {
		return err
	}
	fmt.Println("the key of a daily digest fired at 09:00:", key)
	// snippet:end

	close(release)
	st, err := workflow.Await(ctx, c, first, workflow.AwaitOptions{Poll: 5 * time.Millisecond})
	if err != nil {
		return err
	}
	fmt.Println("the run:", st)
	return nil
}
