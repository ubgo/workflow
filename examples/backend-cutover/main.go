// Command backend-cutover moves an app from one backend to another while
// it keeps running, with package cutover: new jobs go to the new backend,
// the old one finishes the runs it holds, refs stored before the cutover
// still work, the schedules the app made on the old backend are moved, and
// Remaining says when the old backend can be retired. Both backends are
// in-memory drivers here; in production the old one is the database the
// app is leaving (say sqlstore on SQLite) and the new one where it goes
// (sqlstore on Postgres, Redis, Hatchet).
//
// It is the program behind docs/operations/cutover.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cutover"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "backend-cutover:", err)
		os.Exit(1)
	}
}

var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release := make(chan struct{})
	export := workflow.Workflow{Name: "export", Steps: []workflow.Step{{Name: "write",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			<-release // a long export, still running during the cutover
			return in, nil
		}}}}

	// snippet:client
	oldBackend, newBackend := memory.New(), memory.New()
	c := cutover.New(oldBackend, newBackend)   // schedules go to the new backend
	if err := c.Register(export); err != nil { // on both: the old one finishes what it holds
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:end

	// What the app did before the cutover, on the old backend: a run it
	// stored the ref of, and a schedule it made.
	stored, err := oldBackend.Enqueue(ctx, workflow.Job{Workflow: "export", Payload: []byte(`"march"`)})
	if err != nil {
		return err
	}
	if _, err := oldBackend.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "export", Cron: "0 2 * * *",
		Location: time.UTC, Input: []byte(`"nightly"`)}); err != nil {
		return err
	}

	fmt.Println("-- New work goes to the new backend")
	if err := waitRunning(ctx, oldBackend, stored); err != nil {
		return err
	}
	// snippet:enqueue
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export", Payload: []byte(`"april"`)})
	if err != nil {
		return err
	}
	fmt.Println("enqueued:", ref.RunID) // new:<the new backend's own ID>
	// snippet:end
	if err := waitRunning(ctx, c, ref); err != nil {
		return err
	}
	// snippet:status
	for _, r := range []workflow.RunRef{stored, ref} {
		st, err := c.Status(ctx, r) // an unprefixed ref stored before the cutover is looked up on the old backend first
		if err != nil {
			return err
		}
		fmt.Printf("status of %s: %s\n", r.RunID, st)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "export"}) // both backends, one feed
	if err != nil {
		return err
	}
	// snippet:end
	var ids []string
	for _, r := range page.Runs {
		ids = append(ids, string(r.Ref.RunID))
	}
	slices.Sort(ids)
	fmt.Println("runs:", strings.Join(ids, ", "))

	fmt.Println("-- Move the schedules")
	if err := printSchedules(ctx, c); err != nil {
		return err
	}
	// snippet:move
	inputs := map[string][]byte{"nightly": []byte(`"nightly"`)} // a ScheduleInfo carries no Input: the app supplies it
	moved, err := c.MoveSchedules(ctx, func(info workflow.ScheduleInfo) ([]byte, error) {
		return inputs[info.Name], nil
	})
	if err != nil {
		return err
	}
	for _, m := range moved {
		fmt.Printf("moved %q from %s to %s\n", m.Name, m.From, m.To)
	}
	// snippet:end
	if err := printSchedules(ctx, c); err != nil {
		return err
	}

	fmt.Println("-- Retire the old backend")
	// snippet:remaining
	n, err := c.Remaining(ctx)
	if err != nil {
		return err
	}
	fmt.Println("unfinished runs on the old backend:", n)
	// snippet:end
	close(release)
	for _, r := range []workflow.RunRef{stored, ref} {
		if _, err := workflow.Await(ctx, c, r, await); err != nil {
			return err
		}
	}
	if n, err = c.Remaining(ctx); err != nil {
		return err
	}
	fmt.Println("after the export finished:", n, "- the app can drop the old backend")
	return nil
}

// waitRunning polls until the run's step has started, so the statuses the
// program prints do not depend on how fast a worker picks it up.
func waitRunning(ctx context.Context, c workflow.Client, ref workflow.RunRef) error {
	for range 1000 {
		st, err := c.Status(ctx, ref)
		if err != nil || st == workflow.StatusRunning {
			return err
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("run %s did not start", ref.RunID)
}

func printSchedules(ctx context.Context, c *cutover.Client) error {
	list, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range list {
		fmt.Printf("schedule %q on %s\n", s.Name, s.ID)
	}
	return nil
}
